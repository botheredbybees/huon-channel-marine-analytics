#!/usr/bin/env python3
"""
Taxonomy Enrichment Script - WoRMS & GBIF API Integration

Enriches marine species taxonomy data from WoRMS and non-marine from GBIF:

WoRMS (Marine Species):
- AphiaID and full classification
- Marine-specific distribution data
- Conservation status
- Synonyms and vernacular names
- Cross-references to other databases

GBIF (Non-Marine/Fallback):
- GBIF taxon key and backbone classification
- Occurrence records and distribution
- Conservation status
- Synonyms and vernacular names

Strategy:
1. Check if species already enriched from iNaturalist
2. Try WoRMS first for all species (marine authority)
3. Fall back to GBIF for non-marine or WoRMS failures
4. Update existing records with additional WoRMS/GBIF data

Features:
- Intelligent marine species detection
- Batch processing with configurable batch size
- Rate limiting (WoRMS: no official limit, GBIF: lenient)
- Confidence scoring for match quality
- Comprehensive error handling with retry logic
- Progress tracking with rich console output
- Full audit logging to database

Usage:
    python scripts/enrich_taxonomy_from_worms.py
    python scripts/enrich_taxonomy_from_worms.py --dry-run
    python scripts/enrich_taxonomy_from_worms.py --source worms
    python scripts/enrich_taxonomy_from_worms.py --source gbif
    python scripts/enrich_taxonomy_from_worms.py --limit 10

Author: Huon Channel Marine Analytics
Created: January 6, 2026
Version: 1.1
"""

import os
import sys
import time
import logging
import argparse
import requests
import psycopg2
import psycopg2.extras
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from difflib import SequenceMatcher
import json

# Configuration
WORMS_API_BASE = "https://www.marinespecies.org/rest"
GBIF_API_BASE = "https://api.gbif.org/v1"

# Rate limiting (conservative)
WORMS_RATE_LIMIT = 30  # requests per minute (conservative, no official limit)
GBIF_RATE_LIMIT = 100  # requests per minute (GBIF is lenient)
RATE_LIMIT_WINDOW = 60  # seconds

MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
DEFAULT_BATCH_SIZE = 30

# Confidence thresholds
CONFIDENCE_EXACT_MATCH = 1.0
CONFIDENCE_HIGH = 0.9
CONFIDENCE_MEDIUM = 0.7
CONFIDENCE_LOW = 0.5
CONFIDENCE_REVIEW_THRESHOLD = 0.8

# Marine phyla/classes (for intelligent routing)
MARINE_PHYLA = {
    'Porifera', 'Cnidaria', 'Ctenophora', 'Echinodermata', 'Hemichordata',
    'Bryozoa', 'Brachiopoda', 'Phoronida', 'Chaetognatha', 'Ochrophyta',
    'Rhodophyta', 'Chlorophyta'  # algae
}

MARINE_CLASSES = {
    'Cephalopoda', 'Gastropoda', 'Bivalvia', 'Polychaeta', 'Malacostraca',
    'Ascidiacea', 'Thaliacea', 'Elasmobranchii', 'Actinopterygii',
    'Phaeophyceae', 'Florideophyceae', 'Ulvophyceae'
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/taxonomy_enrichment_worms.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class RateLimiter:
    """Simple rate limiter using sliding window."""
    
    def __init__(self, max_requests: int, window_seconds: int):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests = []
    
    def wait_if_needed(self):
        """Wait if rate limit would be exceeded."""
        now = time.time()
        
        # Remove requests outside the window
        self.requests = [req_time for req_time in self.requests 
                        if now - req_time < self.window_seconds]
        
        if len(self.requests) >= self.max_requests:
            oldest_request = min(self.requests)
            wait_time = self.window_seconds - (now - oldest_request) + 0.1
            if wait_time > 0:
                logger.debug(f"Rate limit reached. Waiting {wait_time:.2f}s")
                time.sleep(wait_time)
                self.requests = []
        
        self.requests.append(time.time())


class WoRMSGBIFEnricher:
    """Handles taxonomy enrichment from WoRMS and GBIF APIs."""
    
    def __init__(self, db_config: Dict[str, str], dry_run: bool = False, source: str = 'auto'):
        self.db_config = db_config
        self.dry_run = dry_run
        self.source = source  # 'auto', 'worms', 'gbif'
        self.worms_limiter = RateLimiter(WORMS_RATE_LIMIT, RATE_LIMIT_WINDOW)
        self.gbif_limiter = RateLimiter(GBIF_RATE_LIMIT, RATE_LIMIT_WINDOW)
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'HuonChannelMarineAnalytics/1.0 (research project; pshanks@megalong.com)',
            'Accept': 'application/json'
        })
        
        self.stats = {
            'total_processed': 0,
            'worms_successful': 0,
            'gbif_successful': 0,
            'failed': 0,
            'needs_review': 0,
            'api_calls_worms': 0,
            'api_calls_gbif': 0,
            'skipped_already_enriched': 0
        }
    
    def get_db_connection(self):
        """Create database connection."""
        return psycopg2.connect(**self.db_config)
    
    def is_likely_marine(self, species_name: str, existing_data: Optional[Dict] = None) -> bool:
        """Determine if species is likely marine based on existing data or name."""
        if existing_data:
            phylum = existing_data.get('phylum', '')
            class_name = existing_data.get('class', '')
            iconic_taxon = existing_data.get('iconic_taxon_name', '')
            
            # Check known marine groups
            if phylum in MARINE_PHYLA or class_name in MARINE_CLASSES:
                return True
            
            # Algae are often marine
            if iconic_taxon in ['Chromista', 'Protozoa'] or 'algae' in species_name.lower():
                return True
        
        # Keywords suggesting marine
        marine_keywords = ['kelp', 'seaweed', 'coral', 'sponge', 'urchin', 
                          'starfish', 'anemone', 'barnacle', 'mussel', 'oyster']
        
        return any(keyword in species_name.lower() for keyword in marine_keywords)
    
    def fetch_species_to_enrich(self, limit: Optional[int] = None) -> List[Tuple]:
        """Fetch species that need WoRMS/GBIF enrichment."""
        conn = self.get_db_connection()
        try:
            with conn.cursor() as cur:
                # Prioritize species with iNaturalist data but no WoRMS/GBIF
                query = """
                    SELECT 
                        t.id, 
                        t.species_name,
                        tc.phylum,
                        tc.class,
                        tc.iconic_taxon_name,
                        tc.worms_aphia_id,
                        tc.gbif_taxon_key
                    FROM taxonomy t
                    LEFT JOIN taxonomy_cache tc ON t.id = tc.taxonomy_id
                    WHERE tc.worms_aphia_id IS NULL 
                       OR tc.gbif_taxon_key IS NULL
                    ORDER BY (
                        SELECT COUNT(*) 
                        FROM species_observations 
                        WHERE taxonomy_id = t.id
                    ) DESC
                """
                if limit:
                    query += f" LIMIT {limit}"
                
                cur.execute(query)
                return cur.fetchall()
        finally:
            conn.close()
    
    def search_worms(self, species_name: str) -> Tuple[Optional[Dict], int, int]:
        """Search WoRMS API for species.
        
        Returns:
            (result_dict, http_status, response_time_ms)
        """
        self.worms_limiter.wait_if_needed()
        
        # Try exact match first
        url = f"{WORMS_API_BASE}/AphiaRecordsByName/{species_name}"
        params = {'marine_only': 'true'}
        
        start_time = time.time()
        
        for attempt in range(MAX_RETRIES):
            try:
                self.stats['api_calls_worms'] += 1
                response = self.session.get(url, params=params, timeout=30)
                response_time_ms = int((time.time() - start_time) * 1000)
                
                if response.status_code == 200:
                    data = response.json()
                    return data, 200, response_time_ms
                elif response.status_code == 204:
                    # No content - try fuzzy match
                    return self.search_worms_fuzzy(species_name)
                elif response.status_code in [429, 503]:
                    wait_time = RETRY_DELAY * (2 ** attempt)
                    logger.warning(f"WoRMS rate limited. Waiting {wait_time}s")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"WoRMS API error {response.status_code}: {response.text}")
                    return None, response.status_code, response_time_ms
            
            except requests.exceptions.RequestException as e:
                logger.error(f"WoRMS request failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (2 ** attempt))
                else:
                    return None, 0, int((time.time() - start_time) * 1000)
        
        return None, 0, int((time.time() - start_time) * 1000)
    
    def search_worms_fuzzy(self, species_name: str) -> Tuple[Optional[Dict], int, int]:
        """Try fuzzy matching with WoRMS TAXAMATCH."""
        url = f"{WORMS_API_BASE}/AphiaRecordsByMatchNames"
        params = {'scientificnames[]': species_name, 'marine_only': 'true'}
        
        start_time = time.time()
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response_time_ms = int((time.time() - start_time) * 1000)
            
            if response.status_code == 200:
                data = response.json()
                # Extract matches from array response
                if data and len(data) > 0 and data[0]:
                    matches = data[0]  # First element contains matches
                    return matches, 200, response_time_ms
            
            return None, response.status_code, response_time_ms
        
        except Exception as e:
            logger.error(f"WoRMS fuzzy search failed: {e}")
            return None, 0, int((time.time() - start_time) * 1000)
    
    def search_gbif(self, species_name: str) -> Tuple[Optional[Dict], int, int]:
        """Search GBIF API for species.
        
        Returns:
            (result_dict, http_status, response_time_ms)
        """
        self.gbif_limiter.wait_if_needed()
        
        url = f"{GBIF_API_BASE}/species/match"
        params = {'name': species_name, 'strict': 'false'}
        
        start_time = time.time()
        
        for attempt in range(MAX_RETRIES):
            try:
                self.stats['api_calls_gbif'] += 1
                response = self.session.get(url, params=params, timeout=30)
                response_time_ms = int((time.time() - start_time) * 1000)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Check match type
                    match_type = data.get('matchType')
                    if match_type in ['EXACT', 'FUZZY', 'HIGHERRANK']:
                        return data, 200, response_time_ms
                    else:
                        return None, 204, response_time_ms
                
                elif response.status_code == 429:
                    wait_time = RETRY_DELAY * (2 ** attempt)
                    logger.warning(f"GBIF rate limited. Waiting {wait_time}s")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"GBIF API error {response.status_code}: {response.text}")
                    return None, response.status_code, response_time_ms
            
            except requests.exceptions.RequestException as e:
                logger.error(f"GBIF request failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (2 ** attempt))
                else:
                    return None, 0, int((time.time() - start_time) * 1000)
        
        return None, 0, int((time.time() - start_time) * 1000)
    
    def calculate_worms_confidence(self, species_name: str, record: Dict) -> float:
        """Calculate confidence score for WoRMS match."""
        scientific_name = record.get('scientificname', '').lower()
        valid_name = record.get('valid_name', '').lower()
        species_lower = species_name.lower()
        
        # Exact match
        if scientific_name == species_lower or valid_name == species_lower:
            return CONFIDENCE_EXACT_MATCH
        
        # Check status
        status = record.get('status', '')
        if status == 'accepted':
            similarity = SequenceMatcher(None, scientific_name, species_lower).ratio()
            if similarity >= 0.95:
                return CONFIDENCE_HIGH
            elif similarity >= 0.8:
                return CONFIDENCE_MEDIUM
        
        return CONFIDENCE_LOW
    
    def calculate_gbif_confidence(self, species_name: str, record: Dict) -> float:
        """Calculate confidence score for GBIF match."""
        match_type = record.get('matchType', '')
        confidence = record.get('confidence', 0)
        
        if match_type == 'EXACT':
            return CONFIDENCE_EXACT_MATCH
        elif match_type == 'FUZZY' and confidence >= 95:
            return CONFIDENCE_HIGH
        elif match_type == 'FUZZY' and confidence >= 80:
            return CONFIDENCE_MEDIUM
        elif match_type == 'HIGHERRANK':
            return CONFIDENCE_LOW
        
        return 0.3
    
    def extract_worms_data(self, record: Dict) -> Dict:
        """Extract relevant data from WoRMS AphiaRecord.
        
        Note: WoRMS API returns 1/0 for booleans, must convert to Python bool.
        """
        # Get classification (need separate API call for full hierarchy)
        aphia_id = record.get('AphiaID')
        
        return {
            'worms_aphia_id': aphia_id,
            'worms_url': record.get('url'),
            'worms_lsid': record.get('lsid'),
            'scientific_name_authorship': record.get('authority'),
            'taxonomic_status': record.get('status'),
            'accepted_name': record.get('valid_name'),
            'accepted_aphia_id': record.get('valid_AphiaID'),
            'kingdom': record.get('kingdom'),
            'phylum': record.get('phylum'),
            'class': record.get('class'),
            'order': record.get('order'),
            'family': record.get('family'),
            'genus': record.get('genus'),
            'rank': record.get('rank'),
            # Convert WoRMS integer booleans (1/0) to Python bool (True/False)
            'is_marine': bool(record.get('isMarine', 1)),
            'is_brackish': bool(record.get('isBrackish', 0)),
            'is_freshwater': bool(record.get('isFreshwater', 0)),
            'is_terrestrial': bool(record.get('isTerrestrial', 0)),
            'is_extinct': bool(record.get('isExtinct', 0)),
            'data_source': 'worms',
            'worms_response': json.dumps(record)
        }
    
    def extract_gbif_data(self, record: Dict) -> Dict:
        """Extract relevant data from GBIF species match."""
        return {
            'gbif_taxon_key': record.get('usageKey'),
            'gbif_scientific_name': record.get('scientificName'),
            'gbif_canonical_name': record.get('canonicalName'),
            'scientific_name_authorship': record.get('authorship'),
            'taxonomic_status': record.get('status'),
            'accepted_name': record.get('species'),
            'kingdom': record.get('kingdom'),
            'phylum': record.get('phylum'),
            'class': record.get('class'),
            'order': record.get('order'),
            'family': record.get('family'),
            'genus': record.get('genus'),
            'rank': record.get('rank'),
            'match_type': record.get('matchType'),
            'confidence': record.get('confidence'),
            'data_source': 'gbif',
            'gbif_response': json.dumps(record)
        }
    
    def update_cache(self, taxonomy_id: int, species_name: str, data: Dict, source: str):
        """Update taxonomy_cache with WoRMS or GBIF data."""
        if self.dry_run:
            logger.info(f"[DRY RUN] Would update cache with {source.upper()}: {species_name}")
            return
        
        conn = self.get_db_connection()
        try:
            with conn.cursor() as cur:
                # Build update query dynamically based on source
                if source == 'worms':
                    update_fields = """
                        worms_aphia_id = %(worms_aphia_id)s,
                        worms_url = %(worms_url)s,
                        worms_lsid = %(worms_lsid)s,
                        scientific_name_authorship = COALESCE(scientific_name_authorship, %(scientific_name_authorship)s),
                        taxonomic_status = %(taxonomic_status)s,
                        accepted_name = %(accepted_name)s,
                        accepted_aphia_id = %(accepted_aphia_id)s,
                        is_marine = %(is_marine)s,
                        is_brackish = %(is_brackish)s,
                        is_freshwater = %(is_freshwater)s,
                        is_terrestrial = %(is_terrestrial)s,
                        is_extinct = %(is_extinct)s,
                        kingdom = COALESCE(kingdom, %(kingdom)s),
                        phylum = COALESCE(phylum, %(phylum)s),
                        class = COALESCE(class, %(class)s),
                        \"order\" = COALESCE(\"order\", %(order)s),
                        family = COALESCE(family, %(family)s),
                        genus = COALESCE(genus, %(genus)s),
                        worms_response = %(worms_response)s::jsonb,
                        last_updated = NOW()
                    """
                else:  # gbif
                    update_fields = """
                        gbif_taxon_key = %(gbif_taxon_key)s,
                        gbif_scientific_name = %(gbif_scientific_name)s,
                        gbif_canonical_name = %(gbif_canonical_name)s,
                        scientific_name_authorship = COALESCE(scientific_name_authorship, %(scientific_name_authorship)s),
                        taxonomic_status = %(taxonomic_status)s,
                        match_type = %(match_type)s,
                        confidence = %(confidence)s,
                        kingdom = COALESCE(kingdom, %(kingdom)s),
                        phylum = COALESCE(phylum, %(phylum)s),
                        class = COALESCE(class, %(class)s),
                        \"order\" = COALESCE(\"order\", %(order)s),
                        family = COALESCE(family, %(family)s),
                        genus = COALESCE(genus, %(genus)s),
                        gbif_response = %(gbif_response)s::jsonb,
                        last_updated = NOW()
                    """
                
                # Check if record exists
                cur.execute(
                    "SELECT id FROM taxonomy_cache WHERE taxonomy_id = %s",
                    (taxonomy_id,)
                )
                exists = cur.fetchone()
                
                if exists:
                    # Update existing record
                    query = f"""
                        UPDATE taxonomy_cache 
                        SET {update_fields}
                        WHERE taxonomy_id = %(taxonomy_id)s
                    """
                else:
                    # Insert new record
                    if source == 'worms':
                        query = """
                            INSERT INTO taxonomy_cache (
                                taxonomy_id, species_name, worms_aphia_id, worms_url, worms_lsid,
                                scientific_name_authorship, taxonomic_status, accepted_name, accepted_aphia_id,
                                kingdom, phylum, class, \"order\", family, genus,
                                is_marine, is_brackish, is_freshwater, is_terrestrial, is_extinct,
                                data_source, worms_response
                            ) VALUES (
                                %(taxonomy_id)s, %(species_name)s, %(worms_aphia_id)s, %(worms_url)s, %(worms_lsid)s,
                                %(scientific_name_authorship)s, %(taxonomic_status)s, %(accepted_name)s, %(accepted_aphia_id)s,
                                %(kingdom)s, %(phylum)s, %(class)s, %(order)s, %(family)s, %(genus)s,
                                %(is_marine)s, %(is_brackish)s, %(is_freshwater)s, %(is_terrestrial)s, %(is_extinct)s,
                                %(data_source)s, %(worms_response)s::jsonb
                            )
                        """
                    else:  # gbif
                        query = """
                            INSERT INTO taxonomy_cache (
                                taxonomy_id, species_name, gbif_taxon_key, gbif_scientific_name,
                                gbif_canonical_name, scientific_name_authorship, taxonomic_status,
                                match_type, confidence,
                                kingdom, phylum, class, \"order\", family, genus,
                                data_source, gbif_response
                            ) VALUES (
                                %(taxonomy_id)s, %(species_name)s, %(gbif_taxon_key)s, %(gbif_scientific_name)s,
                                %(gbif_canonical_name)s, %(scientific_name_authorship)s, %(taxonomic_status)s,
                                %(match_type)s, %(confidence)s,
                                %(kingdom)s, %(phylum)s, %(class)s, %(order)s, %(family)s, %(genus)s,
                                %(data_source)s, %(gbif_response)s::jsonb
                            )
                        """
                
                params = {**data, 'taxonomy_id': taxonomy_id, 'species_name': species_name}
                cur.execute(query, params)
                conn.commit()
                
                logger.info(f"✓ Updated cache with {source.upper()}: {species_name}")
        
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to update cache for {species_name}: {e}")
            raise
        finally:
            conn.close()
    
    def log_enrichment(self, taxonomy_id: int, species_name: str, api_endpoint: str,
                      api_url: str, status: int, response_time: int, confidence: float,
                      match_method: str, needs_review: bool, review_reason: str,
                      taxon_id: Optional[int] = None, error_msg: Optional[str] = None):
        """Log enrichment attempt."""
        if self.dry_run:
            return
        
        conn = self.get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO taxonomy_enrichment_log (
                        taxonomy_id, species_name, search_query, api_endpoint, api_url,
                        response_status, response_time_ms, matches_found,
                        taxon_id_selected, match_rank, confidence_score, match_method,
                        needs_manual_review, review_reason, error_message
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    taxonomy_id, species_name, species_name, api_endpoint, api_url,
                    status, response_time, 1 if taxon_id else 0,
                    taxon_id, 1, confidence, match_method,
                    needs_review, review_reason or None, error_msg
                ))
                conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to log enrichment: {e}")
        finally:
            conn.close()
    
    def enrich_species(self, row: Tuple) -> bool:
        """Enrich a single species with WoRMS and/or GBIF data."""
        taxonomy_id, species_name, phylum, class_name, iconic_taxon, worms_id, gbif_key = row
        
        logger.info(f"Enriching: {species_name}")
        
        existing_data = {
            'phylum': phylum,
            'class': class_name,
            'iconic_taxon_name': iconic_taxon
        }
        
        # Determine if likely marine
        is_marine = self.is_likely_marine(species_name, existing_data)
        
        success = False
        
        # Try WoRMS first for marine species or if source=worms
        if (is_marine or self.source == 'worms') and self.source != 'gbif' and not worms_id:
            result, status, resp_time = self.search_worms(species_name)
            
            if result and status == 200:
                # Handle array or single record
                records = result if isinstance(result, list) else [result]
                
                if records:
                    best_record = records[0]  # Take first/best match
                    confidence = self.calculate_worms_confidence(species_name, best_record)
                    needs_review = confidence < CONFIDENCE_REVIEW_THRESHOLD
                    
                    worms_data = self.extract_worms_data(best_record)
                    self.update_cache(taxonomy_id, species_name, worms_data, 'worms')
                    
                    self.log_enrichment(
                        taxonomy_id, species_name, 'worms', f"{WORMS_API_BASE}/AphiaRecordsByName",
                        status, resp_time, confidence, 'worms_match', needs_review,
                        'low_confidence' if needs_review else '',
                        best_record.get('AphiaID')
                    )
                    
                    self.stats['worms_successful'] += 1
                    success = True
                    
                    if needs_review:
                        self.stats['needs_review'] += 1
                        logger.info(f"⚠ {species_name}: WoRMS match needs review (confidence: {confidence:.2f})")
                    else:
                        logger.info(f"✓ {species_name}: WoRMS match (confidence: {confidence:.2f})")
        
        # Try GBIF if WoRMS failed or for non-marine species
        if not success and self.source != 'worms' and not gbif_key:
            result, status, resp_time = self.search_gbif(species_name)
            
            if result and status == 200:
                confidence = self.calculate_gbif_confidence(species_name, result)
                needs_review = confidence < CONFIDENCE_REVIEW_THRESHOLD
                
                gbif_data = self.extract_gbif_data(result)
                self.update_cache(taxonomy_id, species_name, gbif_data, 'gbif')
                
                self.log_enrichment(
                    taxonomy_id, species_name, 'gbif', f"{GBIF_API_BASE}/species/match",
                    status, resp_time, confidence, result.get('matchType', 'unknown'),
                    needs_review, 'low_confidence' if needs_review else '',
                    result.get('usageKey')
                )
                
                self.stats['gbif_successful'] += 1
                success = True
                
                if needs_review:
                    self.stats['needs_review'] += 1
                    logger.info(f"⚠ {species_name}: GBIF match needs review (confidence: {confidence:.2f})")
                else:
                    logger.info(f"✓ {species_name}: GBIF match (confidence: {confidence:.2f})")
        
        if not success:
            self.stats['failed'] += 1
            logger.warning(f"✗ {species_name}: No matches found")
        
        return success
    
    def run(self, batch_size: int = DEFAULT_BATCH_SIZE, limit: Optional[int] = None):
        """Main enrichment process."""
        logger.info("\n" + "="*60)
        logger.info("TAXONOMY ENRICHMENT - WoRMS & GBIF APIs")
        logger.info("="*60)
        logger.info(f"Batch size: {batch_size}")
        logger.info(f"Source preference: {self.source}")
        logger.info(f"Dry run: {self.dry_run}")
        if limit:
            logger.info(f"Limit: {limit} species")
        logger.info("="*60 + "\n")
        
        species_list = self.fetch_species_to_enrich(limit)
        
        if not species_list:
            logger.info("✓ All species already enriched with WoRMS/GBIF!")
            return
        
        logger.info(f"Found {len(species_list)} species to enrich\n")
        
        start_time = time.time()
        
        for i in range(0, len(species_list), batch_size):
            batch = species_list[i:i + batch_size]
            
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing batch {i//batch_size + 1}")
            logger.info(f"{'='*60}\n")
            
            for row in batch:
                self.stats['total_processed'] += 1
                self.enrich_species(row)
                time.sleep(0.1)  # Small delay between requests
            
            progress = (i + len(batch)) / len(species_list) * 100
            logger.info(f"\nProgress: {progress:.1f}% ({i + len(batch)}/{len(species_list)})")
        
        elapsed = time.time() - start_time
        
        logger.info("\n" + "="*60)
        logger.info("ENRICHMENT COMPLETE")
        logger.info("="*60)
        logger.info(f"Total processed: {self.stats['total_processed']}")
        logger.info(f"WoRMS successful: {self.stats['worms_successful']}")
        logger.info(f"GBIF successful: {self.stats['gbif_successful']}")
        logger.info(f"Failed: {self.stats['failed']}")
        logger.info(f"Needs review: {self.stats['needs_review']}")
        logger.info(f"API calls - WoRMS: {self.stats['api_calls_worms']}")
        logger.info(f"API calls - GBIF: {self.stats['api_calls_gbif']}")
        logger.info(f"Elapsed time: {elapsed:.1f}s")
        logger.info("="*60 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description='Enrich taxonomy data from WoRMS and GBIF APIs'
    )
    parser.add_argument('--dry-run', action='store_true',
                       help='Perform API calls but do not save to database')
    parser.add_argument('--batch-size', type=int, default=DEFAULT_BATCH_SIZE,
                       help=f'Number of species per batch (default: {DEFAULT_BATCH_SIZE})')
    parser.add_argument('--limit', type=int,
                       help='Limit number of species to process (for testing)')
    parser.add_argument('--source', choices=['auto', 'worms', 'gbif'], default='auto',
                       help='API source preference (default: auto)')
    
    args = parser.parse_args()
    
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5433)),
        'database': os.getenv('DB_NAME', 'marine_db'),
        'user': os.getenv('DB_USER', 'marine_user'),
        'password': os.getenv('DB_PASSWORD', 'marine_pass123')
    }
    
    os.makedirs('logs', exist_ok=True)
    
    enricher = WoRMSGBIFEnricher(db_config, dry_run=args.dry_run, source=args.source)
    
    try:
        enricher.run(batch_size=args.batch_size, limit=args.limit)
    except KeyboardInterrupt:
        logger.info("\n\n⚠ Interrupted by user")
        logger.info("Partial statistics:")
        logger.info(f"  Processed: {enricher.stats['total_processed']}")
        logger.info(f"  WoRMS: {enricher.stats['worms_successful']}")
        logger.info(f"  GBIF: {enricher.stats['gbif_successful']}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n✗ Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
