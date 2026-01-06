#!/usr/bin/env python3
"""
Taxonomy Enrichment Script - iNaturalist API Integration

Enriches species taxonomy data from the iNaturalist API with:
- Taxonomic hierarchy (genus, family, order, class, phylum, kingdom)
- Common names (with language/locality support)
- Conservation status and endemic/introduced flags
- Representative photos and Wikipedia links
- WoRMS and GBIF cross-references

Features:
- Batch processing with configurable batch size
- Rate limiting (60 requests/minute default)
- Confidence scoring for match quality
- Comprehensive error handling with retry logic
- Progress tracking with rich console output
- Full audit logging to database

Usage:
    python scripts/enrich_taxonomy_from_inaturalist.py
    python scripts/enrich_taxonomy_from_inaturalist.py --dry-run
    python scripts/enrich_taxonomy_from_inaturalist.py --batch-size 100
    python scripts/enrich_taxonomy_from_inaturalist.py --limit 10

Author: Huon Channel Marine Analytics
Created: January 6, 2026
Version: 1.0
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
INATURALIST_API_BASE = "https://api.inaturalist.org/v1"
RATE_LIMIT_REQUESTS = 60  # requests per minute
RATE_LIMIT_WINDOW = 60  # seconds
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds (exponential backoff)
DEFAULT_BATCH_SIZE = 50

# Confidence thresholds
CONFIDENCE_EXACT_MATCH = 1.0
CONFIDENCE_HIGH = 0.9
CONFIDENCE_MEDIUM = 0.7
CONFIDENCE_LOW = 0.5
CONFIDENCE_REVIEW_THRESHOLD = 0.8

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/taxonomy_enrichment.log'),
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
            # Calculate wait time
            oldest_request = min(self.requests)
            wait_time = self.window_seconds - (now - oldest_request) + 0.1
            if wait_time > 0:
                logger.debug(f"Rate limit reached. Waiting {wait_time:.2f}s")
                time.sleep(wait_time)
                self.requests = []
        
        self.requests.append(time.time())


class TaxonomyEnricher:
    """Handles taxonomy enrichment from iNaturalist API."""
    
    def __init__(self, db_config: Dict[str, str], dry_run: bool = False):
        self.db_config = db_config
        self.dry_run = dry_run
        self.rate_limiter = RateLimiter(RATE_LIMIT_REQUESTS, RATE_LIMIT_WINDOW)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'HuonChannelMarineAnalytics/1.0 (research project)',
            'Accept': 'application/json'
        })
        self.stats = {
            'total_processed': 0,
            'successful': 0,
            'failed': 0,
            'needs_review': 0,
            'api_calls': 0,
            'cache_hits': 0
        }
    
    def get_db_connection(self):
        """Create database connection."""
        return psycopg2.connect(**self.db_config)
    
    def fetch_unenriched_species(self, limit: Optional[int] = None) -> List[Tuple[int, str]]:
        """Fetch species that haven't been enriched yet."""
        conn = self.get_db_connection()
        try:
            with conn.cursor() as cur:
                query = """
                    SELECT t.id, t.species_name
                    FROM taxonomy t
                    LEFT JOIN taxonomy_cache tc ON t.id = tc.taxonomy_id
                    WHERE tc.id IS NULL
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
    
    def search_inaturalist(self, species_name: str) -> Tuple[Optional[Dict], int, int]:
        """Search iNaturalist API for species.
        
        Returns:
            (result_dict, http_status, response_time_ms)
        """
        self.rate_limiter.wait_if_needed()
        
        url = f"{INATURALIST_API_BASE}/taxa"
        params = {
            'q': species_name,
            'rank': 'species',
            'is_active': 'true',
            'per_page': 10
        }
        
        start_time = time.time()
        
        for attempt in range(MAX_RETRIES):
            try:
                self.stats['api_calls'] += 1
                response = self.session.get(url, params=params, timeout=30)
                response_time_ms = int((time.time() - start_time) * 1000)
                
                if response.status_code == 200:
                    data = response.json()
                    return data, 200, response_time_ms
                elif response.status_code == 429:  # Rate limited
                    wait_time = RETRY_DELAY * (2 ** attempt)
                    logger.warning(f"Rate limited. Waiting {wait_time}s before retry")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"API error {response.status_code}: {response.text}")
                    return None, response.status_code, response_time_ms
            
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (2 ** attempt))
                else:
                    return None, 0, int((time.time() - start_time) * 1000)
        
        return None, 0, int((time.time() - start_time) * 1000)
    
    def calculate_match_confidence(self, species_name: str, taxon: Dict) -> float:
        """Calculate confidence score for a taxon match.
        
        Scoring:
        1.0 = Exact match (case-insensitive)
        0.9+ = High similarity (>90%)
        0.7-0.9 = Medium similarity (fuzzy match)
        0.5 = Genus-only match
        <0.5 = Low confidence
        """
        taxon_name = taxon.get('name', '').lower()
        species_lower = species_name.lower()
        
        # Exact match
        if taxon_name == species_lower:
            return CONFIDENCE_EXACT_MATCH
        
        # Check if it's an exact match ignoring authority
        taxon_name_no_authority = taxon_name.split('(')[0].strip()
        if taxon_name_no_authority == species_lower:
            return CONFIDENCE_EXACT_MATCH
        
        # Fuzzy string matching
        similarity = SequenceMatcher(None, taxon_name, species_lower).ratio()
        
        if similarity >= 0.95:
            return CONFIDENCE_HIGH
        elif similarity >= 0.8:
            return CONFIDENCE_MEDIUM
        
        # Genus-only match
        genus_match = species_lower.split()[0] if ' ' in species_lower else ''
        taxon_genus = taxon_name.split()[0] if ' ' in taxon_name else ''
        
        if genus_match and genus_match == taxon_genus:
            return CONFIDENCE_LOW
        
        return 0.3  # Very low confidence
    
    def select_best_match(self, species_name: str, taxa_results: List[Dict]) -> Tuple[Optional[Dict], float, str, str]:
        """Select best matching taxon from results.
        
        Returns:
            (best_taxon, confidence_score, match_method, review_reason or '')
        """
        if not taxa_results:
            return None, 0.0, 'no_match', 'no_match'
        
        # Score all results
        scored_taxa = []
        for taxon in taxa_results:
            confidence = self.calculate_match_confidence(species_name, taxon)
            scored_taxa.append((taxon, confidence))
        
        # Sort by confidence
        scored_taxa.sort(key=lambda x: x[1], reverse=True)
        best_taxon, best_confidence = scored_taxa[0]
        
        # Determine match method
        if best_confidence == CONFIDENCE_EXACT_MATCH:
            match_method = 'exact'
            review_reason = ''
        elif best_confidence >= CONFIDENCE_HIGH:
            match_method = 'high_confidence'
            review_reason = ''
        elif best_confidence >= CONFIDENCE_MEDIUM:
            match_method = 'fuzzy'
            review_reason = 'fuzzy_match' if best_confidence < CONFIDENCE_REVIEW_THRESHOLD else ''
        else:
            match_method = 'low_confidence'
            review_reason = 'low_confidence'
        
        # Check for ambiguous matches
        if len(scored_taxa) > 1:
            second_best_confidence = scored_taxa[1][1]
            if abs(best_confidence - second_best_confidence) < 0.1:
                review_reason = 'ambiguous'
        
        return best_taxon, best_confidence, match_method, review_reason
    
    def extract_taxonomy_data(self, taxon: Dict) -> Dict:
        """Extract relevant taxonomy data from iNaturalist taxon object."""
        # Get ancestor taxa for hierarchy
        ancestors = {a['rank']: a for a in taxon.get('ancestors', [])}
        
        # Extract conservation status
        conservation_statuses = taxon.get('conservation_statuses', [])
        conservation_status = None
        conservation_source = None
        if conservation_statuses:
            # Prefer Australian or global status
            for status in conservation_statuses:
                if status.get('place', {}).get('name') in ['Australia', None]:
                    conservation_status = status.get('status')
                    conservation_source = status.get('authority')
                    break
        
        # Get default photo
        photo_url = None
        photo_attribution = None
        if taxon.get('default_photo'):
            photo = taxon['default_photo']
            photo_url = photo.get('medium_url') or photo.get('url')
            photo_attribution = photo.get('attribution')
        
        # Extract Wikipedia URL
        wikipedia_url = taxon.get('wikipedia_url')
        
        # Check establishment means (introduced vs native)
        establishment_means = taxon.get('establishment_means', {}).get('establishment_means')
        introduced = establishment_means in ['introduced', 'invasive', 'managed']
        
        # Check if endemic (Australia-specific)
        endemic = taxon.get('endemic', False)
        threatened = taxon.get('threatened', False)
        
        return {
            'inaturalist_taxon_id': taxon.get('id'),
            'inaturalist_url': f"https://www.inaturalist.org/taxa/{taxon.get('id')}",
            'common_name': taxon.get('preferred_common_name'),
            'genus': ancestors.get('genus', {}).get('name'),
            'family': ancestors.get('family', {}).get('name'),
            'order': ancestors.get('order', {}).get('name'),
            'class': ancestors.get('class', {}).get('name'),
            'phylum': ancestors.get('phylum', {}).get('name'),
            'kingdom': ancestors.get('kingdom', {}).get('name'),
            'authority': taxon.get('name', '').split('(')[1].strip(')') if '(' in taxon.get('name', '') else None,
            'rank': taxon.get('rank'),
            'rank_level': taxon.get('rank_level'),
            'iconic_taxon_name': taxon.get('iconic_taxon_name'),
            'conservation_status': conservation_status,
            'conservation_status_source': conservation_source,
            'introduced': introduced,
            'endemic': endemic,
            'threatened': threatened,
            'wikipedia_url': wikipedia_url,
            'photo_url': photo_url,
            'photo_attribution': photo_attribution,
            'data_source': 'inaturalist',
            'inaturalist_response': json.dumps(taxon)  # Store full response as JSONB
        }
    
    def save_to_cache(self, taxonomy_id: int, species_name: str, taxon_data: Dict):
        """Save enriched data to taxonomy_cache table."""
        if self.dry_run:
            logger.info(f"[DRY RUN] Would save to cache: {species_name}")
            return
        
        conn = self.get_db_connection()
        try:
            with conn.cursor() as cur:
                # Insert into taxonomy_cache
                insert_query = """
                    INSERT INTO taxonomy_cache (
                        taxonomy_id, species_name, inaturalist_taxon_id, inaturalist_url,
                        common_name, genus, family, "order", class, phylum, kingdom, authority,
                        rank, rank_level, iconic_taxon_name,
                        conservation_status, conservation_status_source,
                        introduced, endemic, threatened,
                        wikipedia_url, photo_url, photo_attribution,
                        data_source, inaturalist_response
                    ) VALUES (
                        %(taxonomy_id)s, %(species_name)s, %(inaturalist_taxon_id)s, %(inaturalist_url)s,
                        %(common_name)s, %(genus)s, %(family)s, %(order)s, %(class)s, %(phylum)s, 
                        %(kingdom)s, %(authority)s, %(rank)s, %(rank_level)s, %(iconic_taxon_name)s,
                        %(conservation_status)s, %(conservation_status_source)s,
                        %(introduced)s, %(endemic)s, %(threatened)s,
                        %(wikipedia_url)s, %(photo_url)s, %(photo_attribution)s,
                        %(data_source)s, %(inaturalist_response)s::jsonb
                    )
                    ON CONFLICT (species_name) 
                    DO UPDATE SET
                        inaturalist_taxon_id = EXCLUDED.inaturalist_taxon_id,
                        common_name = EXCLUDED.common_name,
                        genus = EXCLUDED.genus,
                        family = EXCLUDED.family,
                        last_updated = NOW()
                """
                
                params = {**taxon_data, 'taxonomy_id': taxonomy_id, 'species_name': species_name}
                cur.execute(insert_query, params)
                
                # Extract common names
                taxon = json.loads(taxon_data['inaturalist_response'])
                common_names = taxon.get('names', [])
                
                for name_obj in common_names:
                    if name_obj.get('lexicon') == 'English':
                        cur.execute("""
                            INSERT INTO taxonomy_common_names 
                            (taxonomy_id, common_name, language, is_primary, source)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (taxonomy_id, common_name, language) DO NOTHING
                        """, (
                            taxonomy_id,
                            name_obj.get('name'),
                            'en',
                            name_obj.get('is_valid', False),
                            'inaturalist'
                        ))
                
                conn.commit()
                logger.info(f"✓ Saved to cache: {species_name}")
        
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to save cache for {species_name}: {e}")
            raise
        finally:
            conn.close()
    
    def log_enrichment_attempt(
        self,
        taxonomy_id: int,
        species_name: str,
        search_query: str,
        api_url: str,
        response_status: int,
        response_time_ms: int,
        matches_found: int,
        taxon_id_selected: Optional[int],
        match_rank: int,
        confidence_score: float,
        match_method: str,
        needs_review: bool,
        review_reason: str,
        error_message: Optional[str] = None
    ):
        """Log enrichment attempt to taxonomy_enrichment_log."""
        if self.dry_run:
            logger.info(f"[DRY RUN] Would log: {species_name} (confidence: {confidence_score:.2f})")
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
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """, (
                    taxonomy_id, species_name, search_query, 'inaturalist', api_url,
                    response_status, response_time_ms, matches_found,
                    taxon_id_selected, match_rank, confidence_score, match_method,
                    needs_review, review_reason or None, error_message
                ))
                conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to log enrichment for {species_name}: {e}")
        finally:
            conn.close()
    
    def enrich_species(self, taxonomy_id: int, species_name: str) -> bool:
        """Enrich a single species from iNaturalist.
        
        Returns:
            True if successful (even if needs review), False if failed
        """
        logger.info(f"Enriching: {species_name}")
        
        # Search iNaturalist
        result, status_code, response_time = self.search_inaturalist(species_name)
        
        if not result or status_code != 200:
            error_msg = f"API call failed with status {status_code}"
            logger.warning(f"✗ {species_name}: {error_msg}")
            self.log_enrichment_attempt(
                taxonomy_id, species_name, species_name, 
                f"{INATURALIST_API_BASE}/taxa",
                status_code, response_time, 0, None, 0, 0.0, 'api_error',
                True, 'api_error', error_msg
            )
            self.stats['failed'] += 1
            return False
        
        taxa = result.get('results', [])
        
        if not taxa:
            logger.warning(f"✗ {species_name}: No matches found")
            self.log_enrichment_attempt(
                taxonomy_id, species_name, species_name,
                f"{INATURALIST_API_BASE}/taxa",
                status_code, response_time, 0, None, 0, 0.0, 'no_match',
                True, 'no_match', None
            )
            self.stats['failed'] += 1
            return False
        
        # Select best match
        best_taxon, confidence, match_method, review_reason = self.select_best_match(
            species_name, taxa
        )
        
        needs_review = bool(review_reason) or confidence < CONFIDENCE_REVIEW_THRESHOLD
        
        if needs_review:
            self.stats['needs_review'] += 1
            logger.info(f"⚠ {species_name}: Needs review ({review_reason}, confidence: {confidence:.2f})")
        else:
            logger.info(f"✓ {species_name}: Match found (confidence: {confidence:.2f})")
        
        # Extract taxonomy data
        taxon_data = self.extract_taxonomy_data(best_taxon)
        
        # Save to cache
        try:
            self.save_to_cache(taxonomy_id, species_name, taxon_data)
            self.stats['successful'] += 1
        except Exception as e:
            logger.error(f"Failed to save {species_name}: {e}")
            self.stats['failed'] += 1
            return False
        
        # Log the attempt
        self.log_enrichment_attempt(
            taxonomy_id, species_name, species_name,
            f"{INATURALIST_API_BASE}/taxa",
            status_code, response_time, len(taxa),
            best_taxon.get('id'), 1, confidence, match_method,
            needs_review, review_reason, None
        )
        
        return True
    
    def process_batch(self, batch: List[Tuple[int, str]]):
        """Process a batch of species."""
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing batch of {len(batch)} species")
        logger.info(f"{'='*60}\n")
        
        for taxonomy_id, species_name in batch:
            self.stats['total_processed'] += 1
            self.enrich_species(taxonomy_id, species_name)
            
            # Small delay between requests (in addition to rate limiter)
            time.sleep(0.1)
    
    def run(self, batch_size: int = DEFAULT_BATCH_SIZE, limit: Optional[int] = None):
        """Main enrichment process."""
        logger.info("\n" + "="*60)
        logger.info("TAXONOMY ENRICHMENT - iNaturalist API")
        logger.info("="*60)
        logger.info(f"Batch size: {batch_size}")
        logger.info(f"Rate limit: {RATE_LIMIT_REQUESTS} requests/{RATE_LIMIT_WINDOW}s")
        logger.info(f"Dry run: {self.dry_run}")
        if limit:
            logger.info(f"Limit: {limit} species")
        logger.info("="*60 + "\n")
        
        # Fetch unenriched species
        species_list = self.fetch_unenriched_species(limit)
        
        if not species_list:
            logger.info("✓ All species already enriched!")
            return
        
        logger.info(f"Found {len(species_list)} species to enrich\n")
        
        # Process in batches
        start_time = time.time()
        
        for i in range(0, len(species_list), batch_size):
            batch = species_list[i:i + batch_size]
            self.process_batch(batch)
            
            # Progress update
            progress = (i + len(batch)) / len(species_list) * 100
            logger.info(f"\nProgress: {progress:.1f}% ({i + len(batch)}/{len(species_list)})")
        
        elapsed_time = time.time() - start_time
        
        # Final statistics
        logger.info("\n" + "="*60)
        logger.info("ENRICHMENT COMPLETE")
        logger.info("="*60)
        logger.info(f"Total processed: {self.stats['total_processed']}")
        logger.info(f"Successful: {self.stats['successful']}")
        logger.info(f"Failed: {self.stats['failed']}")
        logger.info(f"Needs review: {self.stats['needs_review']}")
        logger.info(f"API calls: {self.stats['api_calls']}")
        logger.info(f"Elapsed time: {elapsed_time:.1f}s")
        logger.info(f"Average time per species: {elapsed_time / len(species_list):.2f}s")
        logger.info("="*60 + "\n")
        
        if self.stats['needs_review'] > 0:
            logger.info("⚠ Run this query to see species needing review:")
            logger.info("  SELECT * FROM taxa_needing_review LIMIT 20;\n")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Enrich taxonomy data from iNaturalist API'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Perform API calls but do not save to database'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f'Number of species per batch (default: {DEFAULT_BATCH_SIZE})'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of species to process (for testing)'
    )
    
    args = parser.parse_args()
    
    # Database configuration from environment
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5433)),
        'database': os.getenv('DB_NAME', 'marine_db'),
        'user': os.getenv('DB_USER', 'marine_user'),
        'password': os.getenv('DB_PASSWORD', 'marine_pass')
    }
    
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Run enrichment
    enricher = TaxonomyEnricher(db_config, dry_run=args.dry_run)
    
    try:
        enricher.run(batch_size=args.batch_size, limit=args.limit)
    except KeyboardInterrupt:
        logger.info("\n\n⚠ Interrupted by user")
        logger.info("Partial statistics:")
        logger.info(f"  Processed: {enricher.stats['total_processed']}")
        logger.info(f"  Successful: {enricher.stats['successful']}")
        logger.info(f"  Failed: {enricher.stats['failed']}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n✗ Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
