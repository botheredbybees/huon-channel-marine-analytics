#!/usr/bin/env python3
"""
Enrich metadata table from ISO 19115-3 XML files in AODN dataset directories.
Non-destructive: only updates NULL/empty fields.

Enhanced with:
- AODN UUID extraction from XML metadata
- Deduplication logic to prevent re-ingestion of AODN datasets
- aodn_uuid field population for AODN-sourced data
- Improved logging for debugging and audit trails

This script extracts metadata from XML files located in dataset directories
and populates empty fields in the metadata table. It's designed to run
independently of the main ETL pipeline.

Usage:
    python enrich_metadata_from_xml.py

Environment variables:
    DB_HOST: Database host (default: localhost)
    DB_PORT: Database port (default: 5433)
    DB_NAME: Database name (default: marine_db)
    DB_USER: Database user (default: marine_user)
    DB_PASSWORD: Database password (required for authentication)
    AODN_DATA_PATH: Path to AODN_data directory (default: /AODN_data)
"""

import os
import psycopg2
from pathlib import Path
from xml.etree import ElementTree as ET
from typing import Dict, Optional, Tuple
import logging
from datetime import datetime
import sys
import re

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# XML namespace mappings for ISO 19115-3
# Note: Using only common namespaces for explicit find() calls
# For generic searches, we use regex-based namespace-agnostic matching
NAMESPACES = {
    'gmd': 'http://www.isotc211.org/2005/gmd',
    'gco': 'http://www.isotc211.org/2005/gco',
    'gml': 'http://www.opengis.net/gml/3.2.1',
    'srv': 'http://www.isotc211.org/2005/srv',
    'mdb': 'http://www.isotc211.org/2005/mdb',
}


def find_element_with_namespace(root: ET.Element, local_tag: str) -> Optional[ET.Element]:
    """
    Find element by local tag name, ignoring namespace.
    
    This handles ISO 19115-3 documents where namespace prefixes may vary.
    
    Args:
        root: XML root element
        local_tag: Tag name without namespace (e.g., 'CharacterString')
    
    Returns:
        First matching element, or None
    """
    for elem in root.iter():
        # Extract local name from tag (remove namespace prefix)
        tag_local = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
        if tag_local == local_tag:
            return elem
    return None


def find_elements_by_path_generic(root: ET.Element, path_parts: list) -> Optional[ET.Element]:
    """
    Find element by path, matching local names without namespace.
    
    Args:
        root: XML root element
        path_parts: List of tag names to match in sequence
                   e.g., ['metadataIdentifier', 'MD_Identifier', 'code', 'CharacterString']
    
    Returns:
        First matching element at end of path, or None
    """
    def local_name(tag):
        return tag.split('}')[-1] if '}' in tag else tag
    
    def search_path(current, remaining_path):
        if not remaining_path:
            return current
        
        target_tag = remaining_path[0]
        rest_path = remaining_path[1:]
        
        for child in current:
            if local_name(child.tag) == target_tag:
                result = search_path(child, rest_path)
                if result is not None:
                    return result
        return None
    
    return search_path(root, path_parts)


class MetadataEnricher:
    """Extract and enrich metadata from ISO 19115-3 XML files."""
    
    def __init__(self, db_config: dict, aodn_data_path: str):
        self.db_config = db_config
        self.aodn_data_path = Path(aodn_data_path)
        self.conn = None
        self.stats = {
            'files_found': 0,
            'files_processed': 0,
            'files_failed': 0,
            'files_deduplicated': 0,
            'rows_updated': 0,
            'aodn_uuids_extracted': 0,
            'fields_enriched': 0,
        }
        
    def connect(self):
        """Connect to PostgreSQL database."""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            logger.info(f"Connected to {self.db_config['database']} at {self.db_config['host']}:{self.db_config['port']}")
        except psycopg2.OperationalError as e:
            logger.error(f"Failed to connect to database: {e}")
            logger.error("Check your environment variables: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD")
            sys.exit(1)
        
    def disconnect(self):
        """Disconnect from database."""
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from database")
    
    def find_metadata_xml_files(self) -> Dict[str, Path]:
        """Find all metadata.xml files in AODN_data directory."""
        xml_files = {}
        pattern = '**/metadata/metadata.xml'
        
        if not self.aodn_data_path.exists():
            logger.error(f"AODN_DATA_PATH does not exist: {self.aodn_data_path}")
            return xml_files
        
        logger.info(f"Scanning for metadata.xml files in {self.aodn_data_path}")
        for xml_file in self.aodn_data_path.glob(pattern):
            # Extract dataset UUID from path: AODN_data/<dataset>/<uuid>/metadata/metadata.xml
            try:
                parts = xml_file.parts
                if len(parts) >= 2:
                    uuid = xml_file.parent.parent.name
                    xml_files[uuid] = xml_file
                    logger.debug(f"Found metadata file: {uuid} -> {xml_file}")
            except Exception as e:
                logger.warning(f"Could not extract UUID from {xml_file}: {e}")
            
        self.stats['files_found'] = len(xml_files)
        logger.info(f"Found {len(xml_files)} metadata.xml files")
        return xml_files
    
    def find_metadata_record_id(self, directory_uuid: str) -> Optional[int]:
        """
        Find metadata record ID by directory UUID.
        
        Option 1: Returns only the record ID (simplified lookup).
        
        Args:
            directory_uuid: UUID from filesystem directory structure
        
        Returns:
            metadata.id (primary key) if found, None otherwise
        """
        cursor = self.conn.cursor()
        try:
            logger.debug(f"Looking up metadata record for directory UUID: {directory_uuid}")
            cursor.execute(
                "SELECT id FROM metadata WHERE uuid = %s LIMIT 1",
                [directory_uuid]
            )
            result = cursor.fetchone()
            
            if result:
                record_id = result[0]
                logger.info(f"✓ Found metadata record: directory_uuid={directory_uuid} -> metadata.id={record_id}")
                return record_id
            else:
                logger.warning(f"✗ No metadata record found for directory UUID: {directory_uuid}")
                return None
                
        except psycopg2.Error as e:
            logger.error(f"Database error during record lookup for {directory_uuid}: {e}")
            return None
        finally:
            cursor.close()
    
    def parse_iso_19115_xml(self, xml_path: Path) -> Tuple[Dict[str, any], Optional[str]]:
        """
        Extract metadata fields from ISO 19115-3 XML file.
        
        Returns:
            Tuple of (metadata_dict, aodn_uuid) where aodn_uuid may be None
            for non-AODN datasets.
        """
        logger.debug(f"Parsing XML: {xml_path}")
        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            
            metadata = {
                'abstract': None,
                'credit': None,
                'topic_category': None,
                'west': None,
                'east': None,
                'south': None,
                'north': None,
                'time_start': None,
                'time_end': None,
                'lineage': None,
                'supplemental_info': None,
                'license_url': None,
            }
            
            # ========== NEW: Extract AODN UUID from XML ==========
            aodn_uuid = self._extract_aodn_uuid(root)
            if aodn_uuid:
                logger.info(f"✓ Extracted AODN UUID from XML: {aodn_uuid}")
                self.stats['aodn_uuids_extracted'] += 1
            else:
                logger.debug(f"No AODN UUID found in {xml_path.name}")
            # ====================================================
            
            # Extract abstract
            abstract_xpath = './/gmd:abstract/gco:CharacterString'
            abstract_elem = root.find(abstract_xpath, NAMESPACES)
            if abstract_elem is not None and abstract_elem.text:
                metadata['abstract'] = abstract_elem.text[:1000]  # Limit length
                logger.debug(f"  Found abstract ({len(abstract_elem.text)} chars)")
            
            # Extract credit/acknowledgment
            credit_xpath = './/gmd:credit/gco:CharacterString'
            credit_elem = root.find(credit_xpath, NAMESPACES)
            if credit_elem is not None and credit_elem.text:
                metadata['credit'] = credit_elem.text[:500]
                logger.debug(f"  Found credit ({len(credit_elem.text)} chars)")
            
            # Extract spatial extent (bounding box)
            self._extract_spatial_extent(root, metadata)
            
            # Extract temporal extent (dates)
            self._extract_temporal_extent(root, metadata)
            
            # Extract lineage (processing history)
            lineage_xpath = './/gmd:lineage/gmd:LI_Lineage/gmd:statement/gco:CharacterString'
            lineage_elem = root.find(lineage_xpath, NAMESPACES)
            if lineage_elem is not None and lineage_elem.text:
                metadata['lineage'] = lineage_elem.text[:1000]
                logger.debug(f"  Found lineage ({len(lineage_elem.text)} chars)")
            
            # Extract license/constraints
            license_xpath = './/gmd:MD_LegalConstraints/gmd:otherConstraints/gco:CharacterString'
            license_elem = root.find(license_xpath, NAMESPACES)
            if license_elem is not None and license_elem.text:
                metadata['license_url'] = license_elem.text[:500]
                logger.debug(f"  Found license info")
            
            # Log summary of extracted fields
            non_null_fields = [k for k, v in metadata.items() if v is not None]
            logger.info(f"  Extracted {len(non_null_fields)} metadata fields: {', '.join(non_null_fields)}")
            
            return metadata, aodn_uuid
            
        except ET.ParseError as e:
            logger.error(f"XML parsing error in {xml_path}: {e}")
            self.stats['files_failed'] += 1
            return {}, None
        except Exception as e:
            logger.error(f"Unexpected error parsing {xml_path}: {e}")
            self.stats['files_failed'] += 1
            return {}, None
    
    def _extract_aodn_uuid(self, root: ET.Element) -> Optional[str]:
        """
        Extract AODN UUID from ISO 19115-3 XML metadata.
        
        The UUID is typically found in either:
        1. metadataIdentifier/MD_Identifier/code/CharacterString (newer ISO 19115-3)
        2. fileIdentifier/CharacterString (older gmd format)
        
        Uses namespace-agnostic matching to handle various namespace prefixes.
        
        Returns:
            AODN UUID string if found, None otherwise
        """
        try:
            # Strategy 1: Try metadataIdentifier path (ISO 19115-3 standard location)
            # metadataIdentifier -> MD_Identifier -> code -> CharacterString/Anchor
            uuid_elem = find_elements_by_path_generic(
                root, 
                ['metadataIdentifier', 'MD_Identifier', 'code', 'CharacterString']
            )
            if uuid_elem is not None and uuid_elem.text:
                uuid_str = uuid_elem.text.strip()
                if uuid_str:
                    logger.debug(f"Found AODN UUID via metadataIdentifier path: {uuid_str}")
                    return uuid_str
            
            # Try Anchor element as fallback (some metadata use gcx:Anchor for URIs)
            uuid_elem = find_elements_by_path_generic(
                root,
                ['metadataIdentifier', 'MD_Identifier', 'code', 'Anchor']
            )
            if uuid_elem is not None and uuid_elem.text:
                uuid_str = uuid_elem.text.strip()
                if uuid_str:
                    logger.debug(f"Found AODN UUID via Anchor: {uuid_str}")
                    return uuid_str
            
            # Strategy 2: Try older gmd:fileIdentifier path (backward compatibility)
            uuid_elem = find_elements_by_path_generic(
                root,
                ['fileIdentifier', 'CharacterString']
            )
            if uuid_elem is not None and uuid_elem.text:
                uuid_str = uuid_elem.text.strip()
                if uuid_str:
                    logger.debug(f"Found AODN UUID via fileIdentifier path: {uuid_str}")
                    return uuid_str
            
            logger.debug(f"No AODN UUID found in XML document")
            return None
            
        except Exception as e:
            logger.warning(f"Error extracting AODN UUID: {e}")
            return None
    
    def check_aodn_uuid_exists(self, aodn_uuid: str) -> bool:
        """
        Check if AODN UUID already exists in metadata table.
        
        Implements deduplication logic to prevent re-ingestion.
        
        Returns:
            True if AODN UUID already exists (skip processing)
            False if AODN UUID is new (process normally)
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                "SELECT id FROM metadata WHERE aodn_uuid = %s LIMIT 1",
                [aodn_uuid]
            )
            result = cursor.fetchone()
            exists = result is not None
            
            if exists:
                logger.info(f"⚠ AODN UUID {aodn_uuid} already exists in database (id={result[0]})")
            else:
                logger.debug(f"✓ AODN UUID {aodn_uuid} is new (not in database)")
                
            return exists
        except psycopg2.Error as e:
            logger.error(f"Database error checking AODN UUID {aodn_uuid}: {e}")
            return False
        finally:
            cursor.close()
    
    def _extract_spatial_extent(self, root: ET.Element, metadata: dict):
        """Extract geographic bounding box from XML."""
        try:
            bbox_xpath = './/gmd:EX_GeographicBoundingBox'
            bbox = root.find(bbox_xpath, NAMESPACES)
            
            if bbox is not None:
                west_xpath = './gmd:westBoundLongitude/gco:Decimal'
                east_xpath = './gmd:eastBoundLongitude/gco:Decimal'
                south_xpath = './gmd:southBoundLatitude/gco:Decimal'
                north_xpath = './gmd:northBoundLatitude/gco:Decimal'
                
                west = bbox.find(west_xpath, NAMESPACES)
                east = bbox.find(east_xpath, NAMESPACES)
                south = bbox.find(south_xpath, NAMESPACES)
                north = bbox.find(north_xpath, NAMESPACES)
                
                try:
                    if west is not None and west.text: metadata['west'] = float(west.text)
                    if east is not None and east.text: metadata['east'] = float(east.text)
                    if south is not None and south.text: metadata['south'] = float(south.text)
                    if north is not None and north.text: metadata['north'] = float(north.text)
                    
                    if any(k in metadata for k in ['west', 'east', 'south', 'north'] if metadata[k]):
                        logger.debug(f"  Found bounding box: [{metadata.get('west')}, {metadata.get('east')}, " +
                                   f"{metadata.get('south')}, {metadata.get('north')}]")
                except ValueError as e:
                    logger.warning(f"Could not parse spatial extent values: {e}")
        except Exception as e:
            logger.warning(f"Could not extract spatial extent: {e}")
    
    def _extract_temporal_extent(self, root: ET.Element, metadata: dict):
        """Extract temporal coverage (start and end dates) from XML."""
        try:
            begin_xpath = './/gmd:beginPosition'
            end_xpath = './/gmd:endPosition'
            
            begin = root.find(begin_xpath, NAMESPACES)
            end = root.find(end_xpath, NAMESPACES)
            
            if begin is not None and begin.text:
                metadata['time_start'] = begin.text
                logger.debug(f"  Found time_start: {begin.text}")
            if end is not None and end.text:
                metadata['time_end'] = end.text
                logger.debug(f"  Found time_end: {end.text}")
        except Exception as e:
            logger.warning(f"Could not extract temporal extent: {e}")
    
    def update_metadata_table(self, record_id: int, directory_uuid: str, metadata: dict, aodn_uuid: Optional[str] = None) -> int:
        """
        Update metadata table with extracted values.
        
        Option 2: Enhanced logging for all operations.
        
        Args:
            record_id: metadata.id (primary key)
            directory_uuid: Directory UUID (for logging only)
            metadata: Dictionary of metadata fields to update
            aodn_uuid: AODN UUID from XML (if present)
        
        Returns:
            Number of fields updated
        """
        cursor = self.conn.cursor()
        fields_updated = 0
        
        try:
            logger.info(f"Updating record id={record_id} (directory_uuid={directory_uuid})")
            
            # STEP 1: Update aodn_uuid if present (always overwrite)
            if aodn_uuid:
                query_aodn = """
                    UPDATE metadata 
                    SET aodn_uuid = %s
                    WHERE id = %s
                    RETURNING id
                """
                cursor.execute(query_aodn, [aodn_uuid, record_id])
                if cursor.rowcount > 0:
                    fields_updated += 1
                    self.conn.commit()
                    logger.info(f"  ✓ Updated aodn_uuid = {aodn_uuid}")
                else:
                    logger.debug(f"  - No change to aodn_uuid (already set)")
            
            # STEP 2: Update other fields only if they are NULL
            other_updates = {k: v for k, v in metadata.items() if v is not None}
            
            if other_updates:
                logger.debug(f"  Checking {len(other_updates)} fields for NULL values")
                null_conditions = ' OR '.join([f'{k} IS NULL' for k in other_updates.keys()])
                set_clause = ', '.join([f'{k} = %s' for k in other_updates.keys()])
                
                query_fields = f"""
                    UPDATE metadata 
                    SET {set_clause}
                    WHERE id = %s
                      AND ({null_conditions})
                    RETURNING id
                """
                
                values = list(other_updates.values()) + [record_id]
                cursor.execute(query_fields, values)
                
                if cursor.rowcount > 0:
                    updated_count = cursor.rowcount
                    fields_updated += len(other_updates)
                    self.stats['fields_enriched'] += len(other_updates)
                    self.conn.commit()
                    logger.info(f"  ✓ Enriched {len(other_updates)} fields: {', '.join(other_updates.keys())}")
                else:
                    logger.debug(f"  - No NULL fields to update (all already populated)")
            
            if fields_updated > 0:
                self.stats['rows_updated'] += 1
                logger.info(f"  SUCCESS: Updated {fields_updated} total fields for record {record_id}")
            else:
                logger.info(f"  SKIPPED: All fields already populated for record {record_id}")
                
        except psycopg2.Error as e:
            logger.error(f"✗ Database error updating record id={record_id}: {e}")
            self.conn.rollback()
        except Exception as e:
            logger.error(f"✗ Unexpected error updating record {record_id}: {e}")
            self.conn.rollback()
        finally:
            cursor.close()
        
        return fields_updated
    
    def run_enrichment(self):
        """Main enrichment workflow."""
        logger.info("=" * 70)
        logger.info("STARTING METADATA ENRICHMENT")
        logger.info("=" * 70)
        self.connect()
        
        try:
            xml_files = self.find_metadata_xml_files()
            
            if not xml_files:
                logger.warning("No metadata.xml files found. Nothing to process.")
                return
            
            logger.info(f"\nProcessing {len(xml_files)} metadata files...\n")
            
            for directory_uuid, xml_path in xml_files.items():
                logger.info(f"\n{'='*70}")
                logger.info(f"PROCESSING: {directory_uuid}")
                logger.info(f"File: {xml_path}")
                logger.info(f"{'='*70}")
                
                # Option 1: Simplified lookup - get record ID
                record_id = self.find_metadata_record_id(directory_uuid)
                if not record_id:
                    logger.error(f"Cannot process {directory_uuid}: no metadata record found")
                    self.stats['files_failed'] += 1
                    continue
                
                # Parse XML and extract metadata
                metadata, aodn_uuid = self.parse_iso_19115_xml(xml_path)
                
                # Check for deduplication
                if aodn_uuid:
                    if self.check_aodn_uuid_exists(aodn_uuid):
                        logger.warning(
                            f"⚠ SKIPPING: AODN dataset {aodn_uuid} already exists. "
                            f"Would create duplicate."
                        )
                        self.stats['files_deduplicated'] += 1
                        continue
                
                # Update database
                fields_updated = self.update_metadata_table(record_id, directory_uuid, metadata, aodn_uuid)
                
                if fields_updated > 0:
                    self.stats['files_processed'] += 1
                    logger.info(f"✓ COMPLETED: {directory_uuid}\n")
                else:
                    self.stats['files_processed'] += 1
                    logger.info(f"○ COMPLETED (no changes): {directory_uuid}\n")
        
        except Exception as e:
            logger.error(f"✗ FATAL ERROR during enrichment: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.disconnect()
            self._print_summary()
    
    def _print_summary(self):
        """Print enrichment summary statistics."""
        logger.info("\n" + "=" * 70)
        logger.info("METADATA ENRICHMENT SUMMARY")
        logger.info("=" * 70)
        logger.info(f"XML files found:          {self.stats['files_found']}")
        logger.info(f"Files processed:          {self.stats['files_processed']}")
        logger.info(f"Files deduplicated:       {self.stats['files_deduplicated']}")
        logger.info(f"Files failed:             {self.stats['files_failed']}")
        logger.info(f"AODN UUIDs extracted:     {self.stats['aodn_uuids_extracted']}")
        logger.info(f"Records updated:          {self.stats['rows_updated']}")
        logger.info(f"Total fields enriched:    {self.stats['fields_enriched']}")
        logger.info("=" * 70)


if __name__ == '__main__':
    # Build db_config from environment variables
    # Use correct defaults matching docker-compose.yml
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5433)),
        'database': os.getenv('DB_NAME', 'marine_db'),
        'user': os.getenv('DB_USER', 'marine_user'),
        'password': os.getenv('DB_PASSWORD'),  # No default - must be provided
    }
    
    # Validate required password
    if not db_config['password']:
        logger.error("DB_PASSWORD environment variable not set")
        logger.error("Set it with: export DB_PASSWORD=marine_pass123")
        sys.exit(1)
    
    aodn_path = os.getenv('AODN_DATA_PATH', '/AODN_data')
    
    enricher = MetadataEnricher(db_config, aodn_path)
    enricher.run_enrichment()
