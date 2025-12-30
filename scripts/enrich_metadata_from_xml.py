#!/usr/bin/env python3
"""
Enrich metadata table from ISO 19115-3 XML files in AODN dataset directories.
Non-destructive: only updates NULL/empty fields.

This script extracts metadata from XML files located in dataset directories
and populates empty fields in the metadata table. It's designed to run
independently of the main ETL pipeline.

Usage:
    python enrich_metadata_from_xml.py

Environment variables:
    DB_HOST: Database host (default: localhost)
    DB_PORT: Database port (default: 5432)
    DB_NAME: Database name (default: marine_data)
    DB_USER: Database user (default: postgres)
    DB_PASSWORD: Database password (default: empty)
    AODN_DATA_PATH: Path to AODN_data directory (default: /AODN_data)
"""

import os
import psycopg2
from pathlib import Path
from xml.etree import ElementTree as ET
from typing import Dict, Optional
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# XML namespace mappings for ISO 19115-3
NAMESPACES = {
    'gmd': 'http://www.isotc211.org/2005/gmd',
    'gco': 'http://www.isotc211.org/2005/gco',
    'gml': 'http://www.opengis.net/gml/3.2.1',
    'srv': 'http://www.isotc211.org/2005/srv',
    'mdb': 'http://www.isotc211.org/2005/mdb',
}


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
            'rows_updated': 0,
        }
        
    def connect(self):
        """Connect to PostgreSQL database."""
        self.conn = psycopg2.connect(**self.db_config)
        logger.info(f"Connected to {self.db_config['database']}")
        
    def disconnect(self):
        """Disconnect from database."""
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from database")
    
    def find_metadata_xml_files(self) -> Dict[str, Path]:
        """Find all metadata.xml files in AODN_data directory."""
        xml_files = {}
        pattern = '**/metadata/metadata.xml'
        
        for xml_file in self.aodn_data_path.glob(pattern):
            # Extract dataset UUID from path: AODN_data/<dataset>/<uuid>/metadata/metadata.xml
            try:
                parts = xml_file.parts
                if len(parts) >= 2:
                    uuid = xml_file.parent.parent.name
                    xml_files[uuid] = xml_file
            except Exception as e:
                logger.warning(f"Could not extract UUID from {xml_file}: {e}")
            
        self.stats['files_found'] = len(xml_files)
        logger.info(f"Found {len(xml_files)} metadata.xml files")
        return xml_files
    
    def parse_iso_19115_xml(self, xml_path: Path) -> Dict[str, any]:
        """Extract metadata fields from ISO 19115-3 XML file."""
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
            
            # Extract abstract
            abstract_xpath = './/gmd:abstract/gco:CharacterString'
            abstract_elem = root.find(abstract_xpath, NAMESPACES)
            if abstract_elem is not None and abstract_elem.text:
                metadata['abstract'] = abstract_elem.text[:1000]  # Limit length
            
            # Extract credit/acknowledgment
            credit_xpath = './/gmd:credit/gco:CharacterString'
            credit_elem = root.find(credit_xpath, NAMESPACES)
            if credit_elem is not None and credit_elem.text:
                metadata['credit'] = credit_elem.text[:500]
            
            # Extract spatial extent (bounding box)
            self._extract_spatial_extent(root, metadata)
            
            # Extract temporal extent (dates)
            self._extract_temporal_extent(root, metadata)
            
            # Extract lineage (processing history)
            lineage_xpath = './/gmd:lineage/gmd:LI_Lineage/gmd:statement/gco:CharacterString'
            lineage_elem = root.find(lineage_xpath, NAMESPACES)
            if lineage_elem is not None and lineage_elem.text:
                metadata['lineage'] = lineage_elem.text[:1000]
            
            # Extract license/constraints
            license_xpath = './/gmd:MD_LegalConstraints/gmd:otherConstraints/gco:CharacterString'
            license_elem = root.find(license_xpath, NAMESPACES)
            if license_elem is not None and license_elem.text:
                metadata['license_url'] = license_elem.text[:500]
            
            return metadata
            
        except ET.ParseError as e:
            logger.error(f"XML parsing error in {xml_path}: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error parsing {xml_path}: {e}")
            return {}
    
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
            if end is not None and end.text:
                metadata['time_end'] = end.text
        except Exception as e:
            logger.warning(f"Could not extract temporal extent: {e}")
    
    def update_metadata_table(self, uuid: str, metadata: dict) -> int:
        """Update metadata table with extracted values. Returns rows updated."""
        # Filter out None values and prepare update
        updates_dict = {k: v for k, v in metadata.items() if v is not None}
        
        if not updates_dict:
            return 0
        
        cursor = self.conn.cursor()
        rows_updated = 0
        
        try:
            # Build WHERE clause to only update NULL fields
            null_conditions = ' OR '.join([f'{k} IS NULL' for k in updates_dict.keys()])
            
            # Build SET clause
            set_clause = ', '.join([f'{k} = %s' for k in updates_dict.keys()])
            
            # Build query
            query = f"""
                UPDATE metadata 
                SET {set_clause}
                WHERE uuid = %s
                  AND ({null_conditions})
                RETURNING id
            """
            
            values = list(updates_dict.values()) + [uuid]
            
            cursor.execute(query, values)
            rows_updated = cursor.rowcount
            self.conn.commit()
            
            if rows_updated > 0:
                logger.info(f"Updated metadata for {uuid}: {rows_updated} rows affected")
                
        except psycopg2.Error as e:
            logger.error(f"Database error updating metadata for {uuid}: {e}")
            self.conn.rollback()
        except Exception as e:
            logger.error(f"Unexpected error updating metadata for {uuid}: {e}")
            self.conn.rollback()
        finally:
            cursor.close()
        
        return rows_updated
    
    def run_enrichment(self):
        """Main enrichment workflow."""
        logger.info("Starting metadata enrichment")
        self.connect()
        
        try:
            xml_files = self.find_metadata_xml_files()
            
            for uuid, xml_path in xml_files.items():
                logger.info(f"Processing {uuid}: {xml_path.name}")
                metadata = self.parse_iso_19115_xml(xml_path)
                
                rows = self.update_metadata_table(uuid, metadata)
                if rows > 0:
                    self.stats['rows_updated'] += rows
                    self.stats['files_processed'] += 1
                else:
                    self.stats['files_processed'] += 1
        
        except Exception as e:
            logger.error(f"Fatal error during enrichment: {e}")
        finally:
            self.disconnect()
            self._print_summary()
    
    def _print_summary(self):
        """Print enrichment summary statistics."""
        logger.info("=" * 60)
        logger.info("METADATA ENRICHMENT SUMMARY")
        logger.info("=" * 60)
        logger.info(f"XML files found:     {self.stats['files_found']}")
        logger.info(f"Files processed:     {self.stats['files_processed']}")
        logger.info(f"Rows updated:        {self.stats['rows_updated']}")
        logger.info("=" * 60)


if __name__ == '__main__':
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'marine_data'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', ''),
    }
    
    aodn_path = os.getenv('AODN_DATA_PATH', '/AODN_data')
    
    enricher = MetadataEnricher(db_config, aodn_path)
    enricher.run_enrichment()
