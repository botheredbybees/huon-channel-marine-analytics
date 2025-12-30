#!/usr/bin/env python3
"""
Rebuild UUID mapping by extracting UUIDs from metadata files in the filesystem.

This script:
1. Queries all dataset_path values from the metadata table
2. Looks for metadata.xml files at those filesystem paths
3. Extracts the UUID from each XML file
4. Compares with the database UUID and reports mismatches
5. Creates a mapping table for fixing data without deletion

Usage:
    export DB_HOST=localhost
    export DB_PORT=5433
    export DB_NAME=marine_db
    export DB_USER=marine_user
    export DB_PASSWORD=your_password
    
    python scripts/rebuild_uuid_mapping.py --check-only  # See what would change
    python scripts/rebuild_uuid_mapping.py --apply       # Apply the fixes
"""

import os
import sys
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from collections import defaultdict
import psycopg2
from psycopg2.extras import execute_values
import argparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class UUIDMappingBuilder:
    """Extract real UUIDs from XML files and map to database records."""
    
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5433)),
            'database': os.getenv('DB_NAME', 'marine_db'),
            'user': os.getenv('DB_USER', 'marine_user'),
            'password': os.getenv('DB_PASSWORD'),
        }
        
        if not self.db_config['password']:
            logger.error("DB_PASSWORD environment variable not set")
            sys.exit(1)
        
        self.uuid_from_xml = {}      # Maps filesystem_uuid -> metadata path
        self.database_records = {}    # Maps database_uuid -> id, title, path
        self.mismatches = []          # Records that need correction
    
    def get_database_records(self):
        """Query all metadata records from database."""
        logger.info("Fetching metadata records from database...")
        
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT id, uuid, dataset_path, title 
                FROM metadata 
                ORDER BY uuid
            """)
            
            for record_id, uuid, dataset_path, title in cursor.fetchall():
                if uuid:
                    self.database_records[str(uuid).lower()] = {
                        'id': record_id,
                        'uuid': uuid,
                        'dataset_path': dataset_path,
                        'title': title
                    }
            
            cursor.close()
            conn.close()
            
            logger.info(f"Found {len(self.database_records)} metadata records in database")
            return len(self.database_records)
        
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            sys.exit(1)
    
    def extract_uuid_from_xml(self, xml_path):
        """Extract UUID from metadata.xml file using multiple XPath attempts."""
        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            
            # Try multiple namespace variations
            namespaces = {
                'gmd': 'http://www.opengis.net/gmd',
                'gco': 'http://www.opengis.net/gco',
            }
            
            # Try with full namespace
            xpath_variants = [
                './/{http://www.opengis.net/gmd/gmd}uuid',
                './/{http://www.opengis.net/gmd}uuid',
                './/uuid',  # No namespace
                './/{*}uuid',  # Any namespace
            ]
            
            for xpath in xpath_variants:
                elem = root.find(xpath)
                if elem is not None and elem.text:
                    return elem.text.strip()
                
                # If element found but text is nested in gco:CharacterString
                elem = root.find(xpath)
                if elem is not None:
                    char_string = elem.find('.//{http://www.opengis.net/gco}CharacterString')
                    if char_string is not None and char_string.text:
                        return char_string.text.strip()
                    char_string = elem.find('.//CharacterString')  # No namespace
                    if char_string is not None and char_string.text:
                        return char_string.text.strip()
            
            return None
        
        except Exception as e:
            logger.warning(f"Error parsing {xml_path}: {e}")
            return None
    
    def process_database_paths(self):
        """For each database record with a dataset_path, look for the metadata.xml file."""
        logger.info("\nProcessing database paths to find metadata.xml files...")
        
        found_xml = 0
        missing_xml = 0
        uuids_extracted = 0
        
        for db_uuid, record in self.database_records.items():
            dataset_path = record['dataset_path']
            
            if not dataset_path:
                logger.debug(f"No path for {record['uuid'][:8]}... ({record['title'][:30]}...)")
                missing_xml += 1
                continue
            
            # Try to find metadata.xml
            path_obj = Path(dataset_path)
            
            # Check if path is directory containing metadata.xml
            if path_obj.is_dir():
                xml_path = path_obj / 'metadata.xml'
            else:
                # If path points to metadata.xml directly
                xml_path = path_obj if str(path_obj).endswith('metadata.xml') else path_obj.parent / 'metadata.xml'
            
            if xml_path.exists():
                found_xml += 1
                filesystem_uuid = self.extract_uuid_from_xml(str(xml_path))
                
                if filesystem_uuid:
                    uuids_extracted += 1
                    filesystem_uuid_lower = filesystem_uuid.lower()
                    
                    # Check if UUIDs match
                    if filesystem_uuid_lower != db_uuid:
                        self.mismatches.append({
                            'database_id': record['id'],
                            'database_uuid': record['uuid'],
                            'filesystem_uuid': filesystem_uuid,
                            'dataset_path': dataset_path,
                            'title': record['title']
                        })
            else:
                logger.debug(f"No XML found at {xml_path}")
                missing_xml += 1
        
        logger.info(f"\nResults:")
        logger.info(f"  Metadata records checked: {len(self.database_records)}")
        logger.info(f"  XML files found: {found_xml}")
        logger.info(f"  UUIDs extracted: {uuids_extracted}")
        logger.info(f"  UUID mismatches found: {len(self.mismatches)}")
        logger.info(f"  Missing XML files: {missing_xml}")
    
    def report_mismatches(self):
        """Print detailed report of UUID mismatches."""
        if not self.mismatches:
            logger.info("\n✓ No UUID mismatches found! Database UUIDs match filesystem.")
            return
        
        logger.warning(f"\n⚠ UUID MISMATCHES FOUND ({len(self.mismatches)} records)")
        logger.warning("\nFirst 10 mismatches:")
        logger.warning("=" * 100)
        
        for i, mismatch in enumerate(self.mismatches[:10]):
            logger.warning(f"\n{i+1}. {mismatch['title'][:60]}")
            logger.warning(f"   Database UUID:    {mismatch['database_uuid']}")
            logger.warning(f"   Filesystem UUID:  {mismatch['filesystem_uuid']}")
            logger.warning(f"   Path:             {mismatch['dataset_path'][:80]}")
    
    def create_mapping_table(self, apply_fixes=False):
        """Create or update UUID mapping in database."""
        if not self.mismatches:
            logger.info("No mismatches to map. Skipping mapping table creation.")
            return
        
        logger.info(f"\nPreparing to {'APPLY' if apply_fixes else 'SHOW'} UUID corrections...")
        logger.info(f"This will update {len(self.mismatches)} records")
        
        if apply_fixes:
            try:
                conn = psycopg2.connect(**self.db_config)
                cursor = conn.cursor()
                
                # Create temporary mapping table if it doesn't exist
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS uuid_corrections (
                        id SERIAL PRIMARY KEY,
                        metadata_id INTEGER NOT NULL REFERENCES metadata(id) ON DELETE CASCADE,
                        old_uuid UUID,
                        new_uuid UUID,
                        title TEXT,
                        dataset_path TEXT,
                        corrected_at TIMESTAMP DEFAULT NOW(),
                        UNIQUE(metadata_id)
                    )
                """)
                
                # Insert corrections (as UUIDs, not TEXT)
                values = []
                for mismatch in self.mismatches:
                    values.append((
                        mismatch['database_id'],
                        mismatch['database_uuid'],
                        mismatch['filesystem_uuid'],
                        mismatch['title'],
                        mismatch['dataset_path']
                    ))
                
                execute_values(
                    cursor,
                    """
                    INSERT INTO uuid_corrections 
                    (metadata_id, old_uuid, new_uuid, title, dataset_path)
                    VALUES %s
                    ON CONFLICT (metadata_id) DO UPDATE
                    SET new_uuid = EXCLUDED.new_uuid,
                        corrected_at = NOW()
                    """,
                    values
                )
                
                conn.commit()
                
                # Count total corrections
                cursor.execute("SELECT COUNT(*) FROM uuid_corrections")
                count = cursor.fetchone()[0]
                
                logger.info(f"\n✓ Created uuid_corrections table with {count} records")
                logger.info("\nNext steps:")
                logger.info("  1. Review the corrections:")
                logger.info("     psql -h localhost -p 5433 -U marine_user -d marine_db \\")
                logger.info("       -c 'SELECT * FROM uuid_corrections LIMIT 5;'")
                logger.info("\n  2. To apply corrections, run:")
                logger.info("     python scripts/apply_uuid_corrections.py")
                
                cursor.close()
                conn.close()
            
            except Exception as e:
                logger.error(f"Error creating mapping table: {e}")
                sys.exit(1)
        
        else:
            logger.info(f"\nWould create corrections for {len(self.mismatches)} records")
            logger.info("Run with --apply flag to actually create the mapping table")
    
    def run(self, apply_fixes=False):
        """Execute full workflow."""
        logger.info("=" * 70)
        logger.info("UUID MAPPING BUILDER")
        logger.info("=" * 70)
        
        self.get_database_records()
        self.process_database_paths()
        self.report_mismatches()
        self.create_mapping_table(apply_fixes)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Rebuild UUID mapping from filesystem metadata')
    parser.add_argument('--apply', action='store_true', help='Apply corrections to database')
    parser.add_argument('--check-only', action='store_true', help='Only check, do not apply')
    args = parser.parse_args()
    
    builder = UUIDMappingBuilder()
    builder.run(apply_fixes=args.apply)
