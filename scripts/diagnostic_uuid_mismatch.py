#!/usr/bin/env python3
"""
Diagnostic script to identify UUID mismatches between filesystem and database.

Usage:
    export AODN_DATA_PATH=/path/to/AODN_data
    export DB_HOST=localhost
    export DB_PORT=5433
    export DB_NAME=marine_db
    export DB_USER=marine_user
    export DB_PASSWORD=your_password
    
    python scripts/diagnostic_uuid_mismatch.py
"""

import os
import sys
import logging
from pathlib import Path
import psycopg2
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class UUIDMismatchDiagnostic:
    def __init__(self):
        self.aodn_data_path = Path(os.getenv('AODN_DATA_PATH', '/AODN_data'))
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
    
    def get_filesystem_uuids(self):
        """Extract UUIDs from AODN_data directory structure."""
        logger.info(f"Scanning {self.aodn_data_path} for UUIDs...")
        
        filesystem_uuids = {}
        
        # Look for directories that match UUID pattern
        # Pattern: any dir with hyphens at specific positions (8-4-4-4-12)
        for item in self.aodn_data_path.rglob('metadata.xml'):
            # Extract UUID from path
            # Expected: AODN_data/<dataset>/<uuid>/metadata/metadata.xml
            parts = item.parts
            
            # Find 'metadata' in path and get parent dir
            try:
                metadata_idx = parts.index('metadata')
                if metadata_idx > 0:
                    potential_uuid = parts[metadata_idx - 1]
                    
                    # Validate UUID format (rough check)
                    if '-' in str(potential_uuid) and len(str(potential_uuid)) >= 35:
                        filesystem_uuids[str(potential_uuid)] = str(item)
            except (ValueError, IndexError):
                continue
        
        logger.info(f"Found {len(filesystem_uuids)} UUIDs in filesystem")
        return filesystem_uuids
    
    def get_database_uuids(self):
        """Get all UUIDs from database metadata table."""
        logger.info("Fetching UUIDs from database...")
        
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT uuid FROM metadata ORDER BY uuid
            """)
            
            database_uuids = {}
            for (uuid,) in cursor.fetchall():
                if uuid:
                    database_uuids[str(uuid)] = True
            
            cursor.close()
            conn.close()
            
            logger.info(f"Found {len(database_uuids)} UUIDs in database")
            return database_uuids
        
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            sys.exit(1)
    
    def compare_uuids(self, filesystem_uuids, database_uuids):
        """Compare filesystem and database UUIDs."""
        logger.info("\n" + "="*70)
        logger.info("UUID MISMATCH DIAGNOSTIC")
        logger.info("="*70)
        
        # Case-insensitive comparison
        fs_uuids_lower = {uuid.lower(): uuid for uuid in filesystem_uuids.keys()}
        db_uuids_lower = {uuid.lower(): uuid for uuid in database_uuids.keys()}
        
        # Find matches
        matches = set(fs_uuids_lower.keys()) & set(db_uuids_lower.keys())
        fs_only = set(fs_uuids_lower.keys()) - set(db_uuids_lower.keys())
        db_only = set(db_uuids_lower.keys()) - set(fs_uuids_lower.keys())
        
        logger.info(f"\nFilesystem UUIDs: {len(filesystem_uuids)}")
        logger.info(f"Database UUIDs:   {len(database_uuids)}")
        logger.info(f"\nMatches (UUID in both): {len(matches)}")
        logger.info(f"Filesystem only:        {len(fs_only)}")
        logger.info(f"Database only:          {len(db_only)}")
        
        # Show sample matches
        if matches:
            logger.info(f"\n✓ Sample UUIDs found in both (first 3):")
            for uuid in list(matches)[:3]:
                logger.info(f"  {uuid}")
        else:
            logger.warning(f"\n✗ NO UUIDs matched between filesystem and database!")
            logger.warning(f"  This is your UUID MISMATCH problem.")
        
        # Show sample filesystem-only UUIDs
        if fs_only:
            logger.info(f"\n✗ Sample UUIDs in filesystem only (first 5):")
            for uuid in list(fs_only)[:5]:
                logger.info(f"  {uuid}")
                logger.info(f"    Path: {filesystem_uuids[fs_uuids_lower[uuid]]}")
        
        # Show sample database-only UUIDs
        if db_only:
            logger.info(f"\n✗ Sample UUIDs in database only (first 5):")
            for uuid in list(db_only)[:5]:
                logger.info(f"  {uuid}")
        
        # Detailed format comparison
        logger.info(f"\n" + "="*70)
        logger.info("UUID FORMAT ANALYSIS")
        logger.info("="*70)
        
        if filesystem_uuids:
            sample_fs_uuid = list(filesystem_uuids.keys())[0]
            logger.info(f"\nFilesystem UUID sample: {sample_fs_uuid}")
            logger.info(f"  Length: {len(sample_fs_uuid)}")
            logger.info(f"  Case: {'UPPERCASE' if sample_fs_uuid.isupper() else 'lowercase' if sample_fs_uuid.islower() else 'Mixed'}")
        
        if database_uuids:
            sample_db_uuid = list(database_uuids.keys())[0]
            logger.info(f"\nDatabase UUID sample: {sample_db_uuid}")
            logger.info(f"  Length: {len(sample_db_uuid)}")
            logger.info(f"  Case: {'UPPERCASE' if sample_db_uuid.isupper() else 'lowercase' if sample_db_uuid.islower() else 'Mixed'}")
        
        # Recommendations
        logger.info(f"\n" + "="*70)
        logger.info("RECOMMENDATIONS")
        logger.info("="*70)
        
        if len(matches) == 0:
            logger.warning(f"\n⚠ NO MATCHES FOUND - UUID Mismatch Confirmed")
            logger.warning(f"\nThe enrichment script cannot find metadata records because:")
            logger.warning(f"  1. UUIDs in AODN_data directory don't match database UUIDs")
            logger.warning(f"  2. Different UUID format/case between filesystem and database")
            logger.warning(f"  3. AODN_data_path points to wrong directory")
            logger.warning(f"\nNext steps:")
            logger.warning(f"  1. Check AODN_DATA_PATH environment variable is correct")
            logger.warning(f"  2. Verify UUID format in database matches your actual data")
            logger.warning(f"  3. Review enrich_metadata_from_xml.py UUID extraction logic")
            logger.warning(f"  4. Consider normalizing UUIDs (lowercase) in both sources")
        elif len(matches) == len(filesystem_uuids):
            logger.info(f"\n✓ PERFECT MATCH - All filesystem UUIDs are in database")
            logger.info(f"\nNext steps:")
            logger.info(f"  1. Check XML files contain extractable abstract/credit/lineage fields")
            logger.info(f"  2. Run enrich_metadata_from_xml.py with debug logging enabled")
            logger.info(f"  3. Verify database permissions allow UPDATE operations")
        else:
            logger.info(f"\n⚠ PARTIAL MATCH - Some UUIDs matched but not all")
            logger.info(f"\nPossible issues:")
            logger.info(f"  1. Some data directories lack metadata.xml files")
            logger.info(f"  2. Some database records lack corresponding filesystem data")
            logger.info(f"  3. Data is in subdirectories not scanned by enrichment script")
    
    def run(self):
        """Execute diagnostic."""
        filesystem_uuids = self.get_filesystem_uuids()
        database_uuids = self.get_database_uuids()
        self.compare_uuids(filesystem_uuids, database_uuids)


if __name__ == '__main__':
    diagnostic = UUIDMismatchDiagnostic()
    diagnostic.run()
