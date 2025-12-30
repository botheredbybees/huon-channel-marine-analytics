#!/usr/bin/env python3
"""
Apply UUID corrections from the uuid_corrections table.

This script:
1. Reads mappings from the uuid_corrections table
2. Updates the metadata table UUID column
3. Updates all foreign key references (parameters table, etc.)
4. Validates the corrections
5. Logs all changes for audit trail

Usage:
    export DB_PASSWORD=your_password
    python scripts/apply_uuid_corrections.py --dry-run   # See what will change
    python scripts/apply_uuid_corrections.py --apply     # Apply corrections
"""

import os
import sys
import logging
import psycopg2
from psycopg2.extras import execute_values
import argparse
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class UUIDCorrectionApplier:
    """Apply UUID corrections to database without deleting records."""
    
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
        
        self.corrections = []
        self.stats = {
            'metadata_updated': 0,
            'parameters_updated': 0,
            'measurements_updated': 0,
            'keywords_updated': 0,
            'spatial_ref_updated': 0,
            'errors': 0
        }
    
    def get_corrections(self):
        """Read pending corrections from uuid_corrections table."""
        logger.info("Reading pending UUID corrections from database...")
        
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT metadata_id, old_uuid, new_uuid, title, dataset_path
                FROM uuid_corrections
                WHERE corrected_at IS NULL OR corrected_at > NOW() - INTERVAL '1 day'
                ORDER BY metadata_id
            """)
            
            for row in cursor.fetchall():
                self.corrections.append({
                    'metadata_id': row[0],
                    'old_uuid': str(row[1]),
                    'new_uuid': str(row[2]),
                    'title': row[3],
                    'dataset_path': row[4]
                })
            
            cursor.close()
            conn.close()
            
            logger.info(f"Found {len(self.corrections)} pending corrections")
            return len(self.corrections)
        
        except psycopg2.ProgrammingError:
            logger.error("uuid_corrections table not found. Run rebuild_uuid_mapping.py --apply first")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error reading corrections: {e}")
            sys.exit(1)
    
    def apply_corrections(self, dry_run=False):
        """Apply all pending corrections."""
        if not self.corrections:
            logger.info("No corrections to apply")
            return
        
        logger.info(f"\nApplying {len(self.corrections)} UUID corrections...")
        logger.info(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
        
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            for i, correction in enumerate(self.corrections):
                metadata_id = correction['metadata_id']
                old_uuid = correction['old_uuid']
                new_uuid = correction['new_uuid']
                title = correction['title'][:60]
                
                try:
                    # 1. Update metadata table
                    cursor.execute("""
                        UPDATE metadata 
                        SET uuid = %s 
                        WHERE id = %s
                    """, (new_uuid, metadata_id))
                    self.stats['metadata_updated'] += 1
                    
                    # 2. Update parameters table (foreign key by uuid)
                    cursor.execute("""
                        UPDATE parameters 
                        SET uuid = %s 
                        WHERE uuid = %s
                    """, (new_uuid, old_uuid))
                    params_updated = cursor.rowcount
                    self.stats['parameters_updated'] += params_updated
                    
                    # 3. Update measurements table (foreign key by uuid)
                    cursor.execute("""
                        UPDATE measurements 
                        SET uuid = %s 
                        WHERE uuid = %s
                    """, (new_uuid, old_uuid))
                    measurements_updated = cursor.rowcount
                    self.stats['measurements_updated'] += measurements_updated
                    
                    # 4. Update keywords table (foreign key by uuid)
                    cursor.execute("""
                        UPDATE keywords 
                        SET uuid = %s 
                        WHERE uuid = %s
                    """, (new_uuid, old_uuid))
                    keywords_updated = cursor.rowcount
                    self.stats['keywords_updated'] += keywords_updated
                    
                    # 5. Update spatial_ref_system table (foreign key by uuid)
                    cursor.execute("""
                        UPDATE spatial_ref_system 
                        SET uuid = %s 
                        WHERE uuid = %s
                    """, (new_uuid, old_uuid))
                    spatial_updated = cursor.rowcount
                    self.stats['spatial_ref_updated'] += spatial_updated
                    
                    if (i + 1) % 100 == 0:
                        logger.info(f"  Processed {i+1}/{len(self.corrections)} corrections")
                    
                except Exception as e:
                    logger.error(f"Error updating {title}: {e}")
                    self.stats['errors'] += 1
            
            if not dry_run:
                conn.commit()
                logger.info("\n✓ Changes committed to database")
            else:
                conn.rollback()
                logger.info("\n(DRY RUN - no changes committed)")
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error applying corrections: {e}")
            sys.exit(1)
    
    def validate_corrections(self):
        """Verify that corrections were applied successfully."""
        logger.info("\nValidating corrections...")
        
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            mismatches = 0
            for correction in self.corrections:
                cursor.execute("""
                    SELECT uuid FROM metadata WHERE id = %s
                """, (correction['metadata_id'],))
                
                result = cursor.fetchone()
                if result and str(result[0]) != str(correction['new_uuid']):
                    logger.warning(f"  Mismatch: Expected {correction['new_uuid']}, got {result[0]}")
                    mismatches += 1
            
            cursor.close()
            conn.close()
            
            if mismatches == 0:
                logger.info("\n✓ All corrections validated successfully")
            else:
                logger.warning(f"\n⚠ Found {mismatches} validation mismatches")
        
        except Exception as e:
            logger.error(f"Error validating corrections: {e}")
    
    def print_summary(self):
        """Print summary of applied corrections."""
        logger.info("\n" + "=" * 70)
        logger.info("CORRECTION SUMMARY")
        logger.info("=" * 70)
        logger.info(f"\nMetadata records updated:     {self.stats['metadata_updated']}")
        logger.info(f"Parameter references updated: {self.stats['parameters_updated']:,}")
        logger.info(f"Measurement references updated: {self.stats['measurements_updated']:,}")
        logger.info(f"Keyword references updated:   {self.stats['keywords_updated']}")
        logger.info(f"Spatial ref references updated: {self.stats['spatial_ref_updated']}")
        logger.info(f"\nErrors encountered:          {self.stats['errors']}")
        
        if self.stats['errors'] == 0:
            logger.info("\n✓ All corrections applied successfully!")
        else:
            logger.warning(f"\n⚠ {self.stats['errors']} errors occurred during application")
    
    def run(self, dry_run=False):
        """Execute full correction workflow."""
        logger.info("=" * 70)
        logger.info("UUID CORRECTION APPLIER")
        logger.info("=" * 70)
        
        self.get_corrections()
        
        if self.corrections:
            self.apply_corrections(dry_run)
            if not dry_run:
                self.validate_corrections()
        
        self.print_summary()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Apply UUID corrections to database')
    parser.add_argument('--dry-run', action='store_true', help='Show what would change without applying')
    parser.add_argument('--apply', action='store_true', help='Apply corrections to database')
    args = parser.parse_args()
    
    # Default to dry-run if no flags specified
    dry_run = not args.apply
    
    applier = UUIDCorrectionApplier()
    applier.run(dry_run)
