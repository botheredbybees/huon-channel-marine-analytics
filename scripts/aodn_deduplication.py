#!/usr/bin/env python3
"""
AODN Deduplication Utility Module

Provides reusable functions for checking AODN UUID existence and managing
deduplication logic across ETL scripts.

Usage:
    from aodn_deduplication import AODNDeduplicator
    
    dedup = AODNDeduplicator(db_connection)
    if dedup.aodn_uuid_exists(aodn_uuid):
        logger.info(f"Dataset {aodn_uuid} already ingested. Skipping.")
        dedup.log_skip(aodn_uuid, reason="duplicate")
    else:
        # Process dataset
        dedup.log_ingest(aodn_uuid)
"""

import psycopg2
import logging
from typing import Optional, List, Dict
from datetime import datetime

logger = logging.getLogger(__name__)


class AODNDeduplicator:
    """
    Manages deduplication of AODN datasets using the aodn_uuid field.
    
    Provides methods to:
    - Check if AODN UUID already exists in database
    - Log deduplication events for audit trail
    - Batch check multiple UUIDs
    - Generate deduplication reports
    """
    
    def __init__(self, db_connection):
        """
        Initialize deduplicator with database connection.
        
        Args:
            db_connection: Active psycopg2 database connection
        """
        self.conn = db_connection
        self.stats = {
            'checks_performed': 0,
            'duplicates_found': 0,
            'new_uuids': 0,
            'errors': 0,
        }
    
    def aodn_uuid_exists(self, aodn_uuid: str) -> bool:
        """
        Check if AODN UUID already exists in metadata table.
        
        Args:
            aodn_uuid: AODN UUID to check
            
        Returns:
            True if UUID exists (skip processing)
            False if UUID is new (process normally)
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                "SELECT id FROM metadata WHERE aodn_uuid = %s LIMIT 1",
                [aodn_uuid]
            )
            result = cursor.fetchone()
            self.stats['checks_performed'] += 1
            
            if result is not None:
                self.stats['duplicates_found'] += 1
                return True
            else:
                self.stats['new_uuids'] += 1
                return False
                
        except psycopg2.Error as e:
            logger.error(f"Database error checking AODN UUID {aodn_uuid}: {e}")
            self.stats['errors'] += 1
            return False
        finally:
            cursor.close()
    
    def batch_check_aodn_uuids(self, aodn_uuids: List[str]) -> Dict[str, bool]:
        """
        Check multiple AODN UUIDs in a single database query.
        
        More efficient than individual checks for large batches.
        
        Args:
            aodn_uuids: List of AODN UUIDs to check
            
        Returns:
            Dictionary mapping UUID -> exists (True/False)
        """
        if not aodn_uuids:
            return {}
        
        cursor = self.conn.cursor()
        results = {uuid: False for uuid in aodn_uuids}  # Default to not existing
        
        try:
            # Use ANY operator for efficient batch checking
            placeholders = ','.join(['%s'] * len(aodn_uuids))
            query = f"SELECT DISTINCT aodn_uuid FROM metadata WHERE aodn_uuid = ANY(ARRAY[{placeholders}])"
            
            cursor.execute(query, aodn_uuids)
            existing = {row[0] for row in cursor.fetchall()}
            
            # Mark existing UUIDs
            for uuid in aodn_uuids:
                if uuid in existing:
                    results[uuid] = True
            
            self.stats['checks_performed'] += len(aodn_uuids)
            self.stats['duplicates_found'] += len(existing)
            self.stats['new_uuids'] += len(aodn_uuids) - len(existing)
            
            return results
            
        except psycopg2.Error as e:
            logger.error(f"Database error in batch check: {e}")
            self.stats['errors'] += 1
            return results
        finally:
            cursor.close()
    
    def log_skip(self, aodn_uuid: str, reason: str = "duplicate", details: Optional[str] = None):
        """
        Log a skipped AODN UUID for audit trail.
        
        Creates an entry in a deduplication log for compliance/audit.
        This is optional and depends on having a dedup_log table.
        
        Args:
            aodn_uuid: AODN UUID that was skipped
            reason: Reason for skipping ("duplicate", "invalid", etc.)
            details: Optional additional details
        """
        cursor = self.conn.cursor()
        try:
            # Check if dedup_log table exists first
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='dedup_log')"
            )
            table_exists = cursor.fetchone()[0]
            
            if not table_exists:
                logger.debug("dedup_log table not found. Skipping audit log.")
                return
            
            cursor.execute(
                """
                INSERT INTO dedup_log (aodn_uuid, skip_reason, details, logged_at)
                VALUES (%s, %s, %s, %s)
                """,
                [aodn_uuid, reason, details, datetime.utcnow()]
            )
            self.conn.commit()
            logger.info(f"Logged deduplication skip for {aodn_uuid}: {reason}")
            
        except psycopg2.Error as e:
            logger.warning(f"Could not log deduplication skip: {e}")
            self.conn.rollback()
        finally:
            cursor.close()
    
    def get_aodn_uuid_for_internal_uuid(self, internal_uuid: str) -> Optional[str]:
        """
        Look up AODN UUID using internal UUID.
        
        Useful for reconciling datasets between internal and AODN systems.
        
        Args:
            internal_uuid: Internal system UUID
            
        Returns:
            AODN UUID if present, None otherwise
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                "SELECT aodn_uuid FROM metadata WHERE uuid = %s",
                [internal_uuid]
            )
            result = cursor.fetchone()
            return result[0] if result else None
            
        except psycopg2.Error as e:
            logger.error(f"Database error looking up AODN UUID: {e}")
            return None
        finally:
            cursor.close()
    
    def get_duplicate_aodn_datasets(self) -> List[Dict[str, any]]:
        """
        Find any duplicate AODN UUIDs in the database.
        
        Returns list of duplicate entries for investigation.
        Should normally return empty list if deduplication is working.
        
        Returns:
            List of dicts with aodn_uuid and count of occurrences
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                """
                SELECT aodn_uuid, COUNT(*) as occurrence_count
                FROM metadata
                WHERE aodn_uuid IS NOT NULL
                GROUP BY aodn_uuid
                HAVING COUNT(*) > 1
                ORDER BY occurrence_count DESC
                """
            )
            results = []
            for row in cursor.fetchall():
                results.append({
                    'aodn_uuid': row[0],
                    'occurrence_count': row[1]
                })
            return results
            
        except psycopg2.Error as e:
            logger.error(f"Database error checking duplicates: {e}")
            return []
        finally:
            cursor.close()
    
    def print_stats(self):
        """
        Print deduplication statistics.
        """
        logger.info("=" * 60)
        logger.info("AODN DEDUPLICATION STATISTICS")
        logger.info("=" * 60)
        logger.info(f"Checks performed:      {self.stats['checks_performed']}")
        logger.info(f"Duplicates found:      {self.stats['duplicates_found']}")
        logger.info(f"New UUIDs:             {self.stats['new_uuids']}")
        logger.info(f"Errors:                {self.stats['errors']}")
        logger.info("=" * 60)
    
    def reset_stats(self):
        """
        Reset statistics counters.
        """
        self.stats = {
            'checks_performed': 0,
            'duplicates_found': 0,
            'new_uuids': 0,
            'errors': 0,
        }


def create_dedup_log_table(db_connection):
    """
    Create dedup_log table for audit trail (optional).
    
    Call this once during database initialization if you want to enable
    deduplication logging and audit trail.
    
    Args:
        db_connection: Active psycopg2 database connection
    """
    cursor = db_connection.cursor()
    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS dedup_log (
                id SERIAL PRIMARY KEY,
                aodn_uuid TEXT NOT NULL,
                skip_reason TEXT,
                details TEXT,
                logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_dedup_log_aodn_uuid 
            ON dedup_log(aodn_uuid);
            
            CREATE INDEX IF NOT EXISTS idx_dedup_log_timestamp 
            ON dedup_log(logged_at);
            """
        )
        db_connection.commit()
        logger.info("dedup_log table created successfully")
        
    except psycopg2.Error as e:
        logger.error(f"Error creating dedup_log table: {e}")
        db_connection.rollback()
    finally:
        cursor.close()
