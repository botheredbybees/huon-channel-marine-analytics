#!/usr/bin/env python3
"""
Populate Parameters Table from IMOS Vocabulary

This script populates empty fields in the parameters table with data 
from the imos_vocab_parameters table. It matches records based on the 
parameter URIs and fills in missing standard_name and cf_uri information.

Usage:
    python populate_parameters_from_imos_vocab.py

Database Connection:
    host: localhost
    port: 5433
    database: marine_db
    user: marine_user
    password: marine_pass123
"""

import psycopg2
import psycopg2.extras
import logging
from typing import Dict, Tuple
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_db_connection():
    """Create database connection."""
    return psycopg2.connect(
        host="localhost",
        port=5433,
        database="marine_db",
        user="marine_user",
        password="marine_pass123"
    )


def load_imos_vocab_mapping(conn) -> Dict[str, Tuple[str, str]]:
    """
    Load IMOS vocabulary data into a dictionary keyed by URI.
    
    Returns:
        Dictionary mapping URI -> (cf_standard_name, cf_uri)
    """
    logger.info("Loading IMOS vocabulary mapping...")
    
    query = """
        SELECT uri, cf_standard_name, cf_uri, pref_label
        FROM imos_vocab_parameters
        WHERE cf_standard_name IS NOT NULL OR cf_uri IS NOT NULL
    """
    
    mapping = {}
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(query)
        rows = cur.fetchall()
        
        for row in rows:
            uri = row['uri']
            cf_standard_name = row['cf_standard_name']
            cf_uri = row['cf_uri']
            
            mapping[uri] = (cf_standard_name, cf_uri)
        
        logger.info(f"Loaded {len(mapping)} vocabulary entries")
    
    return mapping


def get_parameters_needing_update(conn):
    """
    Get parameters that have empty standard_name or missing imos_parameter_uri.
    
    Returns:
        List of parameter records
    """
    logger.info("Fetching parameters needing update...")
    
    query = """
        SELECT 
            id,
            parameter_code,
            parameter_label,
            standard_name,
            aodn_parameter_uri,
            imos_parameter_uri,
            unit_uri,
            imos_unit_uri
        FROM parameters
        WHERE 
            standard_name IS NULL 
            OR imos_parameter_uri IS NULL
            OR (aodn_parameter_uri IS NOT NULL AND imos_parameter_uri IS NULL)
        ORDER BY id
    """
    
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(query)
        rows = cur.fetchall()
        logger.info(f"Found {len(rows)} parameters needing update")
        return rows


def update_parameter(conn, param_id: int, updates: Dict[str, str]) -> bool:
    """
    Update a single parameter record with vocabulary data.
    
    Args:
        conn: Database connection
        param_id: Parameter ID to update
        updates: Dictionary of field updates
    
    Returns:
        True if update successful, False otherwise
    """
    if not updates:
        return False
    
    # Build SET clause dynamically
    set_clauses = []
    values = []
    
    for field, value in updates.items():
        set_clauses.append(f"{field} = %s")
        values.append(value)
    
    # Add parameter ID for WHERE clause
    values.append(param_id)
    
    query = f"""
        UPDATE parameters
        SET {', '.join(set_clauses)}
        WHERE id = %s
    """
    
    try:
        with conn.cursor() as cur:
            cur.execute(query, values)
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Error updating parameter {param_id}: {e}")
        conn.rollback()
        return False


def populate_parameters(conn, vocab_mapping: Dict):
    """
    Main function to populate parameter fields from vocabulary data.
    
    Args:
        conn: Database connection
        vocab_mapping: Dictionary mapping URIs to vocabulary data
    """
    logger.info("Starting parameter population...")
    
    parameters = get_parameters_needing_update(conn)
    
    update_count = 0
    skip_count = 0
    error_count = 0
    
    for param in parameters:
        param_id = param['id']
        param_label = param['parameter_label']
        aodn_uri = param['aodn_parameter_uri']
        
        # Skip if no URI to match against
        if not aodn_uri:
            logger.debug(f"Skipping parameter {param_id} ({param_label}) - no AODN URI")
            skip_count += 1
            continue
        
        # Check if URI exists in vocabulary
        if aodn_uri not in vocab_mapping:
            logger.debug(f"No vocab match for parameter {param_id} ({param_label})")
            skip_count += 1
            continue
        
        # Get vocabulary data
        cf_standard_name, cf_uri = vocab_mapping[aodn_uri]
        
        # Prepare updates
        updates = {}
        
        # Update standard_name if empty and vocab has value
        if not param['standard_name'] and cf_standard_name:
            updates['standard_name'] = cf_standard_name
        
        # Set imos_parameter_uri (copy from aodn_parameter_uri if empty)
        if not param['imos_parameter_uri']:
            updates['imos_parameter_uri'] = aodn_uri
        
        # Set imos_unit_uri (copy from unit_uri if available)
        if not param['imos_unit_uri'] and param['unit_uri']:
            updates['imos_unit_uri'] = param['unit_uri']
        
        # Perform update if there are changes
        if updates:
            logger.info(f"Updating parameter {param_id} ({param_label})")
            logger.debug(f"  Updates: {updates}")
            
            if update_parameter(conn, param_id, updates):
                update_count += 1
            else:
                error_count += 1
        else:
            skip_count += 1
    
    # Summary
    logger.info("=" * 60)
    logger.info("Parameter Population Complete")
    logger.info(f"  Parameters updated: {update_count}")
    logger.info(f"  Parameters skipped: {skip_count}")
    logger.info(f"  Errors encountered: {error_count}")
    logger.info("=" * 60)


def verify_updates(conn):
    """Verify the updates by checking remaining NULL values."""
    logger.info("Verifying updates...")
    
    query = """
        SELECT 
            COUNT(*) FILTER (WHERE standard_name IS NULL) as null_standard_name,
            COUNT(*) FILTER (WHERE imos_parameter_uri IS NULL) as null_imos_param_uri,
            COUNT(*) FILTER (WHERE imos_unit_uri IS NULL) as null_imos_unit_uri,
            COUNT(*) as total
        FROM parameters
    """
    
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(query)
        result = cur.fetchone()
        
        logger.info("Current state of parameters table:")
        logger.info(f"  Total parameters: {result['total']}")
        logger.info(f"  NULL standard_name: {result['null_standard_name']}")
        logger.info(f"  NULL imos_parameter_uri: {result['null_imos_param_uri']}")
        logger.info(f"  NULL imos_unit_uri: {result['null_imos_unit_uri']}")


def main():
    """Main execution function."""
    logger.info("Starting IMOS vocabulary parameter population")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    
    try:
        # Connect to database
        conn = get_db_connection()
        logger.info("Database connection established")
        
        # Load vocabulary mapping
        vocab_mapping = load_imos_vocab_mapping(conn)
        
        # Populate parameters
        populate_parameters(conn, vocab_mapping)
        
        # Verify results
        verify_updates(conn)
        
        # Close connection
        conn.close()
        logger.info("Database connection closed")
        
        logger.info("Script completed successfully")
        
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise


if __name__ == "__main__":
    main()
