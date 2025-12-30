#!/usr/bin/env python3
"""
Automated metadata extraction and population script for AODN/IMOS datasets.

This script:
1. Scans AODN_data/ directory for datasets
2. Extracts basic metadata from directory structure and filenames
3. Populates the metadata table with UUID, title, paths, and bounding boxes
4. Generates UUIDs for datasets without ISO 19115 XML metadata

Usage:
    python populate_metadata.py
    python populate_metadata.py --force  # Re-process existing datasets
"""

import os
import re
import uuid
import psycopg2
import logging
from pathlib import Path
from datetime import datetime
import argparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Database connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'marine_db',
    'user': 'marine_user',
    'password': 'marine_pass123'
}

# Default bounding box for Tasmania region
DEFAULT_BBOX = {
    'west': 144.0,
    'east': 149.0,
    'south': -44.0,
    'north': -40.0
}

def connect_to_database():
    """
    Connect to PostgreSQL database.
    
    Returns:
        psycopg2.connection: Database connection
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Connected to database successfully")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Database connection failed: {e}")
        raise

def generate_uuid_from_path(dataset_path):
    """
    Generate a deterministic UUID from dataset path.
    
    Args:
        dataset_path: Path to dataset directory
        
    Returns:
        str: UUID string
    """
    # Use UUID5 with a namespace to generate deterministic UUIDs
    namespace = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')  # DNS namespace
    dataset_uuid = uuid.uuid5(namespace, str(dataset_path))
    return str(dataset_uuid)

def extract_bounding_box_from_name(dataset_name):
    """
    Try to extract bounding box hints from dataset name.
    
    Args:
        dataset_name: Dataset directory name
        
    Returns:
        dict: Bounding box or default Tasmania box
    """
    # Check for specific regions in name
    name_lower = dataset_name.lower()
    
    # Huon Estuary / D'Entrecasteaux Channel
    if any(x in name_lower for x in ['huon', 'dentrecasteaux', "d'entrecasteaux"]):
        return {'west': 146.8, 'east': 147.3, 'south': -43.5, 'north': -43.0}
    
    # Storm Bay
    if 'storm bay' in name_lower or 'storm-bay' in name_lower:
        return {'west': 147.0, 'east': 147.8, 'south': -43.5, 'north': -42.8}
    
    # Tasmania-wide
    if 'tasmania' in name_lower or 'tasman' in name_lower:
        return DEFAULT_BBOX
    
    # South-east Tasmania
    if 'south' in name_lower and 'east' in name_lower and 'tasmania' in name_lower:
        return {'west': 147.0, 'east': 148.5, 'south': -43.5, 'north': -42.0}
    
    # Default to Tasmania bounding box
    return DEFAULT_BBOX

def clean_dataset_name(directory_name):
    """
    Clean dataset directory name for use as dataset_name.
    
    Args:
        directory_name: Raw directory name
        
    Returns:
        str: Cleaned name
    """
    # Remove special characters but keep underscores and hyphens
    cleaned = re.sub(r'[^a-zA-Z0-9_\-\s]', '', directory_name)
    # Replace spaces with underscores
    cleaned = re.sub(r'\s+', '_', cleaned)
    # Limit length
    if len(cleaned) > 100:
        cleaned = cleaned[:100]
    return cleaned

def scan_aodn_directory(base_path='AODN_data'):
    """
    Scan AODN_data directory and identify datasets.
    
    Args:
        base_path: Path to AODN_data directory
        
    Returns:
        list: List of dataset dictionaries
    """
    base_path = Path(base_path)
    
    if not base_path.exists():
        logger.error(f"AODN_data directory not found at {base_path}")
        return []
    
    datasets = []
    
    # Iterate through subdirectories
    for dataset_dir in base_path.iterdir():
        if not dataset_dir.is_dir():
            continue
        
        # Skip hidden directories
        if dataset_dir.name.startswith('.'):
            continue
        
        # Extract metadata
        title = dataset_dir.name  # Use directory name as title
        dataset_name = clean_dataset_name(dataset_dir.name)
        dataset_path = str(dataset_dir)
        
        # Generate UUID
        dataset_uuid = generate_uuid_from_path(dataset_path)
        
        # Extract bounding box
        bbox = extract_bounding_box_from_name(dataset_dir.name)
        
        # Count files
        file_count = len(list(dataset_dir.glob('*')))
        
        dataset_info = {
            'uuid': dataset_uuid,
            'title': title,
            'dataset_name': dataset_name,
            'dataset_path': dataset_path,
            'west': bbox['west'],
            'east': bbox['east'],
            'south': bbox['south'],
            'north': bbox['north'],
            'file_count': file_count
        }
        
        datasets.append(dataset_info)
        logger.debug(f"Found dataset: {title} ({file_count} files)")
    
    logger.info(f"Found {len(datasets)} datasets in {base_path}")
    return datasets

def populate_metadata_table(conn, datasets, force=False):
    """
    Populate metadata table with dataset information.
    
    Args:
        conn: Database connection
        datasets: List of dataset dictionaries
        force: If True, update existing records
    """
    cursor = conn.cursor()
    
    # SQL insert/update statement
    if force:
        insert_sql = """
            INSERT INTO metadata 
                (uuid, title, dataset_name, dataset_path, west, east, south, north, extracted_at)
            VALUES 
                (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (uuid) DO UPDATE SET
                title = EXCLUDED.title,
                dataset_name = EXCLUDED.dataset_name,
                dataset_path = EXCLUDED.dataset_path,
                west = EXCLUDED.west,
                east = EXCLUDED.east,
                south = EXCLUDED.south,
                north = EXCLUDED.north,
                extracted_at = EXCLUDED.extracted_at;
        """
    else:
        insert_sql = """
            INSERT INTO metadata 
                (uuid, title, dataset_name, dataset_path, west, east, south, north, extracted_at)
            VALUES 
                (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (uuid) DO NOTHING;
        """
    
    inserted_count = 0
    updated_count = 0
    skipped_count = 0
    
    for dataset in datasets:
        try:
            cursor.execute(insert_sql, (
                dataset['uuid'],
                dataset['title'],
                dataset['dataset_name'],
                dataset['dataset_path'],
                dataset['west'],
                dataset['east'],
                dataset['south'],
                dataset['north'],
                datetime.now()
            ))
            
            if cursor.rowcount > 0:
                if force:
                    updated_count += 1
                    logger.debug(f"Updated: {dataset['title']}")
                else:
                    inserted_count += 1
                    logger.debug(f"Inserted: {dataset['title']}")
            else:
                skipped_count += 1
                logger.debug(f"Skipped (exists): {dataset['title']}")
                
        except psycopg2.Error as e:
            logger.error(f"Failed to process {dataset['title']}: {e}")
            continue
    
    conn.commit()
    
    if force:
        logger.info(f"Updated {updated_count} metadata records")
        logger.info(f"Skipped {inserted_count + skipped_count} existing records")
    else:
        logger.info(f"Inserted {inserted_count} new metadata records")
        logger.info(f"Skipped {skipped_count} existing records")
    
    cursor.close()

def verify_population(conn):
    """
    Verify that metadata was populated correctly.
    
    Args:
        conn: Database connection
    """
    cursor = conn.cursor()
    
    # Count total metadata records
    cursor.execute("SELECT COUNT(*) FROM metadata;")
    total_count = cursor.fetchone()[0]
    
    logger.info(f"\nTotal metadata records in database: {total_count}")
    
    # Show sample records
    cursor.execute("""
        SELECT title, dataset_name, west, east, south, north
        FROM metadata
        ORDER BY title
        LIMIT 10;
    """)
    
    logger.info("\nSample metadata records:")
    for row in cursor.fetchall():
        logger.info(f"  {row[0][:50]:50} | {row[1][:30]:30} | bbox: [{row[2]:.1f}, {row[3]:.1f}, {row[4]:.1f}, {row[5]:.1f}]")
    
    # Check for datasets without measurements
    cursor.execute("""
        SELECT COUNT(*)
        FROM metadata m
        LEFT JOIN measurements meas ON m.id = meas.metadata_id
        WHERE meas.metadata_id IS NULL;
    """)
    
    empty_count = cursor.fetchone()[0]
    logger.info(f"\nDatasets without measurements: {empty_count}")
    
    if empty_count > 0:
        logger.info("\nRun 'python populate_measurements.py' to ingest measurements for these datasets.")
    
    cursor.close()

def main():
    """
    Main execution function.
    """
    parser = argparse.ArgumentParser(
        description='Populate metadata table from AODN_data directory'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Update existing metadata records'
    )
    parser.add_argument(
        '--path',
        default='AODN_data',
        help='Path to AODN_data directory (default: AODN_data)'
    )
    
    args = parser.parse_args()
    
    logger.info("="*60)
    logger.info("Metadata Population Script")
    logger.info("="*60)
    
    try:
        # Scan AODN directory
        datasets = scan_aodn_directory(args.path)
        
        if not datasets:
            logger.warning("No datasets found. Exiting.")
            return
        
        # Connect to database
        conn = connect_to_database()
        
        # Populate metadata table
        populate_metadata_table(conn, datasets, force=args.force)
        
        # Verify population
        verify_population(conn)
        
        # Close connection
        conn.close()
        logger.info("\nMetadata population completed successfully!")
        logger.info("Next step: Run 'python populate_measurements.py' to ingest measurements.")
        
    except Exception as e:
        logger.error(f"Metadata population failed: {e}")
        raise

if __name__ == '__main__':
    main()
