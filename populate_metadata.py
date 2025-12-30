#!/usr/bin/env python3
"""
Automated metadata extraction and population script for AODN/IMOS datasets.

This script:
1. Scans AODN_data/ directory for datasets
2. Extracts metadata from ISO 19115 XML files when available
3. Populates the metadata table with UUID, title, paths, and bounding boxes
4. Generates UUIDs for datasets without ISO 19115 XML metadata

Enhanced with:
- XML metadata parsing (ISO 19115 support)
- Detailed logging with progress tracking
- Verbose and debug modes
- Better error handling and reporting

Usage:
    python populate_metadata.py
    python populate_metadata.py --force --verbose
    python populate_metadata.py --debug
"""

import os
import re
import uuid
import psycopg2
import logging
from pathlib import Path
from datetime import datetime
import argparse
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional

# Configure detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] [%(funcName)s] %(message)s',
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

# XML namespace mappings for ISO 19115
XML_NAMESPACES = {
    'gmd': 'http://www.isotc211.org/2005/gmd',
    'gco': 'http://www.isotc211.org/2005/gco',
    'gml': 'http://www.opengis.net/gml'
}


def connect_to_database():
    """Connect to PostgreSQL database with detailed logging."""
    try:
        logger.info(f"Connecting to {DB_CONFIG['database']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}")
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("✓ Database connection successful")
        return conn
    except psycopg2.Error as e:
        logger.error(f"✗ Database connection failed: {e}")
        raise


def find_metadata_xml(dataset_dir: Path) -> Optional[Path]:
    """Search for metadata.xml file in dataset directory."""
    logger.debug(f"Searching for metadata.xml in: {dataset_dir.name}")
    
    # Try common locations
    for path in [dataset_dir / 'metadata.xml', dataset_dir / 'METADATA' / 'metadata.xml']:
        if path.exists():
            logger.debug(f"  ✓ Found at: {path.relative_to(dataset_dir)}")
            return path
    
    # Search recursively
    for xml_file in dataset_dir.rglob('metadata.xml'):
        logger.debug(f"  ✓ Found at: {xml_file.relative_to(dataset_dir)}")
        return xml_file
    
    logger.debug(f"  ✗ No metadata.xml found")
    return None


def parse_xml_metadata(xml_path: Path, verbose: bool = False) -> Dict:
    """Parse ISO 19115 XML metadata file."""
    logger.info(f"Parsing XML: {xml_path.name}")
    
    metadata = {'uuid': None, 'title': None, 'west': None, 'east': None, 'south': None, 'north': None}
    
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        if verbose:
            logger.debug(f"  XML root tag: {root.tag}")
        
        # Extract UUID
        for xpath in ['./gmd:fileIdentifier/gco:CharacterString', './fileIdentifier/CharacterString']:
            elem = root.find(xpath, XML_NAMESPACES)
            if elem is not None and elem.text:
                metadata['uuid'] = elem.text.strip()
                logger.info(f"  ✓ UUID: {metadata['uuid']}")
                break
        
        # Extract title
        for xpath in ['.//gmd:title/gco:CharacterString', './/title/CharacterString']:
            elem = root.find(xpath, XML_NAMESPACES)
            if elem is not None and elem.text:
                metadata['title'] = elem.text.strip()
                logger.info(f"  ✓ Title: {metadata['title'][:60]}...")
                break
        
        # Extract bounding box
        bbox_paths = {
            'west': ['.//gmd:westBoundLongitude/gco:Decimal'],
            'east': ['.//gmd:eastBoundLongitude/gco:Decimal'],
            'south': ['.//gmd:southBoundLatitude/gco:Decimal'],
            'north': ['.//gmd:northBoundLatitude/gco:Decimal']
        }
        
        for coord, xpaths in bbox_paths.items():
            for xpath in xpaths:
                elem = root.find(xpath, XML_NAMESPACES)
                if elem is not None and elem.text:
                    try:
                        metadata[coord] = float(elem.text.strip())
                    except ValueError:
                        pass
        
        if all(metadata[c] is not None for c in ['west', 'east', 'south', 'north']):
            logger.info(f"  ✓ Bounding box: [{metadata['west']:.2f}, {metadata['east']:.2f}, {metadata['south']:.2f}, {metadata['north']:.2f}]")
        else:
            if verbose:
                logger.warning(f"  ⚠ Incomplete bounding box, will use defaults")
        
        logger.info(f"  ✓ XML parsing completed")
        
    except ET.ParseError as e:
        logger.error(f"  ✗ XML parsing error: {e}")
    except Exception as e:
        logger.error(f"  ✗ Unexpected error: {e}")
    
    return metadata


def generate_uuid_from_path(dataset_path: Path) -> str:
    """Generate a deterministic UUID from dataset path."""
    namespace = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')
    dataset_uuid = uuid.uuid5(namespace, str(dataset_path))
    logger.debug(f"Generated UUID: {dataset_uuid}")
    return str(dataset_uuid)


def extract_bounding_box_from_name(dataset_name: str) -> Dict[str, float]:
    """Extract bounding box hints from dataset name."""
    name_lower = dataset_name.lower()
    
    if any(x in name_lower for x in ['huon', 'dentrecasteaux', "d'entrecasteaux"]):
        logger.debug(f"  Identified Huon/D'Entrecasteaux region")
        return {'west': 146.8, 'east': 147.3, 'south': -43.5, 'north': -43.0}
    
    if 'storm bay' in name_lower:
        logger.debug(f"  Identified Storm Bay region")
        return {'west': 147.0, 'east': 147.8, 'south': -43.5, 'north': -42.8}
    
    logger.debug(f"  Using default Tasmania bounding box")
    return DEFAULT_BBOX


def clean_dataset_name(directory_name: str) -> str:
    """Clean dataset directory name."""
    cleaned = re.sub(r'[^a-zA-Z0-9_\-\s]', '', directory_name)
    cleaned = re.sub(r'\s+', '_', cleaned)
    return cleaned[:100]


def scan_aodn_directory(base_path: str = 'AODN_data', verbose: bool = False) -> List[Dict]:
    """Scan AODN_data directory with XML metadata parsing."""
    base_path = Path(base_path)
    
    if not base_path.exists():
        logger.error(f"✗ Directory not found: {base_path}")
        return []
    
    logger.info(f"Scanning: {base_path}")
    subdirs = [d for d in base_path.iterdir() if d.is_dir() and not d.name.startswith('.')]
    logger.info(f"Found {len(subdirs)} potential datasets")
    
    datasets = []
    
    for idx, dataset_dir in enumerate(subdirs, 1):
        logger.info(f"\n[{idx}/{len(subdirs)}] Processing: {dataset_dir.name}")
        
        # Try to find and parse metadata.xml
        xml_path = find_metadata_xml(dataset_dir)
        
        if xml_path:
            xml_metadata = parse_xml_metadata(xml_path, verbose=verbose)
            dataset_uuid = xml_metadata.get('uuid') or generate_uuid_from_path(dataset_dir)
            title = xml_metadata.get('title') or dataset_dir.name
            
            if all(xml_metadata.get(c) for c in ['west', 'east', 'south', 'north']):
                bbox = {k: xml_metadata[k] for k in ['west', 'east', 'south', 'north']}
                logger.info(f"  Using bounding box from XML")
            else:
                bbox = extract_bounding_box_from_name(dataset_dir.name)
                logger.info(f"  Using estimated bounding box")
        else:
            logger.warning(f"  No metadata.xml found, using directory-based metadata")
            dataset_uuid = generate_uuid_from_path(dataset_dir)
            title = dataset_dir.name
            bbox = extract_bounding_box_from_name(dataset_dir.name)
        
        file_count = sum(1 for _ in dataset_dir.rglob('*') if _.is_file())
        logger.info(f"  File count: {file_count}")
        
        dataset_info = {
            'uuid': dataset_uuid,
            'title': title,
            'dataset_name': clean_dataset_name(dataset_dir.name),
            'dataset_path': str(dataset_dir),
            'west': bbox['west'],
            'east': bbox['east'],
            'south': bbox['south'],
            'north': bbox['north'],
            'file_count': file_count
        }
        
        datasets.append(dataset_info)
        logger.info(f"  ✓ Dataset processed successfully")
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Scan complete: {len(datasets)} datasets identified")
    logger.info(f"{'='*60}")
    
    return datasets


def populate_metadata_table(conn, datasets: List[Dict], force: bool = False):
    """Populate metadata table with detailed logging."""
    cursor = conn.cursor()
    logger.info(f"\nPopulating metadata (mode: {'UPDATE' if force else 'INSERT ONLY'})")
    
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
    
    inserted, updated, skipped, failed = 0, 0, 0, 0
    
    for idx, dataset in enumerate(datasets, 1):
        try:
            logger.info(f"\n[{idx}/{len(datasets)}] Inserting: {dataset['title'][:60]}...")
            logger.debug(f"  UUID: {dataset['uuid']}")
            
            cursor.execute(insert_sql, (
                dataset['uuid'], dataset['title'], dataset['dataset_name'],
                dataset['dataset_path'], dataset['west'], dataset['east'],
                dataset['south'], dataset['north'], datetime.now()
            ))
            
            if cursor.rowcount > 0:
                if force:
                    updated += 1
                    logger.info(f"  ✓ Updated existing record")
                else:
                    inserted += 1
                    logger.info(f"  ✓ Inserted new record")
            else:
                skipped += 1
                logger.info(f"  ○ Skipped (already exists)")
                
        except psycopg2.Error as e:
            failed += 1
            logger.error(f"  ✗ Failed: {e}")
    
    conn.commit()
    logger.info(f"\n✓ Transaction committed")
    
    logger.info(f"\n{'='*60}")
    logger.info(f"SUMMARY: {'Updated' if force else 'Inserted'}: {updated if force else inserted}, Skipped: {skipped}, Failed: {failed}")
    logger.info(f"{'='*60}")
    
    cursor.close()


def verify_population(conn):
    """Verify metadata population."""
    cursor = conn.cursor()
    logger.info(f"\n{'='*60}")
    logger.info(f"VERIFICATION")
    logger.info(f"{'='*60}")
    
    cursor.execute("SELECT COUNT(*) FROM metadata;")
    total = cursor.fetchone()[0]
    logger.info(f"Total metadata records: {total}")
    
    cursor.execute("SELECT title, dataset_name, west, east, south, north FROM metadata ORDER BY title LIMIT 5;")
    logger.info(f"\nSample records:")
    for row in cursor.fetchall():
        logger.info(f"  {row[0][:40]:40} | [{row[2]:.1f}, {row[3]:.1f}, {row[4]:.1f}, {row[5]:.1f}]")
    
    cursor.close()


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Populate metadata table from AODN_data directory')
    parser.add_argument('--force', action='store_true', help='Update existing records')
    parser.add_argument('--path', default='AODN_data', help='Path to data directory')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose XML parsing logs')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info("="*60)
    logger.info("METADATA POPULATION SCRIPT")
    logger.info("="*60)
    
    try:
        datasets = scan_aodn_directory(args.path, verbose=args.verbose)
        
        if not datasets:
            logger.warning("⚠ No datasets found")
            return 1
        
        conn = connect_to_database()
        populate_metadata_table(conn, datasets, force=args.force)
        verify_population(conn)
        conn.close()
        
        logger.info("\n✓ METADATA POPULATION COMPLETED SUCCESSFULLY")
        return 0
        
    except Exception as e:
        logger.error(f"\n✗ Failed: {e}")
        return 1


if __name__ == '__main__':
    exit(main())
