#!/usr/bin/env python3
"""
Enrich metadata records with details extracted from metadata.xml files.
"""

import os
import sys
import logging
from pathlib import Path
from typing import Optional
import xml.etree.ElementTree as ET
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection parameters
DB_CONFIG = {
    'dbname': os.getenv('POSTGRES_DB', 'marine_db'),
    'user': os.getenv('POSTGRES_USER', 'marine_user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'marine_pass123'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5433')
}

# AODN data directory
BASE_DIR = Path(__file__).parent.parent
AODN_DATA_DIR = BASE_DIR / 'AODN_data'

# XML namespaces
NAMESPACES = {
    'gmd': 'http://www.isotc211.org/2005/gmd',
    'gco': 'http://www.isotc211.org/2005/gco',
    'gml': 'http://www.opengis.net/gml',
    'srv': 'http://www.isotc211.org/2005/srv'
}


def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(**DB_CONFIG)


def find_metadata_files(base_dir: Path) -> list[Path]:
    """Find all metadata.xml files in the AODN data directory"""
    metadata_files = []
    for root, dirs, files in os.walk(base_dir):
        if 'metadata.xml' in files:
            metadata_files.append(Path(root) / 'metadata.xml')
    return metadata_files


def extract_text(element, xpath: str, namespaces: dict) -> Optional[str]:
    """Extract text from XML element using XPath"""
    try:
        found = element.find(xpath, namespaces)
        if found is not None and found.text:
            return found.text.strip()
    except Exception as e:
        logger.debug(f"Error extracting {xpath}: {e}")
    return None


def extract_metadata_from_xml(xml_file: Path) -> dict:
    """Extract metadata fields from XML file"""
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        
        metadata = {}
        
        # Extract AODN UUID from filename
        uuid_from_file = xml_file.parent.parent.name
        metadata['aodn_uuid'] = uuid_from_file
        
        # Extract abstract
        abstract_xpath = './/gmd:identificationInfo//gmd:abstract/gco:CharacterString'
        metadata['abstract'] = extract_text(root, abstract_xpath, NAMESPACES)
        
        # Extract credit
        credit_xpath = './/gmd:identificationInfo//gmd:credit/gco:CharacterString'
        metadata['credit'] = extract_text(root, credit_xpath, NAMESPACES)
        
        # Extract supplemental information
        supp_xpath = './/gmd:identificationInfo//gmd:supplementalInformation/gco:CharacterString'
        metadata['supplemental_info'] = extract_text(root, supp_xpath, NAMESPACES)
        
        # Extract lineage
        lineage_xpath = './/gmd:dataQualityInfo//gmd:lineage//gmd:statement/gco:CharacterString'
        metadata['lineage'] = extract_text(root, lineage_xpath, NAMESPACES)
        
        # Extract use limitation
        use_lim_xpath = './/gmd:identificationInfo//gmd:resourceConstraints//gmd:useLimitation/gco:CharacterString'
        metadata['use_limitation'] = extract_text(root, use_lim_xpath, NAMESPACES)
        
        # Extract license URL
        license_xpath = './/gmd:identificationInfo//gmd:resourceConstraints//gmd:otherConstraints/gco:CharacterString'
        license_text = extract_text(root, license_xpath, NAMESPACES)
        if license_text and 'http' in license_text:
            metadata['license_url'] = license_text
        
        return metadata
        
    except Exception as e:
        logger.error(f"Error parsing {xml_file}: {e}")
        return {}


def make_relative_path(full_path: Path, base_dir: Path) -> str:
    """Convert full path to relative path from base directory"""
    try:
        return str(full_path.relative_to(base_dir))
    except ValueError:
        # If paths don't share a common base, return as-is
        return str(full_path)


def update_metadata_record(conn, dataset_path: str, metadata: dict) -> bool:
    """Update metadata record in database"""
    try:
        with conn.cursor() as cur:
            # Build UPDATE statement dynamically based on available fields
            update_fields = []
            values = []
            
            for field in ['abstract', 'credit', 'supplemental_info', 'lineage', 
                         'use_limitation', 'license_url', 'aodn_uuid']:
                if metadata.get(field):
                    update_fields.append(f"{field} = %s")
                    values.append(metadata[field])
            
            if not update_fields:
                logger.warning(f"No fields to update for {dataset_path}")
                return False
            
            # Add dataset_path to values
            values.append(dataset_path)
            
            query = f"""
                UPDATE metadata 
                SET {', '.join(update_fields)}
                WHERE dataset_path = %s
            """
            
            cur.execute(query, values)
            
            if cur.rowcount > 0:
                logger.info(f"✓ Updated metadata for {dataset_path}")
                return True
            else:
                logger.warning(f"✗ No metadata record found for dataset_path: {dataset_path}")
                return False
                
    except Exception as e:
        logger.error(f"Error updating metadata for {dataset_path}: {e}")
        return False


def main():
    """Main enrichment process"""
    logger.info("=" * 70)
    logger.info("STARTING METADATA ENRICHMENT")
    logger.info("=" * 70)
    
    # Connect to database
    try:
        conn = get_db_connection()
        logger.info(f"Connected to {DB_CONFIG['dbname']} at {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        return 1
    
    # Find all metadata.xml files
    logger.info(f"Scanning for metadata.xml files in {AODN_DATA_DIR}")
    metadata_files = find_metadata_files(AODN_DATA_DIR)
    logger.info(f"Found {len(metadata_files)} metadata.xml files")
    
    if not metadata_files:
        logger.error(f"No metadata.xml files found in {AODN_DATA_DIR}")
        return 1
    
    # Process each metadata file
    logger.info(f"\nProcessing {len(metadata_files)} metadata files...")
    
    success_count = 0
    error_count = 0
    
    for xml_file in metadata_files:
        try:
            logger.info(f"\n{'=' * 70}")
            
            # Extract dataset directory (3 levels up from metadata.xml)
            # metadata.xml -> metadata/ -> UUID/ -> Dataset Name/
            dataset_dir = xml_file.parent.parent.parent
            
            # Make path relative to AODN_data directory
            relative_path = make_relative_path(dataset_dir, BASE_DIR)
            
            logger.info(f"PROCESSING: {relative_path}")
            logger.info(f"File: {xml_file}")
            logger.info("=" * 70)
            
            # Extract metadata from XML
            metadata = extract_metadata_from_xml(xml_file)
            
            if not metadata:
                logger.error(f"Failed to extract metadata from {xml_file}")
                error_count += 1
                continue
            
            # Debug: Show what we extracted
            logger.info(f"Extracted {len([k for k,v in metadata.items() if v])} fields from XML")
            if metadata.get('aodn_uuid'):
                logger.info(f"AODN UUID: {metadata['aodn_uuid']}")
            
            # Update database record
            if update_metadata_record(conn, relative_path, metadata):
                success_count += 1
                conn.commit()
            else:
                error_count += 1
                
        except Exception as e:
            logger.error(f"Error processing {xml_file}: {e}")
            error_count += 1
            continue
    
    # Close database connection
    conn.close()
    
    # Summary
    logger.info(f"\n{'=' * 70}")
    logger.info("ENRICHMENT COMPLETE")
    logger.info("=" * 70)
    logger.info(f"Successfully enriched: {success_count} records")
    logger.info(f"Errors: {error_count} records")
    logger.info(f"Total processed: {len(metadata_files)} files")
    
    return 0 if error_count == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
