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
    'srv': 'http://www.isotc211.org/2005/srv',
    'mdb': 'http://standards.iso.org/iso/19115/-3/mdb/2.0',
    'cit': 'http://standards.iso.org/iso/19115/-3/cit/2.0',
    'mri': 'http://standards.iso.org/iso/19115/-3/mri/1.0',
    'gex': 'http://standards.iso.org/iso/19115/-3/gex/1.0'
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


def extract_date(element, xpath: str, namespaces: dict) -> Optional[str]:
    """Extract date from XML element"""
    date_text = extract_text(element, xpath, namespaces)
    if date_text:
        # Try to parse and standardize date format
        try:
            # Handle ISO format dates
            if 'T' in date_text:
                date_text = date_text.split('T')[0]
            # Return as-is if valid date format
            return date_text
        except:
            pass
    return None


def extract_bbox(root, namespaces: dict) -> dict:
    """Extract bounding box coordinates"""
    bbox = {}
    
    # Try different XPath patterns for bounding box
    patterns = [
        ('.//gmd:EX_GeographicBoundingBox', 'gmd'),
        ('.//gex:EX_GeographicBoundingBox', 'gex')
    ]
    
    for pattern, ns in patterns:
        bbox_elem = root.find(pattern, namespaces)
        if bbox_elem is not None:
            west = extract_text(bbox_elem, f'.//{ns}:westBoundLongitude/gco:Decimal', namespaces)
            east = extract_text(bbox_elem, f'.//{ns}:eastBoundLongitude/gco:Decimal', namespaces)
            south = extract_text(bbox_elem, f'.//{ns}:southBoundLatitude/gco:Decimal', namespaces)
            north = extract_text(bbox_elem, f'.//{ns}:northBoundLatitude/gco:Decimal', namespaces)
            
            if west: bbox['west'] = west
            if east: bbox['east'] = east
            if south: bbox['south'] = south
            if north: bbox['north'] = north
            
            if bbox:
                break
    
    return bbox


def extract_temporal_extent(root, namespaces: dict) -> dict:
    """Extract temporal extent (time start and end)"""
    temporal = {}
    
    # Try different XPath patterns for temporal extent
    patterns = [
        './/gmd:EX_TemporalExtent',
        './/gex:EX_TemporalExtent'
    ]
    
    for pattern in patterns:
        temp_elem = root.find(pattern, namespaces)
        if temp_elem is not None:
            # Look for begin/end position
            begin = extract_text(temp_elem, './/gml:beginPosition', namespaces)
            end = extract_text(temp_elem, './/gml:endPosition', namespaces)
            
            if begin:
                temporal['time_start'] = begin
            if end:
                temporal['time_end'] = end
            
            if temporal:
                break
    
    return temporal


def extract_metadata_from_xml(xml_file: Path) -> dict:
    """Extract metadata fields from XML file"""
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        
        metadata = {}
        
        # Extract AODN UUID from filename
        uuid_from_file = xml_file.parent.parent.name
        metadata['aodn_uuid'] = uuid_from_file
        
        # === DESCRIPTIVE FIELDS ===
        
        # Abstract
        abstract_xpaths = [
            './/gmd:identificationInfo//gmd:abstract/gco:CharacterString',
            './/mdb:identificationInfo//mri:abstract/gco:CharacterString'
        ]
        for xpath in abstract_xpaths:
            abstract = extract_text(root, xpath, NAMESPACES)
            if abstract:
                metadata['abstract'] = abstract
                break
        
        # Credit
        credit_xpaths = [
            './/gmd:identificationInfo//gmd:credit/gco:CharacterString',
            './/mdb:identificationInfo//mri:credit/gco:CharacterString'
        ]
        for xpath in credit_xpaths:
            credit = extract_text(root, xpath, NAMESPACES)
            if credit:
                metadata['credit'] = credit
                break
        
        # Supplemental information
        supp_xpaths = [
            './/gmd:identificationInfo//gmd:supplementalInformation/gco:CharacterString',
            './/mdb:identificationInfo//mri:supplementalInformation/gco:CharacterString'
        ]
        for xpath in supp_xpaths:
            supp = extract_text(root, xpath, NAMESPACES)
            if supp:
                metadata['supplemental_info'] = supp
                break
        
        # Lineage
        lineage_xpaths = [
            './/gmd:dataQualityInfo//gmd:lineage//gmd:statement/gco:CharacterString',
            './/mdb:dataQualityInfo//gmd:lineage//gmd:statement/gco:CharacterString'
        ]
        for xpath in lineage_xpaths:
            lineage = extract_text(root, xpath, NAMESPACES)
            if lineage:
                metadata['lineage'] = lineage
                break
        
        # Use limitation
        use_lim_xpaths = [
            './/gmd:identificationInfo//gmd:resourceConstraints//gmd:useLimitation/gco:CharacterString',
            './/mdb:identificationInfo//mri:resourceConstraints//gmd:useLimitation/gco:CharacterString'
        ]
        for xpath in use_lim_xpaths:
            use_lim = extract_text(root, xpath, NAMESPACES)
            if use_lim:
                metadata['use_limitation'] = use_lim
                break
        
        # License URL (from otherConstraints)
        license_xpaths = [
            './/gmd:identificationInfo//gmd:resourceConstraints//gmd:otherConstraints/gco:CharacterString',
            './/mdb:identificationInfo//mri:resourceConstraints//gmd:otherConstraints/gco:CharacterString'
        ]
        for xpath in license_xpaths:
            license_text = extract_text(root, xpath, NAMESPACES)
            if license_text and 'http' in license_text:
                metadata['license_url'] = license_text
                break
        
        # === CLASSIFICATION FIELDS ===
        
        # Topic category
        topic_xpaths = [
            './/gmd:identificationInfo//gmd:topicCategory/gmd:MD_TopicCategoryCode',
            './/mdb:identificationInfo//mri:topicCategory/mri:MD_TopicCategoryCode'
        ]
        for xpath in topic_xpaths:
            topic = extract_text(root, xpath, NAMESPACES)
            if topic:
                metadata['topic_category'] = topic
                break
        
        # Language
        lang_xpaths = [
            './/gmd:identificationInfo//gmd:language/gco:CharacterString',
            './/gmd:identificationInfo//gmd:language/gmd:LanguageCode',
            './/mdb:identificationInfo//mri:defaultLocale//gco:CharacterString'
        ]
        for xpath in lang_xpaths:
            lang = extract_text(root, xpath, NAMESPACES)
            if lang:
                metadata['language'] = lang
                break
        
        # Character set
        charset_xpaths = [
            './/gmd:characterSet/gmd:MD_CharacterSetCode',
            './/mdb:metadataScope//gmd:MD_CharacterSetCode'
        ]
        for xpath in charset_xpaths:
            charset = extract_text(root, xpath, NAMESPACES)
            if charset:
                metadata['character_set'] = charset
                break
        
        # Status
        status_xpaths = [
            './/gmd:identificationInfo//gmd:status/gmd:MD_ProgressCode',
            './/mdb:identificationInfo//mri:status/mri:MD_ProgressCode'
        ]
        for xpath in status_xpaths:
            status = extract_text(root, xpath, NAMESPACES)
            if status:
                metadata['status'] = status
                break
        
        # === DATES ===
        
        # Metadata creation date
        creation_xpaths = [
            './/gmd:dateStamp/gco:DateTime',
            './/gmd:dateStamp/gco:Date',
            './/mdb:dateInfo//cit:date/gco:DateTime'
        ]
        for xpath in creation_xpaths:
            creation = extract_date(root, xpath, NAMESPACES)
            if creation:
                metadata['metadata_creation_date'] = creation
                break
        
        # Metadata revision date (look for dateType="revision")
        for date_elem in root.findall('.//gmd:dateInfo//gmd:CI_Date', NAMESPACES):
            date_type = extract_text(date_elem, './/gmd:dateType/gmd:CI_DateTypeCode', NAMESPACES)
            if date_type == 'revision':
                revision = extract_date(date_elem, './/gmd:date/gco:DateTime', NAMESPACES)
                if revision:
                    metadata['metadata_revision_date'] = revision
                    break
        
        # Citation date
        citation_xpaths = [
            './/gmd:identificationInfo//gmd:citation//gmd:date//gco:Date',
            './/gmd:identificationInfo//gmd:citation//gmd:date//gco:DateTime',
            './/mdb:identificationInfo//mri:citation//cit:date//gco:DateTime'
        ]
        for xpath in citation_xpaths:
            citation_date = extract_date(root, xpath, NAMESPACES)
            if citation_date:
                metadata['citation_date'] = citation_date
                break
        
        # === SPATIAL EXTENT ===
        bbox = extract_bbox(root, NAMESPACES)
        metadata.update(bbox)
        
        # === TEMPORAL EXTENT ===
        temporal = extract_temporal_extent(root, NAMESPACES)
        metadata.update(temporal)
        
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
            
            # List of all possible fields to update
            possible_fields = [
                'abstract', 'credit', 'supplemental_info', 'lineage',
                'use_limitation', 'license_url', 'aodn_uuid',
                'topic_category', 'language', 'character_set', 'status',
                'metadata_creation_date', 'metadata_revision_date', 'citation_date',
                'west', 'east', 'south', 'north',
                'time_start', 'time_end'
            ]
            
            for field in possible_fields:
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
                logger.info(f"✓ Updated {len(update_fields)} fields for {dataset_path}")
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
