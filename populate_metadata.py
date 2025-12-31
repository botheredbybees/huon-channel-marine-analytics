#!/usr/bin/env python3
"""
Automated metadata extraction and population script for AODN/IMOS datasets.

This script:
1. Scans AODN_data/ directory for datasets
2. Extracts comprehensive metadata from ISO 19115 XML files
3. Populates the metadata table with UUID, title, paths, bbox, and enriched fields
4. Generates UUIDs for datasets without ISO 19115 XML metadata

Enhanced with:
- Complete XML metadata parsing (ISO 19115-1 and ISO 19115-3 support)
- Extraction of 20+ metadata fields:
  * Descriptive: abstract, credit, supplemental_info, lineage
  * Constraints: use_limitation, license_url
  * Classification: topic_category, language, character_set, status
  * Dates: metadata_creation_date, metadata_revision_date, citation_date
  * Temporal: time_start, time_end
  * Spatial: west, east, south, north (bounding box)
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

# XML namespace mappings for ISO 19115-1 and ISO 19115-3
XML_NAMESPACES = {
    'gmd': 'http://www.isotc211.org/2005/gmd',
    'gco': 'http://www.isotc211.org/2005/gco',
    'gml': 'http://www.opengis.net/gml',
    'srv': 'http://www.isotc211.org/2005/srv',
    'mdb': 'http://standards.iso.org/iso/19115/-3/mdb/2.0',
    'cit': 'http://standards.iso.org/iso/19115/-3/cit/2.0',
    'mri': 'http://standards.iso.org/iso/19115/-3/mri/1.0',
    'gex': 'http://standards.iso.org/iso/19115/-3/gex/1.0'
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


def extract_text(element, xpath: str, namespaces: dict) -> Optional[str]:
    """Extract text from XML element using XPath."""
    try:
        found = element.find(xpath, namespaces)
        if found is not None and found.text:
            return found.text.strip()
    except Exception as e:
        logger.debug(f"Error extracting {xpath}: {e}")
    return None


def extract_date(root, xpaths: List[str], namespaces: dict, verbose: bool = False) -> Optional[str]:
    """Extract date from XML using multiple XPath patterns."""
    for xpath in xpaths:
        date_text = extract_text(root, xpath, namespaces)
        if date_text:
            # Handle ISO format dates
            if 'T' in date_text:
                date_text = date_text.split('T')[0]
            if verbose:
                logger.debug(f"    Found date: {date_text} (xpath: {xpath})")
            return date_text
    return None


def extract_temporal_extent(root, namespaces: dict, verbose: bool = False) -> Dict[str, Optional[str]]:
    """Extract temporal extent (time start and end)."""
    temporal = {'time_start': None, 'time_end': None}
    
    # Try different XPath patterns for temporal extent
    patterns = [
        './/gmd:EX_TemporalExtent',
        './/gex:EX_TemporalExtent'
    ]
    
    for pattern in patterns:
        temp_elem = root.find(pattern, namespaces)
        if temp_elem is not None:
            begin = extract_text(temp_elem, './/gml:beginPosition', namespaces)
            end = extract_text(temp_elem, './/gml:endPosition', namespaces)
            
            if begin:
                # Handle ISO format dates
                if 'T' in begin:
                    begin = begin.split('T')[0]
                temporal['time_start'] = begin
                if verbose:
                    logger.debug(f"    Found time_start: {begin}")
            
            if end:
                if 'T' in end:
                    end = end.split('T')[0]
                temporal['time_end'] = end
                if verbose:
                    logger.debug(f"    Found time_end: {end}")
            
            if temporal['time_start'] or temporal['time_end']:
                break
    
    return temporal


def parse_xml_metadata(xml_path: Path, verbose: bool = False) -> Dict:
    """Parse ISO 19115 XML metadata file and extract all available fields."""
    logger.info(f"Parsing XML: {xml_path.name}")
    
    metadata = {
        'uuid': None, 'title': None, 'abstract': None, 'credit': None,
        'supplemental_info': None, 'lineage': None, 'use_limitation': None,
        'license_url': None, 'topic_category': None, 'language': None,
        'character_set': None, 'status': None, 'metadata_creation_date': None,
        'metadata_revision_date': None, 'citation_date': None,
        'west': None, 'east': None, 'south': None, 'north': None,
        'time_start': None, 'time_end': None
    }
    
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        if verbose:
            logger.debug(f"  XML root tag: {root.tag}")
        
        # === CORE IDENTIFIERS ===
        
        # Extract UUID
        for xpath in ['./gmd:fileIdentifier/gco:CharacterString', './fileIdentifier/CharacterString']:
            elem = root.find(xpath, XML_NAMESPACES)
            if elem is not None and elem.text:
                metadata['uuid'] = elem.text.strip()
                logger.info(f"  ✓ UUID: {metadata['uuid']}")
                break
        
        # Extract title
        title_xpaths = [
            './/gmd:title/gco:CharacterString',
            './/title/CharacterString',
            './/mdb:identificationInfo//mri:citation//cit:title/gco:CharacterString'
        ]
        for xpath in title_xpaths:
            elem = root.find(xpath, XML_NAMESPACES)
            if elem is not None and elem.text:
                metadata['title'] = elem.text.strip()
                logger.info(f"  ✓ Title: {metadata['title'][:60]}...")
                break
        
        # === DESCRIPTIVE FIELDS ===
        
        if verbose:
            logger.debug("  Extracting descriptive fields...")
        
        # Abstract
        abstract_xpaths = [
            './/gmd:identificationInfo//gmd:abstract/gco:CharacterString',
            './/mdb:identificationInfo//mri:abstract/gco:CharacterString'
        ]
        for xpath in abstract_xpaths:
            elem = root.find(xpath, XML_NAMESPACES)
            if elem is not None and elem.text:
                metadata['abstract'] = elem.text.strip()
                if verbose:
                    logger.debug(f"    Abstract: {metadata['abstract'][:80]}...")
                break
        
        # Credit
        credit_xpaths = [
            './/gmd:identificationInfo//gmd:credit/gco:CharacterString',
            './/mdb:identificationInfo//mri:credit/gco:CharacterString'
        ]
        for xpath in credit_xpaths:
            elem = root.find(xpath, XML_NAMESPACES)
            if elem is not None and elem.text:
                metadata['credit'] = elem.text.strip()
                if verbose:
                    logger.debug(f"    Credit: {metadata['credit'][:80]}...")
                break
        
        # Supplemental information
        supp_xpaths = [
            './/gmd:identificationInfo//gmd:supplementalInformation/gco:CharacterString',
            './/mdb:identificationInfo//mri:supplementalInformation/gco:CharacterString'
        ]
        for xpath in supp_xpaths:
            elem = root.find(xpath, XML_NAMESPACES)
            if elem is not None and elem.text:
                metadata['supplemental_info'] = elem.text.strip()
                if verbose:
                    logger.debug(f"    Supplemental info: {metadata['supplemental_info'][:80]}...")
                break
        
        # Lineage
        lineage_xpaths = [
            './/gmd:dataQualityInfo//gmd:lineage//gmd:statement/gco:CharacterString',
            './/mdb:dataQualityInfo//gmd:lineage//gmd:statement/gco:CharacterString'
        ]
        for xpath in lineage_xpaths:
            elem = root.find(xpath, XML_NAMESPACES)
            if elem is not None and elem.text:
                metadata['lineage'] = elem.text.strip()
                if verbose:
                    logger.debug(f"    Lineage: {metadata['lineage'][:80]}...")
                break
        
        # === CONSTRAINTS ===
        
        if verbose:
            logger.debug("  Extracting constraints...")
        
        # Use limitation
        use_lim_xpaths = [
            './/gmd:identificationInfo//gmd:resourceConstraints//gmd:useLimitation/gco:CharacterString',
            './/mdb:identificationInfo//mri:resourceConstraints//gmd:useLimitation/gco:CharacterString'
        ]
        for xpath in use_lim_xpaths:
            elem = root.find(xpath, XML_NAMESPACES)
            if elem is not None and elem.text:
                metadata['use_limitation'] = elem.text.strip()
                if verbose:
                    logger.debug(f"    Use limitation: {metadata['use_limitation'][:80]}...")
                break
        
        # License URL (from otherConstraints)
        license_xpaths = [
            './/gmd:identificationInfo//gmd:resourceConstraints//gmd:otherConstraints/gco:CharacterString',
            './/mdb:identificationInfo//mri:resourceConstraints//gmd:otherConstraints/gco:CharacterString'
        ]
        for xpath in license_xpaths:
            elem = root.find(xpath, XML_NAMESPACES)
            if elem is not None and elem.text and 'http' in elem.text:
                metadata['license_url'] = elem.text.strip()
                if verbose:
                    logger.debug(f"    License URL: {metadata['license_url']}")
                break
        
        # === CLASSIFICATION FIELDS ===
        
        if verbose:
            logger.debug("  Extracting classification fields...")
        
        # Topic category
        topic_xpaths = [
            './/gmd:identificationInfo//gmd:topicCategory/gmd:MD_TopicCategoryCode',
            './/mdb:identificationInfo//mri:topicCategory/mri:MD_TopicCategoryCode'
        ]
        for xpath in topic_xpaths:
            elem = root.find(xpath, XML_NAMESPACES)
            if elem is not None and elem.text:
                metadata['topic_category'] = elem.text.strip()
                if verbose:
                    logger.debug(f"    Topic category: {metadata['topic_category']}")
                break
        
        # Language
        lang_xpaths = [
            './/gmd:identificationInfo//gmd:language/gco:CharacterString',
            './/gmd:identificationInfo//gmd:language/gmd:LanguageCode',
            './/mdb:identificationInfo//mri:defaultLocale//gco:CharacterString'
        ]
        for xpath in lang_xpaths:
            elem = root.find(xpath, XML_NAMESPACES)
            if elem is not None and elem.text:
                metadata['language'] = elem.text.strip()
                if verbose:
                    logger.debug(f"    Language: {metadata['language']}")
                break
        
        # Character set
        charset_xpaths = [
            './/gmd:characterSet/gmd:MD_CharacterSetCode',
            './/mdb:metadataScope//gmd:MD_CharacterSetCode'
        ]
        for xpath in charset_xpaths:
            elem = root.find(xpath, XML_NAMESPACES)
            if elem is not None and elem.text:
                metadata['character_set'] = elem.text.strip()
                if verbose:
                    logger.debug(f"    Character set: {metadata['character_set']}")
                break
        
        # Status
        status_xpaths = [
            './/gmd:identificationInfo//gmd:status/gmd:MD_ProgressCode',
            './/mdb:identificationInfo//mri:status/mri:MD_ProgressCode'
        ]
        for xpath in status_xpaths:
            elem = root.find(xpath, XML_NAMESPACES)
            if elem is not None and elem.text:
                metadata['status'] = elem.text.strip()
                if verbose:
                    logger.debug(f"    Status: {metadata['status']}")
                break
        
        # === DATES ===
        
        if verbose:
            logger.debug("  Extracting dates...")
        
        # Metadata creation date
        creation_xpaths = [
            './/gmd:dateStamp/gco:DateTime',
            './/gmd:dateStamp/gco:Date',
            './/mdb:dateInfo//cit:date/gco:DateTime'
        ]
        metadata['metadata_creation_date'] = extract_date(root, creation_xpaths, XML_NAMESPACES, verbose)
        
        # Metadata revision date (look for dateType="revision")
        for date_elem in root.findall('.//gmd:dateInfo//gmd:CI_Date', XML_NAMESPACES):
            date_type = extract_text(date_elem, './/gmd:dateType/gmd:CI_DateTypeCode', XML_NAMESPACES)
            if date_type == 'revision':
                revision_xpaths = ['.//gmd:date/gco:DateTime', './/gmd:date/gco:Date']
                metadata['metadata_revision_date'] = extract_date(date_elem, revision_xpaths, XML_NAMESPACES, verbose)
                break
        
        # Citation date
        citation_xpaths = [
            './/gmd:identificationInfo//gmd:citation//gmd:date//gco:Date',
            './/gmd:identificationInfo//gmd:citation//gmd:date//gco:DateTime',
            './/mdb:identificationInfo//mri:citation//cit:date//gco:DateTime'
        ]
        metadata['citation_date'] = extract_date(root, citation_xpaths, XML_NAMESPACES, verbose)
        
        # === SPATIAL EXTENT (Bounding Box) ===
        
        if verbose:
            logger.debug("  Extracting bounding box...")
        
        bbox_patterns = [
            ('.//gmd:EX_GeographicBoundingBox', 'gmd'),
            ('.//gex:EX_GeographicBoundingBox', 'gex')
        ]
        
        for pattern, ns in bbox_patterns:
            bbox_elem = root.find(pattern, XML_NAMESPACES)
            if bbox_elem is not None:
                west = extract_text(bbox_elem, f'.//{ns}:westBoundLongitude/gco:Decimal', XML_NAMESPACES)
                east = extract_text(bbox_elem, f'.//{ns}:eastBoundLongitude/gco:Decimal', XML_NAMESPACES)
                south = extract_text(bbox_elem, f'.//{ns}:southBoundLatitude/gco:Decimal', XML_NAMESPACES)
                north = extract_text(bbox_elem, f'.//{ns}:northBoundLatitude/gco:Decimal', XML_NAMESPACES)
                
                if west:
                    try:
                        metadata['west'] = float(west)
                    except ValueError:
                        pass
                if east:
                    try:
                        metadata['east'] = float(east)
                    except ValueError:
                        pass
                if south:
                    try:
                        metadata['south'] = float(south)
                    except ValueError:
                        pass
                if north:
                    try:
                        metadata['north'] = float(north)
                    except ValueError:
                        pass
                
                if all(metadata.get(c) is not None for c in ['west', 'east', 'south', 'north']):
                    logger.info(f"  ✓ Bounding box: [{metadata['west']:.2f}, {metadata['east']:.2f}, {metadata['south']:.2f}, {metadata['north']:.2f}]")
                    break
        
        if not all(metadata.get(c) is not None for c in ['west', 'east', 'south', 'north']):
            if verbose:
                logger.warning(f"  ⚠ Incomplete bounding box, will use defaults")
        
        # === TEMPORAL EXTENT ===
        
        if verbose:
            logger.debug("  Extracting temporal extent...")
        
        temporal = extract_temporal_extent(root, XML_NAMESPACES, verbose)
        metadata.update(temporal)
        
        if metadata['time_start'] or metadata['time_end']:
            logger.info(f"  ✓ Temporal extent: {metadata['time_start'] or 'N/A'} to {metadata['time_end'] or 'N/A'}")
        
        # === SUMMARY ===
        
        fields_extracted = sum(1 for v in metadata.values() if v is not None)
        logger.info(f"  ✓ XML parsing completed: {fields_extracted} fields extracted")
        
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
    """Scan AODN_data directory with comprehensive XML metadata parsing."""
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
            
            # Use bounding box from XML if complete, otherwise estimate
            if all(xml_metadata.get(c) for c in ['west', 'east', 'south', 'north']):
                bbox = {k: xml_metadata[k] for k in ['west', 'east', 'south', 'north']}
                logger.info(f"  Using bounding box from XML")
            else:
                bbox = extract_bounding_box_from_name(dataset_dir.name)
                logger.info(f"  Using estimated bounding box")
                # Update xml_metadata with estimated bbox
                xml_metadata.update(bbox)
            
            # Merge all metadata fields
            dataset_info = {
                'uuid': dataset_uuid,
                'title': title,
                'dataset_name': clean_dataset_name(dataset_dir.name),
                'dataset_path': str(dataset_dir),
                **xml_metadata  # Include all extracted fields
            }
        else:
            logger.warning(f"  No metadata.xml found, using directory-based metadata")
            dataset_uuid = generate_uuid_from_path(dataset_dir)
            title = dataset_dir.name
            bbox = extract_bounding_box_from_name(dataset_dir.name)
            
            dataset_info = {
                'uuid': dataset_uuid,
                'title': title,
                'dataset_name': clean_dataset_name(dataset_dir.name),
                'dataset_path': str(dataset_dir),
                **bbox
            }
        
        file_count = sum(1 for _ in dataset_dir.rglob('*') if _.is_file())
        logger.info(f"  File count: {file_count}")
        dataset_info['file_count'] = file_count
        
        datasets.append(dataset_info)
        logger.info(f"  ✓ Dataset processed successfully")
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Scan complete: {len(datasets)} datasets identified")
    logger.info(f"{'='*60}")
    
    return datasets


def populate_metadata_table(conn, datasets: List[Dict], force: bool = False):
    """Populate metadata table with all extracted fields."""
    cursor = conn.cursor()
    logger.info(f"\nPopulating metadata (mode: {'UPDATE' if force else 'INSERT ONLY'})")
    
    # Build SQL with all possible fields
    fields = [
        'uuid', 'title', 'dataset_name', 'dataset_path',
        'abstract', 'credit', 'supplemental_info', 'lineage',
        'use_limitation', 'license_url', 'topic_category', 'language',
        'character_set', 'status', 'metadata_creation_date',
        'metadata_revision_date', 'citation_date',
        'west', 'east', 'south', 'north',
        'time_start', 'time_end', 'extracted_at'
    ]
    
    placeholders = ', '.join(['%s'] * len(fields))
    field_names = ', '.join(fields)
    
    if force:
        update_set = ', '.join([f"{field} = EXCLUDED.{field}" for field in fields if field != 'uuid'])
        insert_sql = f"""
            INSERT INTO metadata ({field_names})
            VALUES ({placeholders})
            ON CONFLICT (uuid) DO UPDATE SET {update_set};
        """
    else:
        insert_sql = f"""
            INSERT INTO metadata ({field_names})
            VALUES ({placeholders})
            ON CONFLICT (uuid) DO NOTHING;
        """
    
    inserted, updated, skipped, failed = 0, 0, 0, 0
    
    for idx, dataset in enumerate(datasets, 1):
        try:
            logger.info(f"\n[{idx}/{len(datasets)}] Inserting: {dataset.get('title', 'Unknown')[:60]}...")
            logger.debug(f"  UUID: {dataset.get('uuid', 'None')}")
            
            # Prepare values tuple in correct order, using .get() with None default
            values = tuple(
                dataset.get(field) for field in fields[:-1]  # All fields except 'extracted_at'
            ) + (datetime.now(),)  # Add extracted_at timestamp
            
            cursor.execute(insert_sql, values)
            
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
    """Verify metadata population with field statistics."""
    cursor = conn.cursor()
    logger.info(f"\n{'='*60}")
    logger.info(f"VERIFICATION")
    logger.info(f"{'='*60}")
    
    cursor.execute("SELECT COUNT(*) FROM metadata;")
    total = cursor.fetchone()[0]
    logger.info(f"Total metadata records: {total}")
    
    # Check field population stats
    fields_to_check = [
        'abstract', 'credit', 'lineage', 'topic_category',
        'language', 'status', 'time_start', 'time_end'
    ]
    
    logger.info(f"\nField population statistics:")
    for field in fields_to_check:
        cursor.execute(f"SELECT COUNT(*) FROM metadata WHERE {field} IS NOT NULL;")
        count = cursor.fetchone()[0]
        percentage = (count / total * 100) if total > 0 else 0
        logger.info(f"  {field:25} : {count:3}/{total} ({percentage:5.1f}%)")
    
    cursor.execute("""
        SELECT title, dataset_name, west, east, south, north, 
               CASE WHEN abstract IS NOT NULL THEN '✓' ELSE '✗' END as has_abstract,
               CASE WHEN time_start IS NOT NULL THEN '✓' ELSE '✗' END as has_temporal
        FROM metadata 
        ORDER BY title 
        LIMIT 5;
    """)
    
    logger.info(f"\nSample records:")
    for row in cursor.fetchall():
        logger.info(f"  {row[0][:30]:30} | [{row[2]:.1f}, {row[3]:.1f}, {row[4]:.1f}, {row[5]:.1f}] | Abstract:{row[6]} Temporal:{row[7]}")
    
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
    logger.info("METADATA POPULATION SCRIPT (Enhanced)")
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
        import traceback
        logger.debug(traceback.format_exc())
        return 1


if __name__ == '__main__':
    exit(main())