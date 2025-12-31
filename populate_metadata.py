#!/usr/bin/env python3
"""
Automated metadata extraction and population script for AODN/IMOS datasets.

This script:
1. Scans AODN_data/ directory for datasets
2. Extracts comprehensive metadata from ISO 19115 XML files
3. Populates the metadata table with UUID, title, paths, bbox, and enriched fields
4. Extracts AODN UUID from directory structure (no fallback generation)

Enhanced with:
- AODN UUID extraction from directory structure: [dataset_name]/[aodn_uuid]/metadata/metadata.xml
- Namespace-agnostic XML parsing for ISO 19115-1 and ISO 19115-3
- Extraction of 20+ metadata fields:
  * Descriptive: abstract, credit, supplemental_info, lineage
  * Constraints: use_limitation, license_url
  * Classification: topic_category, language, character_set, status
  * Dates: metadata_creation_date, metadata_revision_date, citation_date
  * Temporal: time_start, time_end
  * Spatial: west, east, south, north (bounding box)
- File-based debug logging to logs/ directory
- Detailed progress tracking with console and file output separation
- Better error handling and reporting
- Normalized date handling for year-only temporal extents
- UUID validation with regex pattern matching

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
import traceback

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


def setup_logging(debug: bool = False) -> logging.Logger:
    """Setup logging with both console and file output."""
    # Create logs directory if it doesn't exist
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    
    # Generate timestamped log filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f'populate_metadata_{timestamp}.log'
    
    # Create logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    
    # Console handler (INFO and above)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter(
        '%(asctime)s - [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_format)
    
    # File handler (DEBUG and above)
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter(
        '%(asctime)s - [%(levelname)s] [%(funcName)s:%(lineno)d] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_format)
    
    # Add handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    logger.info(f"Debug logging to: {log_file}")
    
    return logger


# Will be initialized in main()
logger = None


def normalize_date(date_str: Optional[str]) -> Optional[str]:
    """
    Normalize date string to PostgreSQL DATE format (YYYY-MM-DD).
    
    Handles:
    - Year only: '1984' -> '1984-01-01'
    - Already formatted: '2020-05-15' -> '2020-05-15'
    - With time component: '2020-05-15T12:30:00' -> '2020-05-15'
    
    Args:
        date_str: Input date string or None
    
    Returns:
        Normalized date string or None
    """
    if not date_str or date_str == 'NA':
        return None
    
    # Remove time component if present
    if 'T' in date_str:
        date_str = date_str.split('T')[0]
    
    # If it's just a year (4 digits), pad with -01-01
    if re.match(r'^\d{4}$', date_str):
        return f"{date_str}-01-01"
    
    # Return as-is if already in YYYY-MM-DD format
    return date_str


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


def extract_aodn_uuid_from_path(xml_path: Path) -> Optional[str]:
    """
    Extract AODN UUID from directory structure.
    
    Expected structure: [dataset_name]/[aodn_uuid]/metadata/metadata.xml
    The AODN UUID is the parent directory of the metadata folder.
    
    Args:
        xml_path: Path to metadata.xml file
        
    Returns:
        AODN UUID string or None if not found
    """
    try:
        # metadata.xml -> metadata/ -> [UUID]/ -> [Dataset Name]/
        # So we need to go up 2 levels to get the UUID directory
        uuid_dir = xml_path.parent.parent
        potential_uuid = uuid_dir.name
        
        # Validate that it looks like a UUID (8-4-4-4-12 format)
        uuid_pattern = re.compile(
            r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
            re.IGNORECASE
        )
        
        if uuid_pattern.match(potential_uuid):
            logger.debug(f"  ✓ Extracted AODN UUID from path: {potential_uuid}")
            return potential_uuid
        else:
            logger.debug(f"  ✗ Directory name doesn't match UUID pattern: {potential_uuid}")
            return None
            
    except Exception as e:
        logger.debug(f"  ✗ Error extracting UUID from path: {e}")
        return None


def find_element_by_tag_suffix(root, tag_suffix: str):
    """Find first element whose tag ends with the given suffix (namespace-agnostic)."""
    for elem in root.iter():
        if elem.tag.endswith('}' + tag_suffix) or elem.tag == tag_suffix:
            return elem
    return None


def get_element_text(element) -> Optional[str]:
    """Extract text from element, checking both direct text and gco:CharacterString/gco:Decimal children."""
    if element is None:
        return None
    
    # Try direct text first
    if element.text and element.text.strip():
        return element.text.strip()
    
    # Try gco:CharacterString or gco:Decimal children
    for child in element:
        if child.tag.endswith('}CharacterString') or child.tag.endswith('}Decimal'):
            if child.text and child.text.strip():
                return child.text.strip()
    
    return None


def extract_field_by_path(root, path_components: List[str]) -> Optional[str]:
    """
    Extract field by navigating through path components (namespace-agnostic).
    
    Args:
        root: XML root element
        path_components: List of tag names to traverse (without namespaces)
    
    Returns:
        Text content if found, None otherwise
    """
    current = root
    
    for component in path_components:
        found = False
        for child in current:
            if child.tag.endswith('}' + component) or child.tag == component:
                current = child
                found = True
                break
        if not found:
            return None
    
    return get_element_text(current)


def parse_xml_metadata(xml_path: Path, verbose: bool = False) -> Dict:
    """Parse ISO 19115 XML metadata file with namespace-agnostic element matching."""
    logger.info(f"Parsing XML: {xml_path.name}")
    logger.debug(f"  Full path: {xml_path}")
    
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
        
        logger.debug(f"  Root tag: {root.tag}")
        
        # === CORE IDENTIFIERS ===
        
        # Extract AODN UUID from directory structure (ONLY source for uuid field)
        aodn_uuid = extract_aodn_uuid_from_path(xml_path)
        if aodn_uuid:
            metadata['uuid'] = aodn_uuid
            logger.info(f"  ✓ UUID (AODN from path): {metadata['uuid']}")
        else:
            # No fallback - leave as None
            metadata['uuid'] = None
            logger.warning("  ⚠ No AODN UUID found in directory structure - uuid will be NULL")
        
        # Title - navigate through identificationInfo -> citation -> title
        for base_patterns in [
            ['identificationInfo', 'MD_DataIdentification', 'citation', 'CI_Citation', 'title'],
            ['identificationInfo', 'citation', 'title']
        ]:
            title_text = extract_field_by_path(root, base_patterns)
            if title_text:
                metadata['title'] = title_text
                logger.info(f"  ✓ Title: {metadata['title'][:60]}...")
                break
        
        # === DESCRIPTIVE FIELDS ===
        
        # Abstract
        for pattern in [
            ['identificationInfo', 'MD_DataIdentification', 'abstract'],
            ['identificationInfo', 'abstract']
        ]:
            abstract_text = extract_field_by_path(root, pattern)
            if abstract_text:
                metadata['abstract'] = abstract_text
                if verbose:
                    logger.debug(f"    Abstract: {abstract_text[:80]}...")
                break
        
        # Credit
        for pattern in [
            ['identificationInfo', 'MD_DataIdentification', 'credit'],
            ['identificationInfo', 'credit']
        ]:
            credit_text = extract_field_by_path(root, pattern)
            if credit_text:
                metadata['credit'] = credit_text
                if verbose:
                    logger.debug(f"    Credit: {credit_text[:80]}...")
                break
        
        # Supplemental Information
        for pattern in [
            ['identificationInfo', 'MD_DataIdentification', 'supplementalInformation'],
            ['identificationInfo', 'supplementalInformation']
        ]:
            supp_text = extract_field_by_path(root, pattern)
            if supp_text:
                metadata['supplemental_info'] = supp_text
                if verbose:
                    logger.debug(f"    Supplemental: {supp_text[:80]}...")
                break
        
        # Lineage
        for pattern in [
            ['dataQualityInfo', 'DQ_DataQuality', 'lineage', 'LI_Lineage', 'statement'],
            ['dataQualityInfo', 'lineage', 'statement']
        ]:
            lineage_text = extract_field_by_path(root, pattern)
            if lineage_text:
                metadata['lineage'] = lineage_text
                if verbose:
                    logger.debug(f"    Lineage: {lineage_text[:80]}...")
                break
        
        # === CONSTRAINTS ===
        
        # Use limitation
        for pattern in [
            ['identificationInfo', 'MD_DataIdentification', 'resourceConstraints', 'MD_LegalConstraints', 'useLimitation'],
            ['identificationInfo', 'resourceConstraints', 'useLimitation']
        ]:
            use_lim_text = extract_field_by_path(root, pattern)
            if use_lim_text:
                metadata['use_limitation'] = use_lim_text
                if verbose:
                    logger.debug(f"    Use limitation: {use_lim_text[:80]}...")
                break
        
        # License URL (from otherConstraints)
        for pattern in [
            ['identificationInfo', 'MD_DataIdentification', 'resourceConstraints', 'MD_LegalConstraints', 'otherConstraints'],
            ['identificationInfo', 'resourceConstraints', 'otherConstraints']
        ]:
            license_text = extract_field_by_path(root, pattern)
            if license_text and 'http' in license_text:
                metadata['license_url'] = license_text
                if verbose:
                    logger.debug(f"    License URL: {license_text}")
                break
        
        # === CLASSIFICATION ===
        
        # Topic Category
        topic_elem = find_element_by_tag_suffix(root, 'MD_TopicCategoryCode')
        if topic_elem is not None:
            topic_text = get_element_text(topic_elem)
            if topic_text:
                metadata['topic_category'] = topic_text
                if verbose:
                    logger.debug(f"    Topic: {topic_text}")
        
        # Language
        for pattern in [
            ['identificationInfo', 'MD_DataIdentification', 'language'],
            ['identificationInfo', 'language'],
            ['identificationInfo', 'defaultLocale']
        ]:
            lang_text = extract_field_by_path(root, pattern)
            if lang_text:
                metadata['language'] = lang_text
                if verbose:
                    logger.debug(f"    Language: {lang_text}")
                break
        
        # Character Set
        charset_elem = find_element_by_tag_suffix(root, 'MD_CharacterSetCode')
        if charset_elem is not None:
            charset_text = get_element_text(charset_elem)
            if charset_text:
                metadata['character_set'] = charset_text
                if verbose:
                    logger.debug(f"    Charset: {charset_text}")
        
        # Status
        status_elem = find_element_by_tag_suffix(root, 'MD_ProgressCode')
        if status_elem is not None:
            status_text = get_element_text(status_elem)
            if status_text:
                metadata['status'] = status_text
                if verbose:
                    logger.debug(f"    Status: {status_text}")
        
        # === DATES ===
        
        # Metadata creation date
        for pattern in [
            ['dateStamp'],
            ['dateInfo', 'CI_Date', 'date']
        ]:
            date_text = extract_field_by_path(root, pattern)
            if date_text:
                metadata['metadata_creation_date'] = normalize_date(date_text)
                if verbose:
                    logger.debug(f"    Creation date: {metadata['metadata_creation_date']}")
                break
        
        # Citation date
        for pattern in [
            ['identificationInfo', 'MD_DataIdentification', 'citation', 'CI_Citation', 'date', 'CI_Date', 'date'],
            ['identificationInfo', 'citation', 'date']
        ]:  
            date_text = extract_field_by_path(root, pattern)
            if date_text:
                metadata['citation_date'] = normalize_date(date_text)
                if verbose:
                    logger.debug(f"    Citation date: {metadata['citation_date']}")
                break
        
        # === BOUNDING BOX ===
        
        bbox_elem = find_element_by_tag_suffix(root, 'EX_GeographicBoundingBox')
        if bbox_elem is not None:
            logger.debug("  Found bounding box element")
            
            # Extract coordinates using namespace-agnostic search
            for coord_elem in bbox_elem:
                tag = coord_elem.tag
                value_text = get_element_text(coord_elem)
                
                if value_text:
                    try:
                        if tag.endswith('}westBoundLongitude') or tag == 'westBoundLongitude':
                            metadata['west'] = float(value_text)
                            logger.debug(f"    westBoundLongitude: {value_text}")
                        elif tag.endswith('}eastBoundLongitude') or tag == 'eastBoundLongitude':
                            metadata['east'] = float(value_text)
                            logger.debug(f"    eastBoundLongitude: {value_text}")
                        elif tag.endswith('}southBoundLatitude') or tag == 'southBoundLatitude':
                            metadata['south'] = float(value_text)
                            logger.debug(f"    southBoundLatitude: {value_text}")
                        elif tag.endswith('}northBoundLatitude') or tag == 'northBoundLatitude':
                            metadata['north'] = float(value_text)
                            logger.debug(f"    northBoundLatitude: {value_text}")
                    except ValueError as e:
                        logger.error(f"    Error converting coordinate {tag}: {e}")
            
            if all(metadata.get(c) is not None for c in ['west', 'east', 'south', 'north']):  
                logger.info(f"  ✓ Bounding box: [{metadata['west']:.2f}, {metadata['east']:.2f}, {metadata['south']:.2f}, {metadata['north']:.2f}]")
            else:
                logger.warning(f"  ⚠ Incomplete bounding box: W={metadata.get('west')}, E={metadata.get('east')}, S={metadata.get('south')}, N={metadata.get('north')}")
        
        # === TEMPORAL EXTENT ===
        
        temporal_elem = find_element_by_tag_suffix(root, 'EX_TemporalExtent')
        if temporal_elem is not None:
            logger.debug("  Found temporal extent element")
            
            # Find TimePeriod element
            for elem in temporal_elem.iter():
                if elem.tag.endswith('}TimePeriod'):
                    # Extract begin and end positions
                    for child in elem:
                        text = get_element_text(child)
                        if text:
                            if child.tag.endswith('}beginPosition') or child.tag == 'beginPosition':
                                metadata['time_start'] = normalize_date(text)
                            elif child.tag.endswith('}endPosition') or child.tag == 'endPosition':
                                metadata['time_end'] = normalize_date(text)
                    break
        
        if metadata.get('time_start') or metadata.get('time_end'):
            logger.info(f"  ✓ Temporal extent: {metadata.get('time_start') or 'N/A'} to {metadata.get('time_end') or 'N/A'}")
        
        # === SUMMARY ===
        
        fields_extracted = sum(1 for v in metadata.values() if v is not None)
        logger.info(f"  ✓ XML parsing completed: {fields_extracted} fields extracted")
        
        if not metadata.get('uuid'):
            logger.warning(f"  ⚠ No UUID - record cannot be inserted (missing PRIMARY KEY)")
        
        if not all(metadata.get(c) is not None for c in ['west', 'east', 'south', 'north']):
            logger.warning(f"  ⚠ Incomplete bounding box, will use defaults")
        
    except ET.ParseError as e:
        logger.error(f"  ✗ XML parsing error: {e}")
    except Exception as e:
        logger.error(f"  ✗ Unexpected error: {e}")
        logger.debug(traceback.format_exc())
    
    return metadata


def extract_bounding_box_from_name(dataset_name: str) -> Dict[str, float]:
    """Extract bounding box hints from dataset name."""
    logger.debug(f"Estimating bbox for: {dataset_name}")
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
    logger.info(f"Scanning: {base_path}")
    base_path = Path(base_path)
    
    if not base_path.exists():
        logger.error(f"✗ Directory not found: {base_path}")
        return []
    
    subdirs = [d for d in base_path.iterdir() if d.is_dir() and not d.name.startswith('.')]
    logger.info(f"Found {len(subdirs)} potential datasets")
    
    datasets = []
    skipped_no_uuid = 0
    
    for idx, dataset_dir in enumerate(subdirs, 1):
        logger.info(f"\n[{idx}/{len(subdirs)}] Processing: {dataset_dir.name}")
        
        try:
            # Try to find and parse metadata.xml
            xml_path = find_metadata_xml(dataset_dir)
            
            if xml_path:
                xml_metadata = parse_xml_metadata(xml_path, verbose=verbose)
                
                # Check if we got an AODN UUID
                if not xml_metadata.get('uuid'):
                    logger.error(f"  ✗ Skipping dataset - no AODN UUID found in directory structure")
                    skipped_no_uuid += 1
                    continue
                
                # Use title from XML or fallback to directory name
                title = xml_metadata.get('title') or dataset_dir.name
                xml_metadata['title'] = title
                
                # Use bounding box from XML if complete, otherwise estimate
                if all(xml_metadata.get(c) for c in ['west', 'east', 'south', 'north']):
                    logger.info(f"  Using bounding box from XML")
                else:
                    bbox = extract_bounding_box_from_name(dataset_dir.name)
                    logger.info(f"  Using estimated bounding box")
                    xml_metadata.update(bbox)
                
                # Create dataset info with base fields
                dataset_info = {
                    'dataset_name': clean_dataset_name(dataset_dir.name),
                    'dataset_path': str(dataset_dir),
                }
                
                # Merge all metadata fields
                dataset_info.update(xml_metadata)
            else:
                logger.error(f"  ✗ Skipping dataset - no metadata.xml found")
                skipped_no_uuid += 1
                continue
            
            file_count = sum(1 for _ in dataset_dir.rglob('*') if _.is_file())
            logger.info(f"  File count: {file_count}")
            dataset_info['file_count'] = file_count
            
            # Only add datasets with UUID and title (UUID is now mandatory)
            if dataset_info.get('uuid') and dataset_info.get('title'):
                datasets.append(dataset_info)
                logger.info(f"  ✓ Dataset processed successfully")
            else:
                logger.error(f"  ✗ Skipping dataset - missing required fields (UUID: {dataset_info.get('uuid')}, Title: {dataset_info.get('title')})")
                skipped_no_uuid += 1
        
        except Exception as e:
            logger.error(f"  ✗ Error processing dataset: {e}")
            logger.debug(traceback.format_exc())
            skipped_no_uuid += 1
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Scan complete: {len(datasets)} datasets identified")
    logger.info(f"Skipped (no AODN UUID): {skipped_no_uuid}")
    logger.info(f"{'='*60}")
    
    return datasets


def populate_metadata_table(conn, datasets: List[Dict], force: bool = False):
    """Populate metadata table with all extracted fields."""
    logger.info(f"\nPopulating metadata (mode: {'UPDATE' if force else 'INSERT ONLY'})")
    
    cursor = conn.cursor()
    
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
            # Skip if no UUID (should have been filtered earlier, but double-check)
            if not dataset.get('uuid'):
                logger.error(f"\n[{idx}/{len(datasets)}] ✗ Skipping - no UUID")
                failed += 1
                continue
                
            logger.info(f"\n[{idx}/{len(datasets)}] Inserting: {dataset.get('title', 'Unknown')[:60]}...")
            logger.info(f"  UUID: {dataset.get('uuid')}")
            
            # Prepare values tuple in correct order
            values = tuple([dataset.get(field, None) for field in fields[:-1]] + [datetime.now()])
            
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
                
        except psycopg2.IntegrityError as e:
            failed += 1
            logger.error(f"  ✗ Integrity error (likely NULL uuid): {e}")
        except psycopg2.Error as e:
            failed += 1
            logger.error(f"  ✗ Database error: {e}")
        except Exception as e:
            failed += 1
            logger.error(f"  ✗ Unexpected error: {e}")
            logger.debug(traceback.format_exc())
    
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
    
    # Initialize logger
    global logger
    logger = setup_logging(debug=args.debug)
    
    logger.info("="*60)
    logger.info("METADATA POPULATION SCRIPT (AODN UUID Extraction)")
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
        logger.debug(traceback.format_exc())
        return 1


if __name__ == '__main__':
    exit(main())