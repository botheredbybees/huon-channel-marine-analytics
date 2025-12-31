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
import traceback

# Configure detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] [%(funcName)s:%(lineno)d] %(message)s',
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
        logger.debug(f"    Attempting to extract text from xpath: {xpath}")
        logger.debug(f"    Element type: {type(element)}, Element: {element}")
        
        if element is None:
            logger.debug(f"    Element is None, cannot extract")
            return None
            
        found = element.find(xpath, namespaces)
        logger.debug(f"    find() returned: {type(found)}")
        
        if found is not None:
            logger.debug(f"    found.text type: {type(found.text) if hasattr(found, 'text') else 'no text attr'}")
            if hasattr(found, 'text') and found.text:
                result = found.text.strip()
                logger.debug(f"    Extracted text: {result[:50] if len(result) > 50 else result}")
                return result
        logger.debug(f"    No text found at xpath: {xpath}")
    except Exception as e:
        logger.error(f"    Error extracting {xpath}: {e}")
        logger.error(f"    Stack trace: {traceback.format_exc()}")
    return None


def extract_date(root, xpaths: List[str], namespaces: dict, verbose: bool = False) -> Optional[str]:
    """Extract date from XML using multiple XPath patterns."""
    logger.debug(f"  extract_date called with {len(xpaths)} xpath patterns")
    logger.debug(f"  root type: {type(root)}, root: {root}")
    
    for idx, xpath in enumerate(xpaths):
        logger.debug(f"  Trying xpath {idx+1}/{len(xpaths)}: {xpath}")
        try:
            date_text = extract_text(root, xpath, namespaces)
            if date_text:
                # Handle ISO format dates
                if 'T' in date_text:
                    date_text = date_text.split('T')[0]
                if verbose:
                    logger.debug(f"    Found date: {date_text} (xpath: {xpath})")
                return date_text
        except Exception as e:
            logger.error(f"  Exception in extract_date for xpath {xpath}: {e}")
            logger.error(f"  Stack trace: {traceback.format_exc()}")
    return None


def extract_temporal_extent(root, namespaces: dict, verbose: bool = False) -> Dict[str, Optional[str]]:
    """Extract temporal extent (time start and end)."""
    logger.debug(f"extract_temporal_extent called")
    logger.debug(f"  root type: {type(root)}")
    
    temporal = {'time_start': None, 'time_end': None}
    
    # Try different XPath patterns for temporal extent
    patterns = [
        './/gmd:EX_TemporalExtent',
        './/gex:EX_TemporalExtent'
    ]
    
    for idx, pattern in enumerate(patterns):
        logger.debug(f"  Trying temporal pattern {idx+1}/{len(patterns)}: {pattern}")
        try:
            temp_elem = root.find(pattern, namespaces)
            logger.debug(f"  temp_elem type: {type(temp_elem)}")
            
            if temp_elem is not None:
                logger.debug(f"  Found temporal element, extracting begin/end positions")
                begin = extract_text(temp_elem, './/gml:beginPosition', namespaces)
                end = extract_text(temp_elem, './/gml:endPosition', namespaces)
                
                logger.debug(f"  begin result: {begin}")
                logger.debug(f"  end result: {end}")
                
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
        except Exception as e:
            logger.error(f"  Exception in extract_temporal_extent for pattern {pattern}: {e}")
            logger.error(f"  Stack trace: {traceback.format_exc()}")
    
    logger.debug(f"  Returning temporal: {temporal}")
    return temporal


def parse_xml_metadata(xml_path: Path, verbose: bool = False) -> Dict:
    """Parse ISO 19115 XML metadata file and extract all available fields."""
    logger.info(f"parse_xml_metadata called for: {xml_path.name}")
    logger.debug(f"  xml_path type: {type(xml_path)}, full path: {xml_path}")
    
    metadata = {
        'uuid': None, 'title': None, 'abstract': None, 'credit': None,
        'supplemental_info': None, 'lineage': None, 'use_limitation': None,
        'license_url': None, 'topic_category': None, 'language': None,
        'character_set': None, 'status': None, 'metadata_creation_date': None,
        'metadata_revision_date': None, 'citation_date': None,
        'west': None, 'east': None, 'south': None, 'north': None,
        'time_start': None, 'time_end': None
    }
    
    logger.debug(f"  Initial metadata dict created with {len(metadata)} keys")
    
    try:
        logger.debug(f"  Attempting to parse XML file...")
        tree = ET.parse(xml_path)
        logger.debug(f"  XML parsed successfully, tree type: {type(tree)}")
        
        root = tree.getroot()
        logger.debug(f"  Got root element, type: {type(root)}, tag: {root.tag if hasattr(root, 'tag') else 'no tag'}")
        
        if verbose:
            logger.debug(f"  XML root tag: {root.tag}")
        
        # === CORE IDENTIFIERS ===
        logger.debug("  === Extracting CORE IDENTIFIERS ===")
        
        # Extract UUID
        logger.debug("  Extracting UUID...")
        for xpath in ['./gmd:fileIdentifier/gco:CharacterString', './fileIdentifier/CharacterString']:
            logger.debug(f"    Trying UUID xpath: {xpath}")
            try:
                elem = root.find(xpath, XML_NAMESPACES)
                logger.debug(f"    elem type: {type(elem)}, elem: {elem}")
                if elem is not None and hasattr(elem, 'text') and elem.text:
                    metadata['uuid'] = elem.text.strip()
                    logger.info(f"  ✓ UUID: {metadata['uuid']}")
                    break
            except Exception as e:
                logger.error(f"    Exception extracting UUID: {e}")
                logger.error(f"    Stack trace: {traceback.format_exc()}")
        
        # Extract title
        logger.debug("  Extracting title...")
        title_xpaths = [
            './/gmd:title/gco:CharacterString',
            './/title/CharacterString',
            './/mdb:identificationInfo//mri:citation//cit:title/gco:CharacterString'
        ]
        for idx, xpath in enumerate(title_xpaths):
            logger.debug(f"    Trying title xpath {idx+1}/{len(title_xpaths)}: {xpath}")
            try:
                elem = root.find(xpath, XML_NAMESPACES)
                logger.debug(f"    elem type: {type(elem)}")
                if elem is not None and hasattr(elem, 'text') and elem.text:
                    metadata['title'] = elem.text.strip()
                    logger.info(f"  ✓ Title: {metadata['title'][:60]}...")
                    break
            except Exception as e:
                logger.error(f"    Exception extracting title: {e}")
                logger.error(f"    Stack trace: {traceback.format_exc()}")
        
        # === DESCRIPTIVE FIELDS ===
        logger.debug("  === Extracting DESCRIPTIVE FIELDS ===")
        
        if verbose:
            logger.debug("  Extracting descriptive fields...")
        
        # Abstract
        logger.debug("  Extracting abstract...")
        abstract_xpaths = [
            './/gmd:identificationInfo//gmd:abstract/gco:CharacterString',
            './/mdb:identificationInfo//mri:abstract/gco:CharacterString'
        ]
        for xpath in abstract_xpaths:
            try:
                elem = root.find(xpath, XML_NAMESPACES)
                if elem is not None and hasattr(elem, 'text') and elem.text:
                    metadata['abstract'] = elem.text.strip()
                    if verbose:
                        logger.debug(f"    Abstract: {metadata['abstract'][:80]}...")
                    break
            except Exception as e:
                logger.error(f"    Exception extracting abstract: {e}")
        
        # Credit
        logger.debug("  Extracting credit...")
        credit_xpaths = [
            './/gmd:identificationInfo//gmd:credit/gco:CharacterString',
            './/mdb:identificationInfo//mri:credit/gco:CharacterString'
        ]
        for xpath in credit_xpaths:
            try:
                elem = root.find(xpath, XML_NAMESPACES)
                if elem is not None and hasattr(elem, 'text') and elem.text:
                    metadata['credit'] = elem.text.strip()
                    if verbose:
                        logger.debug(f"    Credit: {metadata['credit'][:80]}...")
                    break
            except Exception as e:
                logger.error(f"    Exception extracting credit: {e}")
        
        # Supplemental information
        logger.debug("  Extracting supplemental info...")
        supp_xpaths = [
            './/gmd:identificationInfo//gmd:supplementalInformation/gco:CharacterString',
            './/mdb:identificationInfo//mri:supplementalInformation/gco:CharacterString'
        ]
        for xpath in supp_xpaths:
            try:
                elem = root.find(xpath, XML_NAMESPACES)
                if elem is not None and hasattr(elem, 'text') and elem.text:
                    metadata['supplemental_info'] = elem.text.strip()
                    if verbose:
                        logger.debug(f"    Supplemental info: {metadata['supplemental_info'][:80]}...")
                    break
            except Exception as e:
                logger.error(f"    Exception extracting supplemental_info: {e}")
        
        # Lineage
        logger.debug("  Extracting lineage...")
        lineage_xpaths = [
            './/gmd:dataQualityInfo//gmd:lineage//gmd:statement/gco:CharacterString',
            './/mdb:dataQualityInfo//gmd:lineage//gmd:statement/gco:CharacterString'
        ]
        for xpath in lineage_xpaths:
            try:
                elem = root.find(xpath, XML_NAMESPACES)
                if elem is not None and hasattr(elem, 'text') and elem.text:
                    metadata['lineage'] = elem.text.strip()
                    if verbose:
                        logger.debug(f"    Lineage: {metadata['lineage'][:80]}...")
                    break
            except Exception as e:
                logger.error(f"    Exception extracting lineage: {e}")
        
        # === CONSTRAINTS ===
        logger.debug("  === Extracting CONSTRAINTS ===")
        
        if verbose:
            logger.debug("  Extracting constraints...")
        
        # Use limitation
        logger.debug("  Extracting use limitation...")
        use_lim_xpaths = [
            './/gmd:identificationInfo//gmd:resourceConstraints//gmd:useLimitation/gco:CharacterString',
            './/mdb:identificationInfo//mri:resourceConstraints//gmd:useLimitation/gco:CharacterString'
        ]
        for xpath in use_lim_xpaths:
            try:
                elem = root.find(xpath, XML_NAMESPACES)
                if elem is not None and hasattr(elem, 'text') and elem.text:
                    metadata['use_limitation'] = elem.text.strip()
                    if verbose:
                        logger.debug(f"    Use limitation: {metadata['use_limitation'][:80]}...")
                    break
            except Exception as e:
                logger.error(f"    Exception extracting use_limitation: {e}")
        
        # License URL (from otherConstraints)
        logger.debug("  Extracting license URL...")
        license_xpaths = [
            './/gmd:identificationInfo//gmd:resourceConstraints//gmd:otherConstraints/gco:CharacterString',
            './/mdb:identificationInfo//mri:resourceConstraints//gmd:otherConstraints/gco:CharacterString'
        ]
        for xpath in license_xpaths:
            try:
                elem = root.find(xpath, XML_NAMESPACES)
                if elem is not None and hasattr(elem, 'text') and elem.text and 'http' in elem.text:
                    metadata['license_url'] = elem.text.strip()
                    if verbose:
                        logger.debug(f"    License URL: {metadata['license_url']}")
                    break
            except Exception as e:
                logger.error(f"    Exception extracting license_url: {e}")
        
        # === CLASSIFICATION FIELDS ===
        logger.debug("  === Extracting CLASSIFICATION FIELDS ===")
        
        if verbose:
            logger.debug("  Extracting classification fields...")
        
        # Topic category
        logger.debug("  Extracting topic category...")
        topic_xpaths = [
            './/gmd:identificationInfo//gmd:topicCategory/gmd:MD_TopicCategoryCode',
            './/mdb:identificationInfo//mri:topicCategory/mri:MD_TopicCategoryCode'
        ]
        for xpath in topic_xpaths:
            try:
                elem = root.find(xpath, XML_NAMESPACES)
                if elem is not None and hasattr(elem, 'text') and elem.text:
                    metadata['topic_category'] = elem.text.strip()
                    if verbose:
                        logger.debug(f"    Topic category: {metadata['topic_category']}")
                    break
            except Exception as e:
                logger.error(f"    Exception extracting topic_category: {e}")
        
        # Language
        logger.debug("  Extracting language...")
        lang_xpaths = [
            './/gmd:identificationInfo//gmd:language/gco:CharacterString',
            './/gmd:identificationInfo//gmd:language/gmd:LanguageCode',
            './/mdb:identificationInfo//mri:defaultLocale//gco:CharacterString'
        ]
        for xpath in lang_xpaths:
            try:
                elem = root.find(xpath, XML_NAMESPACES)
                if elem is not None and hasattr(elem, 'text') and elem.text:
                    metadata['language'] = elem.text.strip()
                    if verbose:
                        logger.debug(f"    Language: {metadata['language']}")
                    break
            except Exception as e:
                logger.error(f"    Exception extracting language: {e}")
        
        # Character set
        logger.debug("  Extracting character set...")
        charset_xpaths = [
            './/gmd:characterSet/gmd:MD_CharacterSetCode',
            './/mdb:metadataScope//gmd:MD_CharacterSetCode'
        ]
        for xpath in charset_xpaths:
            try:
                elem = root.find(xpath, XML_NAMESPACES)
                if elem is not None and hasattr(elem, 'text') and elem.text:
                    metadata['character_set'] = elem.text.strip()
                    if verbose:
                        logger.debug(f"    Character set: {metadata['character_set']}")
                    break
            except Exception as e:
                logger.error(f"    Exception extracting character_set: {e}")
        
        # Status
        logger.debug("  Extracting status...")
        status_xpaths = [
            './/gmd:identificationInfo//gmd:status/gmd:MD_ProgressCode',
            './/mdb:identificationInfo//mri:status/mri:MD_ProgressCode'
        ]
        for xpath in status_xpaths:
            try:
                elem = root.find(xpath, XML_NAMESPACES)
                if elem is not None and hasattr(elem, 'text') and elem.text:
                    metadata['status'] = elem.text.strip()
                    if verbose:
                        logger.debug(f"    Status: {metadata['status']}")
                    break
            except Exception as e:
                logger.error(f"    Exception extracting status: {e}")
        
        # === DATES ===
        logger.debug("  === Extracting DATES ===")
        
        if verbose:
            logger.debug("  Extracting dates...")
        
        # Metadata creation date
        logger.debug("  Extracting metadata creation date...")
        creation_xpaths = [
            './/gmd:dateStamp/gco:DateTime',
            './/gmd:dateStamp/gco:Date',
            './/mdb:dateInfo//cit:date/gco:DateTime'
        ]
        try:
            metadata['metadata_creation_date'] = extract_date(root, creation_xpaths, XML_NAMESPACES, verbose)
            logger.debug(f"    metadata_creation_date result: {metadata['metadata_creation_date']}")
        except Exception as e:
            logger.error(f"    Exception extracting metadata_creation_date: {e}")
            logger.error(f"    Stack trace: {traceback.format_exc()}")
        
        # Metadata revision date (look for dateType="revision")
        logger.debug("  Extracting metadata revision date...")
        try:
            for date_elem in root.findall('.//gmd:dateInfo//gmd:CI_Date', XML_NAMESPACES):
                date_type = extract_text(date_elem, './/gmd:dateType/gmd:CI_DateTypeCode', XML_NAMESPACES)
                if date_type == 'revision':
                    revision_xpaths = ['.//gmd:date/gco:DateTime', './/gmd:date/gco:Date']
                    metadata['metadata_revision_date'] = extract_date(date_elem, revision_xpaths, XML_NAMESPACES, verbose)
                    break
            logger.debug(f"    metadata_revision_date result: {metadata['metadata_revision_date']}")
        except Exception as e:
            logger.error(f"    Exception extracting metadata_revision_date: {e}")
            logger.error(f"    Stack trace: {traceback.format_exc()}")
        
        # Citation date
        logger.debug("  Extracting citation date...")
        citation_xpaths = [
            './/gmd:identificationInfo//gmd:citation//gmd:date//gco:Date',
            './/gmd:identificationInfo//gmd:citation//gmd:date//gco:DateTime',
            './/mdb:identificationInfo//mri:citation//cit:date//gco:DateTime'
        ]
        try:
            metadata['citation_date'] = extract_date(root, citation_xpaths, XML_NAMESPACES, verbose)
            logger.debug(f"    citation_date result: {metadata['citation_date']}")
        except Exception as e:
            logger.error(f"    Exception extracting citation_date: {e}")
            logger.error(f"    Stack trace: {traceback.format_exc()}")
        
        # === SPATIAL EXTENT (Bounding Box) ===
        logger.debug("  === Extracting BOUNDING BOX ===")
        
        if verbose:
            logger.debug("  Extracting bounding box...")
        
        bbox_patterns = [
            ('.//gmd:EX_GeographicBoundingBox', 'gmd'),
            ('.//gex:EX_GeographicBoundingBox', 'gex')
        ]
        
        for pattern, ns in bbox_patterns:
            logger.debug(f"  Trying bbox pattern: {pattern} (namespace: {ns})")
            try:
                bbox_elem = root.find(pattern, XML_NAMESPACES)
                logger.debug(f"    bbox_elem type: {type(bbox_elem)}")
                
                if bbox_elem is not None:
                    logger.debug(f"    Found bbox element, extracting coordinates...")
                    west = extract_text(bbox_elem, f'.//{ns}:westBoundLongitude/gco:Decimal', XML_NAMESPACES)
                    east = extract_text(bbox_elem, f'.//{ns}:eastBoundLongitude/gco:Decimal', XML_NAMESPACES)
                    south = extract_text(bbox_elem, f'.//{ns}:southBoundLatitude/gco:Decimal', XML_NAMESPACES)
                    north = extract_text(bbox_elem, f'.//{ns}:northBoundLatitude/gco:Decimal', XML_NAMESPACES)
                    
                    logger.debug(f"    west: {west}, east: {east}, south: {south}, north: {north}")
                    
                    if west:
                        try:
                            metadata['west'] = float(west)
                        except ValueError as e:
                            logger.error(f"    ValueError converting west to float: {e}")
                    if east:
                        try:
                            metadata['east'] = float(east)
                        except ValueError as e:
                            logger.error(f"    ValueError converting east to float: {e}")
                    if south:
                        try:
                            metadata['south'] = float(south)
                        except ValueError as e:
                            logger.error(f"    ValueError converting south to float: {e}")
                    if north:
                        try:
                            metadata['north'] = float(north)
                        except ValueError as e:
                            logger.error(f"    ValueError converting north to float: {e}")
                    
                    if all(metadata.get(c) is not None for c in ['west', 'east', 'south', 'north']):
                        logger.info(f"  ✓ Bounding box: [{metadata['west']:.2f}, {metadata['east']:.2f}, {metadata['south']:.2f}, {metadata['north']:.2f}]")
                        break
            except Exception as e:
                logger.error(f"    Exception extracting bounding box: {e}")
                logger.error(f"    Stack trace: {traceback.format_exc()}")
        
        if not all(metadata.get(c) is not None for c in ['west', 'east', 'south', 'north']):
            if verbose:
                logger.warning(f"  ⚠ Incomplete bounding box, will use defaults")
        
        # === TEMPORAL EXTENT ===
        logger.debug("  === Extracting TEMPORAL EXTENT ===")
        
        if verbose:
            logger.debug("  Extracting temporal extent...")
        
        try:
            temporal = extract_temporal_extent(root, XML_NAMESPACES, verbose)
            logger.debug(f"  temporal result: {temporal}")
            logger.debug(f"  Updating metadata with temporal data...")
            metadata.update(temporal)
            logger.debug(f"  metadata after update: time_start={metadata.get('time_start')}, time_end={metadata.get('time_end')}")
        except Exception as e:
            logger.error(f"  Exception extracting temporal extent: {e}")
            logger.error(f"  Stack trace: {traceback.format_exc()}")
        
        if metadata.get('time_start') or metadata.get('time_end'):
            logger.info(f"  ✓ Temporal extent: {metadata.get('time_start') or 'N/A'} to {metadata.get('time_end') or 'N/A'}")
        
        # === SUMMARY ===
        logger.debug("  === PARSING SUMMARY ===")
        
        fields_extracted = sum(1 for v in metadata.values() if v is not None)
        logger.info(f"  ✓ XML parsing completed: {fields_extracted} fields extracted")
        logger.debug(f"  Final metadata keys: {list(metadata.keys())}")
        logger.debug(f"  Final metadata values summary: {[(k, type(v)) for k, v in metadata.items()]}")
        
    except ET.ParseError as e:
        logger.error(f"  ✗ XML parsing error: {e}")
        logger.error(f"  Stack trace: {traceback.format_exc()}")
    except Exception as e:
        logger.error(f"  ✗ Unexpected error in parse_xml_metadata: {e}")
        logger.error(f"  Stack trace: {traceback.format_exc()}")
    
    logger.debug(f"parse_xml_metadata returning metadata dict with {len(metadata)} keys")
    return metadata


def generate_uuid_from_path(dataset_path: Path) -> str:
    """Generate a deterministic UUID from dataset path."""
    namespace = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')
    dataset_uuid = uuid.uuid5(namespace, str(dataset_path))
    logger.debug(f"Generated UUID: {dataset_uuid}")
    return str(dataset_uuid)


def extract_bounding_box_from_name(dataset_name: str) -> Dict[str, float]:
    """Extract bounding box hints from dataset name."""
    logger.debug(f"extract_bounding_box_from_name called for: {dataset_name}")
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
    logger.info(f"scan_aodn_directory called with base_path: {base_path}")
    base_path = Path(base_path)
    logger.debug(f"  base_path type: {type(base_path)}, value: {base_path}")
    
    if not base_path.exists():
        logger.error(f"✗ Directory not found: {base_path}")
        return []
    
    logger.info(f"Scanning: {base_path}")
    subdirs = [d for d in base_path.iterdir() if d.is_dir() and not d.name.startswith('.')]
    logger.info(f"Found {len(subdirs)} potential datasets")
    logger.debug(f"  subdirs: {[d.name for d in subdirs]}")
    
    datasets = []
    
    for idx, dataset_dir in enumerate(subdirs, 1):
        logger.info(f"\n[{idx}/{len(subdirs)}] Processing: {dataset_dir.name}")
        logger.debug(f"  dataset_dir type: {type(dataset_dir)}, value: {dataset_dir}")
        
        try:
            # Try to find and parse metadata.xml
            xml_path = find_metadata_xml(dataset_dir)
            logger.debug(f"  xml_path: {xml_path}")
            
            if xml_path:
                logger.debug(f"  Calling parse_xml_metadata...")
                xml_metadata = parse_xml_metadata(xml_path, verbose=verbose)
                logger.debug(f"  parse_xml_metadata returned type: {type(xml_metadata)}")
                logger.debug(f"  xml_metadata keys: {list(xml_metadata.keys()) if xml_metadata else 'None'}")
                
                if xml_metadata is None:
                    logger.error(f"  ✗ parse_xml_metadata returned None!")
                    xml_metadata = {}
                
                logger.debug(f"  Getting 'uuid' from xml_metadata...")
                dataset_uuid = xml_metadata.get('uuid') or generate_uuid_from_path(dataset_dir)
                logger.debug(f"  dataset_uuid: {dataset_uuid}")
                
                logger.debug(f"  Getting 'title' from xml_metadata...")
                title = xml_metadata.get('title') or dataset_dir.name
                logger.debug(f"  title: {title}")
                
                # Use bounding box from XML if complete, otherwise estimate
                logger.debug(f"  Checking bbox completeness...")
                logger.debug(f"  west: {xml_metadata.get('west')}, east: {xml_metadata.get('east')}, south: {xml_metadata.get('south')}, north: {xml_metadata.get('north')}")
                
                if all(xml_metadata.get(c) for c in ['west', 'east', 'south', 'north']):
                    logger.debug(f"  Extracting bbox from xml_metadata...")
                    bbox = {k: xml_metadata.get(k, None) for k in ['west', 'east', 'south', 'north']}
                    logger.debug(f"  bbox: {bbox}")
                    logger.info(f"  Using bounding box from XML")
                else:
                    logger.debug(f"  Calling extract_bounding_box_from_name...")
                    bbox = extract_bounding_box_from_name(dataset_dir.name)
                    logger.debug(f"  bbox from name: {bbox}")
                    logger.info(f"  Using estimated bounding box")
                    # Update xml_metadata with estimated bbox
                    logger.debug(f"  Updating xml_metadata with bbox...")
                    xml_metadata.update(bbox)
                
                # Merge all metadata fields
                logger.debug(f"  Creating dataset_info dict...")
                dataset_info = {
                    'uuid': dataset_uuid,
                    'title': title,
                    'dataset_name': clean_dataset_name(dataset_dir.name),
                    'dataset_path': str(dataset_dir),
                }
                logger.debug(f"  dataset_info before merge: {list(dataset_info.keys())}")
                logger.debug(f"  Merging xml_metadata into dataset_info...")
                dataset_info.update(xml_metadata)
                logger.debug(f"  dataset_info after merge: {list(dataset_info.keys())}")
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
                }
                dataset_info.update(bbox)
            
            file_count = sum(1 for _ in dataset_dir.rglob('*') if _.is_file())
            logger.info(f"  File count: {file_count}")
            dataset_info['file_count'] = file_count
            
            logger.debug(f"  Appending dataset_info to datasets list...")
            datasets.append(dataset_info)
            logger.info(f"  ✓ Dataset processed successfully")
            logger.debug(f"  Current datasets count: {len(datasets)}")
            
        except Exception as e:
            logger.error(f"  ✗ Exception processing dataset {dataset_dir.name}: {e}")
            logger.error(f"  Stack trace: {traceback.format_exc()}")
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Scan complete: {len(datasets)} datasets identified")
    logger.info(f"{'='*60}")
    logger.debug(f"Returning datasets list with {len(datasets)} items")
    
    return datasets


def populate_metadata_table(conn, datasets: List[Dict], force: bool = False):
    """Populate metadata table with all extracted fields."""
    logger.info(f"populate_metadata_table called with {len(datasets)} datasets")
    logger.debug(f"  force: {force}")
    
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
    
    logger.debug(f"  fields list: {fields}")
    
    placeholders = ', '.join(['%s'] * len(fields))
    field_names = ', '.join(fields)
    
    logger.debug(f"  Building SQL statement...")
    
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
    
    logger.debug(f"  SQL prepared (length: {len(insert_sql)})")
    
    inserted, updated, skipped, failed = 0, 0, 0, 0
    
    for idx, dataset in enumerate(datasets, 1):
        try:
            logger.info(f"\n[{idx}/{len(datasets)}] Inserting: {dataset.get('title', 'Unknown')[:60]}...")
            logger.debug(f"  dataset type: {type(dataset)}")
            logger.debug(f"  dataset keys: {list(dataset.keys())}")
            logger.debug(f"  UUID: {dataset.get('uuid', 'None')}")
            
            logger.debug(f"  Preparing values tuple...")
            # Prepare values tuple in correct order, using .get() with None default
            values_list = []
            for field_idx, field in enumerate(fields[:-1]):  # All fields except 'extracted_at'
                logger.debug(f"    Getting field {field_idx}: {field}")
                value = dataset.get(field, None)
                logger.debug(f"      Value type: {type(value)}, Value: {value}")
                values_list.append(value)
            
            logger.debug(f"  Adding extracted_at timestamp...")
            values_list.append(datetime.now())
            
            values = tuple(values_list)
            logger.debug(f"  values tuple length: {len(values)}")
            logger.debug(f"  values summary: {[(i, type(v)) for i, v in enumerate(values[:5])]}")
            
            logger.debug(f"  Executing SQL...")
            cursor.execute(insert_sql, values)
            logger.debug(f"  SQL executed, rowcount: {cursor.rowcount}")
            
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
            logger.error(f"  ✗ Database error: {e}")
            logger.error(f"  Stack trace: {traceback.format_exc()}")
        except Exception as e:
            failed += 1
            logger.error(f"  ✗ Unexpected error: {e}")
            logger.error(f"  Stack trace: {traceback.format_exc()}")
    
    logger.debug(f"Committing transaction...")
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
        logger.info("Debug logging enabled")
    
    logger.info("="*60)
    logger.info("METADATA POPULATION SCRIPT (Enhanced with Debug Logging)")
    logger.info("="*60)
    
    try:
        logger.debug("Calling scan_aodn_directory...")
        datasets = scan_aodn_directory(args.path, verbose=args.verbose)
        logger.debug(f"scan_aodn_directory returned {len(datasets) if datasets else 0} datasets")
        
        if not datasets:
            logger.warning("⚠ No datasets found")
            return 1
        
        logger.debug("Connecting to database...")
        conn = connect_to_database()
        logger.debug("Connected successfully")
        
        logger.debug("Calling populate_metadata_table...")
        populate_metadata_table(conn, datasets, force=args.force)
        logger.debug("populate_metadata_table completed")
        
        logger.debug("Calling verify_population...")
        verify_population(conn)
        logger.debug("verify_population completed")
        
        logger.debug("Closing database connection...")
        conn.close()
        logger.debug("Database connection closed")
        
        logger.info("\n✓ METADATA POPULATION COMPLETED SUCCESSFULLY")
        return 0
        
    except Exception as e:
        logger.error(f"\n✗ Failed: {e}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return 1


if __name__ == '__main__':
    exit(main())
