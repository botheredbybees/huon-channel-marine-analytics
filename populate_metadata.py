#!/usr/bin/env python3
"""
Automated metadata extraction and population script for AODN/IMOS datasets.

This script:
1. Scans AODN_data/ directory for datasets
2. Extracts comprehensive metadata from ISO 19115 XML files
3. Populates the metadata table with UUID, title, paths, bbox, and enriched fields
4. Extracts AODN UUID from XML metadata (ISO 19115-3) with directory fallback

Enhanced with:
- AODN UUID extraction from XML metadataIdentifier (ISO 19115-3)
- Fallback to fileIdentifier (ISO 19115-1) and directory structure
- Namespace-agnostic XML parsing for ISO 19115-1 and ISO 19115-3
- Extraction of 30+ metadata fields including:
  * Descriptive: abstract, credit (multiple), supplemental_info, lineage
  * Constraints: use_limitation, license_url
  * Classification: topic_category, language, character_set, status
  * Dates: metadata_creation_date, metadata_revision_date, citation_date
  * Temporal: time_start, time_end
  * Spatial: west, east, south, north (bounding box)
  * Distribution: wfs_url, wms_url, portal_url, publication_url
  * Relationships: parent_uuid
- File-based debug logging to logs/ directory
- Detailed progress tracking with console and file output separation
- Better error handling and reporting
- Normalized date handling for year-only temporal extents
- UUID validation with regex pattern matching
- Attribute extraction for code list values (@codeListValue)
- Enhanced debugging for problematic fields
- Fixed foreign key constraint violations with column-specific UPDATE

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


def find_element_by_tag_suffix(root, tag_suffix: str):
    """Find first element whose tag ends with the given suffix (namespace-agnostic)."""
    for elem in root.iter():
        if elem.tag.endswith('}' + tag_suffix) or elem.tag == tag_suffix:
            return elem
    return None


def find_all_elements_by_tag_suffix(root, tag_suffix: str):
    """Find all elements whose tag ends with the given suffix (namespace-agnostic)."""
    found = []
    for elem in root.iter():
        if elem.tag.endswith('}' + tag_suffix) or elem.tag == tag_suffix:
            found.append(elem)
    return found


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


def get_attribute_value(element, attribute_name: str) -> Optional[str]:
    """Extract attribute value from element (e.g., codeListValue, uuidref)."""
    if element is None:
        return None
    
    # Try direct attribute
    if attribute_name in element.attrib:
        return element.attrib[attribute_name]
    
    # Try without namespace
    for key, value in element.attrib.items():
        if key.endswith(attribute_name):
            return value
    
    return None


def extract_uuid_from_xml(xml_path: Path) -> Optional[str]:
    """
    Extract UUID from ISO 19115-3 or ISO 19115-1 metadata XML file.
    
    Looks for UUID in multiple locations (in priority order):
    1. mdb:metadataIdentifier/mcc:MD_Identifier/mcc:code (ISO 19115-3)
    2. gmd:fileIdentifier (ISO 19115-1)
    3. Directory structure fallback
    
    Args:
        xml_path: Path to metadata.xml file
        
    Returns:
        UUID string or None if not found
    """
    uuid_pattern = re.compile(
        r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
        re.IGNORECASE
    )
    
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        # Method 1: ISO 19115-3 (mdb:metadataIdentifier)
        for elem in root.iter():
            if elem.tag.endswith('}metadataIdentifier'):
                # Look for code element
                for child in elem.iter():
                    if child.tag.endswith('}code'):
                        uuid_text = get_element_text(child)
                        if uuid_text and uuid_pattern.match(uuid_text):
                            logger.debug(f"  ✓ Extracted UUID from metadataIdentifier: {uuid_text}")
                            return uuid_text
        
        # Method 2: ISO 19115-1 (gmd:fileIdentifier)
        file_id_elem = find_element_by_tag_suffix(root, 'fileIdentifier')
        if file_id_elem is not None:
            uuid_text = get_element_text(file_id_elem)
            if uuid_text and uuid_pattern.match(uuid_text):
                logger.debug(f"  ✓ Extracted UUID from fileIdentifier: {uuid_text}")
                return uuid_text
        
        # Method 3: Directory structure fallback
        logger.debug(f"  ⚠ No UUID found in XML, trying directory structure...")
        uuid_dir = xml_path.parent.parent
        potential_uuid = uuid_dir.name
        
        if uuid_pattern.match(potential_uuid):
            logger.debug(f"  ✓ Extracted UUID from directory path: {potential_uuid}")
            return potential_uuid
        
        logger.warning(f"  ✗ No UUID found in XML or directory structure")
        return None
            
    except ET.ParseError as e:
        logger.error(f"  ✗ XML parsing error during UUID extraction: {e}")
        return None
    except Exception as e:
        logger.debug(f"  ✗ Error extracting UUID: {e}")
        return None


def extract_parent_uuid(root) -> Optional[str]:
    """
    Extract parent metadata UUID from mdb:parentMetadata element.
    
    Args:
        root: XML root element
        
    Returns:
        Parent UUID string or None if not found
    """
    uuid_pattern = re.compile(
        r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
        re.IGNORECASE
    )
    
    parent_elem = find_element_by_tag_suffix(root, 'parentMetadata')
    if parent_elem is not None:
        # Try uuidref attribute
        parent_uuid = get_attribute_value(parent_elem, 'uuidref')
        if parent_uuid and uuid_pattern.match(parent_uuid):
            logger.debug(f"  [PARENT_UUID] ✓ Found: {parent_uuid}")
            return parent_uuid
    
    logger.debug(f"  [PARENT_UUID] ✗ Not found")
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


def extract_distribution_urls(root) -> Dict[str, Optional[str]]:
    """
    Extract distribution URLs from distributionInfo/transferOptions/onLine elements.
    
    Looks for:
    - WFS: protocol contains "OGC:WFS"
    - WMS: protocol contains "OGC:WMS"
    - Portal: protocol contains "WWW:LINK-1.0-http--portal"
    - Publication: protocol contains "WWW:LINK-1.0-http--publication"
    
    Args:
        root: XML root element
        
    Returns:
        Dictionary with distribution URLs
    """
    urls = {
        'distribution_wfs_url': None,
        'distribution_wms_url': None,
        'distribution_portal_url': None,
        'distribution_publication_url': None
    }
    
    logger.debug("  [DISTRIBUTION] Starting URL extraction...")
    
    # Find all CI_OnlineResource elements
    online_resources = find_all_elements_by_tag_suffix(root, 'CI_OnlineResource')
    logger.debug(f"  [DISTRIBUTION] Found {len(online_resources)} CI_OnlineResource elements")
    
    for resource in online_resources:
        protocol = None
        linkage = None
        
        # Extract protocol and linkage
        for child in resource.iter():
            if child.tag.endswith('}protocol'):
                protocol = get_element_text(child)
            elif child.tag.endswith('}linkage'):
                linkage = get_element_text(child)
        
        if protocol and linkage:
            logger.debug(f"  [DISTRIBUTION] Checking protocol: {protocol[:50]}...")
            
            # Match protocol to URL type
            if 'OGC:WFS' in protocol and not urls['distribution_wfs_url']:
                urls['distribution_wfs_url'] = linkage
                logger.debug(f"  [DISTRIBUTION] ✓ WFS URL: {linkage}")
            elif 'OGC:WMS' in protocol and not urls['distribution_wms_url']:
                urls['distribution_wms_url'] = linkage
                logger.debug(f"  [DISTRIBUTION] ✓ WMS URL: {linkage}")
            elif 'WWW:LINK-1.0-http--portal' in protocol and not urls['distribution_portal_url']:
                urls['distribution_portal_url'] = linkage
                logger.debug(f"  [DISTRIBUTION] ✓ Portal URL: {linkage}")
            elif 'WWW:LINK-1.0-http--publication' in protocol and not urls['distribution_publication_url']:
                urls['distribution_publication_url'] = linkage
                logger.debug(f"  [DISTRIBUTION] ✓ Publication URL: {linkage}")
    
    found_count = sum(1 for v in urls.values() if v is not None)
    logger.debug(f"  [DISTRIBUTION] Extracted {found_count}/4 distribution URLs")
    
    return urls


def parse_xml_metadata(xml_path: Path, verbose: bool = False) -> Dict:
    """Parse ISO 19115 XML metadata file with namespace-agnostic element matching."""
    logger.info(f"Parsing XML: {xml_path.name}")
    logger.debug(f"  Full path: {xml_path}")
    
    metadata = {
        'uuid': None, 'parent_uuid': None, 'title': None, 'abstract': None, 'credit': None,
        'supplemental_info': None, 'lineage': None, 'use_limitation': None,
        'license_url': None, 'topic_category': None, 'language': None,
        'character_set': None, 'status': None, 'metadata_creation_date': None,
        'metadata_revision_date': None, 'citation_date': None,
        'west': None, 'east': None, 'south': None, 'north': None,
        'time_start': None, 'time_end': None,
        'distribution_wfs_url': None, 'distribution_wms_url': None,
        'distribution_portal_url': None, 'distribution_publication_url': None
    }
    
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        logger.debug(f"  Root tag: {root.tag}")
        
        # === CORE IDENTIFIERS ===
        
        # Extract UUID from XML metadata (primary) or directory structure (fallback)
        aodn_uuid = extract_uuid_from_xml(xml_path)
        if aodn_uuid:
            metadata['uuid'] = aodn_uuid
            logger.info(f"  ✓ UUID: {metadata['uuid']}")
        else:
            metadata['uuid'] = None
            logger.warning("  ⚠ No UUID found - uuid will be NULL")
        
        # Extract parent UUID
        parent_uuid = extract_parent_uuid(root)
        if parent_uuid:
            metadata['parent_uuid'] = parent_uuid
            logger.info(f"  ✓ Parent UUID: {metadata['parent_uuid']}")
        
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
        
        # Credit - ENHANCED: Extract all credit elements and concatenate
        logger.debug("  [CREDIT] Starting credit extraction...")
        credit_elements = find_all_elements_by_tag_suffix(root, 'credit')
        if credit_elements:
            credits = []
            for elem in credit_elements:
                credit_text = get_element_text(elem)
                if credit_text:
                    credits.append(credit_text)
                    logger.debug(f"  [CREDIT] Found: {credit_text[:50]}...")
            
            if credits:
                metadata['credit'] = "; ".join(credits)
                logger.debug(f"  [CREDIT] ✓ Concatenated {len(credits)} credit entries")
        else:
            logger.debug(f"  [CREDIT] ✗ No credit elements found")
        
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
        
        # Lineage - AT METADATA LEVEL (mdb:resourceLineage)
        logger.debug("  [LINEAGE] Starting lineage extraction...")
        lineage_found = False
        for pattern in [
            ['resourceLineage', 'LI_Lineage', 'statement'],
            ['dataQualityInfo', 'DQ_DataQuality', 'lineage', 'LI_Lineage', 'statement'],
            ['dataQualityInfo', 'lineage', 'statement']
        ]:
            logger.debug(f"  [LINEAGE] Trying pattern: {' -> '.join(pattern)}")
            lineage_text = extract_field_by_path(root, pattern)
            if lineage_text:
                metadata['lineage'] = lineage_text
                logger.debug(f"  [LINEAGE] ✓ Found lineage: {lineage_text[:80]}...")
                lineage_found = True
                break
        
        if not lineage_found:
            logger.debug(f"  [LINEAGE] ✗ No lineage found in any pattern")
        
        # === CONSTRAINTS ===
        
        # Use limitation
        for pattern in [
            ['identificationInfo', 'MD_DataIdentification', 'resourceConstraints', 'MD_Constraints', 'useLimitation'],
            ['identificationInfo', 'MD_DataIdentification', 'resourceConstraints', 'MD_LegalConstraints', 'useLimitation'],
            ['identificationInfo', 'resourceConstraints', 'useLimitation']
        ]:
            use_lim_text = extract_field_by_path(root, pattern)
            if use_lim_text:
                metadata['use_limitation'] = use_lim_text
                if verbose:
                    logger.debug(f"    Use limitation: {use_lim_text[:80]}...")
                break
        
        # License URL - ENHANCED: Look in onlineResource within MD_LegalConstraints
        logger.debug("  [LICENSE] Starting license URL extraction...")
        license_found = False
        
        # Find all MD_LegalConstraints elements
        legal_constraints = find_all_elements_by_tag_suffix(root, 'MD_LegalConstraints')
        for constraint in legal_constraints:
            # Look for reference/onlineResource with "License" in description
            for child in constraint.iter():
                if child.tag.endswith('}onlineResource') or child.tag.endswith('}CI_OnlineResource'):
                    linkage = None
                    description = None
                    
                    for online_child in child.iter():
                        if online_child.tag.endswith('}linkage'):
                            linkage = get_element_text(online_child)
                        elif online_child.tag.endswith('}description'):
                            description = get_element_text(online_child)
                    
                    if linkage and description and 'license' in description.lower():
                        metadata['license_url'] = linkage
                        logger.debug(f"  [LICENSE] ✓ Found via onlineResource: {linkage}")
                        license_found = True
                        break
            
            if license_found:
                break
        
        # Fallback: try otherConstraints
        if not license_found:
            for pattern in [
                ['identificationInfo', 'MD_DataIdentification', 'resourceConstraints', 'MD_LegalConstraints', 'otherConstraints'],
                ['identificationInfo', 'resourceConstraints', 'otherConstraints']
            ]:
                license_text = extract_field_by_path(root, pattern)
                if license_text and 'http' in license_text:
                    metadata['license_url'] = license_text
                    logger.debug(f"  [LICENSE] ✓ Found via otherConstraints: {license_text}")
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
        
        # Language - AT METADATA LEVEL (mdb:defaultLocale)
        logger.debug("  [LANGUAGE] Starting language extraction...")
        locale_elem = find_element_by_tag_suffix(root, 'defaultLocale')
        if locale_elem is not None:
            logger.debug(f"  [LANGUAGE] Found defaultLocale element")
            lang_found = False
            for elem in locale_elem.iter():
                if elem.tag.endswith('}LanguageCode'):
                    logger.debug(f"  [LANGUAGE] Found LanguageCode element, attributes: {elem.attrib}")
                    lang_code = get_attribute_value(elem, 'codeListValue')
                    if lang_code:
                        metadata['language'] = lang_code
                        logger.debug(f"  [LANGUAGE] ✓ Extracted language: {lang_code}")
                        lang_found = True
                        break
            if not lang_found:
                logger.debug(f"  [LANGUAGE] ✗ No LanguageCode with codeListValue found")
        else:
            logger.debug(f"  [LANGUAGE] ✗ No defaultLocale element found")
        
        # Fallback: try identification info level
        if not metadata['language']:
            logger.debug(f"  [LANGUAGE] Trying fallback patterns...")
            for pattern in [
                ['identificationInfo', 'MD_DataIdentification', 'language'],
                ['identificationInfo', 'language']
            ]:
                lang_text = extract_field_by_path(root, pattern)
                if lang_text:
                    metadata['language'] = lang_text
                    logger.debug(f"  [LANGUAGE] ✓ Found via fallback: {lang_text}")
                    break
        
        # Character Set - AT METADATA LEVEL
        logger.debug("  [CHARSET] Starting character set extraction...")
        if locale_elem is not None:
            charset_found = False
            for elem in locale_elem.iter():
                if elem.tag.endswith('}MD_CharacterSetCode'):
                    logger.debug(f"  [CHARSET] Found MD_CharacterSetCode, attributes: {elem.attrib}")
                    charset_code = get_attribute_value(elem, 'codeListValue')
                    if charset_code:
                        metadata['character_set'] = charset_code
                        logger.debug(f"  [CHARSET] ✓ Extracted charset: {charset_code}")
                        charset_found = True
                        break
            if not charset_found:
                logger.debug(f"  [CHARSET] ✗ No MD_CharacterSetCode with codeListValue found")
        else:
            logger.debug(f"  [CHARSET] Skipping (no defaultLocale element)")
        
        # Fallback for character set
        if not metadata['character_set']:
            logger.debug(f"  [CHARSET] Trying fallback...")
            charset_elem = find_element_by_tag_suffix(root, 'MD_CharacterSetCode')
            if charset_elem is not None:
                charset_code = get_attribute_value(charset_elem, 'codeListValue')
                if not charset_code:
                    charset_code = get_element_text(charset_elem)
                if charset_code:
                    metadata['character_set'] = charset_code
                    logger.debug(f"  [CHARSET] ✓ Found via fallback: {charset_code}")
        
        # Status
        logger.debug("  [STATUS] Starting status extraction...")
        status_elem = find_element_by_tag_suffix(root, 'MD_ProgressCode')
        if status_elem is not None:
            logger.debug(f"  [STATUS] Found MD_ProgressCode, attributes: {status_elem.attrib}")
            status_code = get_attribute_value(status_elem, 'codeListValue')
            if status_code:
                logger.debug(f"  [STATUS] ✓ Extracted from codeListValue: {status_code}")
            else:
                status_code = get_element_text(status_elem)
                if status_code:
                    logger.debug(f"  [STATUS] ✓ Extracted from element text: {status_code}")
            if status_code:
                metadata['status'] = status_code
        else:
            logger.debug(f"  [STATUS] ✗ No MD_ProgressCode element found")
        
        # === DATES ===
        
        # Metadata dates - ENHANCED: Extract both creation and revision dates
        logger.debug("  [DATES] Starting metadata date extraction...")
        date_info_elements = find_all_elements_by_tag_suffix(root, 'dateInfo')
        
        for date_info in date_info_elements:
            date_value = None
            date_type = None
            
            # Extract date and dateType
            for child in date_info.iter():
                if child.tag.endswith('}date'):
                    # Check if this is a DateTime element
                    for date_child in child:
                        if date_child.tag.endswith('}DateTime') or date_child.tag.endswith('}Date'):
                            date_value = get_element_text(date_child)
                            break
                    if not date_value:
                        date_value = get_element_text(child)
                elif child.tag.endswith('}CI_DateTypeCode'):
                    date_type = get_attribute_value(child, 'codeListValue')
                    if not date_type:
                        date_type = get_element_text(child)
            
            if date_value and date_type:
                logger.debug(f"  [DATES] Found date: {date_value}, type: {date_type}")
                if date_type == 'creation' and not metadata['metadata_creation_date']:
                    metadata['metadata_creation_date'] = normalize_date(date_value)
                    logger.debug(f"  [DATES] ✓ Creation date: {metadata['metadata_creation_date']}")
                elif date_type == 'revision' and not metadata['metadata_revision_date']:
                    metadata['metadata_revision_date'] = normalize_date(date_value)
                    logger.debug(f"  [DATES] ✓ Revision date: {metadata['metadata_revision_date']}")
        
        # Fallback for metadata creation date
        if not metadata['metadata_creation_date']:
            date_stamp = extract_field_by_path(root, ['dateStamp'])
            if date_stamp:
                metadata['metadata_creation_date'] = normalize_date(date_stamp)
                logger.debug(f"  [DATES] ✓ Creation date (from dateStamp): {metadata['metadata_creation_date']}")
        
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
            
            for coord_elem in bbox_elem:
                tag = coord_elem.tag
                value_text = get_element_text(coord_elem)
                
                if value_text:
                    try:
                        if tag.endswith('}westBoundLongitude') or tag == 'westBoundLongitude':
                            metadata['west'] = float(value_text)
                        elif tag.endswith('}eastBoundLongitude') or tag == 'eastBoundLongitude':
                            metadata['east'] = float(value_text)
                        elif tag.endswith('}southBoundLatitude') or tag == 'southBoundLatitude':
                            metadata['south'] = float(value_text)
                        elif tag.endswith('}northBoundLatitude') or tag == 'northBoundLatitude':
                            metadata['north'] = float(value_text)
                    except ValueError as e:
                        logger.error(f"    Error converting coordinate {tag}: {e}")
            
            if all(metadata.get(c) is not None for c in ['west', 'east', 'south', 'north']):  
                logger.info(f"  ✓ Bounding box: [{metadata['west']:.2f}, {metadata['east']:.2f}, {metadata['south']:.2f}, {metadata['north']:.2f}]")
            else:
                logger.warning(f"  ⚠ Incomplete bounding box")
        
        # === TEMPORAL EXTENT ===
        
        temporal_elem = find_element_by_tag_suffix(root, 'EX_TemporalExtent')
        if temporal_elem is not None:
            logger.debug("  Found temporal extent element")
            
            for elem in temporal_elem.iter():
                if elem.tag.endswith('}TimePeriod'):
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
        
        # === DISTRIBUTION URLs ===
        
        distribution_urls = extract_distribution_urls(root)
        metadata.update(distribution_urls)
        
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
            xml_path = find_metadata_xml(dataset_dir)
            
            if xml_path:
                xml_metadata = parse_xml_metadata(xml_path, verbose=verbose)
                
                if not xml_metadata.get('uuid'):
                    logger.error(f"  ✗ Skipping dataset - no UUID found")
                    skipped_no_uuid += 1
                    continue
                
                title = xml_metadata.get('title') or dataset_dir.name
                xml_metadata['title'] = title
                
                if all(xml_metadata.get(c) for c in ['west', 'east', 'south', 'north']):
                    logger.info(f"  Using bounding box from XML")
                else:
                    bbox = extract_bounding_box_from_name(dataset_dir.name)
                    logger.info(f"  Using estimated bounding box")
                    xml_metadata.update(bbox)
                
                dataset_info = {
                    'dataset_name': clean_dataset_name(dataset_dir.name),
                    'dataset_path': str(dataset_dir),
                }
                
                dataset_info.update(xml_metadata)
            else:
                logger.error(f"  ✗ Skipping dataset - no metadata.xml found")
                skipped_no_uuid += 1
                continue
            
            file_count = sum(1 for _ in dataset_dir.rglob('*') if _.is_file())
            logger.info(f"  File count: {file_count}")
            dataset_info['file_count'] = file_count
            
            if dataset_info.get('uuid') and dataset_info.get('title'):
                datasets.append(dataset_info)
                logger.info(f"  ✓ Dataset processed successfully")
            else:
                logger.error(f"  ✗ Skipping dataset - missing required fields")
                skipped_no_uuid += 1
        
        except Exception as e:
            logger.error(f"  ✗ Error processing dataset: {e}")
            logger.debug(traceback.format_exc())
            skipped_no_uuid += 1
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Scan complete: {len(datasets)} datasets identified")
    logger.info(f"Skipped (no UUID): {skipped_no_uuid}")
    logger.info(f"{'='*60}")
    
    return datasets


def populate_metadata_table(conn, datasets: List[Dict], force: bool = False):
    """Populate metadata table with all extracted fields."""
    logger.info(f"\nPopulating metadata (mode: {'UPDATE' if force else 'INSERT ONLY'})")
    cursor = conn.cursor()
    
    # ALL fields including new distribution URLs and parent_uuid
    insert_fields = [
        'uuid', 'parent_uuid', 'title', 'dataset_name', 'dataset_path',
        'abstract', 'credit', 'supplemental_info', 'lineage',
        'use_limitation', 'license_url', 'topic_category', 'language',
        'character_set', 'status', 'metadata_creation_date',
        'metadata_revision_date', 'citation_date',
        'west', 'east', 'south', 'north',
        'time_start', 'time_end',
        'distribution_wfs_url', 'distribution_wms_url',
        'distribution_portal_url', 'distribution_publication_url',
        'extracted_at'
    ]
    
    placeholders = ', '.join(['%s'] * len(insert_fields))
    field_names = ', '.join(insert_fields)
    
    insert_sql = f"""
    INSERT INTO metadata ({field_names})
    VALUES ({placeholders})
    ON CONFLICT (dataset_path) DO NOTHING;
    """
    
    update_sql = """
    UPDATE metadata SET
        parent_uuid = %s,
        title = %s,
        dataset_name = %s,
        abstract = %s,
        credit = %s,
        supplemental_info = %s,
        lineage = %s,
        use_limitation = %s,
        license_url = %s,
        topic_category = %s,
        language = %s,
        character_set = %s,
        status = %s,
        metadata_creation_date = %s,
        metadata_revision_date = %s,
        citation_date = %s,
        west = %s,
        east = %s,
        south = %s,
        north = %s,
        time_start = %s,
        time_end = %s,
        distribution_wfs_url = %s,
        distribution_wms_url = %s,
        distribution_portal_url = %s,
        distribution_publication_url = %s,
        extracted_at = %s
    WHERE dataset_path = %s;
    """
    
    check_sql = "SELECT uuid FROM metadata WHERE dataset_path = %s;"
    
    inserted, updated, skipped, failed = 0, 0, 0, 0
    
    for idx, dataset in enumerate(datasets, 1):
        try:
            if not dataset.get('uuid'):
                dataset['uuid'] = str(uuid.uuid4())
                logger.info(f"\n[{idx}/{len(datasets)}] Generated UUID: {dataset['uuid']}")
            else:
                logger.info(f"\n[{idx}/{len(datasets)}] Using AODN UUID: {dataset['uuid']}")
            
            if not dataset.get('dataset_path'):
                logger.error(f"  ✗ Skipping - no dataset_path")
                failed += 1
                continue
            
            logger.info(f"Processing: {dataset.get('title', 'Unknown')[:60]}...")
            
            if force:
                cursor.execute(check_sql, (dataset.get('dataset_path'),))
                exists = cursor.fetchone() is not None
                
                if exists:
                    update_values = [
                        dataset.get('parent_uuid'),
                        dataset.get('title'),
                        dataset.get('dataset_name'),
                        dataset.get('abstract'),
                        dataset.get('credit'),
                        dataset.get('supplemental_info'),
                        dataset.get('lineage'),
                        dataset.get('use_limitation'),
                        dataset.get('license_url'),
                        dataset.get('topic_category'),
                        dataset.get('language'),
                        dataset.get('character_set'),
                        dataset.get('status'),
                        dataset.get('metadata_creation_date'),
                        dataset.get('metadata_revision_date'),
                        dataset.get('citation_date'),
                        dataset.get('west'),
                        dataset.get('east'),
                        dataset.get('south'),
                        dataset.get('north'),
                        dataset.get('time_start'),
                        dataset.get('time_end'),
                        dataset.get('distribution_wfs_url'),
                        dataset.get('distribution_wms_url'),
                        dataset.get('distribution_portal_url'),
                        dataset.get('distribution_publication_url'),
                        datetime.now(),
                        dataset.get('dataset_path')
                    ]
                    
                    cursor.execute(update_sql, tuple(update_values))
                    updated += 1
                    logger.info(f"  ✓ Updated existing record")
                else:
                    insert_values = [dataset.get(field, None) for field in insert_fields[:-1]]
                    insert_values.append(datetime.now())
                    
                    cursor.execute(insert_sql, tuple(insert_values))
                    inserted += 1
                    logger.info(f"  ✓ Inserted new record")
            else:
                insert_values = [dataset.get(field, None) for field in insert_fields[:-1]]
                insert_values.append(datetime.now())
                
                cursor.execute(insert_sql, tuple(insert_values))
                
                if cursor.rowcount > 0:
                    inserted += 1
                    logger.info(f"  ✓ Inserted new record")
                else:
                    skipped += 1
                    logger.info(f"  ○ Skipped (already exists)")
                
        except psycopg2.IntegrityError as e:
            conn.rollback()
            failed += 1
            logger.error(f"  ✗ Integrity error: {e}")
            
        except psycopg2.Error as e:
            conn.rollback()
            failed += 1
            logger.error(f"  ✗ Database error: {e}")
            
        except Exception as e:
            failed += 1
            logger.error(f"  ✗ Unexpected error: {e}")
            logger.debug(traceback.format_exc())
    
    conn.commit()
    logger.info(f"\n✓ Transaction committed")
    logger.info(f"\n{'='*60}")
    logger.info(f"SUMMARY: Inserted: {inserted}, Updated: {updated}, Skipped: {skipped}, Failed: {failed}")
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
    
    fields_to_check = [
        'parent_uuid', 'abstract', 'credit', 'lineage', 'topic_category',
        'language', 'character_set', 'status', 'metadata_revision_date',
        'time_start', 'time_end',
        'distribution_wfs_url', 'distribution_wms_url',
        'distribution_portal_url', 'distribution_publication_url'
    ]
    
    logger.info(f"\nField population statistics:")
    for field in fields_to_check:
        cursor.execute(f"SELECT COUNT(*) FROM metadata WHERE {field} IS NOT NULL;")
        count = cursor.fetchone()[0]
        percentage = (count / total * 100) if total > 0 else 0
        logger.info(f"  {field:30} : {count:3}/{total} ({percentage:5.1f}%)")
    
    cursor.execute("""
        SELECT title, 
               CASE WHEN parent_uuid IS NOT NULL THEN '✓' ELSE '✗' END as has_parent,
               CASE WHEN abstract IS NOT NULL THEN '✓' ELSE '✗' END as has_abstract,
               CASE WHEN distribution_wfs_url IS NOT NULL THEN '✓' ELSE '✗' END as has_wfs,
               CASE WHEN distribution_portal_url IS NOT NULL THEN '✓' ELSE '✗' END as has_portal
        FROM metadata 
        ORDER BY title 
        LIMIT 5;
    """)
    
    logger.info(f"\nSample records:")
    for row in cursor.fetchall():
        logger.info(f"  {row[0][:40]:40} | Parent:{row[1]} Abstract:{row[2]} WFS:{row[3]} Portal:{row[4]}")
    
    cursor.close()


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Populate metadata table from AODN_data directory')
    parser.add_argument('--force', action='store_true', help='Update existing records')
    parser.add_argument('--path', default='AODN_data', help='Path to data directory')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose XML parsing logs')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    global logger
    logger = setup_logging(debug=args.debug)
    
    logger.info("="*60)
    logger.info("METADATA POPULATION SCRIPT - ENHANCED")
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
