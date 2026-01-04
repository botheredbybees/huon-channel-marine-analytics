#!/usr/bin/env python3
"""
Extract parameters from ISO19115-3 metadata XML files and populate parameters table.
This script scans AODN_data directory automatically and links parameters to datasets.

Updated to:
- Auto-scan AODN_data directory (same logic as populate_metadata.py)
- Extract parameter definitions from ISO19115-3 XML metadata
- Link parameters to specific datasets via metadata_id
- Handle AODN parameter URIs and units properly
- Use robust namespace-agnostic XML parsing
- Output detailed debug information to JSON file
- Fix namespace handling for concatenated namespace+tag format
"""

import xml.etree.ElementTree as ET
import psycopg2
from pathlib import Path
import argparse
import logging
import json
from datetime import datetime

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'marine_db',
    'user': 'marine_user',
    'password': 'marine_pass123'
}

# XML Namespaces used in ISO19115-3 metadata
NAMESPACES = {
    'mdb': 'http://standards.iso.org/iso/19115/-3/mdb/2.0',
    'mcc': 'http://standards.iso.org/iso/19115/-3/mcc/1.0',
    'mrc': 'http://standards.iso.org/iso/19115/-3/mrc/2.0',
    'mri': 'http://standards.iso.org/iso/19115/-3/mri/1.0',
    'gcx': 'http://standards.iso.org/iso/19115/-3/gcx/1.0',
    'gco': 'http://standards.iso.org/iso/19115/-3/gco/1.0',
    'gml': 'http://www.opengis.net/gml/3.2',
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)

# Global debug data collector
debug_data = {
    'timestamp': datetime.now().isoformat(),
    'datasets': []
}


def get_db_connection():
    """Create database connection."""
    return psycopg2.connect(**DB_CONFIG)


def find_metadata_xml(dataset_dir: Path):
    """Search for metadata.xml file in dataset directory (same as populate_metadata.py)."""
    # Try common locations
    for path in [dataset_dir / 'metadata.xml', dataset_dir / 'METADATA' / 'metadata.xml']:
        if path.exists():
            return path
    
    # Search recursively
    for xml_file in dataset_dir.rglob('metadata.xml'):
        return xml_file
    
    return None


def get_element_info(elem):
    """Get detailed info about an element for debugging."""
    return {
        'tag': elem.tag,
        'attrib': dict(elem.attrib),
        'text': elem.text.strip() if elem.text else None,
        'children': [child.tag for child in elem]
    }


def tag_matches(element_tag: str, target_suffix: str) -> bool:
    """
    Check if an element tag matches the target suffix.
    Handles multiple namespace formats:
    - {http://namespace}TagName
    - http://namespaceTagName (concatenated without separator)
    - TagName (no namespace)
    """
    # Direct match
    if element_tag == target_suffix:
        return True
    
    # Standard namespace format: {namespace}TagName
    if element_tag.endswith('}' + target_suffix):
        return True
    
    # Concatenated format without separator: just check if it ends with the target
    # But be careful - only match if there's a namespace-like prefix
    if element_tag.endswith(target_suffix) and ('http://' in element_tag or 'https://' in element_tag):
        return True
    
    return False


def find_elements_by_tag_suffix(root, tag_suffix: str):
    """Find all elements whose tag ends with the given suffix (namespace-agnostic)."""
    results = []
    for elem in root.iter():
        if tag_matches(elem.tag, tag_suffix):
            results.append(elem)
    return results


def find_element_by_tag_suffix(elem, tag_suffix: str):
    """Find first child element whose tag ends with the given suffix (namespace-agnostic)."""
    for child in elem.iter():
        if tag_matches(child.tag, tag_suffix):
            return child
    return None


def get_element_text(element):
    """Extract text from element, checking both direct text and gco:CharacterString children."""
    if element is None:
        return None
    
    # Try direct text first
    if element.text and element.text.strip():
        return element.text.strip()
    
    # Try gco:CharacterString or gco:Decimal children
    for child in element:
        if tag_matches(child.tag, 'CharacterString') or tag_matches(child.tag, 'Decimal'):
            if child.text and child.text.strip():
                return child.text.strip()
    
    return None


def extract_params_from_xml(xml_file_path: Path, dataset_name: str):
    """
    Extract parameter information from ISO19115-3 metadata XML.
    Uses namespace-agnostic approach to find MDSampleDimension elements.
    
    Returns: List of dicts containing parameter info
    """
    dataset_debug = {
        'name': dataset_name,
        'xml_path': str(xml_file_path),
        'root_tag': None,
        'namespaces': {},
        'all_tags': [],
        'contentInfo_elements': [],
        'MDSampleDimension_elements': [],
        'parameters_found': [],
        'error': None
    }
    
    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
        
        # Collect debug info about the XML structure
        dataset_debug['root_tag'] = root.tag
        dataset_debug['namespaces'] = dict(root.attrib) if hasattr(root, 'attrib') else {}
        
        # Collect all unique tags in the document
        all_tags = set()
        for elem in root.iter():
            all_tags.add(elem.tag)
        dataset_debug['all_tags'] = sorted(list(all_tags))
        
        # Find contentInfo elements
        content_info_elements = find_elements_by_tag_suffix(root, 'contentInfo')
        dataset_debug['contentInfo_count'] = len(content_info_elements)
        
        for ci in content_info_elements:
            dataset_debug['contentInfo_elements'].append(get_element_info(ci))
        
        # Find MDCoverageDescription elements
        coverage_desc_elements = find_elements_by_tag_suffix(root, 'MDCoverageDescription')
        dataset_debug['MDCoverageDescription_count'] = len(coverage_desc_elements)
        
        # Find all MDSampleDimension elements using namespace-agnostic search
        sample_dims = find_elements_by_tag_suffix(root, 'MDSampleDimension')
        dataset_debug['MDSampleDimension_count'] = len(sample_dims)
        
        logger.debug(f"  Found {len(sample_dims)} MDSampleDimension elements")
        
        parameters = []
        
        for idx, sample_dim in enumerate(sample_dims):
            sample_dim_debug = {
                'index': idx,
                'element_info': get_element_info(sample_dim),
                'processing': {}
            }
            
            param_info = {}
            
            # Find the name element (contains code)
            name_elem = find_element_by_tag_suffix(sample_dim, 'MDIdentifier')
            sample_dim_debug['processing']['found_MDIdentifier'] = name_elem is not None
            
            if name_elem:
                sample_dim_debug['processing']['MDIdentifier_info'] = get_element_info(name_elem)
                
                # Try to get code from Anchor element first
                anchor = find_element_by_tag_suffix(name_elem, 'Anchor')
                sample_dim_debug['processing']['found_Anchor'] = anchor is not None
                
                if anchor is not None:
                    sample_dim_debug['processing']['Anchor_info'] = get_element_info(anchor)
                    if anchor.text:
                        param_info['parameter_code'] = anchor.text.strip()
                        # Get xlink:href for AODN parameter URI
                        href = anchor.get('{http://www.w3.org/1999/xlink}href', '')
                        if not href:
                            # Try without namespace
                            href = anchor.get('href', '')
                        param_info['aodn_parameter_uri'] = href
                else:
                    # Try CharacterString
                    code_elem = find_element_by_tag_suffix(name_elem, 'code')
                    sample_dim_debug['processing']['found_code'] = code_elem is not None
                    
                    if code_elem:
                        sample_dim_debug['processing']['code_info'] = get_element_info(code_elem)
                        code_text = get_element_text(code_elem)
                        if code_text:
                            param_info['parameter_code'] = code_text
                            param_info['aodn_parameter_uri'] = ''
            
            # Get description if available
            desc_elem = find_element_by_tag_suffix(sample_dim, 'description')
            sample_dim_debug['processing']['found_description'] = desc_elem is not None
            
            if desc_elem:
                sample_dim_debug['processing']['description_info'] = get_element_info(desc_elem)
                desc_text = get_element_text(desc_elem)
                if desc_text and desc_text not in ['missing', 'null', 'inapplicable', '']:
                    param_info['parameter_label'] = desc_text
            
            # If no label, use parameter code as label
            if 'parameter_code' in param_info and 'parameter_label' not in param_info:
                param_info['parameter_label'] = param_info['parameter_code']
            
            # Get unit information from BaseUnit
            base_unit = find_element_by_tag_suffix(sample_dim, 'BaseUnit')
            sample_dim_debug['processing']['found_BaseUnit'] = base_unit is not None
            
            if base_unit:
                sample_dim_debug['processing']['BaseUnit_info'] = get_element_info(base_unit)
                # Get unit name from gml:name
                for child in base_unit:
                    if tag_matches(child.tag, 'name') and child.text:
                        param_info['unit_name'] = child.text.strip()
                    elif tag_matches(child.tag, 'identifier') and child.text:
                        param_info['unit_uri'] = child.text.strip()
            
            sample_dim_debug['processing']['extracted_param'] = param_info.copy()
            dataset_debug['MDSampleDimension_elements'].append(sample_dim_debug)
            
            # Only add if we have at least a parameter code
            if 'parameter_code' in param_info and param_info['parameter_code']:
                parameters.append(param_info)
                dataset_debug['parameters_found'].append(param_info)
        
        debug_data['datasets'].append(dataset_debug)
        return parameters
        
    except Exception as e:
        error_msg = f"Error parsing XML: {e}"
        logger.error(f"  ❌ {error_msg}")
        import traceback
        dataset_debug['error'] = error_msg
        dataset_debug['traceback'] = traceback.format_exc()
        debug_data['datasets'].append(dataset_debug)
        logger.debug(traceback.format_exc())
        return []


def get_metadata_id(cursor, dataset_path: str):
    """Get metadata_id for a dataset path."""
    cursor.execute(
        "SELECT id FROM metadata WHERE dataset_path = %s",
        (dataset_path,)
    )
    result = cursor.fetchone()
    return result[0] if result else None


def insert_parameter(cursor, metadata_id: int, param_info: dict):
    """
    Insert a parameter into the database.
    Returns True if inserted, False if skipped (duplicate).
    """
    try:
        # Check if parameter already exists for this dataset
        cursor.execute("""
            SELECT id FROM parameters 
            WHERE metadata_id = %s AND parameter_code = %s
        """, (metadata_id, param_info['parameter_code']))
        
        if cursor.fetchone():
            return False  # Already exists
        
        # Insert new parameter
        cursor.execute("""
            INSERT INTO parameters (
                metadata_id,
                parameter_code,
                parameter_label,
                aodn_parameter_uri,
                unit_name,
                unit_uri,
                content_type
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            metadata_id,
            param_info['parameter_code'],
            param_info.get('parameter_label', ''),
            param_info.get('aodn_parameter_uri', ''),
            param_info.get('unit_name', ''),
            param_info.get('unit_uri', ''),
            'physicalMeasurement'
        ))
        
        return True  # Successfully inserted
        
    except Exception as e:
        logger.error(f"    ❌ Error inserting parameter: {e}")
        return False


def scan_aodn_directory(base_path: str = 'AODN_data'):
    """
    Scan AODN_data directory for datasets with metadata.xml files.
    Same logic as populate_metadata.py.
    """
    base_path = Path(base_path)
    
    if not base_path.exists():
        logger.error(f"❌ Directory not found: {base_path}")
        return []
    
    subdirs = [d for d in base_path.iterdir() if d.is_dir() and not d.name.startswith('.')]
    logger.info(f"Found {len(subdirs)} potential datasets")
    
    datasets_with_xml = []
    
    for dataset_dir in subdirs:
        xml_path = find_metadata_xml(dataset_dir)
        if xml_path:
            datasets_with_xml.append({
                'path': str(dataset_dir),
                'xml_path': xml_path,
                'name': dataset_dir.name
            })
    
    return datasets_with_xml


def write_debug_json(output_path: Path):
    """Write debug data to JSON file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(debug_data, f, indent=2)
    logger.info(f"\n📝 Debug information written to: {output_path}")


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description='Extract parameters from AODN metadata and populate database'
    )
    parser.add_argument(
        '--path',
        default='AODN_data',
        help='Path to AODN data directory (default: AODN_data)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without making changes'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging and write debug JSON'
    )
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info("=" * 80)
    logger.info("AODN PARAMETER EXTRACTOR")
    logger.info("=" * 80)
    
    # Scan for datasets
    logger.info(f"\n📂 Scanning: {args.path}")
    datasets = scan_aodn_directory(args.path)
    
    if not datasets:
        logger.warning("⚠️  No datasets with metadata.xml found")
        return
    
    logger.info(f"Found {len(datasets)} datasets with metadata.xml")
    
    # Process each dataset
    conn = get_db_connection()
    cursor = conn.cursor()
    
    total_params = 0
    total_inserted = 0
    total_skipped = 0
    datasets_processed = 0
    
    for idx, dataset in enumerate(datasets, 1):
        logger.info(f"\n[{idx}/{len(datasets)}] {dataset['name']}")
        
        # Get metadata_id for this dataset
        metadata_id = get_metadata_id(cursor, dataset['path'])
        if not metadata_id:
            logger.warning(f"  ⚠️  Dataset not in metadata table: {dataset['path']}")
            logger.info("     Run populate_metadata.py first!")
            continue
        
        # Extract parameters from XML
        logger.info(f"  🔍 Extracting from: {dataset['xml_path'].name}")
        params = extract_params_from_xml(dataset['xml_path'], dataset['name'])
        
        if not params:
            logger.info("     No parameters found in XML")
            continue
        
        logger.info(f"     Found {len(params)} parameters")
        total_params += len(params)
        
        # Insert parameters
        if not args.dry_run:
            for param in params:
                if insert_parameter(cursor, metadata_id, param):
                    logger.info(f"     ✅ {param['parameter_code'][:50]:50} - {param.get('unit_name', 'N/A')}")
                    total_inserted += 1
                else:
                    logger.info(f"     ⏭️  {param['parameter_code'][:50]:50} - already exists")
                    total_skipped += 1
            conn.commit()
        else:
            for param in params:
                logger.info(f"     [DRY RUN] Would insert: {param['parameter_code'][:50]} ({param.get('unit_name', 'N/A')})")
        
        datasets_processed += 1
    
    cursor.close()
    conn.close()
    
    # Write debug JSON if requested
    if args.debug:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        debug_json_path = Path('logs') / f'parameter_extraction_debug_{timestamp}.json'
        write_debug_json(debug_json_path)
    
    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("✅ COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Datasets processed:    {datasets_processed}")
    logger.info(f"Total parameters found: {total_params}")
    
    if not args.dry_run:
        logger.info(f"Parameters inserted:    {total_inserted}")
        logger.info(f"Parameters skipped:     {total_skipped} (already existed)")
    else:
        logger.info("\n📝 DRY RUN - No changes made to database")
        logger.info("Run without --dry-run to insert parameters")
    
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
