#!/usr/bin/env python3
"""
Extract parameters from ISO19115-3 metadata XML files and populate parameters table.
This script scans AODN_data directory automatically and links parameters to datasets.

Updated to:
- Auto-scan AODN_data directory (same logic as populate_metadata.py)
- Extract parameter definitions from ISO19115-3 XML metadata
- Link parameters to specific datasets via metadata_id
- Handle AODN parameter URIs and units properly
"""

import xml.etree.ElementTree as ET
import psycopg2
from pathlib import Path
import argparse
import logging
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


def find_element_by_tag_suffix(root, tag_suffix: str):
    """Find first element whose tag ends with the given suffix (namespace-agnostic)."""
    for elem in root.iter():
        if elem.tag.endswith('}' + tag_suffix) or elem.tag == tag_suffix:
            return elem
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
        if child.tag.endswith('}CharacterString') or child.tag.endswith('}Decimal'):
            if child.text and child.text.strip():
                return child.text.strip()
    
    return None


def extract_params_from_xml(xml_file_path: Path):
    """
    Extract parameter information from ISO19115-3 metadata XML.
    
    Returns: List of dicts containing parameter info
    """
    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
        
        parameters = []
        
        # Find all MDSampleDimension elements which contain parameter definitions
        for sample_dim in root.findall('.//mrc:MDSampleDimension', NAMESPACES):
            param_info = {}
            
            # Get parameter code - can be in mcc:code/gcx:Anchor or mcc:code/gco:CharacterString
            name_elem = sample_dim.find('.//mcc:code/gcx:Anchor', NAMESPACES)
            if name_elem is not None:
                param_info['parameter_code'] = name_elem.text
                param_info['aodn_parameter_uri'] = name_elem.get('{http://www.w3.org/1999/xlink}href', '')
            else:
                name_elem = sample_dim.find('.//mcc:code/gco:CharacterString', NAMESPACES)
                if name_elem is not None:
                    param_info['parameter_code'] = name_elem.text
                    param_info['aodn_parameter_uri'] = ''
            
            # Get parameter label/description
            desc_elem = sample_dim.find('.//mrc:description/gco:CharacterString', NAMESPACES)
            if desc_elem is not None and desc_elem.text and desc_elem.text.strip() not in ['missing', 'null', '']:
                param_info['parameter_label'] = desc_elem.text.strip()
            elif 'parameter_code' in param_info:
                # Use code as label if no description
                param_info['parameter_label'] = param_info['parameter_code'].replace('_', ' ').title()
            
            # Get unit information from gml:BaseUnit
            unit_name_elem = sample_dim.find('.//gml:name', NAMESPACES)
            if unit_name_elem is not None:
                param_info['unit_name'] = unit_name_elem.text
            
            unit_id_elem = sample_dim.find('.//gml:identifier', NAMESPACES)
            if unit_id_elem is not None:
                param_info['unit_uri'] = unit_id_elem.text
            
            # Only add if we have at least a parameter code
            if 'parameter_code' in param_info and param_info['parameter_code']:
                parameters.append(param_info)
        
        return parameters
        
    except Exception as e:
        logger.error(f"  ❌ Error parsing XML: {e}")
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
    
    args = parser.parse_args()
    
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
        params = extract_params_from_xml(dataset['xml_path'])
        
        if not params:
            logger.info("     No parameters found in XML")
            continue
        
        logger.info(f"     Found {len(params)} parameters")
        total_params += len(params)
        
        # Insert parameters
        if not args.dry_run:
            for param in params:
                if insert_parameter(cursor, metadata_id, param):
                    logger.info(f"     ✅ {param['parameter_code']:20} - {param.get('parameter_label', 'N/A')[:40]}")
                    total_inserted += 1
                else:
                    logger.info(f"     ⏭️  {param['parameter_code']:20} - already exists")
                    total_skipped += 1
            conn.commit()
        else:
            for param in params:
                logger.info(f"     [DRY RUN] Would insert: {param['parameter_code']}")
        
        datasets_processed += 1
    
    cursor.close()
    conn.close()
    
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
