#!/usr/bin/env python3
"""
ENHANCED populate_metadata.py with parameter extraction from XML.

This enhanced version extracts parameter definitions from ISO 19115-3 XML
metadata and automatically populates the parameters table.

New features compared to original:
1. extract_parameters_from_xml() - Parses contentInfo/MD_CoverageDescription
2. insert_parameters() - Inserts parameters with metadata_id linkage
3. Integrated into populate_metadata_table() workflow

Usage:
    python populate_metadata_enhanced.py
    python populate_metadata_enhanced.py --force --verbose
"""

# NOTE: This is a PARTIAL file showing only the NEW/MODIFIED functions.
# To use, integrate these functions into your existing populate_metadata.py

import xml.etree.ElementTree as ET
from typing import List, Dict, Optional
import logging

# Assumes existing helper functions from populate_metadata.py:
# - find_element_by_tag_suffix()
# - find_all_elements_by_tag_suffix()
# - get_element_text()
# - get_attribute_value()

logger = logging.getLogger(__name__)


def extract_parameters_from_xml(root, metadata_id: int) -> List[Dict]:
    """
    Extract parameter information from ISO 19115-3 XML metadata.
    
    Parses contentInfo/MD_CoverageDescription/attributeGroup/attribute elements
    to extract:
    - Parameter code (from MD_Identifier/code)
    - Parameter label (from sequenceIdentifier)
    - Standard name (from description)
    - AODN parameter URI (from Anchor/@href)
    - Unit name and URI
    
    Args:
        root: XML root element from metadata.xml
        metadata_id: Foreign key to metadata table
    
    Returns:
        List of parameter dicts ready for insertion
    """
    parameters = []
    
    logger.debug("  [PARAMETERS] Starting parameter extraction...")
    
    # Find contentInfo/MD_CoverageDescription elements
    content_infos = find_all_elements_by_tag_suffix(root, 'contentInfo')
    logger.debug(f"  [PARAMETERS] Found {len(content_infos)} contentInfo elements")
    
    for content_info in content_infos:
        # Look for MD_CoverageDescription
        coverage_descs = find_all_elements_by_tag_suffix(content_info, 'MD_CoverageDescription')
        
        for coverage_desc in coverage_descs:
            # Look for attributeGroup (parameter definitions)
            attr_groups = find_all_elements_by_tag_suffix(coverage_desc, 'attributeGroup')
            logger.debug(f"  [PARAMETERS] Found {len(attr_groups)} attributeGroup elements")
            
            for attr_group in attr_groups:
                # Find individual attributes (parameters)
                attributes = find_all_elements_by_tag_suffix(attr_group, 'attribute')
                logger.debug(f"  [PARAMETERS] Found {len(attributes)} attribute elements")
                
                for attr in attributes:
                    param = {
                        'metadata_id': metadata_id,
                        'parameter_code': None,
                        'parameter_label': None,
                        'standard_name': None,
                        'aodn_parameter_uri': None,
                        'imos_parameter_uri': None,
                        'unit_name': None,
                        'unit_uri': None,
                        'content_type': 'physicalMeasurement',
                        'is_depth': False
                    }
                    
                    # Extract parameter code from MD_Identifier
                    for child in attr.iter():
                        if child.tag.endswith('}MD_Identifier'):
                            code_elem = find_element_by_tag_suffix(child, 'code')
                            if code_elem:
                                param['parameter_code'] = get_element_text(code_elem)
                                logger.debug(f"    [PARAM] Found code: {param['parameter_code']}")
                    
                    # Extract parameter label and description from MD_RangeDimension
                    range_dims = find_all_elements_by_tag_suffix(attr, 'MD_RangeDimension')
                    for range_dim in range_dims:
                        # sequenceIdentifier = parameter label
                        seq_id = find_element_by_tag_suffix(range_dim, 'sequenceIdentifier')
                        if seq_id:
                            param['parameter_label'] = get_element_text(seq_id)
                            logger.debug(f"    [PARAM] Found label: {param['parameter_label']}")
                        
                        # description = standard name
                        desc_elem = find_element_by_tag_suffix(range_dim, 'description')
                        if desc_elem:
                            param['standard_name'] = get_element_text(desc_elem)
                    
                    # Extract unit information
                    units_elem = find_element_by_tag_suffix(attr, 'units')
                    if units_elem:
                        unit_code = get_element_text(units_elem)
                        if unit_code:
                            param['unit_name'] = unit_code
                            logger.debug(f"    [PARAM] Found unit: {unit_code}")
                    
                    # Extract AODN/IMOS vocabulary URI from Anchor elements
                    name_elems = find_all_elements_by_tag_suffix(attr, 'name')
                    for name_elem in name_elems:
                        anchor = find_element_by_tag_suffix(name_elem, 'Anchor')
                        if anchor:
                            uri = get_attribute_value(anchor, 'href')
                            if uri:
                                if 'vocab.aodn.org.au' in uri:
                                    param['aodn_parameter_uri'] = uri
                                    logger.debug(f"    [PARAM] Found AODN URI: {uri}")
                                elif 'vocab.imos.org.au' in uri:
                                    param['imos_parameter_uri'] = uri
                                    logger.debug(f"    [PARAM] Found IMOS URI: {uri}")
                    
                    # Check if this is a depth parameter
                    if param['parameter_code'] and 'depth' in param['parameter_code'].lower():
                        param['is_depth'] = True
                    
                    # Only add if we have at least a parameter code
                    if param['parameter_code']:
                        parameters.append(param)
                        logger.debug(f"    [PARAM] ✅ Extracted: {param['parameter_code']} - {param['parameter_label']}")
                    else:
                        logger.debug(f"    [PARAM] ❌ Skipped attribute (no code)")
    
    logger.info(f"  ✅ Extracted {len(parameters)} parameters from XML")
    return parameters


def insert_parameters(conn, metadata_id: int, parameters: List[Dict]) -> int:
    """
    Insert parameters extracted from XML metadata into parameters table.
    
    Uses ON CONFLICT to update existing parameters if they already exist
    for this metadata_id + parameter_code combination.
    
    Args:
        conn: Database connection
        metadata_id: Foreign key to metadata table
        parameters: List of parameter dicts from extract_parameters_from_xml()
    
    Returns:
        Number of parameters inserted/updated
    """
    if not parameters:
        return 0
    
    cursor = conn.cursor()
    inserted = 0
    updated = 0
    
    for param in parameters:
        try:
            cursor.execute("""
                INSERT INTO parameters (
                    metadata_id, parameter_code, parameter_label,
                    standard_name, aodn_parameter_uri, imos_parameter_uri,
                    unit_name, unit_uri, content_type, is_depth
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (metadata_id, parameter_code) 
                DO UPDATE SET
                    parameter_label = EXCLUDED.parameter_label,
                    standard_name = EXCLUDED.standard_name,
                    aodn_parameter_uri = EXCLUDED.aodn_parameter_uri,
                    imos_parameter_uri = EXCLUDED.imos_parameter_uri,
                    unit_name = EXCLUDED.unit_name,
                    unit_uri = EXCLUDED.unit_uri,
                    content_type = EXCLUDED.content_type,
                    is_depth = EXCLUDED.is_depth
                RETURNING id, (xmax = 0) AS inserted
            """, (
                param['metadata_id'],
                param['parameter_code'],
                param['parameter_label'],
                param['standard_name'],
                param['aodn_parameter_uri'],
                param['imos_parameter_uri'],
                param['unit_name'],
                param['unit_uri'],
                param['content_type'],
                param['is_depth']
            ))
            
            param_id, was_inserted = cursor.fetchone()
            
            if was_inserted:
                inserted += 1
                logger.debug(f"      ✅ Inserted parameter: {param['parameter_code']}")
            else:
                updated += 1
                logger.debug(f"      🔄 Updated parameter: {param['parameter_code']}")
            
        except Exception as e:
            logger.error(f"      ❌ Failed to insert/update parameter {param.get('parameter_code')}: {e}")
            conn.rollback()
            continue
    
    conn.commit()
    cursor.close()
    
    logger.info(f"  ✅ Inserted {inserted} parameters, updated {updated}")
    return inserted + updated


# ==============================================================================
# INTEGRATION POINT
# ==============================================================================
# 
# Add this code to your existing populate_metadata_table() function
# AFTER the metadata INSERT/UPDATE succeeds (around line 750):
# 
# try:
#     # Extract and insert parameters from XML
#     xml_path = find_metadata_xml(Path(dataset.get('dataset_path')))
#     if xml_path:
#         tree = ET.parse(xml_path)
#         root = tree.getroot()
#         
#         # Get metadata_id for this dataset
#         cursor.execute(
#             "SELECT id FROM metadata WHERE dataset_path = %s", 
#             (dataset.get('dataset_path'),)
#         )
#         result = cursor.fetchone()
#         if result:
#             metadata_id = result[0]
#             
#             # Extract parameters from XML
#             parameters = extract_parameters_from_xml(root, metadata_id)
#             
#             if parameters:
#                 params_inserted = insert_parameters(conn, metadata_id, parameters)
#                 logger.info(f"    ✅ Processed {params_inserted} parameters")
#             else:
#                 logger.debug(f"    ○ No parameters found in XML")
#     
# except Exception as e:
#     logger.error(f"    ❌ Error extracting parameters: {e}")
#     import traceback
#     logger.debug(traceback.format_exc())
# 
# ==============================================================================


if __name__ == '__main__':
    print("""
    This is an ENHANCEMENT module for populate_metadata.py
    
    To use:
    1. Copy extract_parameters_from_xml() and insert_parameters() functions
    2. Add them to your existing populate_metadata.py
    3. Integrate the code block shown above into populate_metadata_table()
    4. Run: python populate_metadata.py --force
    
    Result: Parameters table will be populated automatically from XML!
    """)
