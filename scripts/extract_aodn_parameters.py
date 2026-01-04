#!/usr/bin/env python3
"""
Extract parameters from ISO19115-3 metadata XML files and add to parameters table and JSON config.
This script parses AODN metadata files to discover water quality parameters.
"""

import xml.etree.ElementTree as ET
import json
import psycopg2
from psycopg2.extras import execute_values
import sys
from pathlib import Path

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


def extract_params_from_xml(xml_file_path):
    """
    Extract parameter information from ISO19115-3 metadata XML.
    
    Returns: List of dicts containing parameter info
    """
    tree = ET.parse(xml_file_path)
    root = tree.getroot()
    
    parameters = []
    
    # Find all MDSampleDimension elements which contain parameter definitions
    for sample_dim in root.findall('.//mrc:MDSampleDimension', NAMESPACES):
        param_info = {}
        
        # Get parameter name - can be in mcc:code/gcx:Anchor or mcc:code/gco:CharacterString
        name_elem = sample_dim.find('.//mcc:code/gcx:Anchor', NAMESPACES)
        if name_elem is not None:
            param_info['name'] = name_elem.text
            param_info['uri'] = name_elem.get('{http://www.w3.org/1999/xlink}href', '')
        else:
            name_elem = sample_dim.find('.//mcc:code/gco:CharacterString', NAMESPACES)
            if name_elem is not None:
                param_info['name'] = name_elem.text
                param_info['uri'] = ''
        
        # Get unit information from gml:BaseUnit
        unit_name_elem = sample_dim.find('.//gml:name', NAMESPACES)
        if unit_name_elem is not None:
            param_info['unit'] = unit_name_elem.text
        
        unit_id_elem = sample_dim.find('.//gml:identifier', NAMESPACES)
        if unit_id_elem is not None:
            param_info['unit_uri'] = unit_id_elem.text
            
        # Get description if available
        desc_elem = sample_dim.find('.//mrc:description/gco:CharacterString', NAMESPACES)
        if desc_elem is not None and desc_elem.text not in ['missing', None]:
            param_info['description'] = desc_elem.text
        
        # Only add if we have at least a name
        if 'name' in param_info and param_info['name']:
            parameters.append(param_info)
    
    return parameters


def standardize_parameter_name(name):
    """Convert parameter name to standardized format for database."""
    # Remove special characters and normalize
    name = name.strip()
    # Keep the full descriptive name
    return name


def get_db_connection():
    """Create database connection."""
    return psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="marine_db",
        user="marine_user",
        password="marine_pass123"
    )


def add_parameters_to_db(parameters):
    """
    Add extracted parameters to the database parameters table.
    Handles duplicates gracefully.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    added = 0
    skipped = 0
    
    for param in parameters:
        try:
            # Check if parameter already exists
            cursor.execute(
                "SELECT id FROM parameters WHERE name = %s",
                (param['name'],)
            )
            existing = cursor.fetchone()
            
            if existing:
                print(f"  ⏭️  Skipped (exists): {param['name']}")
                skipped += 1
                continue
            
            # Insert new parameter
            cursor.execute("""
                INSERT INTO parameters (name, unit, description)
                VALUES (%s, %s, %s)
                RETURNING id
            """, (
                param['name'],
                param.get('unit', ''),
                param.get('description', '')
            ))
            
            param_id = cursor.fetchone()[0]
            print(f"  ✅ Added: {param['name']} (ID: {param_id})")
            added += 1
            
        except Exception as e:
            print(f"  ❌ Error adding {param.get('name', 'unknown')}: {e}")
            conn.rollback()
            continue
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return added, skipped


def update_json_config(parameters, json_path):
    """
    Update the parameter mappings JSON configuration file.
    Adds new mappings for discovered parameters.
    """
    # Load existing config
    with open(json_path, 'r') as f:
        config = json.load(f)
    
    added = 0
    
    for param in parameters:
        param_name = param['name']
        
        # Check if already in config
        existing = False
        for dataset in config.get('datasets', []):
            if param_name in dataset.get('column_mappings', {}):
                existing = True
                break
        
        if existing:
            continue
        
        # Create a suggested mapping entry (you'll need to customize this)
        # For now, we'll add it as a comment/template
        suggested_mapping = {
            "aodn_parameter": param_name,
            "unit": param.get('unit', ''),
            "uri": param.get('uri', ''),
            "description": param.get('description', ''),
            "suggested_column_names": []  # User will need to fill this
        }
        
        print(f"  📝 New parameter for JSON: {param_name}")
        added += 1
    
    # Note: Actual JSON update would require more context about structure
    # This is a placeholder for manual review
    
    return added


def main():
    """Main execution function."""
    print("=" * 80)
    print("ISO19115-3 METADATA PARAMETER EXTRACTOR")
    print("=" * 80)
    
    # Parse command line arguments or use defaults
    xml_files = sys.argv[1:] if len(sys.argv) > 1 else ['metadata.xml']
    
    all_parameters = []
    
    for xml_file in xml_files:
        print(f"\n📄 Processing: {xml_file}")
        
        try:
            params = extract_params_from_xml(xml_file)
            print(f"   Found {len(params)} parameters")
            all_parameters.extend(params)
            
            for p in params[:3]:  # Show first 3 as examples
                print(f"     • {p['name'][:60]}...")
            if len(params) > 3:
                print(f"     ... and {len(params) - 3} more")
                
        except Exception as e:
            print(f"   ❌ Error parsing {xml_file}: {e}")
            continue
    
    if not all_parameters:
        print("\n⚠️  No parameters found in XML files")
        return
    
    print(f"\n📊 Total parameters extracted: {len(all_parameters)}")
    print(f"   Unique parameters: {len(set(p['name'] for p in all_parameters))}")
    
    # Add to database
    print("\n💾 Adding parameters to database...")
    added, skipped = add_parameters_to_db(all_parameters)
    print(f"   Added: {added}, Skipped: {skipped}")
    
    # Update JSON (placeholder)
    print("\n📝 Checking JSON configuration...")
    # json_path = 'config_parameter_mapping.json'
    # update_json_config(all_parameters, json_path)
    print("   Note: Review parameters and update JSON mappings manually")
    
    print("\n✅ Done!")


if __name__ == '__main__':
    main()
