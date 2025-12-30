#!/usr/bin/env python3
"""
Searches for metadata.xml files in the nested AODN directory structure.
Pattern: AODN_data/[dataset-name]/[UUID]/metadata/metadata.xml
Or: AODN_data/[dataset-name]/[UUID-or-identifier]/metadata.xml

Usage:
    python scripts/find_metadata_xml_files.py
"""

import os
import sys
from pathlib import Path
import xml.etree.ElementTree as ET

def find_metadata_xml_files(root_path="AODN_data"):
    """Recursively find all metadata.xml files in directory tree."""
    metadata_files = []
    
    root = Path(root_path)
    if not root.exists():
        print(f"ERROR: Root path does not exist: {root_path}")
        return metadata_files
    
    # Find all metadata.xml files recursively
    for xml_file in root.rglob('metadata.xml'):
        metadata_files.append(xml_file)
    
    return sorted(metadata_files)

def extract_uuid_from_xml(xml_path):
    """Extract UUID from ISO 19115-3 metadata XML file."""
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        # Try various XPath expressions for UUID
        # ISO 19115-3 uses gmd namespace
        namespaces = {
            'gmd': 'http://www.opengis.net/gmd',
            'gco': 'http://www.opengis.net/gco',
            'mdb': 'http://standards.iso.org/iso/19115/-3/mdb/2.0'
        }
        
        # Try gmd:uuid (older ISO 19115-2 style)
        uuid_elem = root.find('.//gmd:uuid', namespaces)
        if uuid_elem is not None:
            uuid_text = uuid_elem.find('gco:CharacterString', namespaces)
            if uuid_text is not None and uuid_text.text:
                return uuid_text.text.strip()
        
        # Try mdb:metadataIdentifier (ISO 19115-3 style)
        uuid_elem = root.find('.//mdb:metadataIdentifier', namespaces)
        if uuid_elem is not None:
            uuid_text = uuid_elem.find('.//mcc:code', {'mcc': 'http://standards.iso.org/iso/19115/-3/mcc/1.0'})
            if uuid_text is not None and uuid_text.text:
                return uuid_text.text.strip()
        
        # Fallback: look for any element containing 'uuid' text
        for elem in root.iter():
            if elem.text and len(elem.text.strip()) == 36:  # UUID format length
                text = elem.text.strip()
                if all(c in '0123456789-abcdef' for c in text.lower()):
                    # Looks like a UUID
                    parts = text.split('-')
                    if len(parts) == 5 and len(parts[0]) == 8:
                        return text
        
        return None
    except Exception as e:
        print(f"  ERROR parsing XML: {e}")
        return None

def main():
    print("\n" + "="*100)
    print("SEARCHING FOR METADATA.XML FILES IN AODN DIRECTORY")
    print("="*100)
    
    # Find all metadata.xml files
    xml_files = find_metadata_xml_files()
    
    print(f"\nFound {len(xml_files)} metadata.xml files\n")
    
    # Extract UUIDs from each file
    results = []
    for i, xml_path in enumerate(xml_files, 1):
        relative_path = xml_path.relative_to(Path('AODN_data')).parent.parent.name
        uuid = extract_uuid_from_xml(xml_path)
        
        results.append({
            'number': i,
            'path': str(xml_path),
            'dataset_dir': relative_path,
            'uuid': uuid
        })
        
        print(f"{i}. {relative_path}")
        print(f"   File: {xml_path}")
        print(f"   UUID: {uuid if uuid else 'NOT FOUND'}")
        print()
    
    # Summary
    print("\n" + "="*100)
    print("SUMMARY")
    print("="*100)
    print(f"Total metadata.xml files found: {len(results)}")
    print(f"UUIDs extracted: {sum(1 for r in results if r['uuid'])}")
    print(f"Missing UUIDs: {sum(1 for r in results if not r['uuid'])}")
    
    # Save to CSV for inspection
    import csv
    csv_path = Path('metadata_files_found.csv')
    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['number', 'dataset_dir', 'uuid', 'path'])
        writer.writeheader()
        writer.writerows(results)
    
    print(f"\nResults saved to: {csv_path}")
    print("\n" + "="*100)

if __name__ == '__main__':
    main()
