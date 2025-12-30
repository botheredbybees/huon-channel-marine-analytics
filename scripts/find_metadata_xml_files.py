#!/usr/bin/env python3
"""
Searches for metadata.xml files in the nested AODN directory structure.
Pattern: AODN_data/[dataset-name]/[UUID]/metadata/metadata.xml
Or: AODN_data/[dataset-name]/[UUID-or-identifier]/metadata.xml

UUID Extraction: Searches for the UUID pattern (8-4-4-4-12 hex digits) directly.

Usage:
    python scripts/find_metadata_xml_files.py
"""

import os
import sys
import re
from pathlib import Path

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
    """Extract UUID by searching for the UUID pattern directly.
    
    UUIDs in metadata.xml appear early in the file:
    3c42cb06-d153-450f-9e47-6a3ceaaf8d9b
    urn:uuid
    """
    uuid_pattern = re.compile(
        r'([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})'
    )
    
    try:
        # Read first 1000 chars to find UUID near the beginning
        with open(xml_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read(2000)  # Read first 2000 chars
            
            # Find the first UUID (should be the metadata UUID)
            match = uuid_pattern.search(content)
            if match:
                return match.group(1).lower()
        
        return None
    except Exception as e:
        print(f"  ERROR reading file: {e}")
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
    
    # Now match with database records
    print("\n" + "="*100)
    print("DATABASE MATCHING")
    print("="*100)
    
    # These are the UUIDs from the database (from debug_dataset_paths.py output)
    db_uuids = {
        'd3ecc574-b122-59d3-b0e2-9211c24d72f4': 'Living Shorelines Australia',
        '83d737cb-9ffa-5576-9d89-1de509304e6e': 'Remote sensing Giant Kelp',
        'd32ccbe1-d151-53ee-a6b4-dcf10cbc0eb5': 'Baseline coastal estuarine',
        'f8c506b7-8b49-5951-b7d2-c658e014b9e1': 'Ocean acidification',
        '8f7d1eb7-e129-58bc-922f-9750821f329f': 'IMOS SST L4 GAMSSA',
        '8939735b-dcee-57c6-af1f-8d25f712a774': 'IMOS Ocean Colour',
        '275efd89-f750-5826-a05c-04cd0371bb43': 'Nearshore temperature monitoring',
        'ea696a80-8326-54e1-a96b-2f0dd1bd16b1': 'Pigment sampling',
        'dab97c1c-a55f-5b70-8e06-4def6cd19a9f': 'IMOS Reef Monitoring',
        '31bcd675-9351-595b-a7e8-702404a21300': 'Seagrass Presence Absence',
        'ff59349e-8ca0-5520-a9ef-24aac45c6109': 'IMOS Surface Waves',
        'ac77f84b-3c19-5fad-b52c-783c0e082eb0': 'National Outfall Database',
        '06eb6835-2367-5911-860c-6a2c07dd2632': 'Chlorophyll sampling',
        '00a99b88-27fa-5449-9f50-8f0d35de6faa': 'IMOS AUV',
        '5fa47a25-d181-5fb1-bba4-4f53f1ee0a2d': 'SeaMap Tasmania',
        '1aa76be9-725a-5edf-bb3c-31fd80be6936': 'CAMRIS Seagrass',
        'd8831d07-eb43-524f-84be-b8bedb6f25c5': 'Australian Phytoplankton Database',
        'c91d9b08-f7ee-5f0b-bc49-3b3349a827db': 'IMOS UMI Annotation counts',
        'b41f8abb-8484-5684-b43b-7bb9bef44dc3': 'Australian Seagrass distribution',
        'bd67b0bf-c423-55a7-ae18-79652cf7e9f2': 'Rocky reef communities fish surveys',
        '847ec62b-b55e-56d1-83f6-3469ef359181': 'Redmap range shifting',
        '9e716108-a89c-5f5c-b13c-8b7b497e906e': 'IMOS Satellite Altimetry',
        '0acfdc00-b2d2-59e7-8183-77d4a1b518f2': 'Video surveys sea urchin',
        'f34113ef-0e24-5bc2-8008-280b4b110a36': 'Australian Chlorophyll a Database',
        'bcb98cac-ffa2-5f4a-8693-18aac1edd659': 'IMOS MODIS Chlorophyll-a',
        '56437621-31c6-5f07-90d3-621709986339': 'Aerial surveys giant kelp 2019',
        '225e148f-8cf7-5812-8131-c50bba3f9943': 'Wave buoys',
        '15742a7d-4d5a-5a9b-8364-40e850e2b1df': 'IMOS Larval Fish',
        '44307d39-e279-5084-b9d7-83a29df667ff': 'Estuarine Health Tasmania',
        'f93bd7ae-7ac5-51fd-bc60-ed3bef1fb712': 'Oceanography sampling',
        'b4f2548c-400d-5a19-8a01-f991ed4e943d': 'Zooplankton sampling',
        '78a99924-bad0-59d1-904f-95260ed35db5': 'IMOS UMI Imagery Tracks',
        '737198ca-6ecd-5b35-9cbb-c0c23aadf752': 'Spotted Handfish',
        '025c010e-0f08-5219-a706-26f427c2acf9': 'Phytoplankton sampling',
        'bb54e584-dac8-5b80-88e8-82fef346b81e': 'Rocky reef algal surveys',
        'ec03e9dc-769d-5b6d-90c6-5fb3a322502d': 'Parks Australia Natural Values',
        '371ed940-fc5f-5b02-82d9-4e0b4115f77d': 'Rocky reef invertebrate surveys',
        'ae179821-9a6c-5e67-aa26-871247d059d7': 'Nutrient sampling'
    }
    
    # Extract filesystem UUIDs
    fs_uuid_to_record = {}
    for record in results:
        if record['uuid']:
            fs_uuid_to_record[record['uuid']] = record
    
    print(f"\nDatabase UUIDs: {len(db_uuids)}")
    print(f"Filesystem UUIDs: {len(fs_uuid_to_record)}")
    
    # Check for matches
    matches = sum(1 for uuid in fs_uuid_to_record if uuid in db_uuids)
    print(f"Matching UUIDs: {matches}")
    
    if matches == 0:
        print("\n⚠️  NO UUID MATCHES FOUND")
        print("\nThis means the UUIDs in the directory names don't match the UUIDs in the database.")
        print("We need to map them using the dataset titles.")
    else:
        print(f"\n✓ Found {matches} matching UUIDs")
    
    print("\n" + "="*100)

if __name__ == '__main__':
    main()
