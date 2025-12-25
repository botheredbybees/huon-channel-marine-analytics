#!/usr/bin/env python3
"""
Diagnostic ETL Script - Updated for New Schema (2025)

Enhanced validation for compatibility with:
- measurements (TimescaleDB hypertable)
- locations (PostGIS spatial points)
- parameter_mappings (BODC/CF standardization)
- taxonomy & species_observations
- spatial_features (GIS geometries)
- metadata (ISO 19115 normalized)

Authors: Peter Shanks, Perplexity AI Assistant
Last Updated: December 2025
"""

import os
import json
import sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime
import re

# AODN data directory
DATA_DIR = Path('AODN_data')
REPORT_FILE = 'diagnostic_report.txt'
JSON_REPORT = 'diagnostic_report.json'

# Schema-specific patterns
MEASUREMENT_PARAMS = [
    'temp', 'temperature', 'sal', 'salinity', 'oxygen', 'do', 'chloro',
    'ph', 'turb', 'conductivity', 'density', 'pressure', 'depth'
]

BIOLOGICAL_INDICATORS = [
    'species', 'genus', 'family', 'phylum', 'kingdom', 'scientific',
    'common_name', 'observation', 'abundance', 'count'
]

SPATIAL_COLUMNS = ['lat', 'latitude', 'lon', 'longitude', 'geometry', 'geom']

def scan_directory():
    """Scan AODN_data directory and categorize files."""
    results = {
        'summary': {
            'total_files': 0,
            'by_type': defaultdict(int),
            'scan_date': datetime.now().isoformat()
        },
        'datasets': [],
        'schema_compatibility': {
            'measurements_ready': 0,
            'locations_ready': 0,
            'biological_ready': 0,
            'spatial_features_ready': 0,
            'needs_parameter_mapping': 0
        },
        'issues': []
    }
    
    if not DATA_DIR.exists():
        results['issues'].append(f"Data directory not found: {DATA_DIR}")
        return results
    
    for root, dirs, files in os.walk(DATA_DIR):
        for file in files:
            filepath = Path(root) / file
            ext = filepath.suffix.lower()
            
            results['summary']['total_files'] += 1
            results['summary']['by_type'][ext] += 1
            
            # Analyze file based on type
            if ext == '.nc':
                analyze_netcdf(filepath, results)
            elif ext == '.csv':
                analyze_csv(filepath, results)
            elif ext == '.xml':
                analyze_xml(filepath, results)
            elif ext in ['.shp', '.geojson', '.kml']:
                analyze_spatial(filepath, results)
    
    return results

def analyze_netcdf(filepath, results):
    """Analyze NetCDF file for schema compatibility."""
    try:
        import netCDF4 as nc
        
        dataset_info = {
            'file': str(filepath),
            'type': 'netcdf',
            'measurements_compatible': False,
            'locations_compatible': False,
            'parameters': [],
            'spatial_coverage': None,
            'temporal_coverage': None,
            'issues': []
        }
        
        with nc.Dataset(filepath, 'r') as ds:
            # Check for measurement parameters
            params_found = []
            for var in ds.variables.keys():
                var_lower = var.lower()
                if any(p in var_lower for p in MEASUREMENT_PARAMS):
                    params_found.append(var)
                    dataset_info['measurements_compatible'] = True
            
            dataset_info['parameters'] = params_found
            
            # Check spatial coverage
            has_lat = any('lat' in v.lower() for v in ds.variables.keys())
            has_lon = any('lon' in v.lower() for v in ds.variables.keys())
            
            if has_lat and has_lon:
                dataset_info['locations_compatible'] = True
                try:
                    lat_var = [v for v in ds.variables.keys() if 'lat' in v.lower()][0]
                    lon_var = [v for v in ds.variables.keys() if 'lon' in v.lower()][0]
                    
                    lats = ds.variables[lat_var][:]
                    lons = ds.variables[lon_var][:]
                    
                    dataset_info['spatial_coverage'] = {
                        'min_lat': float(lats.min()),
                        'max_lat': float(lats.max()),
                        'min_lon': float(lons.min()),
                        'max_lon': float(lons.max())
                    }
                except Exception as e:
                    dataset_info['issues'].append(f"Spatial extraction error: {e}")
            
            # Check temporal coverage
            if 'time' in ds.variables:
                try:
                    times = ds.variables['time'][:]
                    dataset_info['temporal_coverage'] = {
                        'start': str(times.min()),
                        'end': str(times.max())
                    }
                except Exception as e:
                    dataset_info['issues'].append(f"Temporal extraction error: {e}")
        
        results['datasets'].append(dataset_info)
        
        # Update compatibility counts
        if dataset_info['measurements_compatible']:
            results['schema_compatibility']['measurements_ready'] += 1
        if dataset_info['locations_compatible']:
            results['schema_compatibility']['locations_ready'] += 1
        if params_found:
            results['schema_compatibility']['needs_parameter_mapping'] += 1
            
    except ImportError:
        results['issues'].append(f"netCDF4 not installed, skipping {filepath}")
    except Exception as e:
        results['issues'].append(f"Error analyzing {filepath}: {e}")

def analyze_csv(filepath, results):
    """Analyze CSV file for schema compatibility."""
    try:
        import pandas as pd
        
        dataset_info = {
            'file': str(filepath),
            'type': 'csv',
            'measurements_compatible': False,
            'locations_compatible': False,
            'biological_compatible': False,
            'parameters': [],
            'biological_columns': [],
            'issues': []
        }
        
        # Read first few rows
        df = pd.read_csv(filepath, nrows=100)
        columns_lower = [c.lower() for c in df.columns]
        
        # Check for measurement parameters
        params_found = []
        for col in df.columns:
            col_lower = col.lower()
            if any(p in col_lower for p in MEASUREMENT_PARAMS):
                params_found.append(col)
                dataset_info['measurements_compatible'] = True
        
        dataset_info['parameters'] = params_found
        
        # Check spatial columns
        has_spatial = any(sc in columns_lower for sc in SPATIAL_COLUMNS)
        if has_spatial:
            dataset_info['locations_compatible'] = True
        
        # Check biological data
        bio_cols = []
        for col in df.columns:
            col_lower = col.lower()
            if any(b in col_lower for b in BIOLOGICAL_INDICATORS):
                bio_cols.append(col)
                dataset_info['biological_compatible'] = True
        
        dataset_info['biological_columns'] = bio_cols
        
        results['datasets'].append(dataset_info)
        
        # Update compatibility counts
        if dataset_info['measurements_compatible']:
            results['schema_compatibility']['measurements_ready'] += 1
        if dataset_info['locations_compatible']:
            results['schema_compatibility']['locations_ready'] += 1
        if dataset_info['biological_compatible']:
            results['schema_compatibility']['biological_ready'] += 1
        if params_found:
            results['schema_compatibility']['needs_parameter_mapping'] += 1
            
    except Exception as e:
        results['issues'].append(f"Error analyzing {filepath}: {e}")

def analyze_xml(filepath, results):
    """Analyze XML metadata file."""
    try:
        import xml.etree.ElementTree as ET
        
        dataset_info = {
            'file': str(filepath),
            'type': 'metadata_xml',
            'has_iso19115': False,
            'parameters_mentioned': [],
            'issues': []
        }
        
        tree = ET.parse(filepath)
        root = tree.getroot()
        
        # Check for ISO 19115 namespace
        if 'iso' in root.tag.lower() or 'gmd' in root.tag.lower():
            dataset_info['has_iso19115'] = True
        
        # Extract parameter mentions from text content
        text_content = ' '.join(root.itertext()).lower()
        params_found = [p for p in MEASUREMENT_PARAMS if p in text_content]
        dataset_info['parameters_mentioned'] = params_found
        
        results['datasets'].append(dataset_info)
        
    except Exception as e:
        results['issues'].append(f"Error analyzing {filepath}: {e}")

def analyze_spatial(filepath, results):
    """Analyze spatial file (Shapefile, GeoJSON, KML)."""
    dataset_info = {
        'file': str(filepath),
        'type': 'spatial_file',
        'spatial_features_compatible': True,
        'issues': []
    }
    
    results['datasets'].append(dataset_info)
    results['schema_compatibility']['spatial_features_ready'] += 1

def generate_report(results):
    """Generate human-readable report."""
    report = []
    report.append("="*80)
    report.append("DIAGNOSTIC ETL REPORT - SCHEMA COMPATIBILITY")
    report.append("="*80)
    report.append(f"Scan Date: {results['summary']['scan_date']}")
    report.append(f"Total Files: {results['summary']['total_files']}")
    report.append("")
    
    # File type breakdown
    report.append("File Types:")
    for ext, count in results['summary']['by_type'].items():
        report.append(f"  {ext}: {count}")
    report.append("")
    
    # Schema compatibility
    report.append("Schema Compatibility:")
    compat = results['schema_compatibility']
    report.append(f"  Measurements table ready: {compat['measurements_ready']} datasets")
    report.append(f"  Locations table ready: {compat['locations_ready']} datasets")
    report.append(f"  Biological data ready: {compat['biological_ready']} datasets")
    report.append(f"  Spatial features ready: {compat['spatial_features_ready']} datasets")
    report.append(f"  Need parameter mapping: {compat['needs_parameter_mapping']} datasets")
    report.append("")
    
    # Dataset details
    report.append("Dataset Details:")
    report.append("-"*80)
    for ds in results['datasets']:
        report.append(f"\nFile: {ds['file']}")
        report.append(f"Type: {ds['type']}")
        
        if 'measurements_compatible' in ds:
            report.append(f"  Measurements compatible: {ds['measurements_compatible']}")
            if ds['parameters']:
                report.append(f"  Parameters: {', '.join(ds['parameters'])}")
        
        if 'locations_compatible' in ds:
            report.append(f"  Locations compatible: {ds['locations_compatible']}")
            if ds.get('spatial_coverage'):
                sc = ds['spatial_coverage']
                report.append(f"  Spatial coverage: lat({sc['min_lat']:.2f}, {sc['max_lat']:.2f}), ")
                report.append(f"                    lon({sc['min_lon']:.2f}, {sc['max_lon']:.2f})")
        
        if 'biological_compatible' in ds and ds['biological_compatible']:
            report.append(f"  Biological data: {', '.join(ds['biological_columns'])}")
        
        if ds.get('issues'):
            report.append("  Issues:")
            for issue in ds['issues']:
                report.append(f"    - {issue}")
    
    # Overall issues
    if results['issues']:
        report.append("")
        report.append("Overall Issues:")
        report.append("-"*80)
        for issue in results['issues']:
            report.append(f"  - {issue}")
    
    report.append("")
    report.append("="*80)
    report.append("END OF REPORT")
    report.append("="*80)
    
    return '\n'.join(report)

def main():
    print("Starting diagnostic scan...")
    results = scan_directory()
    
    # Generate text report
    report = generate_report(results)
    with open(REPORT_FILE, 'w') as f:
        f.write(report)
    print(f"Report saved to {REPORT_FILE}")
    
    # Generate JSON report
    with open(JSON_REPORT, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"JSON report saved to {JSON_REPORT}")
    
    # Print summary to console
    print("\n" + report)

if __name__ == '__main__':
    main()
