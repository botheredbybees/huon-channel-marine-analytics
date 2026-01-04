#!/usr/bin/env python3
"""
Parameter Coverage Analysis Tool

Analyzes the relationship between:
1. parameters table (extracted from AODN metadata)
2. parameter_mappings table (standardized mappings)
3. measurements table (actual data)

Outputs to logs/ directory:
- parameter_coverage.csv: Full analysis with mapping suggestions
- unmeasured_parameters.csv: Parameters without measurements
- parameter_statistics.csv: Summary statistics
"""

import psycopg2
import csv
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import re

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'marine_db',
    'user': 'marine_user',
    'password': 'marine_pass123'
}

def get_db_connection():
    """Create database connection."""
    return psycopg2.connect(**DB_CONFIG)

def normalize_parameter_name(name: str) -> str:
    """
    Normalize parameter name for fuzzy matching.
    Converts to uppercase, removes special chars, standardizes spacing.
    """
    if not name:
        return ''
    # Convert to uppercase
    normalized = name.upper()
    # Remove special characters except spaces
    normalized = re.sub(r'[^A-Z0-9\s]', ' ', normalized)
    # Collapse multiple spaces
    normalized = re.sub(r'\s+', ' ', normalized)
    # Strip
    normalized = normalized.strip()
    return normalized

def find_potential_mappings(param_code: str, param_label: str, mappings: list) -> list:
    """
    Find potential standard mappings for a parameter.
    Returns list of (mapping_id, raw_parameter_name, standard_code, confidence)
    """
    param_norm = normalize_parameter_name(param_code)
    label_norm = normalize_parameter_name(param_label)
    
    matches = []
    
    for mapping in mappings:
        mapping_norm = normalize_parameter_name(mapping['raw_parameter_name'])
        
        # Exact match on normalized names
        if mapping_norm == param_norm:
            matches.append((mapping['id'], mapping['raw_parameter_name'], 
                          mapping['standard_code'], 'EXACT_CODE'))
        elif mapping_norm == label_norm:
            matches.append((mapping['id'], mapping['raw_parameter_name'], 
                          mapping['standard_code'], 'EXACT_LABEL'))
        # Partial match (contains)
        elif mapping_norm and param_norm and (mapping_norm in param_norm or param_norm in mapping_norm):
            matches.append((mapping['id'], mapping['raw_parameter_name'], 
                          mapping['standard_code'], 'PARTIAL_CODE'))
        elif mapping_norm and label_norm and (mapping_norm in label_norm or label_norm in mapping_norm):
            matches.append((mapping['id'], mapping['raw_parameter_name'], 
                          mapping['standard_code'], 'PARTIAL_LABEL'))
    
    # Sort by confidence (exact matches first)
    confidence_order = {'EXACT_CODE': 0, 'EXACT_LABEL': 1, 'PARTIAL_CODE': 2, 'PARTIAL_LABEL': 3}
    matches.sort(key=lambda x: confidence_order.get(x[3], 999))
    
    return matches

def get_parameters_with_metadata(cursor):
    """
    Get all parameters with their metadata information.
    """
    cursor.execute("""
        SELECT 
            p.id,
            p.parameter_code,
            p.parameter_label,
            p.aodn_parameter_uri,
            p.unit_name,
            p.unit_uri,
            p.content_type,
            p.metadata_id,
            m.dataset_name,
            m.dataset_path,
            m.uuid
        FROM parameters p
        JOIN metadata m ON p.metadata_id = m.id
        ORDER BY p.parameter_code, m.dataset_name
    """)
    
    columns = [desc[0] for desc in cursor.description]
    results = []
    for row in cursor.fetchall():
        results.append(dict(zip(columns, row)))
    return results

def get_parameter_mappings(cursor):
    """
    Get all parameter mappings.
    """
    cursor.execute("""
        SELECT 
            id,
            raw_parameter_name,
            standard_code,
            namespace,
            unit,
            description
        FROM parameter_mappings
        ORDER BY standard_code, raw_parameter_name
    """)
    
    columns = [desc[0] for desc in cursor.description]
    results = []
    for row in cursor.fetchall():
        results.append(dict(zip(columns, row)))
    return results

def get_measurement_counts(cursor):
    """
    Get count of measurements per parameter_code.
    FIXED: measurements table uses parameter_code (TEXT), not parameter_id.
    """
    cursor.execute("""
        SELECT 
            parameter_code,
            COUNT(*) as measurement_count,
            MIN(time) as first_measurement,
            MAX(time) as last_measurement
        FROM measurements
        GROUP BY parameter_code
    """)
    
    result = {}
    for row in cursor.fetchall():
        result[row[0]] = {
            'count': row[1],
            'first': row[2],
            'last': row[3]
        }
    return result

def analyze_parameter_coverage():
    """
    Main analysis function.
    """
    print("="*80)
    print("PARAMETER COVERAGE ANALYSIS")
    print("="*80)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get data
    print("\n📊 Fetching data...")
    parameters = get_parameters_with_metadata(cursor)
    mappings = get_parameter_mappings(cursor)
    measurement_counts = get_measurement_counts(cursor)
    
    print(f"   Parameters: {len(parameters)}")
    print(f"   Parameter Mappings: {len(mappings)}")
    print(f"   Unique parameter codes with measurements: {len(measurement_counts)}")
    
    # Analyze each parameter
    print("\n🔍 Analyzing parameters...")
    
    coverage_data = []
    unmeasured_params = []
    
    # Track statistics
    stats = {
        'total_parameters': len(parameters),
        'with_measurements': 0,
        'without_measurements': 0,
        'with_mapping_suggestion': 0,
        'with_aodn_uri': 0,
        'by_dataset': defaultdict(int),
        'by_parameter_code': defaultdict(int)
    }
    
    for param in parameters:
        param_id = param['id']
        param_code = param['parameter_code']
        
        # Check if has measurements - using parameter_code now
        has_measurements = param_code in measurement_counts
        meas_info = measurement_counts.get(param_code, {})
        
        # Find potential mappings
        potential_mappings = find_potential_mappings(
            param['parameter_code'],
            param['parameter_label'] or '',
            mappings
        )
        
        # Get best match
        best_mapping = potential_mappings[0] if potential_mappings else None
        
        # Build coverage record
        record = {
            'parameter_id': param_id,
            'parameter_code': param['parameter_code'],
            'parameter_label': param['parameter_label'] or '',
            'unit_name': param['unit_name'] or '',
            'aodn_uri': param['aodn_parameter_uri'] or '',
            'dataset_name': param['dataset_name'],
            'dataset_uuid': param['uuid'],
            'has_measurements': 'YES' if has_measurements else 'NO',
            'measurement_count': meas_info.get('count', 0),
            'first_measurement': str(meas_info.get('first', '')),
            'last_measurement': str(meas_info.get('last', '')),
            'suggested_mapping': best_mapping[2] if best_mapping else '',
            'mapping_confidence': best_mapping[3] if best_mapping else '',
            'mapping_id': best_mapping[0] if best_mapping else ''
        }
        
        coverage_data.append(record)
        
        # Track unmeasured
        if not has_measurements:
            unmeasured_params.append(record)
        
        # Update statistics
        if has_measurements:
            stats['with_measurements'] += 1
        else:
            stats['without_measurements'] += 1
        
        if best_mapping:
            stats['with_mapping_suggestion'] += 1
        
        if param['aodn_parameter_uri']:
            stats['with_aodn_uri'] += 1
        
        stats['by_dataset'][param['dataset_name']] += 1
        stats['by_parameter_code'][param['parameter_code']] += 1
    
    cursor.close()
    conn.close()
    
    # Output results to logs/ directory
    print("\n💾 Writing reports...")
    
    output_dir = Path('logs')
    output_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # 1. Full coverage report
    coverage_file = output_dir / f'parameter_coverage_{timestamp}.csv'
    with open(coverage_file, 'w', newline='', encoding='utf-8') as f:
        if coverage_data:
            writer = csv.DictWriter(f, fieldnames=coverage_data[0].keys())
            writer.writeheader()
            writer.writerows(coverage_data)
    print(f"   ✅ {coverage_file}")
    
    # 2. Unmeasured parameters
    unmeasured_file = output_dir / f'unmeasured_parameters_{timestamp}.csv'
    with open(unmeasured_file, 'w', newline='', encoding='utf-8') as f:
        if unmeasured_params:
            writer = csv.DictWriter(f, fieldnames=unmeasured_params[0].keys())
            writer.writeheader()
            writer.writerows(unmeasured_params)
    print(f"   ✅ {unmeasured_file}")
    
    # 3. Statistics summary
    stats_file = output_dir / f'parameter_statistics_{timestamp}.csv'
    with open(stats_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Metric', 'Value'])
        writer.writerow(['Total Parameters', stats['total_parameters']])
        writer.writerow(['With Measurements', stats['with_measurements']])
        writer.writerow(['Without Measurements', stats['without_measurements']])
        writer.writerow(['Coverage %', f"{(stats['with_measurements']/stats['total_parameters']*100):.1f}%"])
        writer.writerow(['With AODN URI', stats['with_aodn_uri']])
        writer.writerow(['With Mapping Suggestion', stats['with_mapping_suggestion']])
        writer.writerow([])
        writer.writerow(['Top Parameters by Occurrence', ''])
        for param_code, count in sorted(stats['by_parameter_code'].items(), 
                                       key=lambda x: x[1], reverse=True)[:20]:
            writer.writerow([param_code, count])
        writer.writerow([])
        writer.writerow(['Parameters by Dataset', ''])
        for dataset, count in sorted(stats['by_dataset'].items(), 
                                    key=lambda x: x[1], reverse=True):
            writer.writerow([dataset[:60], count])
    print(f"   ✅ {stats_file}")
    
    # Print summary to console
    print("\n" + "="*80)
    print("📈 SUMMARY")
    print("="*80)
    print(f"Total parameters:              {stats['total_parameters']}")
    print(f"Parameters with measurements:  {stats['with_measurements']} ({stats['with_measurements']/stats['total_parameters']*100:.1f}%)")
    print(f"Parameters WITHOUT measurements: {stats['without_measurements']} ({stats['without_measurements']/stats['total_parameters']*100:.1f}%)")
    print(f"With AODN parameter URI:       {stats['with_aodn_uri']}")
    print(f"With mapping suggestions:      {stats['with_mapping_suggestion']}")
    
    print("\n🔝 Top 10 Most Common Parameters:")
    for param_code, count in sorted(stats['by_parameter_code'].items(), 
                                   key=lambda x: x[1], reverse=True)[:10]:
        print(f"   {param_code[:50]:50} - {count} datasets")
    
    print("\n⚠️  Unmeasured Parameters by Dataset:")
    unmeasured_by_dataset = defaultdict(list)
    for param in unmeasured_params:
        unmeasured_by_dataset[param['dataset_name']].append(param['parameter_code'])
    
    for dataset, params in sorted(unmeasured_by_dataset.items()):
        print(f"\n   {dataset[:60]}:")
        for param_code in params[:5]:  # Show first 5
            print(f"      - {param_code}")
        if len(params) > 5:
            print(f"      ... and {len(params)-5} more")
    
    print("\n" + "="*80)
    print("✅ COMPLETE")
    print("="*80)
    print("\nOutput files in logs/ directory:")
    print(f"  - parameter_coverage_{timestamp}.csv")
    print(f"  - unmeasured_parameters_{timestamp}.csv")
    print(f"  - parameter_statistics_{timestamp}.csv")
    print("="*80)

if __name__ == '__main__':
    try:
        analyze_parameter_coverage()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
