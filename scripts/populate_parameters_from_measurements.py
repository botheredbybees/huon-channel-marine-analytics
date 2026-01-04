#!/usr/bin/env python3
"""
Populate the parameters table from existing measurements and parameter_mappings.
This script addresses the issue where you have 7M measurements but 0 parameter records.

FIXED: Added uuid field generation - the database schema has uuid NOT NULL constraint
"""

import psycopg2
import uuid as uuid_lib
from psycopg2.extras import execute_values
from datetime import datetime

def get_db_connection():
    """Create database connection."""
    return psycopg2.connect(
        host="localhost",
        port=5433,
        database="marine_db",
        user="marine_user",
        password="marine_pass123"
    )


def get_parameters_from_measurements():
    """
    Extract unique parameter codes from measurements table with statistics.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
        SELECT 
            parameter_code,
            COUNT(*) as measurement_count,
            AVG(value) as mean_value,
            STDDEV(value) as std_dev,
            MIN(time) as earliest,
            MAX(time) as latest
        FROM measurements
        GROUP BY parameter_code
        ORDER BY measurement_count DESC
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return results


def get_parameter_mappings():
    """
    Get parameter mappings to enrich parameter definitions.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
        SELECT 
            standard_code,
            unit,
            description,
            namespace
        FROM parameter_mappings
        WHERE standard_code IS NOT NULL
        GROUP BY standard_code, unit, description, namespace
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    # Create lookup dict
    mappings = {}
    for code, unit, desc, namespace in results:
        if code not in mappings:
            mappings[code] = {
                'unit': unit,
                'description': desc,
                'namespace': namespace
            }
    
    cursor.close()
    conn.close()
    
    return mappings


def standardize_parameter_name(code):
    """
    Convert parameter code to human-readable name.
    """
    name_map = {
        'TEMP': 'Temperature',
        'PSAL': 'Salinity',
        'CPHL': 'Chlorophyll-a',
        'DOXY': 'Dissolved Oxygen',
        'PH': 'pH',
        'PRES': 'Pressure',
        'FLUO': 'Fluorescence',
        'NO3': 'Nitrate',
        'PO4': 'Phosphate',
        'SIO4': 'Silicate',
        'AMMONIA': 'Ammonia',
        'NITRITE': 'Nitrite',
        'TOTAL_NITROGEN': 'Total Nitrogen',
        'turbidity': 'Turbidity',
        'current_speed': 'Current Speed',
        'wind_speed': 'Wind Speed',
        'temperature': 'Temperature',
        'salinity': 'Salinity',
        'pressure': 'Pressure',
        'chlorophyll': 'Chlorophyll',
        'oxygen': 'Oxygen',
        'ph': 'pH'
    }
    return name_map.get(code, code.replace('_', ' ').title())


def infer_unit(code, mapping_unit=None):
    """
    Infer unit for parameter if not in mappings.
    """
    if mapping_unit:
        return mapping_unit
    
    unit_map = {
        'TEMP': 'Degrees Celsius',
        'temperature': 'Degrees Celsius',
        'PSAL': 'PSU',
        'salinity': 'PSU',
        'CPHL': 'mg/m³',
        'chlorophyll': 'mg/m³',
        'DOXY': 'ml/L',
        'oxygen': '%',
        'PH': 'pH units',
        'ph': 'pH units',
        'PRES': 'dbar',
        'pressure': 'dbar',
        'FLUO': 'mg/m³',
        'NO3': 'mmol/m³',
        'PO4': 'mmol/m³',
        'SIO4': 'mmol/m³',
        'AMMONIA': 'µg/L',
        'NITRITE': 'µg/L',
        'TOTAL_NITROGEN': 'µg/L',
        'turbidity': 'NTU',
        'current_speed': 'm/s',
        'wind_speed': 'm/s'
    }
    return unit_map.get(code, '')


def populate_parameters():
    """
    Main function to populate parameters table.
    """
    print("=" * 80)
    print("POPULATING PARAMETERS TABLE")
    print("=" * 80)
    
    # Get data
    print("\n1️⃣  Fetching parameter codes from measurements...")
    param_stats = get_parameters_from_measurements()
    print(f"   Found {len(param_stats)} unique parameter codes")
    
    print("\n2️⃣  Loading parameter mappings...")
    mappings = get_parameter_mappings()
    print(f"   Found {len(mappings)} mapped parameters")
    
    # Prepare insert data
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("\n3️⃣  Inserting parameters...")
    inserted = 0
    skipped = 0
    
    for code, count, mean, stddev, earliest, latest in param_stats:
        # Check if already exists (parameter_code is part of UNIQUE constraint with metadata_id)
        # Since we're not linking to specific metadata, just check by code
        cursor.execute("""
            SELECT id FROM parameters 
            WHERE parameter_code = %s AND metadata_id IS NULL
        """, (code,))
        
        if cursor.fetchone():
            print(f"   ⏭️  {code:20} - already exists")
            skipped += 1
            continue
        
        # Get info from mappings or infer
        mapping = mappings.get(code, {})
        name = standardize_parameter_name(code)
        unit = infer_unit(code, mapping.get('unit'))
        description = mapping.get('description', f'{name} measurements')
        
        # Generate UUID for this parameter
        param_uuid = str(uuid_lib.uuid4())
        
        # Insert - NOTE: metadata_id is NULL for global parameters
        try:
            cursor.execute("""
                INSERT INTO parameters (
                    uuid,
                    parameter_code, 
                    parameter_label, 
                    unit_name,
                    standard_name,
                    content_type
                ) VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (param_uuid, code, name, unit, description, 'physicalMeasurement'))
            
            param_id = cursor.fetchone()[0]
            print(f"   ✅ {code:20} - {name:30} [{unit:15}] ({count:>8,} measurements)")
            inserted += 1
            
        except Exception as e:
            print(f"   ❌ {code:20} - Error: {e}")
            conn.rollback()
            continue
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 80)
    print(f"✅ COMPLETE")
    print(f"   Inserted: {inserted} parameters")
    print(f"   Skipped:  {skipped} (already existed)")
    print("=" * 80)
    
    return inserted, skipped


def verify_population():
    """
    Verify that parameters were populated correctly.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM parameters")
    param_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT parameter_code) FROM measurements")
    measurement_codes = cursor.fetchone()[0]
    
    print("\n🔍 VERIFICATION:")
    print(f"   Parameters in database: {param_count}")
    print(f"   Unique codes in measurements: {measurement_codes}")
    
    if param_count >= measurement_codes:
        print("   ✅ All parameter codes have corresponding parameter records!")
    else:
        print(f"   ⚠️  Missing {measurement_codes - param_count} parameter records")
    
    # Show sample
    cursor.execute("""
        SELECT p.parameter_code, p.parameter_label, p.unit_name, COUNT(m.data_id) as measurement_count
        FROM parameters p
        LEFT JOIN measurements m ON m.parameter_code = p.parameter_code
        GROUP BY p.parameter_code, p.parameter_label, p.unit_name
        ORDER BY measurement_count DESC
        LIMIT 10
    """)
    
    print("\n   Top 10 parameters by measurement count:")
    for code, name, unit, count in cursor.fetchall():
        print(f"     • {code:15} {name:30} - {count:>10,} measurements")
    
    cursor.close()
    conn.close()


if __name__ == '__main__':
    try:
        inserted, skipped = populate_parameters()
        verify_population()
        print("\n✨ Success! Your parameters table is now populated.")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
