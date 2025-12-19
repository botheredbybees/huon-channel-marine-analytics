#!/usr/bin/env python3
"""
Migration script to populate parameter_mappings table from config_parameter_mapping.json

Usage:
    python populate_parameter_mappings.py
    
This script:
1. Reads config_parameter_mapping.json
2. Connects to the database
3. Populates the parameter_mappings table
4. Can be run multiple times (uses ON CONFLICT DO NOTHING)
"""

import json
import psycopg2
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Database connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'marine_db',
    'user': 'marine_user',
    'password': 'marine_pass123'
}

def load_json_config(json_path='config_parameter_mapping.json'):
    """
    Load parameter mappings from JSON file.
    
    Args:
        json_path: Path to JSON config file
        
    Returns:
        dict: Parameter mappings
    """
    config_file = Path(json_path)
    
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {json_path}")
    
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    logger.info(f"Loaded config from {json_path}")
    return config

def connect_to_database():
    """
    Connect to PostgreSQL database.
    
    Returns:
        psycopg2.connection: Database connection
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Connected to database successfully")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Database connection failed: {e}")
        raise

def populate_parameter_mappings(conn, config):
    """
    Populate parameter_mappings table from JSON config.
    
    Args:
        conn: Database connection
        config: JSON config dictionary
    """
    cursor = conn.cursor()
    
    # Get parameter mappings from config
    parameter_mapping = config.get('parameter_mapping', {})
    
    if not parameter_mapping:
        logger.warning("No parameter mappings found in config")
        return
    
    # SQL insert statement
    insert_sql = """
        INSERT INTO parameter_mappings 
            (raw_parameter_name, standard_code, namespace, unit, source, description)
        VALUES 
            (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (raw_parameter_name) DO NOTHING;
    """
    
    inserted_count = 0
    skipped_count = 0
    
    for raw_name, mapping in parameter_mapping.items():
        # Parse mapping: [standard_code, namespace, unit]
        if len(mapping) != 3:
            logger.warning(f"Invalid mapping for {raw_name}: {mapping}")
            continue
        
        standard_code, namespace, unit = mapping
        
        # Generate description
        description = generate_description(raw_name, standard_code, namespace)
        
        try:
            cursor.execute(insert_sql, (
                raw_name,
                standard_code,
                namespace,
                unit,
                'system',  # Source: system (from config)
                description
            ))
            
            if cursor.rowcount > 0:
                inserted_count += 1
            else:
                skipped_count += 1
                
        except psycopg2.Error as e:
            logger.error(f"Failed to insert {raw_name}: {e}")
            continue
    
    conn.commit()
    logger.info(f"Inserted {inserted_count} new mappings")
    logger.info(f"Skipped {skipped_count} existing mappings")
    
    cursor.close()

def generate_description(raw_name, standard_code, namespace):
    """
    Generate a human-readable description for a parameter.
    
    Args:
        raw_name: Raw parameter name
        standard_code: Standardized code
        namespace: Namespace (bodc, cf, custom)
        
    Returns:
        str: Description
    """
    # Common parameter descriptions
    descriptions = {
        'TEMP': 'Sea water temperature',
        'SST': 'Sea surface temperature',
        'PSAL': 'Practical salinity',
        'CPHL': 'Chlorophyll-a concentration',
        'DOXY': 'Dissolved oxygen',
        'PH': 'pH',
        'DEPTH': 'Depth below surface',
        'PRES': 'Sea water pressure',
        'COND': 'Electrical conductivity',
        'TURB': 'Turbidity',
        'SCAT': 'Optical backscatter',
        'FLUO': 'Fluorescence',
        'VELOCITY_U': 'Eastward velocity',
        'VELOCITY_V': 'Northward velocity',
        'VELOCITY_W': 'Upward velocity',
        'WAVE_HGT': 'Wave height',
        'WAVE_PER': 'Wave period',
        'WIND_SPEED': 'Wind speed',
        'WIND_U': 'Eastward wind component',
        'WIND_V': 'Northward wind component',
        'CURRENT_SPEED': 'Current speed',
        'CURRENT_DIR': 'Current direction',
        'ZOOP_COUNT': 'Zooplankton count',
        'PHYTO_COUNT': 'Phytoplankton count',
        'ABUNDANCE': 'Species abundance',
        'BIOMASS': 'Biomass',
        'DENSITY': 'Population density',
        'NO3': 'Nitrate',
        'PO4': 'Phosphate',
        'SIO4': 'Silicate',
        'NH4': 'Ammonium',
        'LATITUDE': 'Latitude coordinate',
        'LONGITUDE': 'Longitude coordinate'
    }
    
    base_description = descriptions.get(standard_code, standard_code)
    
    # Add namespace context
    if namespace == 'cf':
        return f"{base_description} (CF standard name)"
    elif namespace == 'bodc':
        return f"{base_description} (BODC P01 code)"
    else:
        return base_description

def verify_population(conn):
    """
    Verify that parameter mappings were populated.
    
    Args:
        conn: Database connection
    """
    cursor = conn.cursor()
    
    # Count total mappings
    cursor.execute("SELECT COUNT(*) FROM parameter_mappings;")
    total_count = cursor.fetchone()[0]
    
    logger.info(f"Total parameter mappings in database: {total_count}")
    
    # Show sample mappings
    cursor.execute("""
        SELECT raw_parameter_name, standard_code, namespace, unit
        FROM parameter_mappings
        ORDER BY namespace, standard_code
        LIMIT 10;
    """)
    
    logger.info("Sample mappings:")
    for row in cursor.fetchall():
        logger.info(f"  {row[0]:30} -> {row[1]:15} ({row[2]:6}) [{row[3]}]")
    
    # Count by namespace
    cursor.execute("""
        SELECT namespace, COUNT(*) 
        FROM parameter_mappings 
        GROUP BY namespace 
        ORDER BY namespace;
    """)
    
    logger.info("\nMappings by namespace:")
    for row in cursor.fetchall():
        logger.info(f"  {row[0]:10} : {row[1]} mappings")
    
    cursor.close()

def main():
    """
    Main execution function.
    """
    logger.info("="*60)
    logger.info("Parameter Mappings Migration Script")
    logger.info("="*60)
    
    try:
        # Load JSON config
        config = load_json_config()
        
        # Connect to database
        conn = connect_to_database()
        
        # Populate parameter mappings
        populate_parameter_mappings(conn, config)
        
        # Verify population
        verify_population(conn)
        
        # Close connection
        conn.close()
        logger.info("\nMigration completed successfully!")
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise

if __name__ == '__main__':
    main()
