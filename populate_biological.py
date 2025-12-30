#!/usr/bin/env python3
"""
populate_biological.py - Biological Data Ingestion (PostGIS-Free Version)

Processes species observation and biological survey data from CSV files.
Uses latitude/longitude coordinates instead of PostGIS geometry columns.

Compatible with TimescaleDB Community Edition (no PostGIS dependency).
Version: 2.0 (PostGIS-free)
Last Updated: December 30, 2025
"""

import os
import sys
import argparse
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import numpy as np
import re

# Database Configuration
DB_CONFIG = {
    'dbname': 'marine_db',
    'user': 'marine_user',
    'password': 'marine_pass123',
    'host': 'localhost',
    'port': '5433'
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def parse_wkt_point(wkt_string):
    """Extract lon, lat from WKT POINT string like 'POINT (147.338943481 -43.038341522)'"""
    if pd.isna(wkt_string):
        return None, None
    
    try:
        match = re.search(r'POINT\s*\(\s*([-\d.]+)\s+([-\d.]+)\s*\)', str(wkt_string))
        if match:
            lon = float(match.group(1))
            lat = float(match.group(2))
            return lat, lon
    except:
        pass
    
    return None, None

def get_or_create_location(cur, lat, lon, metadata_id, name=None):
    """Get or create location by lat/lon (PostGIS-free)"""
    if pd.isna(lat) or pd.isna(lon):
        return None
    
    lat, lon = float(lat), float(lon)
    if name is None:
        name = f"Site at {lat:.4f},{lon:.4f}"
    
    # Find existing location within 10m (~0.0001 degrees)
    cur.execute("""
        SELECT id FROM locations 
        WHERE ABS(latitude - %s) < 0.0001 AND ABS(longitude - %s) < 0.0001
    """, (lat, lon))
    res = cur.fetchone()
    if res:
        return res[0]
    
    try:
        # Insert new location without PostGIS geometry
        cur.execute("""
            INSERT INTO locations (location_name, latitude, longitude)
            VALUES (%s, %s, %s)
            RETURNING id
        """, (name, lat, lon))
        return cur.fetchone()[0]
    except:
        return None

def get_or_create_taxonomy(cur, species_name, common_name=''):
    """Get or create taxonomy entry"""
    if pd.isna(species_name) or str(species_name).strip() == '':
        return None
    
    species_name = str(species_name).strip()
    
    cur.execute("SELECT id FROM taxonomy WHERE species_name = %s", (species_name,))
    res = cur.fetchone()
    if res:
        return res[0]
    
    try:
        cur.execute("""
            INSERT INTO taxonomy (species_name, common_name)
            VALUES (%s, %s)
            ON CONFLICT (species_name) DO NOTHING
            RETURNING id
        """, (species_name, common_name))
        res = cur.fetchone()
        if res:
            return res[0]
        # If conflict occurred, fetch the existing ID
        cur.execute("SELECT id FROM taxonomy WHERE species_name = %s", (species_name,))
        return cur.fetchone()[0]
    except:
        return None

def detect_csv_format(df):
    """Detect the format type of biological CSV"""
    cols = df.columns.tolist()
    cols_lower = [c.lower() for c in cols]
    
    # Check for Australian Phytoplankton Database format
    if 'taxon_name' in cols_lower and 'genus' in cols_lower and 'functional_group' in cols_lower:
        return 'australian_phyto'
    
    # Check for matrix format (species as columns with family_genus_species_id pattern)
    species_cols = [c for c in cols if '_' in c and c.count('_') >= 2 and not c.startswith('Sample')]
    if len(species_cols) > 50:  # Larval fish has 200+ species columns
        return 'matrix'
    
    # Check for phytoplankton format
    if 'genus_species' in cols_lower or ('taxon' in cols_lower and 'biovolume' in cols_lower):
        return 'phytoplankton'
    
    # Check for Redmap format
    if 'species' in cols_lower and 'sighting_date' in cols_lower and 'common_name' in cols_lower:
        return 'redmap'
    
    # Check for standard observation format
    if 'species_name' in cols_lower or 'scientific_name' in cols_lower:
        return 'standard'
    
    return 'unknown'

def process_australian_phyto_format(df, metadata_id, cur, conn):
    """Process Australian Phytoplankton Database format (PostGIS-free)"""
    print("  Format: Australian Phytoplankton Database")
    
    col_map = {c.upper(): c for c in df.columns}
    
    taxon_col = col_map.get('TAXON_NAME')
    genus_col = col_map.get('GENUS')
    species_col = col_map.get('SPECIES')
    lat_col = col_map.get('LATITUDE')
    lon_col = col_map.get('LONGITUDE')
    date_col = col_map.get('SAMPLE_TIME_UTC')
    count_col = col_map.get('CELLS_L')
    biovolume_col = col_map.get('BIOVOLUME_UM3_L')
    
    if not all([lat_col, lon_col]):
        print(f"  Warning: Missing required location columns")
        return 0
    
    records_inserted = 0
    for idx, row in df.iterrows():
        if pd.isna(row.get(lat_col)) or pd.isna(row.get(lon_col)):
            continue
        
        # Build species name
        species_name = None
        if genus_col and species_col:
            genus = row.get(genus_col)
            species = row.get(species_col)
            if pd.notna(genus) and pd.notna(species) and str(species).strip() not in ['', 'spp.']:
                species_name = f"{genus} {species}".strip()
            elif pd.notna(genus):
                species_name = f"{genus} spp."
        
        if not species_name and taxon_col:
            taxon = row.get(taxon_col)
            if pd.notna(taxon) and str(taxon).strip() != '':
                species_name = str(taxon).strip()
        
        if not species_name:
            continue
            
        lat, lon = row[lat_col], row[lon_col]
        obs_date = row.get(date_col) if date_col else None
        
        # Get count value
        count = None
        if count_col:
            count_val = row.get(count_col)
            if pd.notna(count_val):
                try:
                    count = float(count_val)
                except:
                    pass
        
        if count is None and biovolume_col:
            biovolume = row.get(biovolume_col)
            if pd.notna(biovolume):
                count = 1
        
        location_id = get_or_create_location(cur, lat, lon, metadata_id)
        if not location_id:
            continue
        
        taxonomy_id = get_or_create_taxonomy(cur, species_name, '')
        if not taxonomy_id:
            continue
        
        try:
            # PostGIS-free: insert lat/lon directly
            cur.execute("""
                INSERT INTO species_observations 
                (metadata_id, location_id, taxonomy_id, observation_date, count_value, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (metadata_id, location_id, taxonomy_id, obs_date, count, lat, lon))
            records_inserted += 1
        except Exception as e:
            conn.rollback()
            continue
        
        if idx % 1000 == 0 and idx > 0:
            conn.commit()
            print(f"  Processed {idx} observations...")
    
    conn.commit()
    return records_inserted

def process_matrix_format(df, metadata_id, cur, conn):
    """Process larval fish matrix format (PostGIS-free)"""
    print("  Format: Matrix (species as columns)")
    
    species_cols = [c for c in df.columns if '_' in c and c.count('_') >= 2 and not c.startswith('Sample')]
    
    if 'Latitude' not in df.columns or 'Longitude' not in df.columns:
        print("  Warning: No Latitude/Longitude columns found")
        return 0
    
    date_col = 'SampleTime_Local' if 'SampleTime_Local' in df.columns else 'SampleTime_UTC'
    
    records_inserted = 0
    for idx, row in df.iterrows():
        if pd.isna(row.get('Latitude')) or pd.isna(row.get('Longitude')):
            continue
            
        lat, lon = row['Latitude'], row['Longitude']
        obs_date = row.get(date_col) if date_col in row else None
        
        location_id = get_or_create_location(cur, lat, lon, metadata_id)
        if not location_id:
            continue
        
        for species_col in species_cols:
            count = row[species_col]
            if pd.notna(count) and count > 0:
                parts = species_col.split('_')
                if len(parts) >= 2:
                    genus_species = parts[1].replace('.', ' ')
                else:
                    genus_species = parts[0]
                
                taxonomy_id = get_or_create_taxonomy(cur, genus_species, '')
                if not taxonomy_id:
                    continue
                
                try:
                    cur.execute("""
                        INSERT INTO species_observations 
                        (metadata_id, location_id, taxonomy_id, observation_date, count_value, latitude, longitude)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (metadata_id, location_id, taxonomy_id, obs_date, count, lat, lon))
                    records_inserted += 1
                except Exception as e:
                    conn.rollback()
                    continue
        
        if idx % 100 == 0 and idx > 0:
            conn.commit()
            print(f"  Processed {idx} sampling events, {records_inserted} observations...")
    
    conn.commit()
    return records_inserted

def process_phytoplankton_format(df, metadata_id, cur, conn):
    """Process phytoplankton format (PostGIS-free)"""
    print("  Format: Phytoplankton")
    
    col_map = {c.upper(): c for c in df.columns}
    
    species_col = col_map.get('GENUS_SPECIES') or col_map.get('TAXON')
    lat_col = col_map.get('LATITUDE')
    lon_col = col_map.get('LONGITUDE')
    date_col = col_map.get('DATE_TRIP') or col_map.get('DATE')
    count_col = col_map.get('NUMBER_CELLS_COUNTED') or col_map.get('CORRECTED_CELL_CONCENTRATION_CELLS_PER_MILLILITRE')
    
    if not all([species_col, lat_col, lon_col]):
        print(f"  Warning: Missing required columns (need species, lat, lon)")
        return 0
    
    records_inserted = 0
    for idx, row in df.iterrows():
        if pd.isna(row.get(lat_col)) or pd.isna(row.get(lon_col)):
            continue
        
        species = row.get(species_col)
        if pd.isna(species) or str(species).strip() == '':
            continue
            
        lat, lon = row[lat_col], row[lon_col]
        obs_date = row.get(date_col) if date_col else None
        count = row.get(count_col, 1) if count_col else 1
        
        location_id = get_or_create_location(cur, lat, lon, metadata_id)
        if not location_id:
            continue
        
        taxonomy_id = get_or_create_taxonomy(cur, species, '')
        if not taxonomy_id:
            continue
        
        try:
            cur.execute("""
                INSERT INTO species_observations 
                (metadata_id, location_id, taxonomy_id, observation_date, count_value, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (metadata_id, location_id, taxonomy_id, obs_date, count, lat, lon))
            records_inserted += 1
        except Exception as e:
            conn.rollback()
            continue
        
        if idx % 1000 == 0 and idx > 0:
            conn.commit()
            print(f"  Processed {idx} observations...")
    
    conn.commit()
    return records_inserted

def process_redmap_format(df, metadata_id, cur, conn):
    """Process Redmap citizen science sightings (PostGIS-free)"""
    print("  Format: Redmap sightings")
    
    col_map = {c.upper(): c for c in df.columns}
    
    species_col = col_map.get('SPECIES')
    common_col = col_map.get('COMMON_NAME')
    lat_col = col_map.get('LATITUDE')
    lon_col = col_map.get('LONGITUDE')
    date_col = col_map.get('SIGHTING_DATE')
    
    if not all([species_col, lat_col, lon_col]):
        print(f"  Warning: Missing required columns")
        return 0
    
    records_inserted = 0
    for idx, row in df.iterrows():
        if pd.isna(row.get(lat_col)) or pd.isna(row.get(lon_col)):
            continue
        
        species = row.get(species_col)
        common_name = row.get(common_col, '') if common_col else ''
        
        if pd.isna(species) or str(species).strip() == '':
            continue
            
        lat, lon = row[lat_col], row[lon_col]
        obs_date = row.get(date_col) if date_col else None
        
        location_id = get_or_create_location(cur, lat, lon, metadata_id)
        if not location_id:
            continue
        
        taxonomy_id = get_or_create_taxonomy(cur, species, common_name)
        if not taxonomy_id:
            continue
        
        try:
            cur.execute("""
                INSERT INTO species_observations 
                (metadata_id, location_id, taxonomy_id, observation_date, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (metadata_id, location_id, taxonomy_id, obs_date, lat, lon))
            records_inserted += 1
        except Exception as e:
            conn.rollback()
            continue
        
        if idx % 500 == 0 and idx > 0:
            conn.commit()
            print(f"  Processed {idx} sightings...")
    
    conn.commit()
    return records_inserted

def process_standard_format(df, metadata_id, cur, conn):
    """Process standard observation format (PostGIS-free)"""
    print("  Format: Standard")
    
    # Find species column
    species_col = None
    for col in df.columns:
        if col.upper() in ['SPECIES_NAME', 'SPECIES', 'SCIENTIFIC_NAME', 'TAXON']:
            species_col = col
            break
    
    if not species_col:
        return 0
    
    # Find location columns
    lat_col = lon_col = geom_col = None
    
    for col in df.columns:
        col_upper = col.upper()
        if col_upper == 'GEOM':
            geom_col = col
        elif col_upper in ['LATITUDE', 'LAT']:
            lat_col = col
        elif col_upper in ['LONGITUDE', 'LON', 'LONG']:
            lon_col = col
    
    has_location = geom_col or (lat_col and lon_col)
    if not has_location:
        print("  Warning: No location columns found (need GEOM or LATITUDE/LONGITUDE)")
        return 0
    
    # Find date and count columns
    date_col = None
    for col in df.columns:
        col_upper = col.upper()
        if 'DATE' in col_upper or 'SURVEY_DATE' in col_upper:
            date_col = col
            break
    
    count_col = None
    for col in df.columns:
        col_upper = col.upper()
        if 'TOTAL_NUMBER' in col_upper or 'COUNT' in col_upper or 'ABUNDANCE' in col_upper:
            count_col = col
            break
    
    records_inserted = 0
    for idx, row in df.iterrows():
        species = row.get(species_col)
        if pd.isna(species) or str(species).strip() == '':
            continue
        
        # Extract lat/lon from GEOM or separate columns
        if geom_col:
            geom_value = row.get(geom_col)
            lat, lon = parse_wkt_point(geom_value)
            if lat is None and lat_col and lon_col:
                lat = row.get(lat_col)
                lon = row.get(lon_col)
        else:
            lat = row.get(lat_col)
            lon = row.get(lon_col)
        
        if pd.isna(lat) or pd.isna(lon):
            continue
        
        obs_date = row.get(date_col) if date_col else None
        
        count = None
        if count_col:
            count_val = row.get(count_col)
            if pd.notna(count_val):
                try:
                    count = float(count_val)
                except:
                    count = 1
        
        location_id = get_or_create_location(cur, lat, lon, metadata_id)
        if not location_id:
            continue
        
        taxonomy_id = get_or_create_taxonomy(cur, species, '')
        if not taxonomy_id:
            continue
        
        try:
            cur.execute("""
                INSERT INTO species_observations 
                (metadata_id, location_id, taxonomy_id, observation_date, count_value, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (metadata_id, location_id, taxonomy_id, obs_date, count, lat, lon))
            records_inserted += 1
        except:
            conn.rollback()
            continue
        
        if idx % 1000 == 0 and idx > 0:
            conn.commit()
            print(f"  Processed {idx} observations...")
    
    conn.commit()
    return records_inserted

def ingest_dataset(conn, file_path, metadata_id):
    """Main ingestion function with format detection"""
    print(f"Ingesting {os.path.basename(file_path)}...")
    
    if 'Australian_Phytoplankton_Database' in file_path:
        try:
            df = pd.read_csv(file_path, encoding='utf-8', on_bad_lines='skip', 
                           comment='#', low_memory=False, dtype=str)
        except Exception as e:
            print(f"  Error reading Australian Phyto CSV: {e}")
            return
    else:
        try:
            df = pd.read_csv(file_path, encoding='utf-8', on_bad_lines='skip', comment='#')
        except:
            try:
                df = pd.read_csv(file_path, encoding='latin1', on_bad_lines='skip', comment='#')
            except Exception as e:
                print(f"  Error reading CSV: {e}")
                return
    
    cur = conn.cursor()
    
    csv_format = detect_csv_format(df)
    
    if csv_format == 'australian_phyto':
        records = process_australian_phyto_format(df, metadata_id, cur, conn)
    elif csv_format == 'matrix':
        records = process_matrix_format(df, metadata_id, cur, conn)
    elif csv_format == 'phytoplankton':
        records = process_phytoplankton_format(df, metadata_id, cur, conn)
    elif csv_format == 'redmap':
        records = process_redmap_format(df, metadata_id, cur, conn)
    elif csv_format == 'standard':
        records = process_standard_format(df, metadata_id, cur, conn)
    else:
        print(f"  Unknown CSV format, skipping")
        return
    
    print(f"  Inserted {records} observations")
    print(f"Finished {os.path.basename(file_path)}")

def is_biological_csv(file_path):
    """Peek at CSV header to see if it looks biological"""
    try:
        df = pd.read_csv(file_path, encoding='utf-8', on_bad_lines='skip', comment='#', nrows=5)
    except:
        try:
            df = pd.read_csv(file_path, encoding='latin1', on_bad_lines='skip', comment='#', nrows=5)
        except:
            return False
    
    cols = [c.upper() for c in df.columns]
    bio_keywords = ['SPECIES', 'TAXON', 'SCIENTIFIC_NAME', 'GENUS', 'PHYLUM', 'FAMILY', 'GENUS_SPECIES']
    
    for k in bio_keywords:
        if any(k in c for c in cols):
            return True
    
    species_cols = [c for c in df.columns if '_' in c and c.count('_') >= 2]
    if len(species_cols) > 50:
        return True
    
    return False

def main():
    parser = argparse.ArgumentParser(
        description='Import biological observation data from CSV files into the marine database.'
    )
    parser.add_argument(
        '--reprocess',
        action='store_true',
        help='Process ALL datasets with biological CSVs, not just empty ones.'
    )
    
    args = parser.parse_args()
    
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        
        if args.reprocess:
            print("REPROCESS MODE: Processing all datasets with biological data...")
            cur.execute("""
                SELECT m.id, m.title, m.dataset_path 
                FROM metadata m
                WHERE m.dataset_path IS NOT NULL
                ORDER BY m.title
            """)
        else:
            print("Finding empty datasets...")
            cur.execute("""
                SELECT m.id, m.title, m.dataset_path 
                FROM metadata m
                LEFT JOIN measurements mes ON m.id = mes.metadata_id
                LEFT JOIN spatial_features sf ON m.id = sf.metadata_id
                LEFT JOIN species_observations bio ON m.id = bio.metadata_id
                GROUP BY m.id
                HAVING COUNT(mes.data_id) = 0 
                   AND COUNT(sf.id) = 0
                   AND COUNT(bio.id) = 0
                   AND m.dataset_path IS NOT NULL
            """)
        
        datasets = cur.fetchall()
        print(f"Found {len(datasets)} candidate datasets.")
        
        for ds in datasets:
            meta_id, title, path = ds
            if not os.path.exists(path):
                continue
            
            print(f"\nScanning '{title}'...")
            
            found_bio = False
            for root, dirs, files in os.walk(path):
                for file in files:
                    if file.lower().endswith('.csv'):
                        fpath = os.path.join(root, file)
                        if is_biological_csv(fpath):
                            print(f"  -> Found biological file: {file}")
                            ingest_dataset(conn, fpath, meta_id)
                            found_bio = True
            
            if not found_bio:
                print(f"  -> No biological CSVs found for '{title}'.")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
