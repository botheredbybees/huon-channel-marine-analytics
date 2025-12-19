
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import numpy as np

# DB Config
DB_CONFIG = {
    'dbname': 'marine_db',
    'user': 'marine_user',
    'password': 'marine_pass123',
    'host': 'localhost',
    'port': '5433'
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def normalize_location(conn, cur, row, metadata_id):
    """Ensure location exists and return ID"""
    name = row.get('SITE_CODE') or row.get('site_code') or row.get('SITE_DESCRIPTION') or row.get('site_name') or f"Site at {row.get('LATITUDE', 'Unknown')},{row.get('LONGITUDE', 'Unknown')}"
    
    # Cast to string to avoid comparison errors with Integers in DB text fields
    name = str(name).strip() if pd.notna(name) else "Unknown Site"
    desc = str(row.get('SITE_DESCRIPTION') or row.get('site_name') or "").strip()
    
    geom_wkt = row.get('GEOM') or row.get('geom')
    lat = row.get('LATITUDE') or row.get('latitude') or row.get('lat')
    lon = row.get('LONGITUDE') or row.get('longitude') or row.get('lon')
    
    if geom_wkt and isinstance(geom_wkt, str) and 'POINT' in geom_wkt:
        geom_sql = f"ST_GeomFromText('{geom_wkt}', 4326)"
    elif pd.notna(lat) and pd.notna(lon):
        geom_sql = f"ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)"
    else:
        geom_sql = "NULL"

    cur.execute("SELECT id FROM locations WHERE location_name = %s", (name,))
    res = cur.fetchone()
    if res:
        return res[0]
        
    try:
        sql = f"""
        INSERT INTO locations (location_name, description, location_geom, latitude, longitude)
        VALUES (%s, %s, {geom_sql}, %s, %s)
        ON CONFLICT (latitude, longitude) DO UPDATE SET location_name = EXCLUDED.location_name
        RETURNING id
        """
        l = float(lat) if pd.notna(lat) else None
        o = float(lon) if pd.notna(lon) else None
        
        cur.execute(sql, (name, desc, l, o))
        return cur.fetchone()[0]
    except Exception as e:
        # print(f"Error inserting location {name}: {e}")
        conn.rollback()
        return None

def normalize_taxonomy(conn, cur, row):
    sp_name = row.get('SPECIES_NAME') or row.get('species_name') or row.get('SPECIES')
    if pd.isna(sp_name) or str(sp_name).lower() == 'nan':
        return None
    sp_name = str(sp_name).strip()
        
    cur.execute("SELECT id FROM taxonomy WHERE species_name = %s", (sp_name,))
    res = cur.fetchone()
    if res:
        return res[0]
        
    common = str(row.get('COMMON_NAME') or row.get('reporting_name') or "")
    family = str(row.get('FAMILY') or row.get('family') or "")
    phylum = str(row.get('PHYLUM') or row.get('phylum') or "")
    cls = str(row.get('CLASS') or row.get('class') or "")
    order = str(row.get('ORDER') or row.get('order') or "")
    genus = str(row.get('GENUS') or row.get('genus') or "")
    auth = str(row.get('AUTHORITY') or "")
    
    try:
        cur.execute("""
        INSERT INTO taxonomy (species_name, common_name, family, phylum, class, "order", genus, authority)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (species_name) DO NOTHING
        RETURNING id
        """, (sp_name, common, family, phylum, cls, order, genus, auth))
        res = cur.fetchone()
        return res[0] if res else None 
    except Exception as e:
        conn.rollback()
        cur.execute("SELECT id FROM taxonomy WHERE species_name = %s", (sp_name,))
        res = cur.fetchone()
        return res[0] if res else None

def ingest_dataset(conn, file_path, metadata_id):
    print(f"Ingesting {os.path.basename(file_path)}...")
    try:
        # Read CSV with flexible encoding, skipping comments
        try:
            df = pd.read_csv(file_path, encoding='utf-8', on_bad_lines='skip', comment='#')
        except:
            try:
                df = pd.read_csv(file_path, encoding='latin1', on_bad_lines='skip', comment='#')
            except TypeError:
                # Older pandas/different arg?
                df = pd.read_csv(file_path, encoding='latin1', error_bad_lines=False, comment='#')

        cur = conn.cursor()
        
        inserted_count = 0
        for idx, row in df.iterrows():
            loc_id = normalize_location(conn, cur, row, metadata_id)
            tax_id = normalize_taxonomy(conn, cur, row)
            
            if not tax_id:
                continue

            # Values
            count_val = row.get('TOTAL_NUMBER') or row.get('total') or row.get('count_code') # Redmap uses code?
            count_cat = row.get('COUNT_DESCRIPTION') if 'COUNT_DESCRIPTION' in row else None
            
            # Handle numeric count safely
            try:
                numeric_count = float(str(count_val).replace('>','').replace('<','')) if count_val else None
            except:
                numeric_count = None
                count_cat = str(count_val) # treat as category if not number
                
            obs_date = row.get('SURVEY_DATE') or row.get('survey_date') or row.get('SIGHTING_DATE')
            depth = row.get('DEPTH') or row.get('depth')
            
            # Geom for observation (denormalized)
            lat = row.get('LATITUDE') or row.get('latitude') or row.get('LATITUDE')
            lon = row.get('LONGITUDE') or row.get('longitude') or row.get('LONGITUDE')
            geom_sql = "NULL"
            if lat and lon:
                geom_sql = f"ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)"

            sql = f"""
            INSERT INTO species_observations 
            (metadata_id, location_id, taxonomy_id, observation_date, count_value, count_category, depth_m, geom)
            VALUES (%s, %s, %s, %s, %s, %s, %s, {geom_sql})
            """
            
            cur.execute(sql, (
                metadata_id, loc_id, tax_id, obs_date, numeric_count, count_cat, depth
            ))
            
            if idx % 1000 == 0:
                conn.commit()
        
        conn.commit()
        print(f"Finished {file_path}")
        
    except Exception as e:
        print(f"Failed to ingest {file_path}: {e}")
        conn.rollback()

def is_biological_csv(file_path):
    """Peek at CSV header to see if it looks biological"""
    try:
        # Read just the header (first few lines)
        try:
            df = pd.read_csv(file_path, encoding='utf-8', on_bad_lines='skip', comment='#', nrows=5)
        except:
            df = pd.read_csv(file_path, encoding='latin1', on_bad_lines='skip', comment='#', nrows=5)
            
        cols = [c.upper() for c in df.columns]
        bio_keywords = ['SPECIES', 'TAXON', 'SCIENTIFIC_NAME', 'GENUS', 'PHYLUM', 'FAMILY']
        
        # Check if any keyword matches any column substring
        for k in bio_keywords:
            if any(k in c for c in cols):
                return True
        return False
    except Exception as e:
        # print(f"Skipping non-CSV or unreadable {file_path}: {e}")
        return False

def main():
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        
        # 1. Find datasets with 0 records in ALL tables (measurements, spatial, biological)
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
        print(f"Found {len(datasets)} candidate empty datasets.")
        
        for ds in datasets:
            meta_id, title, path = ds
            if not os.path.exists(path):
                continue
                
            print(f"Scanning '{title}'...")
            
            # Walk directory
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
