import os
import glob
import netCDF4
import psycopg2
import pandas as pd
from shapely.geometry import Point
import csv

# --- Configuration ---
DB_CONFIG = {
    "dbname": "marine_db",
    "user": "marine_user",
    "password": "marine_pass123",
    "host": "localhost",
    "port": "5433"
}
DATA_ROOT = "AODN_data"

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def find_file_for_uuid(uuid, root_dir, conn):
    """Finds the source file path, ignoring index.csv."""
    cur = conn.cursor()
    cur.execute("SELECT dataset_path, dataset_name, title FROM metadata WHERE uuid = %s", (uuid,))
    result = cur.fetchone()
    
    if not result:
        return None, None
        
    path, name, title = result
    potential_paths = [
        os.path.join(root_dir, title) if title else None,
        os.path.join(root_dir, name) if name else None,
        os.path.join(root_dir, path) if path else None,
    ]
    
    for p in potential_paths:
        if p and os.path.exists(p):
            if os.path.isdir(p):
                # Priority: NetCDF -> CSV (excluding index.csv)
                nc_files = glob.glob(os.path.join(p, "**", "*.nc"), recursive=True)
                if nc_files: return nc_files[0], title
                
                csv_files = glob.glob(os.path.join(p, "**", "*.csv"), recursive=True)
                # Filter out index.csv unless it's the only file
                real_csvs = [f for f in csv_files if "index.csv" not in os.path.basename(f).lower()]
                
                if real_csvs: 
                    return real_csvs[0], title
                elif csv_files: # Fallback if only index.csv exists (unlikely to have data)
                    return csv_files[0], title
                    
            elif os.path.isfile(p):
                 return p, title
    return None, title

def extract_station_info(file_path, dataset_title):
    print(f"  [DEBUG] Extracting from: {file_path}")
    
    # --- NETCDF HANDLING ---
    if file_path.endswith(".nc"):
        try:
            ds = netCDF4.Dataset(file_path)
            station = None
            for attr in ['station_name', 'site_code', 'platform_code', 'title', 'id']:
                if hasattr(ds, attr):
                    station = str(getattr(ds, attr)).strip()
                    break
            
            lat = lon = None
            for lat_name in ['LATITUDE', 'latitude', 'lat']:
                if lat_name in ds.variables:
                    lat = float(ds.variables[lat_name][0])
                    break
            for lon_name in ['LONGITUDE', 'longitude', 'lon']:
                if lon_name in ds.variables:
                    lon = float(ds.variables[lon_name][0])
                    break
            
            if lat is None and hasattr(ds, 'geospatial_lat_min'):
                lat = float(ds.geospatial_lat_min)
            if lon is None and hasattr(ds, 'geospatial_lon_min'):
                lon = float(ds.geospatial_lon_min)
                
            ds.close()
            return station or dataset_title, lat, lon
        except Exception as e:
            print(f"  [ERROR] NetCDF read failed: {e}")
            return None, None, None

    # --- CSV HANDLING ---
    elif file_path.endswith(".csv"):
        try:
            # Detect delimiter (comma vs semicolon)
            with open(file_path, 'r') as f:
                header = f.readline()
                sep = ';' if ';' in header else ','
            
            # Read just header and first row
            df = pd.read_csv(file_path, nrows=5, sep=sep)
            
            # Normalize columns to upper case
            df.columns = [c.upper().strip() for c in df.columns]
            
            # LATITUDE
            lat_col = next((c for c in df.columns if c in ['LATITUDE', 'LAT', 'START_LAT', 'DECIMAL_LAT']), None)
            lat = df[lat_col].iloc[0] if lat_col else None
            
            # LONGITUDE
            lon_col = next((c for c in df.columns if c in ['LONGITUDE', 'LON', 'LONG', 'START_LON', 'DECIMAL_LONG']), None)
            lon = df[lon_col].iloc[0] if lon_col else None
            
            # STATION NAME
            station_col = next((c for c in df.columns if c in ['STATION', 'SITE', 'SITE_CODE', 'STATION_NAME', 'TRIP_CODE']), None)
            station = str(df[station_col].iloc[0]) if station_col else dataset_title
            
            if station and len(station) < 3:
                station = f"{dataset_title} - Site {station}"

            return station, lat, lon
        except Exception as e:
            print(f"  [ERROR] CSV read failed: {e}")
            return None, None, None

    return None, None, None

def process_datasets():
    conn = get_db_connection()
    cur = conn.cursor()
    
    print("Fetching UUIDs...")
    cur.execute("SELECT DISTINCT uuid FROM metadata WHERE uuid IS NOT NULL")
    uuids = [row[0] for row in cur.fetchall()]
    
    print(f"Found {len(uuids)} datasets.")
    
    updated_count = 0
    for i, uuid in enumerate(uuids):
        file_path, title = find_file_for_uuid(uuid, DATA_ROOT, conn)
        if not file_path:
            continue
            
        station, lat, lon = extract_station_info(file_path, title)
        
        if lat is not None and lon is not None:
            try:
                cur.execute("""
                    INSERT INTO locations (location_name, latitude, longitude, location_geom)
                    VALUES (%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
                    ON CONFLICT (latitude, longitude) 
                    DO UPDATE SET location_name = EXCLUDED.location_name
                    RETURNING id;
                """, (str(station), float(lat), float(lon), float(lon), float(lat)))
                location_id = cur.fetchone()[0]
                
                cur.execute("""
                    UPDATE measurements SET location_id = %s WHERE uuid = %s AND location_id IS NULL;
                """, (location_id, uuid))
                
                conn.commit()
                updated_count += 1
                print(f"  [OK] {uuid} -> {station} ({lat}, {lon})")
            except Exception as e:
                conn.rollback()
                print(f"  [DB ERROR] {uuid}: {e}")
        else:
            print(f"  [SKIP] {uuid}: No lat/lon found in {os.path.basename(file_path)}")

    print(f"Done! Updated {updated_count} datasets.")
    conn.close()

if __name__ == "__main__":
    process_datasets()
