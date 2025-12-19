
import os
import glob
import json
import uuid
import psycopg2
import subprocess
from datetime import datetime

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

def convert_shp_to_geojson(shp_path):
    """Convert Shapefile to GeoJSON using system ogr2ogr"""
    temp_json = f"/tmp/{uuid.uuid4()}.json"
    try:
        # Run ogr2ogr: input format automatic (ESRI Shapefile), output GeoJSON
        # -t_srs EPSG:4326 ensures lat/lon coordinates
        cmd = ["ogr2ogr", "-f", "GeoJSON", "-t_srs", "EPSG:4326", temp_json, shp_path]
        subprocess.check_call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        if os.path.exists(temp_json):
            with open(temp_json, 'r') as f:
                data = json.load(f)
            os.remove(temp_json)
            return data
    except Exception as e:
        print(f"Error converting {shp_path}: {e}")
        if os.path.exists(temp_json):
            os.remove(temp_json)
    return None

def main():
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        
        # 1. Identify Empty Datasets (exclude time-series we known are just empty like Ocean Acid)
        print("Finding empty datasets...")
        cur.execute("""
            SELECT m.id, m.uuid, m.dataset_path, m.title 
            FROM metadata m
            LEFT JOIN measurements mes ON m.id = mes.metadata_id
            LEFT JOIN spatial_features sf ON m.id = sf.metadata_id
            GROUP BY m.id
            HAVING COUNT(mes.data_id) = 0 
               AND COUNT(sf.id) = 0
               AND m.dataset_path IS NOT NULL
        """)
        datasets = cur.fetchall()
        print(f"Found {len(datasets)} candidate datasets.")
        
        for ds in datasets:
            meta_id, meta_uuid, path, title = ds
            if not os.path.exists(path):
                continue
                
            # Look for Shapefiles
            # Search recursively
            shp_files = []
            for root, dirs, files in os.walk(path):
                for file in files:
                    if file.lower().endswith('.shp'):
                        shp_files.append(os.path.join(root, file))
            
            if not shp_files:
                continue
                
            print(f"Processing '{title}' ({len(shp_files)} shapefiles)...")
            
            inserted_count = 0
            for shp in shp_files:
                geojson = convert_shp_to_geojson(shp)
                if not geojson or 'features' not in geojson:
                    continue
                
                features_to_insert = []
                for feature in geojson['features']:
                    if feature.get('geometry') is None:
                        continue
                        
                    geom = json.dumps(feature['geometry'])
                    props = json.dumps(feature['properties'])
                    
                    # Insert
                    # We use ST_GeomFromGeoJSON for the geometry
                    features_to_insert.append((meta_id, meta_uuid, geom, props))
                
                if features_to_insert:
                    from psycopg2.extras import execute_values
                    sql = """
                        INSERT INTO spatial_features (metadata_id, uuid, geom, properties)
                        VALUES %s
                    """
                    # We need to wrap geom in ST_GeomFromGeoJSON in the VALUES template ? 
                    # No, execute_values doesn't easily support function calls in the template for specific columns.
                    # Better to insert as text and cast, or use execute loop for now if not too huge.
                    # Or use a specific template: VALUES %s where the tuple has existing values.
                    
                    # Actually, we can use a template in execute_values
                    template = "(%s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), %s)"
                    
                    try:
                        execute_values(cur, sql, features_to_insert, template=template)
                        inserted_count += len(features_to_insert)
                    except Exception as e:
                        print(f"    Batch error: {e}. Retrying row-by-row to identify culprit...")
                        conn.rollback()
                        
                        # Row-by-row retry
                        for item in features_to_insert:
                            try:
                                with conn.cursor() as cur_single:
                                     execute_values(cur_single, sql, [item], template=template)
                                conn.commit()
                                inserted_count += 1
                            except Exception as single_e:
                                print(f"    Failed Row Error: {single_e}")
                                print(f"    Bad Geom Snippet: {item[2][:100]}...")
                                conn.rollback()
                        # Continue to next file, having saved what we could
                        pass
            
            if inserted_count > 0:
                conn.commit()
                print(f"  -> Inserted {inserted_count} spatial features.")
            else:
                conn.rollback() 
                
    finally:
        conn.close()

if __name__ == "__main__":
    main()
