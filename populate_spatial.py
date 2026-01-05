#!/usr/bin/env python3
"""
populate_spatial.py - Spatial Feature Extraction (PostGIS-Free Version)

Extracts spatial features from ESRI Shapefiles and loads them into the database.
Uses latitude/longitude centroids instead of PostGIS geometries.

Compatible with TimescaleDB Community Edition (no PostGIS dependency).
Version: 2.0 (PostGIS-free)
Last Updated: December 30, 2025
"""

import os
import glob
import json
import uuid
import psycopg2
import subprocess
from datetime import datetime

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

def convert_shp_to_geojson(shp_path):
    """Convert Shapefile to GeoJSON using system ogr2ogr with encoding handling"""
    temp_json = f"/tmp/{uuid.uuid4()}.json"
    
    # Try multiple encoding strategies
    encoding_strategies = [
        [],  # Default (let ogr2ogr auto-detect)
        ["--config", "SHAPE_ENCODING", "UTF-8"],
        ["--config", "SHAPE_ENCODING", "ISO-8859-1"],  # Latin-1
        ["--config", "SHAPE_ENCODING", "WINDOWS-1252"], # Windows encoding
        ["--config", "SHAPE_ENCODING", "CP1252"],       # Alternative Windows
    ]
    
    for strategy in encoding_strategies:
        try:
            # Build command with encoding strategy
            cmd = ["ogr2ogr", "-f", "GeoJSON", "-t_srs", "EPSG:4326"]
            cmd.extend(strategy)
            cmd.extend([temp_json, shp_path])
            
            subprocess.check_call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            if os.path.exists(temp_json):
                # Try reading with UTF-8, fall back to latin-1
                try:
                    with open(temp_json, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                except UnicodeDecodeError:
                    with open(temp_json, 'r', encoding='latin-1') as f:
                        data = json.load(f)
                
                os.remove(temp_json)
                return data
        except (subprocess.CalledProcessError, json.JSONDecodeError):
            if os.path.exists(temp_json):
                os.remove(temp_json)
            continue  # Try next strategy
        except Exception as e:
            if os.path.exists(temp_json):
                os.remove(temp_json)
            continue
    
    # All strategies failed
    print(f"Error converting {shp_path}: all encoding strategies failed")
    return None



def extract_centroid(geometry):
    """
    Extract centroid coordinates from GeoJSON geometry.
    Returns (latitude, longitude) or (None, None) if invalid.
    
    Supports Point, Polygon, MultiPolygon, and LineString geometries.
    """
    if not geometry or geometry.get('type') not in ['Polygon', 'MultiPolygon', 'Point', 'LineString']:
        return None, None
    
    try:
        if geometry['type'] == 'Point':
            coords = geometry['coordinates']
            return coords[1], coords[0]  # lat, lon
        
        elif geometry['type'] == 'Polygon':
            # Get first ring (exterior)
            coords = geometry['coordinates'][0]
            # Calculate centroid
            lon_sum = sum(c[0] for c in coords)
            lat_sum = sum(c[1] for c in coords)
            count = len(coords)
            return lat_sum / count, lon_sum / count
        
        elif geometry['type'] == 'MultiPolygon':
            # Get first polygon's first ring
            coords = geometry['coordinates'][0][0]
            lon_sum = sum(c[0] for c in coords)
            lat_sum = sum(c[1] for c in coords)
            count = len(coords)
            return lat_sum / count, lon_sum / count
        
        elif geometry['type'] == 'LineString':
            # Midpoint of line
            coords = geometry['coordinates']
            mid = len(coords) // 2
            return coords[mid][1], coords[mid][0]  # lat, lon
            
    except Exception as e:
        print(f"Error extracting centroid: {e}")
        return None, None
    
    return None, None

def main():
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        
        # Find empty datasets (no measurements or spatial features yet)
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
            
            # Search recursively for shapefiles
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
                
                # Extract features with centroid lat/lon
                features_to_insert = []
                for feature in geojson['features']:
                    if feature.get('geometry') is None:
                        continue
                    
                    # Extract centroid lat/lon
                    lat, lon = extract_centroid(feature['geometry'])
                    if lat is None or lon is None:
                        continue
                    
                    props = json.dumps(feature.get('properties', {}))
                    # FIXED: Removed meta_uuid from tuple
                    features_to_insert.append((meta_id, lat, lon, props))
                
                if features_to_insert:
                    from psycopg2.extras import execute_values
                    
                    # FIXED: Removed uuid from column list
                    sql = """
                        INSERT INTO spatial_features (metadata_id, latitude, longitude, properties)
                        VALUES %s
                    """
                    # FIXED: Removed one %s from template
                    template = "(%s, %s, %s, %s)"
                    
                    try:
                        execute_values(cur, sql, features_to_insert, template=template)
                        inserted_count += len(features_to_insert)
                    except Exception as e:
                        print(f"  Batch error: {e}. Retrying row-by-row...")
                        conn.rollback()
                        
                        # Retry row-by-row
                        for item in features_to_insert:
                            try:
                                with conn.cursor() as cur_single:
                                    execute_values(cur_single, sql, [item], template=template)
                                    conn.commit()
                                    inserted_count += 1
                            except Exception as single_e:
                                print(f"  Failed Row Error: {single_e}")
                                print(f"  Bad Data: lat={item[1]}, lon={item[2]}")  # FIXED: Adjusted index
                                conn.rollback()
            
            if inserted_count > 0:
                conn.commit()
                print(f"  -> Inserted {inserted_count} spatial features.")
            else:
                conn.rollback()
    
    finally:
        conn.close()

if __name__ == '__main__':
    main()
