#!/usr/bin/env python3
"""
populate_spatial.py - Spatial Feature Extraction (PostGIS-Free Version)

Extracts spatial features from ESRI Shapefiles and loads them into the database.
Uses latitude/longitude centroids instead of PostGIS geometries.

Compatible with TimescaleDB Community Edition (no PostGIS dependency).
Version: 3.0 (Enhanced with --force flag and logging)
Last Updated: January 6, 2026
"""

import os
import argparse
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
    """
    Convert Shapefile to GeoJSON using system ogr2ogr with encoding handling.
    
    Tries multiple encoding strategies to handle various international character sets.
    Falls back gracefully when encoding issues occur.
    
    Args:
        shp_path: Path to the shapefile (.shp)
        
    Returns:
        dict: GeoJSON FeatureCollection or None if conversion fails
    """
    temp_json = f"/tmp/{uuid.uuid4()}.json"
    
    try:
        # Try with Windows-1252 encoding (most common for shapefiles)
        cmd = [
            "ogr2ogr", 
            "-f", "GeoJSON", 
            "-t_srs", "EPSG:4326",
            "--config", "SHAPE_ENCODING", "CP1252",
            temp_json, 
            shp_path
        ]
        subprocess.check_call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        if os.path.exists(temp_json):
            # Try reading the JSON
            try:
                with open(temp_json, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except UnicodeDecodeError:
                # Fall back to latin-1
                with open(temp_json, 'r', encoding='latin-1') as f:
                    data = json.load(f)
            
            os.remove(temp_json)
            return data
            
    except Exception as e:
        # Last resort: extract geometry only using field type conversion
        if os.path.exists(temp_json):
            os.remove(temp_json)
        
        try:
            # Use -fieldTypeToString to force all fields to string (avoids encoding issues)
            cmd = [
                "ogr2ogr", 
                "-f", "GeoJSON", 
                "-t_srs", "EPSG:4326",
                "-fieldTypeToString", "All",
                "--config", "SHAPE_ENCODING", "",  # Empty = auto-detect
                temp_json, 
                shp_path
            ]
            subprocess.check_call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            with open(temp_json, 'r', encoding='utf-8', errors='replace') as f:
                data = json.load(f)
            os.remove(temp_json)
            return data
        except:
            if os.path.exists(temp_json):
                os.remove(temp_json)
            return None

def extract_centroid(geometry):
    """
    Extract centroid coordinates from GeoJSON geometry.
    
    Supports Point, Polygon, MultiPolygon, and LineString geometries.
    
    Args:
        geometry: GeoJSON geometry object
        
    Returns:
        tuple: (latitude, longitude) or (None, None) if invalid
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
        print(f"  Error extracting centroid: {e}")
        return None, None
    
    return None, None

def main():
    parser = argparse.ArgumentParser(
        description='Extract spatial features from ESRI Shapefiles',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process only datasets without spatial features
  python populate_spatial.py
  
  # Force re-process all datasets
  python populate_spatial.py --force
  
  # Force re-process specific dataset
  python populate_spatial.py --force --dataset "SeaMap Tasmania"
        """
    )
    parser.add_argument('--force', action='store_true',
                       help='Force re-processing of datasets that already have spatial features')
    parser.add_argument('--dataset', type=str,
                       help='Process only datasets matching this name (case-insensitive)')
    
    args = parser.parse_args()
    
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        
        # Build query based on arguments
        print("Finding datasets...")
        
        if args.force:
            # Force mode: process all datasets or specific dataset
            if args.dataset:
                cur.execute("""
                    SELECT m.id, m.uuid, m.dataset_path, m.title
                    FROM metadata m
                    WHERE m.dataset_path IS NOT NULL
                      AND LOWER(m.title) LIKE LOWER(%s)
                    ORDER BY m.title
                """, (f'%{args.dataset}%',))
            else:
                cur.execute("""
                    SELECT m.id, m.uuid, m.dataset_path, m.title
                    FROM metadata m
                    WHERE m.dataset_path IS NOT NULL
                    ORDER BY m.title
                """)
        else:
            # Normal mode: only process datasets without spatial features
            if args.dataset:
                cur.execute("""
                    SELECT m.id, m.uuid, m.dataset_path, m.title
                    FROM metadata m
                    LEFT JOIN measurements mes ON m.id = mes.metadata_id
                    LEFT JOIN spatial_features sf ON m.id = sf.metadata_id
                    WHERE m.dataset_path IS NOT NULL
                      AND LOWER(m.title) LIKE LOWER(%s)
                    GROUP BY m.id
                    HAVING COUNT(mes.data_id) = 0
                       AND COUNT(sf.id) = 0
                    ORDER BY m.title
                """, (f'%{args.dataset}%',))
            else:
                cur.execute("""
                    SELECT m.id, m.uuid, m.dataset_path, m.title
                    FROM metadata m
                    LEFT JOIN measurements mes ON m.id = mes.metadata_id
                    LEFT JOIN spatial_features sf ON m.id = sf.metadata_id
                    GROUP BY m.id
                    HAVING COUNT(mes.data_id) = 0
                       AND COUNT(sf.id) = 0
                       AND m.dataset_path IS NOT NULL
                    ORDER BY m.title
                """)
        
        datasets = cur.fetchall()
        print(f"Found {len(datasets)} candidate dataset(s).")
        
        if args.force:
            print("⚠️  Running in FORCE mode - will re-process datasets with existing spatial features")
        
        total_inserted = 0
        total_updated = 0
        datasets_processed = 0
        
        for ds in datasets:
            meta_id, meta_uuid, path, title = ds
            
            if not os.path.exists(path):
                print(f"⚠️  Skipping '{title}' - path not found: {path}")
                continue
            
            # Search recursively for shapefiles
            shp_files = []
            for root, dirs, files in os.walk(path):
                for file in files:
                    if file.lower().endswith('.shp'):
                        shp_files.append(os.path.join(root, file))
            
            if not shp_files:
                continue
            
            print(f"\nProcessing '{title}' ({len(shp_files)} shapefile(s))...")
            
            # If force mode, delete existing features first
            if args.force:
                cur.execute("DELETE FROM spatial_features WHERE metadata_id = %s", (meta_id,))
                deleted = cur.rowcount
                if deleted > 0:
                    print(f"  Deleted {deleted} existing spatial features")
            
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
                    features_to_insert.append((meta_id, lat, lon, props))
                
                if features_to_insert:
                    from psycopg2.extras import execute_values
                    
                    sql = """
                        INSERT INTO spatial_features (metadata_id, latitude, longitude, properties)
                        VALUES %s
                    """
                    template = "(%s, %s, %s, %s)"
                    
                    try:
                        execute_values(cur, sql, features_to_insert, template=template)
                        inserted_count += len(features_to_insert)
                    except Exception as e:
                        print(f"  ⚠️  Batch error: {e}. Retrying row-by-row...")
                        conn.rollback()
                        
                        # Retry row-by-row
                        for item in features_to_insert:
                            try:
                                with conn.cursor() as cur_single:
                                    execute_values(cur_single, sql, [item], template=template)
                                    conn.commit()
                                    inserted_count += 1
                            except Exception as single_e:
                                print(f"  ✗ Failed Row Error: {single_e}")
                                print(f"    Bad Data: lat={item[1]:.4f}, lon={item[2]:.4f}")
                                conn.rollback()
            
            if inserted_count > 0:
                # Update metadata extracted_at timestamp
                cur.execute("""
                    UPDATE metadata 
                    SET extracted_at = CURRENT_TIMESTAMP 
                    WHERE id = %s
                """, (meta_id,))
                
                conn.commit()
                print(f"  ✓ Inserted {inserted_count} spatial features")
                total_inserted += inserted_count
                datasets_processed += 1
            else:
                conn.rollback()
        
        # Final summary
        print("\n" + "="*60)
        print("Processing Complete")
        print("="*60)
        
        # Get total count from database
        cur.execute("SELECT COUNT(*) FROM spatial_features")
        total_features = cur.fetchone()[0]
        
        print(f"Datasets processed: {datasets_processed}")
        print(f"New features inserted: {total_inserted}")
        print(f"Total spatial features in database: {total_features}")
        
    finally:
        conn.close()

if __name__ == '__main__':
    main()
