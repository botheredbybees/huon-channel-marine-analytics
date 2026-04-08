import os
import argparse
import logging
import math
from typing import List, Tuple
import psycopg2
from psycopg2.extras import execute_values
from csiro_dap_client import CSIRODapClient

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the great circle distance in meters between two points 
    on the earth (specified in decimal degrees).
    """
    R = 6371000.0  # Radius of earth in meters
    
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    
    a = math.sin(delta_phi/2.0)**2 + \
        math.cos(phi1) * math.cos(phi2) * \
        math.sin(delta_lambda/2.0)**2
        
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    return R * c

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "marine_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres")
    )

def map_grids_to_locations(conn):
    """
    Maps all locations in public.locations to the nearest nodes in csiro_model_grid.
    """
    logger.info("Mapping existing locations to CSIRO grids using Haversine algorithm...")
    with conn.cursor() as cur:
        # Get all observation locations
        cur.execute("SELECT id, latitude, longitude FROM public.locations WHERE latitude IS NOT NULL AND longitude IS NOT NULL;")
        locations = cur.fetchall()
        
        # Get all CSIRO grid nodes
        cur.execute("SELECT grid_id, latitude, longitude FROM public.csiro_model_grid;")
        grids = cur.fetchall()
        
        if not grids:
            logger.warning("No CSIRO grids found in the database. Run grid ingestion first.")
            return

        mappings_to_insert = []
        for loc in locations:
            loc_id, loc_lat, loc_lon = loc
            
            # Find closest grid node for each location
            closest_grid = None
            min_dist = float('inf')
            
            for grid in grids:
                g_id, g_lat, g_lon = grid
                dist = haversine_distance(loc_lat, loc_lon, g_lat, g_lon)
                if dist < min_dist:
                    min_dist = dist
                    closest_grid = g_id
            
            if closest_grid is not None:
                mappings_to_insert.append((loc_id, closest_grid, min_dist, 1))

        if mappings_to_insert:
            execute_values(
                cur,
                """
                INSERT INTO public.csiro_grid_to_location_mapping (location_id, grid_id, distance_meters, rank)
                VALUES %s
                ON CONFLICT (location_id, grid_id) 
                DO UPDATE SET distance_meters = EXCLUDED.distance_meters, rank = EXCLUDED.rank;
                """,
                mappings_to_insert
            )
            conn.commit()
            logger.info(f"Successfully mapped {len(mappings_to_insert)} locations to CSIRO grid nodes.")

def main():
    parser = argparse.ArgumentParser(description="Ingest CSIRO Data")
    parser.add_argument("--search", action="store_true", help="Search for datasets instead of downloading")
    parser.add_argument("--query", type=str, default='("Storm Bay" OR "Huon" OR "D\'Entrecasteaux" OR "Derwent") AND (hydrodynamic OR model)', help="Search query string")
    parser.add_argument("--map-locations", action="store_true", help="Calculate spatial matching from DB locations to CSIRO grids")
    
    args = parser.parse_args()
    
    client = CSIRODapClient()
    
    if args.search:
        logger.info(f"Running search with query: {args.query}")
        results = client.search_collections(args.query)
        logger.info(f"Found {len(results)} collections")
        for idx, col in enumerate(results):
            logger.info(f"[{idx}] {col.get('title')}")
            # Get full details to show file sizes
            details = client.get_collection_details(col.get('self'))
            if details:
                files = client.list_files_for_collection(details)
                total_mb = sum((f.get('size', 0) for f in files)) / (1024*1024)
                logger.info(f"   -> Contains {len(files)} files totaling ~{total_mb:.2f} MB")
                
    if args.map_locations:
        conn = get_db_connection()
        try:
            map_grids_to_locations(conn)
        finally:
            conn.close()

if __name__ == "__main__":
    main()
