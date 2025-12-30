#!/usr/bin/env python3
"""
Populate measurements table from CSV and NetCDF files with multi-parameter support.

This script extracts measurements from oceanographic data files and inserts them
into the PostgreSQL database. It supports:
- Multiple parameters per file (temperature, salinity, pressure, etc.)
- CSV files with column-based data
- NetCDF files with time-series data
- Automatic location coordinate validation and patching
- Batch insertion for performance
"""

import sys
import logging
import psycopg2
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple
import xarray as xr
import cftime

# ============================================================================
# LOGGING SETUP
# ============================================================================

# Create logs directory if it doesn't exist
import os
from pathlib import Path
logs_dir = Path('logs')
logs_dir.mkdir(exist_ok=True)

# Generate log filename with timestamp
log_filename = logs_dir / f'etl_measurements_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

# Configure logging to write to both file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

logger.info(f"📝 Log file: {log_filename}")

# ============================================================================
# DATABASE CONNECTION
# ============================================================================

def get_db_connection():
    """Create database connection."""
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="marine_db",
        user="marine_user",
        password="marine_pass"
    )

# ============================================================================
# LOCATION VALIDATION
# ============================================================================

def get_or_create_location(cursor, latitude: float, longitude: float, metadata_id: int) -> Optional[int]:
    """Get existing location ID or create new one if coordinates are valid."""
    
    # Validate coordinates
    if not (-90 <= latitude <= 90) or not (-180 <= longitude <= 180):
        return None
        
    # Try to find existing location (within 0.0001 degrees ~ 11 meters)
    cursor.execute("""
        SELECT location_id 
        FROM locations 
        WHERE ST_DWithin(
            coordinates::geography,
            ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
            11
        )
        LIMIT 1
    """, (longitude, latitude))
    
    result = cursor.fetchone()
    if result:
        return result[0]
    
    # Create new location
    cursor.execute("""
        INSERT INTO locations (coordinates)
        VALUES (ST_SetSRID(ST_MakePoint(%s, %s), 4326))
        RETURNING location_id
    """, (longitude, latitude))
    
    return cursor.fetchone()[0]

# ============================================================================
# PARAMETER DETECTION
# ============================================================================

PARAMETER_KEYWORDS = {
    'temperature': ['temp', 'temperature', 'sst', 'sea_surface_temperature', 'TEMP'],
    'salinity': ['sal', 'salinity', 'psal', 'PSAL'],
    'pressure': ['pres', 'pressure', 'depth', 'PRES'],
    'oxygen': ['oxygen', 'o2', 'doxy', 'dissolved_oxygen'],
    'chlorophyll': ['chlorophyll', 'chl', 'chla', 'cphl'],
    'turbidity': ['turbidity', 'turb', 'ntu'],
    'ph': ['ph', 'ph_total', 'ph_insitu'],
    'current_speed': ['current', 'velocity', 'speed', 'ucur', 'vcur'],
    'wave_height': ['wave_height', 'hs', 'significant_wave_height'],
    'wind_speed': ['wind_speed', 'wspd', 'wind']
}

def detect_parameters(columns) -> dict:
    """Detect which oceanographic parameters are present in columns."""
    detected = {}
    
    for param_name, keywords in PARAMETER_KEYWORDS.items():
        for col in columns:
            col_lower = str(col).lower()
            if any(keyword in col_lower for keyword in keywords):
                detected[param_name] = col
                break
    
    return detected

# ============================================================================
# CSV EXTRACTOR
# ============================================================================

class CSVExtractor:
    """Extract measurements from CSV files."""
    
    def __init__(self, cursor):
        self.cursor = cursor
        self.extracted_count = 0
        self.failed_count = 0
    
    def extract(self, file_path: Path, metadata_id: int, dataset_path: str) -> list:
        """Extract measurements from a CSV file."""
        try:
            # Read CSV with better error handling
            df = pd.read_csv(
                file_path,
                parse_dates=True,
                on_bad_lines='skip',
                encoding_errors='ignore'
            )
            
            if df.empty:
                return []
            
            # Detect parameters
            params = detect_parameters(df.columns)
            if not params:
                logger.info(f"    ⚠ No parameter columns detected in {file_path.name}")
                return []
            
            logger.info(f"    ✓ Detected {len(params)} parameters: {list(params.keys())}")
            
            # Find time column
            time_col = None
            for col in df.columns:
                col_lower = str(col).lower()
                if any(t in col_lower for t in ['time', 'date', 'datetime', 'timestamp']):
                    time_col = col
                    break
            
            # Find location columns
            lat_col = next((c for c in df.columns if 'lat' in str(c).lower()), None)
            lon_col = next((c for c in df.columns if 'lon' in str(c).lower()), None)
            
            measurements = []
            
            for idx, row in df.iterrows():
                # Get timestamp
                timestamp = None
                if time_col and pd.notna(row[time_col]):
                    try:
                        timestamp = pd.to_datetime(row[time_col])
                    except:
                        pass
                
                # Get location
                location_id = None
                if lat_col and lon_col:
                    try:
                        lat = float(row[lat_col])
                        lon = float(row[lon_col])
                        location_id = get_or_create_location(self.cursor, lat, lon, metadata_id)
                    except (ValueError, TypeError):
                        pass
                
                # Extract each parameter
                for param_name, param_col in params.items():
                    try:
                        value = float(row[param_col])
                        if pd.notna(value):
                            measurements.append((
                                metadata_id,
                                location_id,
                                param_name,
                                value,
                                timestamp,
                                None,  # qc_flag
                                file_path.name
                            ))
                    except (ValueError, TypeError):
                        continue
            
            self.extracted_count += len(measurements)
            return measurements
            
        except Exception as e:
            logger.error(f"    ❌ CSV extraction failed: {e}")
            self.failed_count += 1
            return []

# ============================================================================
# NETCDF EXTRACTOR  
# ============================================================================

class NetCDFExtractor:
    """Extract measurements from NetCDF files."""
    
    def __init__(self, cursor):
        self.cursor = cursor
        self.extracted_count = 0
        self.failed_count = 0
    
    def extract(self, file_path: Path, metadata_id: int, dataset_path: str) -> list:
        """Extract measurements from a NetCDF file."""
        try:
            ds = xr.open_dataset(file_path)
            
            # Detect parameters
            params = detect_parameters(list(ds.data_vars) + list(ds.coords))
            if not params:
                logger.info(f"    ⚠ No parameter variables detected in {file_path.name}")
                ds.close()
                return []
            
            logger.info(f"    ✓ Detected {len(params)} parameters: {list(params.keys())}")
            
            measurements = []
            
            # Find time dimension
            time_var = None
            for var in ['time', 'TIME', 'Time']:
                if var in ds.coords or var in ds.data_vars:
                    time_var = var
                    break
            
            # Find location variables
            lat_var = next((v for v in ['latitude', 'lat', 'LATITUDE', 'LAT'] if v in ds.coords or v in ds.data_vars), None)
            lon_var = next((v for v in ['longitude', 'lon', 'LONGITUDE', 'LON'] if v in ds.coords or v in ds.data_vars), None)
            
            # Process each parameter
            for param_name, var_name in params.items():
                try:
                    var_data = ds[var_name]
                    
                    # Handle different data structures
                    if time_var and time_var in var_data.dims:
                        # Time series data
                        times = ds[time_var].values
                        values = var_data.values
                        
                        for i, (time_val, value) in enumerate(zip(times, values)):
                            if np.isnan(value):
                                continue
                            
                            # Convert time to datetime
                            timestamp = None
                            try:
                                if isinstance(time_val, (cftime._cftime.DatetimeGregorian, cftime._cftime.DatetimeProlepticGregorian)):
                                    timestamp = datetime(
                                        time_val.year,
                                        time_val.month,
                                        time_val.day,
                                        time_val.hour,
                                        time_val.minute,
                                        time_val.second
                                    )
                                else:
                                    timestamp = pd.to_datetime(str(time_val))
                            except:
                                pass
                            
                            # Get location for this time step
                            location_id = None
                            if lat_var and lon_var:
                                try:
                                    lat = float(ds[lat_var].isel({time_var: i}) if time_var in ds[lat_var].dims else ds[lat_var].values)
                                    lon = float(ds[lon_var].isel({time_var: i}) if time_var in ds[lon_var].dims else ds[lon_var].values)
                                    location_id = get_or_create_location(self.cursor, lat, lon, metadata_id)
                                except:
                                    pass
                            
                            measurements.append((
                                metadata_id,
                                location_id,
                                param_name,
                                float(value),
                                timestamp,
                                None,  # qc_flag
                                file_path.name
                            ))
                    
                    else:
                        # Single value or non-time-series
                        value = float(var_data.values.flat[0])
                        if not np.isnan(value):
                            location_id = None
                            if lat_var and lon_var:
                                try:
                                    lat = float(ds[lat_var].values.flat[0])
                                    lon = float(ds[lon_var].values.flat[0])
                                    location_id = get_or_create_location(self.cursor, lat, lon, metadata_id)
                                except:
                                    pass
                            
                            measurements.append((
                                metadata_id,
                                location_id,
                                param_name,
                                value,
                                None,  # timestamp
                                None,  # qc_flag
                                file_path.name
                            ))
                
                except Exception as e:
                    logger.warning(f"      ⚠ Failed to extract {param_name}: {e}")
                    continue
            
            ds.close()
            self.extracted_count += len(measurements)
            return measurements
            
        except Exception as e:
            logger.error(f"    ❌ NetCDF extraction failed: {e}")
            self.failed_count += 1
            return []

# ============================================================================
# BATCH INSERTER
# ============================================================================

class BatchInserter:
    """Batch insert measurements into database."""
    
    def __init__(self, cursor, batch_size=1000):
        self.cursor = cursor
        self.batch_size = batch_size
        self.total_inserted = 0
        self.total_failed = 0
    
    def insert_batch(self, measurements: list):
        """Insert a batch of measurements."""
        if not measurements:
            return
        
        try:
            # Split into batches
            for i in range(0, len(measurements), self.batch_size):
                batch = measurements[i:i + self.batch_size]
                
                self.cursor.executemany("""
                    INSERT INTO measurements (
                        metadata_id, location_id, parameter_name, 
                        value, timestamp, qc_flag, source_file
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, batch)
                
                self.total_inserted += len(batch)
            
            logger.info(f"    ✓ Inserted {len(measurements)} measurements")
            
        except Exception as e:
            logger.error(f"    ❌ Batch insert failed: {e}")
            self.total_failed += len(measurements)

# ============================================================================
# MAIN PROCESSING
# ============================================================================

def main():
    """Main ETL process."""
    try:
        logger.info(f"{'='*70}")
        logger.info(f"🔍 Detecting parameters in dataset columns...")
        logger.info(f"{'='*70}\n")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Initialize processors
        csv_extractor = CSVExtractor(cursor)
        nc_extractor = NetCDFExtractor(cursor)
        inserter = BatchInserter(cursor)
        
        # Get all datasets
        cursor.execute("""
            SELECT metadata_id, title, file_path
            FROM metadata
            WHERE file_path IS NOT NULL
            ORDER BY metadata_id
        """)
        
        datasets = cursor.fetchall()
        logger.info(f"Found {len(datasets)} datasets to process\n")
        
        # Process each dataset
        for metadata_id, title, dataset_path in datasets:
            logger.info(f"📂 Processing: {title}")
            
            path = Path(dataset_path)
            if not path.exists():
                logger.warning(f"  ⚠ Path not found: {dataset_path}")
                continue
            
            all_measurements = []
            
            # Find all data files
            csv_files = list(path.rglob("*.csv"))
            nc_files = list(path.rglob("*.nc"))
            
            if csv_files:
                logger.info(f"  📊 Processing {len(csv_files)} CSV files")
                for csv_file in csv_files:
                    measurements = csv_extractor.extract(csv_file, metadata_id, dataset_path)
                    all_measurements.extend(measurements)
            
            if nc_files:
                logger.info(f"  📊 Processing {len(nc_files)} NetCDF files")
                for nc_file in nc_files:
                    measurements = nc_extractor.extract(nc_file, metadata_id, dataset_path)
                    all_measurements.extend(measurements)
            
            # Insert all measurements for this dataset
            if all_measurements:
                logger.info(f"  💾 Inserting {len(all_measurements)} measurements")
                inserter.insert_batch(all_measurements)
                conn.commit()
            else:
                logger.info(f"  ⚠ No measurements extracted")
            
            logger.info("")
        
        # Final summary
        logger.info(f"\n{'='*70}")
        logger.info(f"✅ ETL Complete")
        logger.info(f"{'='*70}")
        logger.info(f"Total inserted:        {inserter.total_inserted}")
        logger.info(f"Total failed:          {inserter.total_failed}")
        logger.info(f"CSV extracted:         {csv_extractor.extracted_count} ({csv_extractor.failed_count} failed)")
        logger.info(f"NetCDF extracted:      {nc_extractor.extracted_count} ({nc_extractor.failed_count} failed)")
        logger.info(f"{'='*70}")
        logger.info(f"📝 Full log saved to: {log_filename}")
        logger.info(f"{'='*70}\n")
    
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.info(f"📝 Full log saved to: {log_filename}")
        sys.exit(1)
    
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
