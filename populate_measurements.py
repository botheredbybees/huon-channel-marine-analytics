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
- Metadata-based parameter detection (v4.0 - fixes Issues #5-8)

Version 4.0 Change: Now uses metadata XML CF standard_name as authoritative source
for parameter codes instead of NetCDF variable names. This prevents misidentification
of parameters like PH (pH vs phosphate ambiguity).
"""

import sys
import logging
import psycopg2
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List
import xarray as xr
import cftime
import xml.etree.ElementTree as ET

# ============================================================================
# LOGGING SETUP
# ============================================================================

logs_dir = Path('logs')
logs_dir.mkdir(exist_ok=True)

log_filename = logs_dir / f'etl_measurements_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

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
        port=5433,
        dbname="marine_db",
        user="marine_user",
        password="marine_pass123"
    )

# ============================================================================
# CF STANDARD NAME TO PARAMETER CODE MAPPING
# ============================================================================

# Authoritative mapping from CF standard_name (from metadata XML) to parameter codes
CF_STANDARD_NAME_TO_CODE = {
    # Temperature variants
    'sea_water_temperature': 'TEMP',
    'sea_surface_temperature': 'TEMP',
    'sea_water_conservative_temperature': 'TEMP',
    
    # Salinity variants
    'sea_water_salinity': 'PSAL',
    'sea_water_practical_salinity': 'PSAL',
    'sea_water_absolute_salinity': 'PSAL',
    
    # Pressure/Depth
    'sea_water_pressure': 'PRES',
    'sea_water_pressure_due_to_sea_water': 'PRES',
    'depth': 'DEPTH',
    
    # Oxygen
    'mole_concentration_of_dissolved_molecular_oxygen_in_sea_water': 'DOXY',
    'mass_concentration_of_oxygen_in_sea_water': 'DOXY',
    
    # Chlorophyll  
    'mass_concentration_of_chlorophyll_a_in_sea_water': 'CPHL',
    'mass_concentration_of_chlorophyll_in_sea_water': 'CPHL',
    
    # Nutrients - THESE ARE KEY TO FIXING ISSUE #5
    'mole_concentration_of_phosphate_in_sea_water': 'PO4',  # Phosphate (NOT pH!)
    'mole_concentration_of_nitrate_in_sea_water': 'NO3',
    'mole_concentration_of_silicate_in_sea_water': 'SIO4',
    
    # pH - separate from phosphate
    'sea_water_ph_reported_on_total_scale': 'PH',
    'sea_water_ph': 'PH',
    
    # Turbidity
    'sea_water_turbidity': 'TURB',
    
    # Currents
    'sea_water_speed': 'VCUR',
    'eastward_sea_water_velocity': 'UCUR', 
    'northward_sea_water_velocity': 'VCUR',
}

# ============================================================================
# METADATA EXTRACTION
# ============================================================================

def extract_parameters_from_metadata(metadata_id: int, cursor) -> Dict[str, str]:
    """
    Extract parameter codes from metadata XML using CF standard_name.
    
    This is the NEW authoritative method (v4.0) that replaces NetCDF variable detection.
    
    Args:
        metadata_id: The metadata record ID
        cursor: Database cursor
        
    Returns:
        Dict mapping NetCDF variable names to parameter codes (e.g., {'TEMP': 'TEMP', 'PO4': 'PO4'})
    """
    try:
        # Get metadata XML content
        cursor.execute("""
            SELECT metadata_content 
            FROM metadata 
            WHERE id = %s
        """, (metadata_id,))
        
        result = cursor.fetchone()
        if not result or not result[0]:
            logger.warning(f"    ⚠ No metadata XML found for metadata_id={metadata_id}")
            return {}
        
        metadata_xml = result[0]
        
        # Parse XML
        root = ET.fromstring(metadata_xml)
        
        # Define XML namespaces
        namespaces = {
            'gmd': 'http://www.isotc211.org/2005/gmd',
            'gco': 'http://www.isotc211.org/2005/gco',
            'mcp': 'http://bluenet3.antcrc.utas.edu.au/mcp',
            'gmx': 'http://www.isotc211.org/2005/gmx'
        }
        
        param_mapping = {}
        
        # Extract CF standard_name from contentInfo sections
        content_infos = root.findall('.//gmd:contentInfo', namespaces)
        
        for content_info in content_infos:
            # Get dimension/attribute elements
            dimensions = content_info.findall('.//gmd:dimension', namespaces)
            attributes = content_info.findall('.//gmd:attribute', namespaces)
            
            for element in dimensions + attributes:
                # Get sequence identifier (NetCDF variable name)
                seq_id_elem = element.find('.//gmd:sequenceIdentifier/gco:MemberName/gco:aName/gco:CharacterString', namespaces)
                
                # Get standard name (CF standard_name)
                std_name_elem = element.find('.//gmd:name/gco:CharacterString', namespaces)
                
                if seq_id_elem is not None and std_name_elem is not None:
                    netcdf_var = seq_id_elem.text
                    cf_standard_name = std_name_elem.text
                    
                    # Map CF standard_name to parameter code
                    if cf_standard_name in CF_STANDARD_NAME_TO_CODE:
                        param_code = CF_STANDARD_NAME_TO_CODE[cf_standard_name]
                        param_mapping[netcdf_var] = param_code
                        logger.info(f"    ✓ Mapped '{netcdf_var}' → '{param_code}' (CF: {cf_standard_name})")
        
        return param_mapping
        
    except Exception as e:
        logger.error(f"    ❌ Failed to extract parameters from metadata: {e}")
        return {}

# ============================================================================
# LOCATION VALIDATION
# ============================================================================

def get_or_create_location(cursor, latitude: float, longitude: float, metadata_id: int) -> Optional[int]:
    """Get existing location ID or create new one if coordinates are valid."""
    
    if not (-90 <= latitude <= 90) or not (-180 <= longitude <= 180):
        return None
        
    cursor.execute("""
        SELECT id 
        FROM locations 
        WHERE ABS(latitude - %s) < 0.0001 
          AND ABS(longitude - %s) < 0.0001
        LIMIT 1
    """, (latitude, longitude))
    
    result = cursor.fetchone()
    if result:
        return result[0]
    
    cursor.execute("""
        INSERT INTO locations (latitude, longitude)
        VALUES (%s, %s)
        RETURNING id
    """, (latitude, longitude))
    
    return cursor.fetchone()[0]

# ============================================================================
# FALLBACK: COLUMN-BASED DETECTION (for datasets without metadata)
# ============================================================================

PARAMETER_KEYWORDS = {
    'TEMP': ['temp', 'temperature', 'sst'],
    'PSAL': ['sal', 'salinity', 'psal'],
    'PRES': ['pres', 'pressure'],
    'DOXY': ['oxygen', 'o2', 'doxy'],
    'CPHL': ['chlorophyll', 'chl', 'chla'],
    'TURB': ['turbidity', 'turb'],
    'PH': ['ph_total', 'ph_insitu', 'ph_seawater'],
    'PO4': ['phosphate', 'po4', 'phos'],
}

def detect_parameters_fallback(columns) -> Dict[str, str]:
    """
    Fallback method: Detect parameters from column names when metadata unavailable.
    
    IMPORTANT: This is a fallback only. Metadata-based detection is preferred.
    """
    detected = {}
    
    for param_code, keywords in PARAMETER_KEYWORDS.items():
        for col in columns:
            col_lower = str(col).lower()
            if any(keyword in col_lower for keyword in keywords):
                detected[col] = param_code
                break
    
    return detected

# ============================================================================
# NETCDF EXTRACTOR (v4.0 - Uses Metadata)
# ============================================================================

class NetCDFExtractor:
    """Extract measurements from NetCDF files using metadata-based parameter detection."""
    
    def __init__(self, cursor):
        self.cursor = cursor
        self.extracted_count = 0
        self.failed_count = 0
    
    def extract(self, file_path: Path, metadata_id: int, dataset_path: str) -> list:
        """Extract measurements from a NetCDF file using metadata for parameter codes."""
        try:
            ds = xr.open_dataset(file_path)
            
            # **NEW v4.0**: Get parameter mapping from metadata XML (authoritative source)
            param_mapping = extract_parameters_from_metadata(metadata_id, self.cursor)
            
            if not param_mapping:
                logger.warning(f"    ⚠ No parameters found in metadata, using fallback detection")
                # Fallback: detect from variable names
                param_mapping = detect_parameters_fallback(list(ds.data_vars))
            
            if not param_mapping:
                logger.info(f"    ⚠ No parameter variables detected in {file_path.name}")
                ds.close()
                return []
            
            logger.info(f"    ✓ Detected {len(param_mapping)} parameters from metadata")
            
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
            for netcdf_var, param_code in param_mapping.items():
                if netcdf_var not in ds.data_vars:
                    continue
                    
                try:
                    var_data = ds[netcdf_var]
                    
                    if time_var and time_var in var_data.dims:
                        # Time series data
                        times = ds[time_var].values
                        values = var_data.values
                        
                        for i, (time_val, value) in enumerate(zip(times, values)):
                            if np.isnan(value):
                                continue
                            
                            # Convert time
                            timestamp = datetime.now()
                            try:
                                if isinstance(time_val, (cftime._cftime.DatetimeGregorian, cftime._cftime.DatetimeProlepticGregorian)):
                                    timestamp = datetime(
                                        time_val.year, time_val.month, time_val.day,
                                        time_val.hour, time_val.minute, time_val.second
                                    )
                                else:
                                    timestamp = pd.to_datetime(str(time_val))
                            except:
                                pass
                            
                            # Get location
                            location_id = None
                            if lat_var and lon_var:
                                try:
                                    lat = float(ds[lat_var].isel({time_var: i}) if time_var in ds[lat_var].dims else ds[lat_var].values)
                                    lon = float(ds[lon_var].isel({time_var: i}) if time_var in ds[lon_var].dims else ds[lon_var].values)
                                    location_id = get_or_create_location(self.cursor, lat, lon, metadata_id)
                                except:
                                    pass
                            
                            # Determine namespace
                            namespace = 'bodc' if param_code in ['PO4', 'PH', 'NO3', 'SIO4'] else 'custom'
                            
                            measurements.append((
                                timestamp,
                                metadata_id,
                                location_id,
                                param_code,
                                namespace,
                                float(value),
                                'unknown',
                                None,
                                None,
                                1
                            ))
                    
                except Exception as e:
                    logger.warning(f"      ⚠ Failed to extract {netcdf_var}: {e}")
                    continue
            
            ds.close()
            self.extracted_count += len(measurements)
            return measurements
            
        except Exception as e:
            logger.error(f"    ❌ NetCDF extraction failed: {e}")
            self.failed_count += 1
            return []

# ============================================================================
# CSV EXTRACTOR (unchanged from v3.3)
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
            df = pd.read_csv(
                file_path,
                parse_dates=True,
                on_bad_lines='skip',
                encoding_errors='ignore'
            )
            
            if df.empty:
                return []
            
            # Use fallback detection for CSV (no NetCDF metadata)
            params = detect_parameters_fallback(df.columns)
            
            if not params:
                logger.info(f"    ⚠ No parameter columns detected in {file_path.name}")
                return []
            
            logger.info(f"    ✓ Detected {len(params)} parameters: {list(set(params.values()))}")
            
            # Find time and location columns
            time_col = None
            for col in df.columns:
                col_lower = str(col).lower()
                if any(t in col_lower for t in ['time', 'date', 'datetime', 'timestamp']):
                    time_col = col
                    break
            
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
                for param_col, param_code in params.items():
                    try:
                        value = float(row[param_col])
                        if pd.notna(value):
                            namespace = 'bodc' if param_code in ['PO4', 'PH', 'NO3', 'SIO4'] else 'custom'
                            
                            measurements.append((
                                timestamp or datetime.now(),
                                metadata_id,
                                location_id,
                                param_code,
                                namespace,
                                value,
                                'unknown',
                                None,
                                None,
                                1
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
            for i in range(0, len(measurements), self.batch_size):
                batch = measurements[i:i + self.batch_size]
                
                self.cursor.executemany("""
                    INSERT INTO measurements (
                        time, metadata_id, location_id, parameter_code, 
                        namespace, value, uom, uncertainty, depth_m, quality_flag
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
        logger.info(f"🔍 v4.0: Metadata-based parameter detection (Fixes Issues #5-8)")
        logger.info(f"💡 Using CF standard_name from metadata XML as authoritative source")
        logger.info(f"{'='*70}\n")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        csv_extractor = CSVExtractor(cursor)
        nc_extractor = NetCDFExtractor(cursor)
        inserter = BatchInserter(cursor)
        
        cursor.execute("""
            SELECT id, title, dataset_path
            FROM metadata
            WHERE dataset_path IS NOT NULL
            ORDER BY id
        """)
        
        datasets = cursor.fetchall()
        logger.info(f"Found {len(datasets)} datasets to process\n")
        
        for metadata_id, title, dataset_path in datasets:
            logger.info(f"📂 Processing: {title}")
            
            path = Path(dataset_path)
            if not path.exists():
                logger.warning(f"  ⚠ Path not found: {dataset_path}")
                continue
            
            all_measurements = []
            
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
            
            if all_measurements:
                logger.info(f"  💾 Inserting {len(all_measurements)} measurements")
                inserter.insert_batch(all_measurements)
                conn.commit()
            else:
                logger.info(f"  ⚠ No measurements extracted")
            
            logger.info("")
        
        logger.info(f"\n{'='*70}")
        logger.info(f"✅ ETL Complete")
        logger.info(f"{'='*70}")
        logger.info(f"Total inserted:        {inserter.total_inserted}")
        logger.info(f"Total failed:          {inserter.total_failed}")
        logger.info(f"CSV extracted:         {csv_extractor.extracted_count}")
        logger.info(f"NetCDF extracted:      {nc_extractor.extracted_count}")
        logger.info(f"{'='*70}")
        logger.info(f"📝 Full log saved to: {log_filename}")
        logger.info(f"{'='*70}\n")
    
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.info(f"📝 Full log saved to: {log_filename}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
    
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
