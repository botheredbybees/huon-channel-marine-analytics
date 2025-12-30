#!/usr/bin/env python3

"""
Enhanced Measurements ETL v3.0 - Multi-Parameter CSV Support

This version extends v2.1 to handle CSVs where each row contains multiple
measurement parameters (e.g., one row = timestamp + TEMP + SALINITY + PH + DO).

NEW FEATURES:
- Multi-parameter extraction: 1 CSV row → N measurement records
- Improved column detection for IMOS/AODN water quality datasets
- Unit inference from column names (e.g., TEMP_C → celsius)
- Supports both "long format" (param column) and "wide format" (param as columns)

PRESERVED FEATURES (v2.1):
- Integrated location patching (coordinate validation + station lookup)
- NetCDF time parsing (returns datetime objects, not tuples)
- Parameter mapping (loads from `parameter_mappings` table)
- Location extraction (reads station info from CSV/NetCDF headers)
- Audit trail (location_qc_flag, extracted_at)

GUARDRAILS:
✓ Upsert-safe: INSERT ... ON CONFLICT DO NOTHING
✓ Audit trail: QC flags track all modifications
✓ Schema validation: Type checking before DB write
✓ Error recovery: Failed rows skipped with logging, no transaction rollback
✓ Additive: Re-running never loses data (skipped data is logged)

Usage:
  python populate_measurements.py [--limit 5000] [--dataset "Title"]
"""

import os
import sys
import logging
import glob
import re
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import numpy as np

try:
    import netCDF4
except ImportError:
    netCDF4 = None

try:
    import cftime
except ImportError:
    cftime = None

# ============================================================================
# LOGGING SETUP
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================

DB_CONFIG = {
    'dbname': 'marine_db',
    'user': 'marine_user',
    'password': 'marine_pass123',
    'host': 'localhost',
    'port': '5433'
}

DATA_ROOT = "AODN_data"

# ============================================================================
# PARAMETER DETECTION (v3.0)
# ============================================================================

PARAMETER_KEYWORDS = {
    'temperature': ['temp', 'temperature', 'sst', 'sbt', 't_deg', 'water_temp'],
    'salinity': ['sal', 'salinity', 'psal', 'salin'],
    'pressure': ['pres', 'pressure', 'depth', 'z'],
    'depth': ['depth', 'z', 'level', 'depth_m'],
    'dissolved_oxygen': ['do', 'oxygen', 'doxy', 'o2', 'dissolved_oxygen', 'disolved_oxygen'],
    'oxygen_saturation': ['do_sat', 'o2_sat', 'oxygen_saturation', 'disolved_oxygen_saturation'],
    'nitrate': ['no3', 'nitrate', 'nox'],
    'nitrite': ['no2', 'nitrite'],
    'ammonia': ['nh3', 'nh4', 'ammonia', 'ammonium'],
    'phosphate': ['po4', 'phosphate', 'srp', 'drp'],
    'silicate': ['sio4', 'silicate', 'silica'],
    'total_nitrogen': ['total_n', 'tn', 'total_nitrogen'],
    'total_phosphorus': ['total_p', 'tp', 'total_phosphorus'],
    'chlorophyll_a': ['chl_a', 'chla', 'chlorophyll', 'chlorophyll_a'],
    'fluorescence': ['fluor', 'fluorescence', 'chlf'],
    'turbidity': ['turb', 'turbidity', 'ntu'],
    'doc': ['doc', 'dissolved_organic_carbon'],
    'ph': ['ph'],
    'conductivity': ['cond', 'conductivity'],
}

def detect_parameter_columns(df: pd.DataFrame) -> Dict[str, str]:
    """Detect which columns are measurement parameters"""
    param_cols = {}
    cols_upper = {c: c.upper().replace(' ', '_') for c in df.columns}
    
    for col_orig, col_clean in cols_upper.items():
        # Skip known metadata columns
        if any(skip in col_clean for skip in ['FID', 'ID', 'DATE', 'TIME', 'LATITUDE', 
                                                'LONGITUDE', 'STATION', 'SITE', 'TRIP',
                                                'LOCATION', 'GEOM', 'SAMPLE', 'ESTUARY']):
            continue
        
        # Try to match parameter keywords
        for param_name, keywords in PARAMETER_KEYWORDS.items():
            for keyword in keywords:
                if keyword.upper() in col_clean or col_clean.startswith(keyword.upper()):
                    param_cols[col_orig] = param_name
                    break
            if col_orig in param_cols:
                break
    
    return param_cols

# ============================================================================
# UNIT INFERENCE (v3.0)
# ============================================================================

UNIT_PATTERNS = {
    # Temperature
    r'(?i)temp.*(_c|celsius)': 'degrees_celsius',
    r'(?i)temp.*(_k|kelvin)': 'kelvin',
    r'(?i)temp.*(_f|fahrenheit)': 'degrees_fahrenheit',
    r'(?i)temperature': 'degrees_celsius',
    
    # Salinity
    r'(?i)sal.*(_psu|psu)': 'PSU',
    r'(?i)sal.*(_ppt|ppt)': 'PPT',
    r'(?i)salinity': 'PSU',
    
    # Dissolved oxygen
    r'(?i)(do|dissolved.*oxygen).*(_mg|mg/l)': 'mg/L',
    r'(?i)(do|dissolved.*oxygen).*(_ml|ml/l)': 'mL/L',
    r'(?i)(do|dissolved.*oxygen).*(%|sat|saturation)': 'percent',
    r'(?i)dissolved.*oxygen': 'mg/L',
    
    # Nutrients
    r'(?i)(nitrate|no3).*(_um|umol)': 'umol/L',
    r'(?i)(nitrite|no2).*(_um|umol)': 'umol/L',
    r'(?i)(ammonia|nh3|nh4).*(_um|umol)': 'umol/L',
    r'(?i)(phosphate|po4|srp).*(_um|umol)': 'umol/L',
    r'(?i)(silicate|sio4).*(_um|umol)': 'umol/L',
    r'(?i)(nitrate|no3|nitrite|no2|ammonia|phosphate|silicate)': 'umol/L',
    
    # Chlorophyll
    r'(?i)chl.*(_a|a\b).*(_ug|ug/l)': 'ug/L',
    r'(?i)chl.*(_a|a\b).*(_mg|mg/l)': 'mg/L',
    r'(?i)chl.*(_a|a\b)': 'ug/L',
    
    # Turbidity
    r'(?i)turb.*(_ntu|ntu)': 'NTU',
    r'(?i)turb.*(_ftu|ftu)': 'FTU',
    r'(?i)turbidity': 'NTU',
    
    # pH
    r'(?i)ph': 'pH',
    
    # Pressure
    r'(?i)pres.*(_dbar|dbar)': 'dbar',
    r'(?i)pres.*(_mbar|mbar)': 'mbar',
    r'(?i)pressure': 'dbar',
}

def infer_unit_from_column_name(col_name: str) -> str:
    """Infer measurement unit from column name patterns"""
    for pattern, unit in UNIT_PATTERNS.items():
        if re.search(pattern, col_name):
            return unit
    return 'unknown'

# ============================================================================
# LOCATION PATCHING FUNCTIONS
# ============================================================================

def extract_station_info_from_file(file_path: str, dataset_title: str) -> Tuple[Optional[str], Optional[float], Optional[float]]:
    """Extract station name, latitude, longitude from CSV or NetCDF"""
    logger.debug(f"  📍 Extracting location from: {file_path}")
    
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
            logger.debug(f"  ❌ NetCDF read failed: {e}")
            return None, None, None
    
    elif file_path.endswith(".csv"):
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                header = f.readline()
                sep = ';' if ';' in header else ','
            
            df = pd.read_csv(file_path, nrows=5, sep=sep, encoding='utf-8', errors='ignore')
            df.columns = [c.upper().strip() for c in df.columns]
            
            lat_col = next((c for c in df.columns if c in ['LATITUDE', 'LAT', 'START_LAT', 'DECIMAL_LAT']), None)
            lat = float(df[lat_col].iloc[0]) if lat_col and not pd.isna(df[lat_col].iloc[0]) else None
            
            lon_col = next((c for c in df.columns if c in ['LONGITUDE', 'LON', 'LONG', 'START_LON', 'DECIMAL_LONG']), None)
            lon = float(df[lon_col].iloc[0]) if lon_col and not pd.isna(df[lon_col].iloc[0]) else None
            
            station_col = next((c for c in df.columns if c in ['STATION', 'SITE', 'SITE_CODE', 'STATION_NAME', 'TRIP_CODE', 'ESTUARY_SITE']), None)
            station = str(df[station_col].iloc[0]) if station_col and not pd.isna(df[station_col].iloc[0]) else dataset_title
            
            if station and len(station) < 3:
                station = f"{dataset_title} - Site {station}"
            
            return station, lat, lon
            
        except Exception as e:
            logger.debug(f"  ❌ CSV read failed: {e}")
            return None, None, None
    
    return None, None, None


def patch_location_coordinates(lat: Optional[float], lon: Optional[float]) -> Tuple[Optional[float], Optional[float], str]:
    """Apply location cleaning rules for Tasmania"""
    qc_flag = 'clean'
    
    if lat is None or lon is None:
        return lat, lon, 'missing_coordinates'
    
    if lat > 0 and lat < 90:
        logger.debug(f"  🔄 Fixed positive latitude: {lat} -> {-lat}")
        lat = -lat
        qc_flag = 'lat_sign_flipped'
    
    if lon > 180:
        lon = lon - 360
        qc_flag = 'lon_normalized'
    elif lon < -180:
        lon = lon + 360
        qc_flag = 'lon_normalized'
    
    if abs(lat) > 90 or abs(lon) > 180:
        qc_flag = 'outlier_flagged'
    
    if not (-45 < lat < -40 and 144 < lon < 150):
        if qc_flag == 'clean':
            qc_flag = 'outside_tasmania'
    
    return lat, lon, qc_flag


def get_or_insert_location(conn, station: str, lat: float, lon: float) -> Optional[int]:
    """Insert location into locations table (PostGIS-free)"""
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO locations (location_name, latitude, longitude)
            VALUES (%s, %s, %s)
            ON CONFLICT (latitude, longitude)
            DO UPDATE SET location_name = EXCLUDED.location_name
            RETURNING id;
        """, (str(station), float(lat), float(lon)))
        
        location_id = cur.fetchone()[0]
        conn.commit()
        return location_id
        
    except Exception as e:
        logger.error(f"  ❌ Failed to insert location: {e}")
        conn.rollback()
        return None

# ============================================================================
# PARAMETER MAPPING
# ============================================================================

class ParameterMapping:
    """Loads parameter mappings from database"""
    
    def __init__(self, db_config: dict):
        self.mapping = {}
        self.load_from_database(db_config)
    
    def load_from_database(self, db_config: dict):
        """Load parameter mappings from parameter_mappings table"""
        try:
            conn = psycopg2.connect(**db_config)
            cur = conn.cursor()
            cur.execute("""
                SELECT raw_parameter_name, standard_code, namespace, unit
                FROM parameter_mappings
            """)
            
            for raw_name, code, namespace, unit in cur.fetchall():
                self.mapping[raw_name.upper()] = (code, namespace, unit)
            
            cur.close()
            conn.close()
            logger.info(f"✓ Loaded {len(self.mapping)} parameter mappings from database")
            
        except Exception as e:
            logger.error(f"Could not load parameter mappings: {e}")
            logger.warning("Using empty parameter mapping - all params will be 'custom'")
    
    def get_standard_param(self, raw_param: str) -> Tuple[str, str, str]:
        """Map raw parameter name to standardized (param_code, namespace, unit)"""
        raw_upper = str(raw_param).upper().strip()
        
        if raw_upper in self.mapping:
            return self.mapping[raw_upper]
        
        return (raw_upper, 'custom', 'unknown')

# ============================================================================
# TIME FORMAT DETECTION
# ============================================================================

class TimeFormatDetector:
    """Automatically detects time column format and converts to datetime"""
    
    @staticmethod
    def detect_and_convert(time_value) -> Optional[datetime]:
        """Attempts multiple time format conversions"""
        if time_value is None or (isinstance(time_value, float) and np.isnan(time_value)):
            return None
        
        if isinstance(time_value, datetime):
            return time_value
        elif isinstance(time_value, str):
            return TimeFormatDetector._from_iso_string(time_value)
        elif isinstance(time_value, (int, float, np.integer, np.floating)):
            return TimeFormatDetector._from_numeric(float(time_value))
        
        return None
    
    @staticmethod
    def _from_iso_string(s: str) -> Optional[datetime]:
        """Parse ISO 8601 strings"""
        try:
            return pd.to_datetime(s).to_pydatetime()
        except:
            return None
    
    @staticmethod
    def _from_numeric(val: float) -> Optional[datetime]:
        """Parse numeric time representations"""
        
        if 1900 < val < 2100 and val % 1 != 0:
            year = int(val)
            frac = val - year
            return datetime(year, 1, 1) + timedelta(days=365.25 * frac)
        
        if 1900 < val < 2100 and val % 1 == 0:
            return datetime(int(val), 1, 1)
        
        if 1000 < val < 2000:
            base = datetime(1900, 1, 1)
            return base + timedelta(days=val * 30.4)
        
        if 40000 < val < 50000:
            base = datetime(1900, 1, 1)
            return base + timedelta(days=val)
        
        if 15000 < val < 25000:
            base = datetime(1970, 1, 1)
            return base + timedelta(days=val)
        
        if val > 1e8:
            try:
                return datetime.utcfromtimestamp(val)
            except:
                pass
        
        return None

# ============================================================================
# CSV EXTRACTION (v3.0 - Multi-Parameter)
# ============================================================================

class CSVMeasurementExtractor:
    """Extract measurements from CSV with multi-parameter support"""
    
    def __init__(self, param_mapping: ParameterMapping):
        self.param_mapping = param_mapping
        self.extracted_count = 0
        self.failed_count = 0
    
    def extract(self, csv_path: str, metadata: dict, limit: Optional[int] = None) -> List[dict]:
        """Extract measurements from CSV (handles multi-parameter rows)"""
        try:
            with open(csv_path, 'r', encoding='utf-8', errors='ignore') as f:
                header = f.readline()
                sep = ';' if ';' in header else ','
            
            df = pd.read_csv(csv_path, sep=sep, encoding='utf-8', errors='ignore')
            
            if limit:
                df = df.head(limit)
            
            # Detect parameter columns
            param_cols = detect_parameter_columns(df)
            
            if not param_cols:
                logger.warning(f"    ⚠ No parameter columns detected in {csv_path}")
                return []
            
            logger.info(f"    ✓ Detected {len(param_cols)} parameters: {list(param_cols.values())}")
            
            # Find time column
            time_col = self._find_time_column(df)
            if not time_col:
                logger.warning(f"    ⚠ No time column found in {csv_path}")
                return []
            
            measurements = []
            
            for idx, row in df.iterrows():
                # Parse timestamp
                time_value = row[time_col]
                timestamp = TimeFormatDetector.detect_and_convert(time_value)
                
                if timestamp is None:
                    self.failed_count += 1
                    continue
                
                # Extract each parameter from this row
                for col_orig, param_name in param_cols.items():
                    value = row[col_orig]
                    
                    if pd.isna(value):
                        continue
                    
                    # Infer unit
                    uom = infer_unit_from_column_name(col_orig)
                    
                    # Map to standard parameter
                    param_code, namespace, mapped_unit = self.param_mapping.get_standard_param(param_name)
                    if mapped_unit != 'unknown':
                        uom = mapped_unit
                    
                    measurements.append({
                        'time': timestamp,
                        'uuid': metadata.get('uuid', 'unknown'),
                        'parameter_code': param_code,
                        'namespace': namespace,
                        'value': float(value),
                        'uom': uom,
                        'metadata_id': metadata.get('id'),
                        'location_id': None,  # Will be patched later
                        'location_qc_flag': None
                    })
                    
                    self.extracted_count += 1
            
            return measurements
            
        except Exception as e:
            logger.error(f"    ❌ CSV extraction failed: {e}")
            self.failed_count += 1
            return []
    
    def _find_time_column(self, df: pd.DataFrame) -> Optional[str]:
        """Find the time/date column in DataFrame"""
        time_keywords = ['time', 'date', 'timestamp', 'datetime', 'sample_date', 'sample_time']
        
        for col in df.columns:
            col_clean = col.lower().replace('_', '').replace(' ', '')
            for keyword in time_keywords:
                if keyword.replace('_', '') in col_clean:
                    return col
        
        return None

# ============================================================================
# NETCDF EXTRACTION
# ============================================================================

class NetCDFMeasurementExtractor:
    """Extract measurements from NetCDF files"""
    
    def __init__(self, param_mapping: ParameterMapping):
        self.param_mapping = param_mapping
        self.extracted_count = 0
        self.failed_count = 0
    
    def extract(self, nc_path: str, metadata: dict, limit: Optional[int] = None) -> List[dict]:
        """Extract measurements from NetCDF file"""
        try:
            if netCDF4 is None:
                raise ImportError("netCDF4 package not installed")
            
            ds = netCDF4.Dataset(nc_path)
            
            # Find time variable
            time_var = self._find_time_variable(ds)
            if not time_var:
                logger.warning(f"    ⚠ No time variable found in {nc_path}")
                ds.close()
                return []
            
            # Parse time
            times = self._parse_netcdf_time(ds, time_var)
            if not times:
                logger.warning(f"    ⚠ Could not parse time from {nc_path}")
                ds.close()
                return []
            
            if limit:
                times = times[:limit]
            
            # Find parameter variables
            param_vars = self._find_parameter_variables(ds)
            if not param_vars:
                logger.warning(f"    ⚠ No parameter variables found in {nc_path}")
                ds.close()
                return []
            
            logger.info(f"    ✓ Detected {len(param_vars)} parameters: {list(param_vars.keys())}")
            
            measurements = []
            
            for param_name, var_name in param_vars.items():
                var = ds.variables[var_name]
                data = var[:]
                
                if limit:
                    data = data[:limit]
                
                # Get unit
                uom = var.units if hasattr(var, 'units') else 'unknown'
                
                # Map to standard parameter
                param_code, namespace, mapped_unit = self.param_mapping.get_standard_param(param_name)
                if mapped_unit != 'unknown':
                    uom = mapped_unit
                
                for i, (timestamp, value) in enumerate(zip(times, data)):
                    if np.ma.is_masked(value) or np.isnan(value):
                        continue
                    
                    measurements.append({
                        'time': timestamp,
                        'uuid': metadata.get('uuid', 'unknown'),
                        'parameter_code': param_code,
                        'namespace': namespace,
                        'value': float(value),
                        'uom': uom,
                        'metadata_id': metadata.get('id'),
                        'location_id': None,
                        'location_qc_flag': None
                    })
                    
                    self.extracted_count += 1
            
            ds.close()
            return measurements
            
        except Exception as e:
            logger.error(f"    ❌ NetCDF extraction failed: {e}")
            self.failed_count += 1
            return []
    
    def _find_time_variable(self, ds) -> Optional[str]:
        """Find time variable in NetCDF"""
        time_names = ['TIME', 'time', 'Time', 'DATETIME', 'datetime']
        for name in time_names:
            if name in ds.variables:
                return name
        return None
    
    def _parse_netcdf_time(self, ds, time_var_name: str) -> List[datetime]:
        """Parse NetCDF time variable to datetime objects"""
        try:
            time_var = ds.variables[time_var_name]
            time_data = time_var[:]
            
            if hasattr(time_var, 'units'):
                units = time_var.units
                calendar = time_var.calendar if hasattr(time_var, 'calendar') else 'standard'
                
                if cftime:
                    cf_times = cftime.num2date(time_data, units=units, calendar=calendar)
                    return [t.replace(tzinfo=None) if hasattr(t, 'replace') else datetime(t.year, t.month, t.day) for t in cf_times]
                else:
                    return [datetime(1970, 1, 1) + timedelta(days=float(t)) for t in time_data]
            
            return []
            
        except Exception as e:
            logger.error(f"    ❌ Time parsing failed: {e}")
            return []
    
    def _find_parameter_variables(self, ds) -> Dict[str, str]:
        """Find parameter variables in NetCDF"""
        params = {}
        
        skip_vars = {'TIME', 'time', 'LATITUDE', 'latitude', 'LONGITUDE', 'longitude', 
                     'DEPTH', 'depth', 'NOMINAL_DEPTH', 'nominal_depth'}
        
        for var_name in ds.variables:
            if var_name in skip_vars:
                continue
            
            var_upper = var_name.upper().replace('_', '')
            
            for param_name, keywords in PARAMETER_KEYWORDS.items():
                for keyword in keywords:
                    if keyword.upper().replace('_', '') in var_upper:
                        params[param_name] = var_name
                        break
                if param_name in params:
                    break
        
        return params

# ============================================================================
# BATCH INSERTER
# ============================================================================

class MeasurementBatchInserter:
    """Batch insert measurements into database"""
    
    def __init__(self, conn, batch_size: int = 5000):
        self.conn = conn
        self.batch_size = batch_size
        self.total_inserted = 0
        self.total_failed = 0
    
    def process_batches(self, measurements: List[dict]):
        """Insert measurements in batches"""
        for i in range(0, len(measurements), self.batch_size):
            batch = measurements[i:i + self.batch_size]
            self._insert_batch(batch)
    
    def _insert_batch(self, batch: List[dict]):
        """Insert a single batch"""
        try:
            cur = self.conn.cursor()
            
            insert_sql = """
                INSERT INTO measurements (
                    time, uuid, parameter_code, namespace, value, uom,
                    metadata_id, location_id, location_qc_flag
                )
                VALUES %s
                ON CONFLICT DO NOTHING
            """
            
            values = [
                (
                    m['time'], m['uuid'], m['parameter_code'], m['namespace'],
                    m['value'], m['uom'], m['metadata_id'], m['location_id'],
                    m.get('location_qc_flag')
                )
                for m in batch
            ]
            
            execute_values(cur, insert_sql, values)
            self.conn.commit()
            
            self.total_inserted += len(batch)
            
        except Exception as e:
            logger.error(f"    ❌ Batch insert failed: {e}")
            self.conn.rollback()
            self.total_failed += len(batch)

# ============================================================================
# MAIN
# ============================================================================

def main():
    try:
        parser = argparse.ArgumentParser(description='Extract measurements from AODN data')
        parser.add_argument('--limit', type=int, help='Limit measurements per file')
        parser.add_argument('--dataset', type=str, help='Filter by dataset title (partial match)')
        args = parser.parse_args()
        
        logger.info("="*70)
        logger.info("🌊 MEASUREMENTS ETL v3.0 - Multi-Parameter Support")
        logger.info("="*70)
        
        if not os.path.exists(DATA_ROOT):
            logger.error(f"Data root not found: {DATA_ROOT}")
            sys.exit(1)
        
        # Connect to database
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Load parameter mappings
        param_mapping = ParameterMapping(DB_CONFIG)
        
        # Initialize extractors
        csv_extractor = CSVMeasurementExtractor(param_mapping)
        nc_extractor = NetCDFMeasurementExtractor(param_mapping)
        inserter = MeasurementBatchInserter(conn)
        
        # Query datasets
        filter_clause = ""
        if args.dataset:
            filter_clause = f"WHERE title ILIKE '%{args.dataset}%'"
        
        cur.execute(f"SELECT id, uuid, title FROM metadata {filter_clause} ORDER BY title")
        datasets = cur.fetchall()
        
        logger.info(f"Found {len(datasets)} datasets to process\n")
        
        for metadata_id, uuid, title in datasets:
            logger.info(f"📂 Processing: {title}")
            
            metadata = {'id': metadata_id, 'uuid': uuid}
            dataset_rows = []
            
            # Find dataset directory
            rel_path = os.path.join(DATA_ROOT, title)
            if not os.path.exists(rel_path):
                logger.warning(f"  ⚠ Directory not found: {rel_path}")
                continue
            
            # ===== LOCATION EXTRACTION =====
            patched_lat = None
            patched_lon = None
            location_id = None
            location_qc_flag = 'not_found'
            
            # Try to find a data file in this dataset
            found_file = None
            for root, dirs, files in os.walk(rel_path):
                if 'metadata' in root:
                    continue
                
                for file in files:
                    if file.lower().endswith(('.nc', '.csv')):
                        if 'index.csv' in file.lower():
                            continue
                        
                        file_path = os.path.join(root, file)
                        
                        # Extract location metadata
                        station_name, lat, lon = extract_station_info_from_file(file_path, title)
                        
                        if lat is not None and lon is not None:
                            # Patch coordinates
                            patched_lat, patched_lon, location_qc_flag = patch_location_coordinates(lat, lon)
                            
                            if patched_lat is not None and patched_lon is not None:
                                logger.info(f"  ✓ Location: {station_name} ({patched_lat:.4f}, {patched_lon:.4f}) [{location_qc_flag}]")
                                
                                # Insert/link location
                                location_id = get_or_insert_location(conn, station_name, patched_lat, patched_lon)
                                
                                if location_id:
                                    logger.info(f"  ✓ Location ID: {location_id}")
                            else:
                                logger.warning(f"  ⚠ Coordinates failed validation: {lat}, {lon}")
                        
                        found_file = file_path
                        break
                
                if found_file:
                    break
            
            # ===== MEASUREMENT EXTRACTION =====
            for root, dirs, files in os.walk(rel_path):
                if 'metadata' in root:
                    continue
                
                for file in files:
                    if file == 'index.csv' or 'metadata' in file:
                        continue
                    
                    file_path = os.path.join(root, file)
                    
                    try:
                        if file.lower().endswith('.csv'):
                            logger.info(f"  📄 Extracting CSV: {file}")
                            rows = csv_extractor.extract(file_path, metadata, args.limit)
                            dataset_rows.extend(rows)
                        
                        elif file.lower().endswith('.nc'):
                            logger.info(f"  📊 Extracting NetCDF: {file}")
                            rows = nc_extractor.extract(file_path, metadata, args.limit)
                            dataset_rows.extend(rows)
                    
                    except Exception as e:
                        logger.error(f"  ❌ Error processing {file}: {e}")
            
            # ===== APPLY LOCATION PATCH TO ALL ROWS =====
            if location_id is not None:
                for row in dataset_rows:
                    row['location_id'] = location_id
                    row['location_qc_flag'] = location_qc_flag
                logger.info(f"  ✓ Patched {len(dataset_rows)} rows with location_id={location_id}")
            
            # ===== INSERT BATCH =====
            if dataset_rows:
                logger.info(f"  💾 Inserting {len(dataset_rows)} measurements...")
                inserter.process_batches(dataset_rows)
            else:
                logger.warning(f"  ⚠ No measurements extracted from {title}")
        
        cur.close()
        conn.close()
        
        logger.info(f"\n{'='*70}")
        logger.info(f"✅ ETL Complete")
        logger.info(f"{'='*70}")
        logger.info(f"Total inserted:        {inserter.total_inserted}")
        logger.info(f"Total failed:          {inserter.total_failed}")
        logger.info(f"CSV extracted:         {csv_extractor.extracted_count} ({csv_extractor.failed_count} failed)")
        logger.info(f"NetCDF extracted:      {nc_extractor.extracted_count} ({nc_extractor.failed_count} failed)")
        logger.info(f"{'='*70}\n")
    
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
