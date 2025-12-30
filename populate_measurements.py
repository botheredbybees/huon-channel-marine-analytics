#!/usr/bin/env python3

"""
Enhanced Measurements ETL v3.4 - Date Validation

NEW in v3.4:
- Date validation: Rejects invalid dates (1900-01-01 sentinel, far future, etc.)
- Range enforcement: Only dates between 1901-01-01 and current_year+1 accepted
- Sentinel detection: Filters 1900-01-01 and 1970-01-01 00:00:00 (Unix epoch zero)
- Failed date tracking: Invalid dates logged in failed_count

v3.3 features:
- Stricter QC filtering: Now rejects QC=3 (probably bad) data
- Only accepts QC=1 (good) and QC=2 (probably good)
- Aligns with scientific best practices for data quality

v3.2 features:
- QC flag lookup: Reads QC column values (e.g., TEMP_QUALITY_CONTROL)
- Bad data filtering: Skips measurements where QC flag = 4 (bad) or 9 (missing)
- QC statistics: Tracks skipped measurements and QC flag distribution
- NetCDF QC variables: Reads QC arrays from NetCDF files

v3.1 features:
- 3D gridded NetCDF extraction (time × lat × lon)
- Spatial bounding box filtering (only extract cells within study area)
- Grid cell location creation (each grid cell gets a location record)
- Support for single-point grids (lat=1, lon=1) as station timeseries

v3.0 features:
- Multi-parameter extraction: 1 CSV row → N measurement records
- Improved column detection for IMOS/AODN water quality datasets
- Unit inference from column names (e.g., TEMP_C → celsius)
- Supports both "long format" (param column) and "wide format" (param as columns)

GUARDRAILS:
✓ Upsert-safe: INSERT ... ON CONFLICT DO NOTHING
✓ Audit trail: location_qc_flag, extracted_at
✓ Validation: schema checks, failures logged
✓ QC filtering: Bad data (QC=3,4,9) excluded at extraction
✓ Date validation: Invalid dates (1900-01-01, far future) excluded
✓ Additive: no data loss (skipped data is logged)

Usage:
  python populate_measurements.py [--limit 5000] [--dataset "Title"]
"""

import os
import sys
import logging
import glob
import re
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
# SPATIAL BOUNDING BOX (Huon Estuary and D'Entrecasteaux Channel)
# ============================================================================

STUDY_AREA = {
    'lat_min': -43.558,
    'lat_max': -42.777,
    'lon_min': 146.844,
    'lon_max': 147.783
}

def is_within_study_area(lat: float, lon: float) -> bool:
    """Check if coordinates fall within study area"""
    return (STUDY_AREA['lat_min'] <= lat <= STUDY_AREA['lat_max'] and
            STUDY_AREA['lon_min'] <= lon <= STUDY_AREA['lon_max'])

# ============================================================================
# DATE VALIDATION
# ============================================================================

def is_valid_date(dt: datetime) -> bool:
    """Check if datetime is within reasonable range for marine data
    
    Rejects:
    - Dates before 1901 (before modern marine monitoring)
    - Dates after current_year + 1 (future data invalid)
    - Sentinel values: 1900-01-01, 1970-01-01 00:00:00
    
    Returns True only for dates within valid range and not sentinel values
    """
    if dt is None:
        return False
    
    # Reject sentinel: 1900-01-01 (common null placeholder)
    if dt.year == 1900 and dt.month == 1 and dt.day == 1:
        logger.debug(f"  ⚠ Rejected sentinel date: 1900-01-01")
        return False
    
    # Reject sentinel: 1970-01-01 00:00:00 (Unix epoch zero)
    if dt.year == 1970 and dt.month == 1 and dt.day == 1:
        if dt.hour == 0 and dt.minute == 0 and dt.second == 0:
            logger.debug(f"  ⚠ Rejected Unix epoch zero: 1970-01-01 00:00:00")
            return False
    
    # Reject dates before 1901 (after sentinel year)
    if dt.year < 1901:
        logger.debug(f"  ⚠ Rejected date too old: {dt.year}")
        return False
    
    # Reject far future dates
    current_year = datetime.now().year
    if dt.year > current_year + 1:
        logger.debug(f"  ⚠ Rejected future date: {dt.year} (current: {current_year})")
        return False
    
    return True

# ============================================================================
# QC FLAG HELPERS
# ============================================================================

def find_qc_column(param_col: str, all_columns: List[str]) -> Optional[str]:
    """Find QC column for a parameter column
    
    Examples:
        TEMP -> TEMP_QUALITY_CONTROL
        TEMP -> TEMP_QC
        TEMP -> TEMP_quality_control
        PSAL -> PSAL_quality_control
    """
    param_upper = param_col.upper().replace(' ', '_')
    
    # Try common QC column patterns
    qc_patterns = [
        f"{param_col}_quality_control",
        f"{param_col}_QUALITY_CONTROL",
        f"{param_col}_qc",
        f"{param_col}_QC",
        f"{param_col}_flag",
        f"{param_col}_FLAG",
        f"{param_upper}_quality_control",
        f"{param_upper}_QC"
    ]
    
    for pattern in qc_patterns:
        if pattern in all_columns:
            return pattern
    
    # Case-insensitive search
    for col in all_columns:
        col_upper = col.upper().replace(' ', '_')
        if param_upper in col_upper and any(qc in col_upper for qc in ['QUALITY_CONTROL', 'QC', 'FLAG']):
            return col
    
    return None

def is_valid_qc_flag(qc_value) -> bool:
    """Check if QC flag indicates good data
    
    IMOS QC flags:
        1 = Good data (ACCEPT)
        2 = Probably good data (ACCEPT)
        3 = Probably bad data (REJECT)
        4 = Bad data (REJECT)
        9 = Missing data (REJECT)
    
    Returns True if data should be kept (QC flag 1 or 2 only)
    """
    if pd.isna(qc_value):
        return True  # No QC flag = assume good
    
    try:
        qc_int = int(float(qc_value))
        return qc_int in [1, 2]  # Accept only good and probably good
    except (ValueError, TypeError):
        return True  # Invalid QC value = assume good

def get_qc_flag_value(qc_value) -> int:
    """Convert QC column value to integer flag (default=1 if missing)"""
    if pd.isna(qc_value):
        return 1  # No QC flag = assume good
    
    try:
        return int(float(qc_value))
    except (ValueError, TypeError):
        return 1  # Invalid QC value = assume good

# ============================================================================
# UNIT INFERENCE
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
# PARAMETER DETECTION
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
        # Skip known metadata/QC columns
        if any(skip in col_clean for skip in ['FID', 'ID', 'DATE', 'TIME', 'LATITUDE', 
                                                'LONGITUDE', 'STATION', 'SITE', 'TRIP',
                                                'LOCATION', 'GEOM', 'SAMPLE', 'ESTUARY',
                                                'QUALITY_CONTROL', 'QC', '_FLAG']):
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
# TIME FORMAT DETECTION WITH DATE VALIDATION
# ============================================================================

class TimeFormatDetector:
    """Automatically detects time column format and converts to datetime with validation"""
    
    @staticmethod
    def detect_and_convert(time_value) -> Optional[datetime]:
        """Attempts multiple time format conversions with date validation"""
        if time_value is None or (isinstance(time_value, float) and np.isnan(time_value)):
            return None
        
        dt = None
        
        if isinstance(time_value, datetime):
            dt = time_value
        elif isinstance(time_value, str):
            dt = TimeFormatDetector._from_iso_string(time_value)
        elif isinstance(time_value, (int, float, np.integer, np.floating)):
            dt = TimeFormatDetector._from_numeric(float(time_value))
        
        # === DATE VALIDATION ===
        if dt is not None and not is_valid_date(dt):
            return None  # Reject invalid date
        
        return dt
    
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
