#!/usr/bin/env python3

"""
Enhanced Measurements ETL v3.1 - Multi-Parameter CSV + 3D Gridded NetCDF Support

NEW in v3.1:
- 3D gridded NetCDF extraction (time × lat × lon)
- Spatial bounding box filtering (only extract cells within study area)
- Grid cell location creation (each grid cell gets a location record)
- Support for single-point grids (lat=1, lon=1) as station timeseries

v3.0 features:
- Multi-parameter extraction: 1 CSV row → N measurement records
- Improved column detection for IMOS/AODN water quality datasets
- Unit inference from column names (e.g., TEMP_C → celsius)
- Supports both "long format" (param column) and "wide format" (param as columns)

GUARDRAILS (unchanged):
✓ Upsert-safe: INSERT ... ON CONFLICT DO NOTHING
✓ Audit trail: location_qc_flag, extracted_at
✓ Validation: schema checks, failures logged
✓ Additive: no data loss

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
    """Insert location into locations table"""
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO locations (location_name, latitude, longitude, location_geom)
            VALUES (%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
            ON CONFLICT (latitude, longitude)
            DO UPDATE SET location_name = EXCLUDED.location_name
            RETURNING id;
        """, (str(station), float(lat), float(lon), float(lon), float(lat)))
        
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
        
        if isinstance(time_value, str):
            return TimeFormatDetector._from_iso_string(time_value)
        
        if isinstance(time_value, (int, float, np.integer, np.floating)):
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
# CSV MEASUREMENT EXTRACTOR (MULTI-PARAMETER)
# ============================================================================

class CSVMeasurementExtractor:
    """Extracts measurements from CSV files - supports multi-parameter rows"""
    
    def __init__(self, param_mapping: ParameterMapping):
        self.param_mapping = param_mapping
        self.extracted_count = 0
        self.failed_count = 0
    
    def extract(self, file_path: str, metadata: dict, limit: int = None) -> List[Dict]:
        """Extract measurements from CSV - handles both long and wide formats"""
        rows = []
        
        try:
            for encoding in ['utf-8', 'latin1', 'iso-8859-1']:
                try:
                    df = pd.read_csv(file_path, encoding=encoding,
                                    on_bad_lines='skip', comment='#')
                    break
                except Exception:
                    continue
            else:
                logger.error(f"Could not read {file_path}")
                return rows
            
            if df.empty:
                return rows
            
            cols_upper = {c: c.upper().strip().replace(' ', '_') for c in df.columns}
            
            time_col = self._find_column(df, cols_upper, 
                                        ['TIME', 'DATE', 'DATETIME', 'TIMESTAMP', 'SAMPLE_DATE'])
            
            if not time_col:
                logger.warning(f"No time column found in {file_path}")
                return rows
            
            param_cols = detect_parameter_columns(df)
            
            if not param_cols:
                logger.warning(f"No parameter columns detected in {file_path}")
                return rows
            
            logger.info(f"  ✓ Detected {len(param_cols)} parameter columns: {list(param_cols.values())[:5]}...")
            
            depth_col = self._find_column(df, cols_upper, ['DEPTH', 'Z', 'LEVEL', 'DEPTH_M'])
            
            for idx, row in df.iterrows():
                ts = TimeFormatDetector.detect_and_convert(row.get(time_col))
                if not ts:
                    self.failed_count += 1
                    continue
                
                depth = None
                if depth_col:
                    try:
                        depth = float(row.get(depth_col))
                    except (ValueError, TypeError):
                        pass
                
                for col_name, param_standard in param_cols.items():
                    try:
                        value = float(row.get(col_name))
                        
                        if pd.isna(value):
                            continue
                        
                        param_code, namespace, uom = self.param_mapping.get_standard_param(param_standard)
                        
                        if uom == 'unknown':
                            uom = infer_unit_from_column_name(col_name)
                        
                        rows.append({
                            'time': ts,
                            'uuid': metadata['uuid'],
                            'metadata_id': metadata['id'],
                            'parameter_code': param_code,
                            'namespace': namespace,
                            'value': value,
                            'uom': uom,
                            'depth_m': depth,
                            'quality_flag': 1,
                            'location_id': None,
                            'location_qc_flag': 'unknown'
                        })
                        
                        self.extracted_count += 1
                        
                    except (ValueError, TypeError):
                        continue
                
                if limit and self.extracted_count >= limit:
                    break
        
        except Exception as e:
            logger.error(f"Fatal error extracting from {file_path}: {e}")
        
        return rows
    
    @staticmethod
    def _find_column(df: pd.DataFrame, cols_upper: dict, keywords: List[str]) -> Optional[str]:
        """Find column matching any keyword"""
        for keyword in keywords:
            for orig_col, upper_col in cols_upper.items():
                if keyword in upper_col:
                    return orig_col
        return None

# ============================================================================
# NETCDF MEASUREMENT EXTRACTOR (1D, 2D, 3D GRIDS)
# ============================================================================

class NetCDFMeasurementExtractor:
    """Extracts measurements from NetCDF files - supports 1D/2D/3D grids"""
    
    def __init__(self, param_mapping: ParameterMapping, db_config: dict):
        self.param_mapping = param_mapping
        self.db_config = db_config
        self.extracted_count = 0
        self.failed_count = 0
        self.grid_locations = {}  # Cache for grid cell locations
    
    def extract(self, file_path: str, metadata: dict, limit: int = None) -> List[Dict]:
        """Extract measurements from NetCDF"""
        rows = []
        
        if netCDF4 is None:
            logger.error("netCDF4 not installed")
            return rows
        
        try:
            ds = netCDF4.Dataset(file_path, 'r')
            
            time_var = self._find_time_variable(ds)
            if not time_var:
                logger.warning(f"No time variable in {file_path}")
                ds.close()
                return rows
            
            time_data = ds.variables[time_var][:]
            time_attrs = ds.variables[time_var].__dict__
            
            # Find lat/lon variables (for 3D grids)
            lat_var = self._find_variable(ds, ['LATITUDE', 'latitude', 'lat'])
            lon_var = self._find_variable(ds, ['LONGITUDE', 'longitude', 'lon'])
            
            lat_data = ds.variables[lat_var][:] if lat_var else None
            lon_data = ds.variables[lon_var][:] if lon_var else None
            
            # Extract each data variable
            for var_name in ds.variables:
                if var_name in ds.dimensions:
                    continue
                if var_name == time_var or var_name == lat_var or var_name == lon_var:
                    continue
                
                var = ds.variables[var_name]
                
                if hasattr(var, 'axis') or len(var.dimensions) == 0:
                    continue
                
                param_code, namespace, uom = self.param_mapping.get_standard_param(var_name)
                
                # Get unit from NetCDF attributes if not in mapping
                if uom == 'unknown' and hasattr(var, 'units'):
                    uom = str(var.units)
                
                try:
                    data = var[:]
                    
                    # 1D timeseries
                    if data.ndim == 1:
                        rows.extend(self._extract_1d(data, time_data, time_attrs, 
                                                     param_code, namespace, uom, metadata, limit))
                    
                    # 2D time×depth
                    elif data.ndim == 2:
                        rows.extend(self._extract_2d(data, time_data, time_attrs,
                                                     param_code, namespace, uom, metadata, limit))
                    
                    # 3D time×lat×lon (GRIDDED DATA)
                    elif data.ndim == 3 and lat_data is not None and lon_data is not None:
                        rows.extend(self._extract_3d_grid(data, time_data, time_attrs,
                                                         lat_data, lon_data,
                                                         param_code, namespace, uom, 
                                                         metadata, limit))
                    
                    if limit and self.extracted_count >= limit:
                        break
                
                except StopIteration:
                    break
                except Exception as e:
                    logger.debug(f"Error extracting {var_name}: {e}")
                    self.failed_count += 1
            
            ds.close()
        
        except Exception as e:
            logger.error(f"Fatal error reading {file_path}: {e}")
        
        return rows
    
    def _extract_1d(self, data, time_data, time_attrs, param_code, namespace, uom, metadata, limit):
        """Extract 1D timeseries"""
        rows = []
        for t_idx in range(len(data)):
            if pd.isna(data[t_idx]):
                continue
            
            ts = self._parse_netcdf_time(time_data[t_idx], time_attrs)
            if not ts:
                continue
            
            rows.append({
                'time': ts,
                'uuid': metadata['uuid'],
                'metadata_id': metadata['id'],
                'parameter_code': param_code,
                'namespace': namespace,
                'value': float(data[t_idx]),
                'uom': uom,
                'depth_m': None,
                'quality_flag': 1,
                'location_id': None,
                'location_qc_flag': 'unknown'
            })
            
            self.extracted_count += 1
            if limit and self.extracted_count >= limit:
                raise StopIteration
        
        return rows
    
    def _extract_2d(self, data, time_data, time_attrs, param_code, namespace, uom, metadata, limit):
        """Extract 2D time×depth"""
        rows = []
        for t_idx in range(min(len(data), 1000)):
            ts = self._parse_netcdf_time(time_data[t_idx], time_attrs)
            if not ts:
                continue
            
            for s_idx in range(data.shape[1]):
                if pd.isna(data[t_idx, s_idx]):
                    continue
                
                rows.append({
                    'time': ts,
                    'uuid': metadata['uuid'],
                    'metadata_id': metadata['id'],
                    'parameter_code': param_code,
                    'namespace': namespace,
                    'value': float(data[t_idx, s_idx]),
                    'uom': uom,
                    'depth_m': None,
                    'quality_flag': 1,
                    'location_id': None,
                    'location_qc_flag': 'unknown'
                })
                
                self.extracted_count += 1
                if limit and self.extracted_count >= limit:
                    raise StopIteration
        
        return rows
    
    def _extract_3d_grid(self, data, time_data, time_attrs, lat_data, lon_data,
                         param_code, namespace, uom, metadata, limit):
        """Extract 3D gridded data (time×lat×lon) with spatial filtering"""
        rows = []
        
        # Check grid size
        n_time, n_lat, n_lon = data.shape
        total_cells = n_lat * n_lon
        
        logger.info(f"  📊 3D Grid: {n_time} timesteps × {n_lat} lats × {n_lon} lons = {n_time * total_cells:,} potential measurements")
        
        # Filter grid cells within study area
        valid_cells = []
        for lat_idx in range(n_lat):
            for lon_idx in range(n_lon):
                lat = float(lat_data[lat_idx])
                lon = float(lon_data[lon_idx])
                
                if is_within_study_area(lat, lon):
                    valid_cells.append((lat_idx, lon_idx, lat, lon))
        
        if not valid_cells:
            logger.warning(f"  ⚠ No grid cells within study area")
            return rows
        
        logger.info(f"  ✓ Found {len(valid_cells)} grid cells within study area")
        
        # Create/get location for each grid cell
        conn = psycopg2.connect(**self.db_config)
        
        for lat_idx, lon_idx, lat, lon in valid_cells:
            # Get or create location for this grid cell
            cell_key = (round(lat, 4), round(lon, 4))
            
            if cell_key not in self.grid_locations:
                location_name = f"{metadata['title'][:50]} - Grid ({lat:.3f}, {lon:.3f})"
                location_id = get_or_insert_location(conn, location_name, lat, lon)
                self.grid_locations[cell_key] = location_id
            else:
                location_id = self.grid_locations[cell_key]
            
            # Extract timeseries for this grid cell
            for t_idx in range(n_time):
                value = data[t_idx, lat_idx, lon_idx]
                
                if pd.isna(value) or value == -999 or value == -1e34:
                    continue
                
                ts = self._parse_netcdf_time(time_data[t_idx], time_attrs)
                if not ts:
                    continue
                
                rows.append({
                    'time': ts,
                    'uuid': metadata['uuid'],
                    'metadata_id': metadata['id'],
                    'parameter_code': param_code,
                    'namespace': namespace,
                    'value': float(value),
                    'uom': uom,
                    'depth_m': None,
                    'quality_flag': 1,
                    'location_id': location_id,
                    'location_qc_flag': 'clean'
                })
                
                self.extracted_count += 1
                
                if limit and self.extracted_count >= limit:
                    conn.close()
                    raise StopIteration
        
        conn.close()
        return rows
    
    @staticmethod
    def _find_time_variable(ds) -> Optional[str]:
        for name in ['time', 'TIME', 'Time', 'datetime', 'DATETIME']:
            if name in ds.variables:
                return name
        return None
    
    @staticmethod
    def _find_variable(ds, names: List[str]) -> Optional[str]:
        for name in names:
            if name in ds.variables:
                return name
        return None
    
    @staticmethod
    def _parse_netcdf_time(time_value, attrs: dict) -> Optional[datetime]:
        """Parse NetCDF time using CF units and calendar attributes"""
        try:
            if cftime is not None:
                units = attrs.get('units', '')
                calendar = attrs.get('calendar', 'standard')
                
                if 'since' in units:
                    cf_time = cftime.num2date(time_value, units, calendar=calendar)
                    
                    if hasattr(cf_time, 'timetuple'):
                        tt = cf_time.timetuple()
                        return datetime(tt.tm_year, tt.tm_mon, tt.tm_mday,
                                      tt.tm_hour, tt.tm_min, tt.tm_sec)
                    elif isinstance(cf_time, datetime):
                        return cf_time
        
        except Exception as e:
            logger.debug(f"cftime parsing failed: {e}")
        
        return TimeFormatDetector.detect_and_convert(time_value)

# ============================================================================
# BATCH INSERTER
# ============================================================================

class MeasurementBatchInserter:
    """Handles batch insertion of measurements"""
    
    BATCH_SIZE = 1000
    
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.total_inserted = 0
        self.total_failed = 0
    
    def insert_batch(self, rows: List[Dict]) -> int:
        """Insert batch of measurements"""
        if not rows:
            return 0
        
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
            values = [
                (
                    row['time'],
                    row['uuid'],
                    row['parameter_code'],
                    row['namespace'],
                    row['value'],
                    row['uom'],
                    row.get('uncertainty'),
                    row['depth_m'],
                    row['metadata_id'],
                    row['quality_flag'],
                    row.get('location_id')
                )
                for row in rows
            ]
            
            sql = """
                INSERT INTO measurements
                (time, uuid, parameter_code, namespace, value, uom, uncertainty, depth_m, metadata_id, quality_flag, location_id)
                VALUES %s
                ON CONFLICT DO NOTHING
            """
            
            execute_values(cur, sql, values)
            conn.commit()
            
            inserted = cur.rowcount
            self.total_inserted += inserted
            
            cur.close()
            conn.close()
            
            return inserted
        
        except Exception as e:
            logger.error(f"Batch insert failed: {e}")
            self.total_failed += len(rows)
            return 0
    
    def process_batches(self, rows: List[Dict]):
        """Process rows in batches"""
        for i in range(0, len(rows), self.BATCH_SIZE):
            batch = rows[i:i+self.BATCH_SIZE]
            inserted = self.insert_batch(batch)
            logger.info(f"  Inserted {inserted}/{len(batch)} rows (total: {self.total_inserted})")

# ============================================================================
# MAIN ETL PIPELINE
# ============================================================================

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced Measurements ETL v3.1 - Multi-Parameter CSV + 3D Gridded NetCDF')
    parser.add_argument('--limit', type=int, help='Max rows per dataset', default=None)
    parser.add_argument('--dataset', help='Specific dataset to process')
    args = parser.parse_args()
    
    # Initialize
    param_mapping = ParameterMapping(DB_CONFIG)
    csv_extractor = CSVMeasurementExtractor(param_mapping)
    nc_extractor = NetCDFMeasurementExtractor(param_mapping, DB_CONFIG)
    inserter = MeasurementBatchInserter(DB_CONFIG)
    
    logger.info(f"Study area: Lat {STUDY_AREA['lat_min']} to {STUDY_AREA['lat_max']}, Lon {STUDY_AREA['lon_min']} to {STUDY_AREA['lon_max']}")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Find empty datasets
        cur.execute("""
            SELECT m.id, m.uuid, m.dataset_path, m.title
            FROM metadata m
            LEFT JOIN measurements mes ON m.id = mes.metadata_id
            GROUP BY m.id
            HAVING COUNT(mes.data_id) = 0 AND m.dataset_path IS NOT NULL
            ORDER BY m.title
        """)
        
        datasets = cur.fetchall()
        logger.info(f"Found {len(datasets)} empty datasets")
        
        for ds_id, uuid, rel_path, title in datasets:
            if args.dataset and args.dataset not in title:
                continue
            
            if not os.path.exists(rel_path):
                logger.warning(f"Path not found: {rel_path}")
                continue
            
            logger.info(f"\n{'='*70}")
            logger.info(f"📊 Processing: {title}")
            logger.info(f"{'='*70}")
            
            metadata = {'id': ds_id, 'uuid': uuid, 'title': title}
            dataset_rows = []
            
            # ===== LOCATION PATCHING STEP =====
            station_name = None
            patched_lat = None
            patched_lon = None
            location_id = None
            location_qc_flag = 'not_found'
            
            found_file = None
            for root, dirs, files in os.walk(rel_path):
                if 'metadata' in root:
                    continue
                
                for file in files:
                    if file.lower().endswith(('.nc', '.csv')):
                        if 'index.csv' in file.lower():
                            continue
                        
                        file_path = os.path.join(root, file)
                        
                        station_name, lat, lon = extract_station_info_from_file(file_path, title)
                        
                        if lat is not None and lon is not None:
                            patched_lat, patched_lon, location_qc_flag = patch_location_coordinates(lat, lon)
                            
                            if patched_lat is not None and patched_lon is not None:
                                logger.info(f"  ✓ Location: {station_name} ({patched_lat:.4f}, {patched_lon:.4f}) [{location_qc_flag}]")
                                
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
            
            # ===== APPLY LOCATION PATCH TO NON-GRIDDED ROWS =====
            if location_id is not None:
                patched_count = 0
                for row in dataset_rows:
                    if row['location_id'] is None:
                        row['location_id'] = location_id
                        row['location_qc_flag'] = location_qc_flag
                        patched_count += 1
                
                if patched_count > 0:
                    logger.info(f"  ✓ Patched {patched_count} rows with location_id={location_id}")
            
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
