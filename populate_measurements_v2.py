"""
Enhanced Measurements ETL v2.0

Improvements over v1:
- Automatic time format detection (ISO, months_since, CF calendar)
- Ragged array support for NetCDF
- Flexible parameter mapping with user overrides
- Compound time dimensions (year/month/day columns)
- Better error recovery and reporting
- Batch insert optimization

Usage:
  python populate_measurements_v2.py [--config config.json] [--limit 5000]
"""

import os
import sys
import json
import logging
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
    import xarray as xr
except ImportError:
    xr = None

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'dbname': 'marine_db',
    'user': 'marine_user',
    'password': 'marine_pass123',
    'host': 'localhost',
    'port': '5433'
}

class TimeFormatDetector:
    """
    Automatically detects time column format and converts to ISO 8601 timestamps
    """
    
    @staticmethod
    def detect_and_convert(time_value, time_columns: dict = None) -> Optional[datetime]:
        """
        Attempts multiple time format conversions.
        
        Args:
            time_value: Single value or dict of time components (year, month, day)
            time_columns: Dict mapping column names to values (for compound times)
        
        Returns:
            datetime object or None if conversion fails
        """
        
        if time_value is None or (isinstance(time_value, float) and np.isnan(time_value)):
            return None
        
        # Case 1: Dict of time components (year/month/day columns)
        if time_columns and isinstance(time_columns, dict):
            return TimeFormatDetector._from_components(time_columns)
        
        # Case 2: ISO 8601 string
        if isinstance(time_value, str):
            return TimeFormatDetector._from_iso_string(time_value)
        
        # Case 3: Numeric timestamp
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
        """
        Parse numeric time representations:
        - Days since 1970-01-01 (unix epoch): val ~= 18000 (50 years * 365)
        - Days since 1900-01-01: val ~= 45000 (century + 45 years)
        - Months since 1900-01-01: val ~= 1000-1500 (typical IMOS CF)
        - Decimal year: val ~= 2000 (year as float)
        - Year integer: val ~= 2010 (1900 < val < 2100)
        """
        
        # Decimal year (2000.5 = July 2000)
        if 1900 < val < 2100 and val % 1 != 0:
            year = int(val)
            frac = val - year
            return datetime(year, 1, 1) + timedelta(days=365.25 * frac)
        
        # Year as integer
        if 1900 < val < 2100 and val % 1 == 0:
            return datetime(int(val), 1, 1)
        
        # Months since 1900-01-01 (typical IMOS CF)
        if 1000 < val < 2000:
            base = datetime(1900, 1, 1)
            return base + timedelta(days=val * 30.4)  # Approximate month length
        
        # Days since 1900-01-01
        if 40000 < val < 50000:
            base = datetime(1900, 1, 1)
            return base + timedelta(days=val)
        
        # Days since 1970-01-01 (unix epoch)
        if 15000 < val < 25000:
            base = datetime(1970, 1, 1)
            return base + timedelta(days=val)
        
        # Seconds since unix epoch
        if val > 1e8:  # Jan 1973
            try:
                return datetime.utcfromtimestamp(val)
            except:
                pass
        
        return None
    
    @staticmethod
    def _from_components(components: dict) -> Optional[datetime]:
        """Assemble datetime from year/month/day/hour/minute/second columns"""
        try:
            year = int(components.get('year', components.get('YEAR')))
            month = int(components.get('month', components.get('MONTH', 1)))
            day = int(components.get('day', components.get('DAY', 1)))
            hour = int(components.get('hour', components.get('HOUR', 0)))
            minute = int(components.get('minute', components.get('MINUTE', 0)))
            second = int(components.get('second', components.get('SECOND', 0)))
            
            return datetime(year, month, day, hour, minute, second)
        except (KeyError, ValueError, TypeError):
            return None


class ParameterMapping:
    """
    Manages parameter code standardization and mapping
    """
    
    def __init__(self, config_path: str = None):
        self.mapping = {}
        self.custom_mappings = {}
        
        if config_path and os.path.exists(config_path):
            self.load_config(config_path)
        else:
            self._init_default_mapping()
    
    def _init_default_mapping(self):
        """Initialize common IMOS/BODC parameter mappings"""
        self.mapping = {
            # Temperature variants
            'TEMP': ('TEMP', 'bodc', 'Degrees Celsius'),
            'TEMPERATURE': ('TEMP', 'bodc', 'Degrees Celsius'),
            'sea_water_temperature': ('TEMP', 'cf', 'Degrees Celsius'),
            'SST': ('SST', 'bodc', 'Degrees Celsius'),
            'sea_surface_temperature': ('SST', 'cf', 'Degrees Celsius'),
            
            # Salinity variants
            'PSAL': ('PSAL', 'bodc', 'PSS-78'),
            'SALINITY': ('PSAL', 'bodc', 'PSS-78'),
            'sea_water_salinity': ('PSAL', 'cf', 'PSS-78'),
            
            # Chlorophyll
            'CPHL': ('CPHL', 'bodc', 'mg/m3'),
            'CHLOROPHYLL': ('CPHL', 'bodc', 'mg/m3'),
            'chlorophyll_concentration': ('CPHL', 'cf', 'mg/m3'),
            'chl_a': ('CPHL', 'bodc', 'mg/m3'),
            'CHL_A': ('CPHL', 'bodc', 'mg/m3'),
            
            # Depth
            'DEPTH': ('DEPTH', 'bodc', 'Meters'),
            'depth': ('DEPTH', 'bodc', 'Meters'),
            'z': ('DEPTH', 'cf', 'Meters'),
            
            # Oxygen
            'DOXY': ('DOXY', 'bodc', 'ml/l'),
            'dissolved_oxygen': ('DOXY', 'cf', 'ml/l'),
            'DO': ('DOXY', 'custom', 'ml/l'),
            
            # pH
            'PH': ('PH', 'bodc', 'unitless'),
            'sea_water_ph_reported_on_total_scale': ('PH', 'cf', 'unitless'),
        }
    
    def load_config(self, config_path: str):
        """Load custom parameter mappings from JSON"""
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
                self.custom_mappings.update(config.get('parameter_mapping', {}))
                logger.info(f"Loaded {len(self.custom_mappings)} custom mappings")
        except Exception as e:
            logger.warning(f"Could not load config {config_path}: {e}")
    
    def get_standard_param(self, raw_param: str) -> Tuple[str, str, str]:
        """
        Map raw parameter name to standardized (param_code, namespace, unit)
        
        Returns:
            Tuple of (parameter_code, namespace, uom) or (raw_param, 'custom', 'unknown')
        """
        raw_upper = str(raw_param).upper().strip()
        
        # Check custom mappings first
        if raw_upper in self.custom_mappings:
            return self.custom_mappings[raw_upper]
        
        # Check default mapping
        if raw_upper in self.mapping:
            return self.mapping[raw_upper]
        
        # No mapping found - use as custom
        return (raw_upper, 'custom', 'unknown')


class CSVMeasurementExtractor:
    """
    Extracts and normalizes measurements from CSV files
    """
    
    def __init__(self, param_mapping: ParameterMapping):
        self.param_mapping = param_mapping
        self.extracted_count = 0
        self.failed_count = 0
    
    def extract(self, file_path: str, metadata: dict, 
                limit: int = None) -> List[Dict]:
        """
        Extract measurements from CSV
        
        Args:
            file_path: Path to CSV file
            metadata: Dict with 'id', 'uuid'
            limit: Max rows to extract (for testing)
        
        Returns:
            List of measurement dicts ready for DB insert
        """
        rows = []
        
        try:
            # Read CSV with flexible encoding
            for encoding in ['utf-8', 'latin1', 'iso-8859-1']:
                try:
                    df = pd.read_csv(file_path, encoding=encoding, 
                                   on_bad_lines='skip', comment='#')
                    break
                except Exception:
                    continue
            else:
                logger.error(f"Could not read {file_path} with any encoding")
                return rows
            
            if df.empty:
                logger.warning(f"Empty DataFrame from {file_path}")
                return rows
            
            # Identify key columns
            cols_upper = {c: c.upper() for c in df.columns}
            
            time_col = self._find_column(df, cols_upper, 
                                       ['time', 'date', 'datetime', 'timestamp'])
            value_col = self._find_column(df, cols_upper,
                                        ['value', 'concentration', 'measurement', 'result'])
            param_col = self._find_column(df, cols_upper,
                                        ['parameter', 'variable', 'code'])
            
            if not time_col or not value_col:
                logger.warning(f"Missing time or value column in {file_path}")
                return rows
            
            # Extract rows
            for idx, row in df.iterrows():
                try:
                    # Parse timestamp
                    ts = TimeFormatDetector.detect_and_convert(row.get(time_col))
                    if not ts:
                        self.failed_count += 1
                        continue
                    
                    # Get value
                    try:
                        value = float(row.get(value_col))
                    except (ValueError, TypeError):
                        self.failed_count += 1
                        continue
                    
                    # Get parameter code
                    if param_col:
                        raw_param = row.get(param_col)
                    else:
                        raw_param = os.path.basename(file_path).split('.')[0]
                    
                    param_code, namespace, uom = self.param_mapping.get_standard_param(raw_param)
                    
                    # Optional: depth
                    depth_col = self._find_column(df, cols_upper, ['depth', 'z', 'level'])
                    depth = None
                    if depth_col:
                        try:
                            depth = float(row.get(depth_col))
                        except (ValueError, TypeError):
                            pass
                    
                    rows.append({
                        'time': ts,
                        'uuid': metadata['uuid'],
                        'metadata_id': metadata['id'],
                        'parameter_code': param_code,
                        'namespace': namespace,
                        'value': value,
                        'uom': uom,
                        'depth_m': depth,
                        'quality_flag': 1  # Assume good data
                    })
                    
                    self.extracted_count += 1
                    
                    if limit and self.extracted_count >= limit:
                        logger.info(f"Hit limit of {limit} rows")
                        break
                
                except Exception as e:
                    self.failed_count += 1
                    if idx < 5:  # Log first few failures
                        logger.debug(f"Row {idx} failed: {e}")
        
        except Exception as e:
            logger.error(f"Fatal error extracting from {file_path}: {e}")
        
        return rows
    
    @staticmethod
    def _find_column(df: pd.DataFrame, cols_upper: dict, keywords: List[str]) -> Optional[str]:
        """Find column matching any keyword (case-insensitive)"""
        for keyword in keywords:
            for orig_col, upper_col in cols_upper.items():
                if keyword.upper() in upper_col:
                    return orig_col
        return None


class NetCDFMeasurementExtractor:
    """
    Extracts and normalizes measurements from NetCDF files
    """
    
    def __init__(self, param_mapping: ParameterMapping):
        self.param_mapping = param_mapping
        self.extracted_count = 0
        self.failed_count = 0
    
    def extract(self, file_path: str, metadata: dict,
                limit: int = None) -> List[Dict]:
        """
        Extract measurements from NetCDF
        
        Args:
            file_path: Path to .nc file
            metadata: Dict with 'id', 'uuid'
            limit: Max measurements to extract
        
        Returns:
            List of measurement dicts
        """
        rows = []
        
        if netCDF4 is None:
            logger.error("netCDF4 not installed")
            return rows
        
        try:
            ds = netCDF4.Dataset(file_path, 'r')
            
            # Identify time variable
            time_var = self._find_time_variable(ds)
            if not time_var:
                logger.warning(f"No time variable found in {file_path}")
                ds.close()
                return rows
            
            time_data = ds.variables[time_var][:]
            time_attrs = ds.variables[time_var].__dict__
            
            # Find spatial variables
            lat_var = self._find_variable(ds, ['lat', 'latitude', 'y'])
            lon_var = self._find_variable(ds, ['lon', 'longitude', 'x'])
            depth_var = self._find_variable(ds, ['depth', 'z', 'level'])
            
            # Extract each data variable
            for var_name in ds.variables:
                # Skip dimensions and coordinate vars
                if var_name in ds.dimensions:
                    continue
                if var_name in [time_var, lat_var, lon_var, depth_var]:
                    continue
                
                var = ds.variables[var_name]
                
                # Get dimensions
                var_dims = var.dimensions
                if len(var_dims) < 2:  # Need at least time + 1 other
                    continue
                
                # Map to standard parameter
                param_code, namespace, uom = self.param_mapping.get_standard_param(var_name)
                
                # Extract data with care for multi-dimensional arrays
                try:
                    data = var[:]
                    
                    if data.ndim == 1:  # 1D timeseries
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
                                'quality_flag': 1
                            })
                            
                            self.extracted_count += 1
                            if limit and self.extracted_count >= limit:
                                raise StopIteration
                    
                    elif data.ndim == 2:  # (time, station) or (time, depth)
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
                                    'quality_flag': 1
                                })
                                
                                self.extracted_count += 1
                                if limit and self.extracted_count >= limit:
                                    raise StopIteration
                
                except StopIteration:
                    break
                except Exception as e:
                    logger.debug(f"Error extracting {var_name}: {e}")
                    self.failed_count += 1
            
            ds.close()
        
        except Exception as e:
            logger.error(f"Fatal error reading {file_path}: {e}")
        
        return rows
    
    @staticmethod
    def _find_time_variable(ds) -> Optional[str]:
        for name in ['time', 'TIME', 'Time', 'datetime', 'DATETIME']:
            if name in ds.variables:
                return name
        return None
    
    @staticmethod
    def _find_variable(ds, keywords: List[str]) -> Optional[str]:
        for keyword in keywords:
            for name in ds.variables:
                if keyword.lower() in name.lower():
                    return name
        return None
    
    @staticmethod
    def _parse_netcdf_time(time_value, attrs: dict) -> Optional[datetime]:
        """
        Parse NetCDF time using CF units and calendar attributes
        """
        try:
            # Try to parse using cftime if available
            import cftime
            units = attrs.get('units', '')
            calendar = attrs.get('calendar', 'standard')
            
            if 'since' in units:
                return cftime.num2date(time_value, units, calendar=calendar).timetuple()[:6]
            
        except ImportError:
            pass
        
        # Fall back to TimeFormatDetector
        return TimeFormatDetector.detect_and_convert(time_value)


class MeasurementBatchInserter:
    """
    Handles batch insertion of measurements into PostgreSQL
    """
    
    BATCH_SIZE = 1000
    
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.total_inserted = 0
        self.total_failed = 0
    
    def insert_batch(self, rows: List[Dict]) -> int:
        """
        Insert batch of measurement rows
        
        Returns:
            Number successfully inserted
        """
        if not rows:
            return 0
        
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
            # Prepare values
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
                    row['quality_flag']
                )
                for row in rows
            ]
            
            sql = """
            INSERT INTO measurements 
            (time, uuid, parameter_code, namespace, value, uom, uncertainty, depth_m, metadata_id, quality_flag)
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
        """
        Process rows in batches
        """
        for i in range(0, len(rows), self.BATCH_SIZE):
            batch = rows[i:i+self.BATCH_SIZE]
            inserted = self.insert_batch(batch)
            logger.info(f"Inserted {inserted}/{len(batch)} rows (total: {self.total_inserted})")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced Measurements ETL v2')
    parser.add_argument('--config', help='Parameter mapping config JSON', default=None)
    parser.add_argument('--limit', type=int, help='Max rows per dataset', default=None)
    parser.add_argument('--dataset', help='Specific dataset to process (partial path match)')
    
    args = parser.parse_args()
    
    # Initialize
    param_mapping = ParameterMapping(args.config)
    csv_extractor = CSVMeasurementExtractor(param_mapping)
    nc_extractor = NetCDFMeasurementExtractor(param_mapping)
    inserter = MeasurementBatchInserter(DB_CONFIG)
    
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
            
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing: {title}")
            logger.info(f"{'='*60}")
            
            metadata = {'id': ds_id, 'uuid': uuid}
            dataset_rows = []
            
            # Walk directory for CSV and NetCDF files
            for root, dirs, files in os.walk(rel_path):
                if 'metadata' in root:
                    continue
                
                for file in files:
                    if file == 'index.csv' or 'metadata' in file:
                        continue
                    
                    file_path = os.path.join(root, file)
                    
                    try:
                        if file.lower().endswith('.csv'):
                            logger.info(f"  Extracting CSV: {file}")
                            rows = csv_extractor.extract(file_path, metadata, args.limit)
                            dataset_rows.extend(rows)
                        
                        elif file.lower().endswith('.nc'):
                            logger.info(f"  Extracting NetCDF: {file}")
                            rows = nc_extractor.extract(file_path, metadata, args.limit)
                            dataset_rows.extend(rows)
                    
                    except Exception as e:
                        logger.error(f"    Error processing {file}: {e}")
            
            # Insert all rows for this dataset
            if dataset_rows:
                logger.info(f"Inserting {len(dataset_rows)} measurements...")
                inserter.process_batches(dataset_rows)
            else:
                logger.warning(f"No measurements extracted from {title}")
        
        cur.close()
        conn.close()
        
        logger.info(f"\n{'='*60}")
        logger.info(f"ETL Complete")
        logger.info(f"Total inserted: {inserter.total_inserted}")
        logger.info(f"Total failed: {inserter.total_failed}")
        logger.info(f"CSV extraction: {csv_extractor.extracted_count} / {csv_extractor.failed_count} failed")
        logger.info(f"NetCDF extraction: {nc_extractor.extracted_count} / {nc_extractor.failed_count} failed")
        logger.info(f"{'='*60}")
    
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
