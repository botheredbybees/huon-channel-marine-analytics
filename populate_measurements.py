#!/usr/bin/env python3
"""
Enhanced Measurements ETL v2.1

Fixes:
- NetCDF time parsing now returns datetime objects (not tuples)
- Better error handling for cftime conversion
- Database connection from parameter_mappings table

Usage:
  python populate_measurements_v2.py [--limit 5000]
"""

import os
import sys
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
    import cftime
except ImportError:
    cftime = None

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

class ParameterMapping:
    """
    Loads parameter mappings from database
    """
    
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
            
            logger.info(f"Loaded {len(self.mapping)} parameter mappings from database")
        
        except Exception as e:
            logger.error(f"Could not load parameter mappings: {e}")
            logger.warning("Using empty parameter mapping - all params will be 'custom'")
    
    def get_standard_param(self, raw_param: str) -> Tuple[str, str, str]:
        """
        Map raw parameter name to standardized (param_code, namespace, unit)
        
        Returns:
            Tuple of (parameter_code, namespace, uom) or (raw_param, 'custom', 'unknown')
        """
        raw_upper = str(raw_param).upper().strip()
        
        if raw_upper in self.mapping:
            return self.mapping[raw_upper]
        
        # No mapping found - use as custom
        return (raw_upper, 'custom', 'unknown')


class TimeFormatDetector:
    """
    Automatically detects time column format and converts to datetime
    """
    
    @staticmethod
    def detect_and_convert(time_value) -> Optional[datetime]:
        """
        Attempts multiple time format conversions.
        
        Args:
            time_value: Single value (string, numeric, or datetime)
        
        Returns:
            datetime object or None if conversion fails
        """
        
        if time_value is None or (isinstance(time_value, float) and np.isnan(time_value)):
            return None
        
        # Already a datetime
        if isinstance(time_value, datetime):
            return time_value
        
        # ISO 8601 string
        if isinstance(time_value, str):
            return TimeFormatDetector._from_iso_string(time_value)
        
        # Numeric timestamp
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
        Parse numeric time representations
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
            return base + timedelta(days=val * 30.4)
        
        # Days since 1900-01-01
        if 40000 < val < 50000:
            base = datetime(1900, 1, 1)
            return base + timedelta(days=val)
        
        # Days since 1970-01-01
        if 15000 < val < 25000:
            base = datetime(1970, 1, 1)
            return base + timedelta(days=val)
        
        # Seconds since unix epoch
        if val > 1e8:
            try:
                return datetime.utcfromtimestamp(val)
            except:
                pass
        
        return None


class CSVMeasurementExtractor:
    """
    Extracts measurements from CSV files
    """
    
    def __init__(self, param_mapping: ParameterMapping):
        self.param_mapping = param_mapping
        self.extracted_count = 0
        self.failed_count = 0
    
    def extract(self, file_path: str, metadata: dict, limit: int = None) -> List[Dict]:
        """Extract measurements from CSV"""
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
                logger.error(f"Could not read {file_path}")
                return rows
            
            if df.empty:
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
                        'quality_flag': 1
                    })
                    
                    self.extracted_count += 1
                    
                    if limit and self.extracted_count >= limit:
                        break
                
                except Exception as e:
                    self.failed_count += 1
        
        except Exception as e:
            logger.error(f"Fatal error extracting from {file_path}: {e}")
        
        return rows
    
    @staticmethod
    def _find_column(df: pd.DataFrame, cols_upper: dict, keywords: List[str]) -> Optional[str]:
        """Find column matching any keyword"""
        for keyword in keywords:
            for orig_col, upper_col in cols_upper.items():
                if keyword.upper() in upper_col:
                    return orig_col
        return None


class NetCDFMeasurementExtractor:
    """
    Extracts measurements from NetCDF files
    """
    
    def __init__(self, param_mapping: ParameterMapping):
        self.param_mapping = param_mapping
        self.extracted_count = 0
        self.failed_count = 0
    
    def extract(self, file_path: str, metadata: dict, limit: int = None) -> List[Dict]:
        """Extract measurements from NetCDF"""
        rows = []
        
        if netCDF4 is None:
            logger.error("netCDF4 not installed")
            return rows
        
        try:
            ds = netCDF4.Dataset(file_path, 'r')
            
            # Find time variable
            time_var = self._find_time_variable(ds)
            if not time_var:
                logger.warning(f"No time variable in {file_path}")
                ds.close()
                return rows
            
            time_data = ds.variables[time_var][:]
            time_attrs = ds.variables[time_var].__dict__
            
            # Extract each data variable
            for var_name in ds.variables:
                if var_name in ds.dimensions:
                    continue
                if var_name == time_var:
                    continue
                
                var = ds.variables[var_name]
                
                # Skip coordinate variables
                if hasattr(var, 'axis') or len(var.dimensions) == 0:
                    continue
                
                # Map to standard parameter
                param_code, namespace, uom = self.param_mapping.get_standard_param(var_name)
                
                # Extract data
                try:
                    data = var[:]
                    
                    if data.ndim == 1:
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
                    
                    elif data.ndim == 2:
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
    def _parse_netcdf_time(time_value, attrs: dict) -> Optional[datetime]:
        """
        Parse NetCDF time using CF units and calendar attributes.
        Returns datetime object (NOT tuple).
        """
        try:
            if cftime is not None:
                units = attrs.get('units', '')
                calendar = attrs.get('calendar', 'standard')
                
                if 'since' in units:
                    # Convert cftime object to datetime
                    cf_time = cftime.num2date(time_value, units, calendar=calendar)
                    
                    # Handle cftime.DatetimeGregorian, etc.
                    if hasattr(cf_time, 'timetuple'):
                        tt = cf_time.timetuple()
                        return datetime(tt.tm_year, tt.tm_mon, tt.tm_mday,
                                      tt.tm_hour, tt.tm_min, tt.tm_sec)
                    elif isinstance(cf_time, datetime):
                        return cf_time
        
        except Exception as e:
            logger.debug(f"cftime parsing failed: {e}")
        
        # Fall back to TimeFormatDetector
        return TimeFormatDetector.detect_and_convert(time_value)


class MeasurementBatchInserter:
    """
    Handles batch insertion of measurements
    """
    
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
        """Process rows in batches"""
        for i in range(0, len(rows), self.BATCH_SIZE):
            batch = rows[i:i+self.BATCH_SIZE]
            inserted = self.insert_batch(batch)
            logger.info(f"Inserted {inserted}/{len(batch)} rows (total: {self.total_inserted})")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced Measurements ETL v2.1')
    parser.add_argument('--limit', type=int, help='Max rows per dataset', default=None)
    parser.add_argument('--dataset', help='Specific dataset to process')
    
    args = parser.parse_args()
    
    # Initialize
    param_mapping = ParameterMapping(DB_CONFIG)
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
            
            # Walk directory
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
            
            # Insert
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
