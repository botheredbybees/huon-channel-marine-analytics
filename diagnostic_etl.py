"""
Diagnostic ETL Scanner for Huon Channel Marine Analytics

Scans all AODN datasets and identifies:
1. File format and structure
2. Parameter availability + data types
3. Time dimension format (ISO, months_since, etc.)
4. Spatial dimensions (lat/lon, depth)
5. Known parsing failures + root causes

Run with: python diagnostic_etl.py > diagnostic_report.txt
"""

import os
import json
import csv
import logging
from pathlib import Path
from datetime import datetime
import traceback

try:
    import pandas as pd
    import numpy as np
except ImportError:
    pd = None
    np = None

try:
    import netCDF4
except ImportError:
    netCDF4 = None

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DiagnosticReport:
    """Collects and formats diagnostic findings"""
    
    def __init__(self):
        self.datasets = {}
        self.summary_stats = {
            'total_datasets': 0,
            'ingested': 0,
            'failed': 0,
            'file_format_distribution': {},
            'failure_reasons': {}
        }
    
    def add_dataset(self, dataset_name, findings):
        self.datasets[dataset_name] = findings
        
        if findings['status'] == 'success':
            self.summary_stats['ingested'] += 1
        elif findings['status'] == 'failed':
            self.summary_stats['failed'] += 1
            reason = findings.get('failure_reason', 'unknown')
            self.summary_stats['failure_reasons'][reason] = \
                self.summary_stats['failure_reasons'].get(reason, 0) + 1
        
        fmt = findings.get('file_format', 'unknown')
        self.summary_stats['file_format_distribution'][fmt] = \
            self.summary_stats['file_format_distribution'].get(fmt, 0) + 1
    
    def print_summary(self):
        print("\n" + "="*80)
        print("DIAGNOSTIC ETL REPORT SUMMARY")
        print("="*80)
        
        total = len(self.datasets)
        self.summary_stats['total_datasets'] = total
        
        print(f"\nDatasets Scanned: {total}")
        print(f"  ✓ Successfully ingested: {self.summary_stats['ingested']}")
        print(f"  ✗ Failed: {self.summary_stats['failed']}")
        print(f"  Coverage: {100 * self.summary_stats['ingested'] / max(total, 1):.1f}%")
        
        if self.summary_stats['file_format_distribution']:
            print("\nFile Format Distribution:")
            for fmt, count in sorted(self.summary_stats['file_format_distribution'].items(), 
                                   key=lambda x: -x[1]):
                print(f"  {fmt}: {count}")
        
        if self.summary_stats['failure_reasons']:
            print("\nFailure Reasons (Priority Order):")
            for reason, count in sorted(self.summary_stats['failure_reasons'].items(), 
                                      key=lambda x: -x[1]):
                print(f"  [{count}] {reason}")
        
        print("\n" + "="*80)
    
    def print_failures(self):
        print("\n" + "="*80)
        print("DETAILED FAILURES (FOR REMEDIATION)")
        print("="*80)
        
        failed_datasets = {k: v for k, v in self.datasets.items() if v['status'] == 'failed'}
        
        if not failed_datasets:
            print("\nNo failures detected! All datasets ingested successfully.")
            return
        
        for name, findings in sorted(failed_datasets.items()):
            print(f"\n{name}")
            print(f"  Reason: {findings.get('failure_reason', 'unknown')}")
            print(f"  Details: {findings.get('failure_details', 'N/A')}")
            if 'sample_data' in findings and findings['sample_data']:
                print(f"  Sample: {findings['sample_data']}")


class CSVDiagnostic:
    """Analyzes CSV file structure and content"""
    
    @staticmethod
    def diagnose(file_path):
        """Returns dict with file structure and issues"""
        findings = {
            'file_format': 'csv',
            'status': 'unknown',
            'failure_reason': None,
            'failure_details': None,
            'rows': 0,
            'columns': [],
            'sample_data': None,
            'time_format': None,
            'spatial_columns': [],
            'issues': []
        }
        
        if not os.path.exists(file_path):
            findings['status'] = 'failed'
            findings['failure_reason'] = 'FILE_NOT_FOUND'
            return findings
        
        if os.path.getsize(file_path) == 0:
            findings['status'] = 'failed'
            findings['failure_reason'] = 'EMPTY_FILE'
            return findings
        
        if pd is None:
            findings['status'] = 'failed'
            findings['failure_reason'] = 'PANDAS_NOT_INSTALLED'
            return findings
        
        try:
            # Try multiple encodings
            df = None
            for encoding in ['utf-8', 'latin1', 'iso-8859-1']:
                try:
                    df = pd.read_csv(file_path, encoding=encoding, on_bad_lines='skip', 
                                    comment='#', nrows=100)
                    break
                except Exception:
                    continue
            
            if df is None:
                findings['status'] = 'failed'
                findings['failure_reason'] = 'ENCODING_ERROR'
                findings['failure_details'] = 'Could not read with utf-8, latin1, or iso-8859-1'
                return findings
            
            findings['rows'] = len(df)
            findings['columns'] = list(df.columns)
            
            # Check for time columns
            time_cols = [c for c in df.columns if any(x in c.lower() for x in 
                        ['time', 'date', 'year', 'month', 'day', 'timestamp'])]
            
            if time_cols:
                findings['time_format'] = CSVDiagnostic._detect_time_format(df, time_cols)
            
            # Check for spatial columns
            spatial_keywords = ['lat', 'lon', 'x', 'y', 'depth', 'z']
            spatial_cols = [c for c in df.columns if any(x in c.lower() for x in spatial_keywords)]
            findings['spatial_columns'] = spatial_cols
            
            # Data quality checks
            if df.empty:
                findings['issues'].append('DataFrame is empty after loading')
            
            if df.isnull().sum().sum() > len(df) * len(df.columns) * 0.5:
                findings['issues'].append('> 50% missing values')
            
            # Show sample
            findings['sample_data'] = df.head(2).to_dict('records')
            
            findings['status'] = 'success'
            
        except Exception as e:
            findings['status'] = 'failed'
            findings['failure_reason'] = 'CSV_PARSE_ERROR'
            findings['failure_details'] = str(e)[:200]
        
        return findings
    
    @staticmethod
    def _detect_time_format(df, time_cols):
        """Attempts to infer time column format"""
        for col in time_cols:
            try:
                sample = df[col].dropna().head(1)
                if sample.empty:
                    continue
                
                val = sample.iloc[0]
                
                # Try ISO format
                try:
                    pd.to_datetime(val)
                    return 'ISO_8601'
                except:
                    pass
                
                # Try numeric (possibly days/months since reference)
                try:
                    float(val)
                    return 'NUMERIC_OFFSET'
                except:
                    pass
                
                # Check for year/month/day columns
                if isinstance(val, (int, float)):
                    if 1900 < val < 2100:
                        return 'YEAR_COLUMN_FOUND'
                
                return f'UNKNOWN_FORMAT ({type(val).__name__})'
            
            except Exception:
                continue
        
        return None


class NetCDFDiagnostic:
    """Analyzes NetCDF file structure"""
    
    @staticmethod
    def diagnose(file_path):
        """Returns dict with NetCDF structure"""
        findings = {
            'file_format': 'netcdf',
            'status': 'unknown',
            'failure_reason': None,
            'failure_details': None,
            'dimensions': {},
            'variables': [],
            'time_variable': None,
            'coordinate_variables': [],
            'issues': []
        }
        
        if not os.path.exists(file_path):
            findings['status'] = 'failed'
            findings['failure_reason'] = 'FILE_NOT_FOUND'
            return findings
        
        if netCDF4 is None:
            findings['status'] = 'failed'
            findings['failure_reason'] = 'NETCDF4_NOT_INSTALLED'
            findings['failure_details'] = 'Install with: pip install netCDF4'
            return findings
        
        try:
            ds = netCDF4.Dataset(file_path, 'r')
            
            # Get dimensions
            findings['dimensions'] = {k: len(v) for k, v in ds.dimensions.items()}
            
            # Get variables
            findings['variables'] = list(ds.variables.keys())
            
            # Identify time variable
            for var_name in ['time', 'TIME', 'Time', 'datetime', 'timestamp']:
                if var_name in ds.variables:
                    findings['time_variable'] = var_name
                    findings['time_format'] = NetCDFDiagnostic._get_time_info(ds, var_name)
                    break
            
            # Identify coordinate variables
            coord_keywords = ['lat', 'lon', 'depth', 'z', 'x', 'y']
            findings['coordinate_variables'] = [v for v in findings['variables'] 
                                               if any(k in v.lower() for k in coord_keywords)]
            
            # Data quality checks
            if not findings['time_variable']:
                findings['issues'].append('No time dimension found')
            
            if not findings['coordinate_variables']:
                findings['issues'].append('No spatial coordinates found')
            
            # Check for ragged arrays
            unlimited_dims = [k for k, v in ds.dimensions.items() if v.isunlimited()]
            if len(unlimited_dims) > 1:
                findings['issues'].append('Multiple unlimited dimensions (potential ragged array)')
            
            findings['status'] = 'success'
            ds.close()
            
        except Exception as e:
            findings['status'] = 'failed'
            findings['failure_reason'] = 'NETCDF_READ_ERROR'
            findings['failure_details'] = str(e)[:200]
        
        return findings
    
    @staticmethod
    def _get_time_info(ds, time_var):
        """Extract time variable attributes"""
        var = ds.variables[time_var]
        info = {
            'length': len(var),
            'data_type': str(var.dtype),
            'attributes': {k: v for k, v in var.__dict__.items()}
        }
        
        # Extract calendar and units from CF attributes
        if 'units' in var.__dict__:
            info['units'] = var.units
        if 'calendar' in var.__dict__:
            info['calendar'] = var.calendar
        
        return info


class ShapefileDiagnostic:
    """Diagnoses shapefile readability"""
    
    @staticmethod
    def diagnose(file_path):
        """Quick check of shapefile validity"""
        findings = {
            'file_format': 'shapefile',
            'status': 'unknown',
            'failure_reason': None,
            'failure_details': None,
            'base_name': os.path.splitext(os.path.basename(file_path))[0],
            'required_files': [],
            'geometry_type': None,
        }
        
        # Check for required shapefile components
        base = os.path.splitext(file_path)[0]
        required = ['.shp', '.shx', '.dbf']
        
        missing = []
        for ext in required:
            path = base + ext
            if os.path.exists(path):
                findings['required_files'].append(ext)
            else:
                missing.append(ext)
        
        if missing:
            findings['status'] = 'failed'
            findings['failure_reason'] = 'MISSING_SHAPEFILE_COMPONENTS'
            findings['failure_details'] = f"Missing: {', '.join(missing)}"
            return findings
        
        try:
            import geopandas as gpd
            gdf = gpd.read_file(file_path)
            findings['status'] = 'success'
            findings['geometry_type'] = str(gdf.geometry.type.iloc[0])
            findings['record_count'] = len(gdf)
        except ImportError:
            findings['status'] = 'failed'
            findings['failure_reason'] = 'GEOPANDAS_NOT_INSTALLED'
            findings['failure_details'] = 'Install with: pip install geopandas'
        except Exception as e:
            findings['status'] = 'failed'
            findings['failure_reason'] = 'GEOPANDAS_READ_ERROR'
            findings['failure_details'] = str(e)[:200]
        
        return findings


class GPXDiagnostic:
    """Diagnoses GPX file readability"""
    
    @staticmethod
    def diagnose(file_path):
        findings = {
            'file_format': 'gpx',
            'status': 'unknown',
            'failure_reason': None,
            'failure_details': None,
        }
        
        if not os.path.exists(file_path):
            findings['status'] = 'failed'
            findings['failure_reason'] = 'FILE_NOT_FOUND'
            return findings
        
        try:
            # Basic XML validation
            import xml.etree.ElementTree as ET
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # Count waypoints, tracks, routes
            ns = {'gpx': 'http://www.topografix.com/GPX/1/1'}
            waypoints = root.findall('.//gpx:wpt', ns)
            tracks = root.findall('.//gpx:trk', ns)
            
            findings['status'] = 'success'
            findings['waypoints'] = len(waypoints)
            findings['tracks'] = len(tracks)
            
        except Exception as e:
            findings['status'] = 'failed'
            findings['failure_reason'] = 'GPX_PARSE_ERROR'
            findings['failure_details'] = str(e)[:200]
        
        return findings


def scan_aodn_directory(root_path):
    """Recursively scan AODN_data directory and diagnose all files"""
    report = DiagnosticReport()
    
    file_handlers = {
        '.csv': CSVDiagnostic.diagnose,
        '.nc': NetCDFDiagnostic.diagnose,
        '.shp': ShapefileDiagnostic.diagnose,
        '.gpx': GPXDiagnostic.diagnose,
    }
    
    scanned_files = set()
    
    for root, dirs, files in os.walk(root_path):
        # Skip metadata directories
        if 'metadata' in root:
            continue
        
        for file in sorted(files):
            file_lower = file.lower()
            ext = os.path.splitext(file_lower)[1]
            
            # Skip index and metadata files
            if file == 'index.csv' or 'metadata' in file:
                continue
            
            # Skip .shx, .dbf (process only .shp)
            if ext in ['.shx', '.dbf', '.prj', '.cpg']:
                continue
            
            if ext not in file_handlers:
                continue
            
            file_path = os.path.join(root, file)
            
            # Avoid duplicate scans (shapefile siblings)
            if file_path in scanned_files:
                continue
            scanned_files.add(file_path)
            
            # Get dataset name from directory hierarchy
            rel_path = os.path.relpath(root, root_path)
            if rel_path == '.':
                dataset_name = file
            else:
                dataset_name = f"{rel_path}/{file}"
            
            logger.info(f"Scanning: {dataset_name}")
            
            try:
                findings = file_handlers[ext](file_path)
                report.add_dataset(dataset_name, findings)
            except Exception as e:
                logger.error(f"Unexpected error scanning {dataset_name}: {e}")
                report.add_dataset(dataset_name, {
                    'status': 'failed',
                    'failure_reason': 'UNEXPECTED_ERROR',
                    'failure_details': str(e)[:200]
                })
    
    return report


def main():
    aodn_path = 'AODN_data'
    
    if not os.path.exists(aodn_path):
        logger.error(f"AODN_data directory not found at {aodn_path}")
        return
    
    logger.info(f"Starting diagnostic scan of {aodn_path}")
    report = scan_aodn_directory(aodn_path)
    
    report.print_summary()
    report.print_failures()
    
    # Save detailed JSON report
    with open('diagnostic_report.json', 'w') as f:
        # Convert report to JSON-serializable format
        json_data = {
            'timestamp': datetime.now().isoformat(),
            'summary': report.summary_stats,
            'datasets': report.datasets
        }
        json.dump(json_data, f, indent=2, default=str)
    
    logger.info("Detailed report saved to diagnostic_report.json")


if __name__ == '__main__':
    main()
