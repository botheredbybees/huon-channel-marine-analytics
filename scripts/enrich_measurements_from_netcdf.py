#!/usr/bin/env python3
"""
Enrich parameter mappings and measurements from NetCDF file headers.
Extracts CF-compliant variable metadata and validates against actual data.

This script scans NetCDF files to extract variable metadata and updates
the parameter_mappings table with descriptions and units. It's designed
to run independently and validate metadata before updating the database.

Usage:
    python enrich_measurements_from_netcdf.py

Environment variables:
    DB_HOST: Database host (default: localhost)
    DB_PORT: Database port (default: 5433)
    DB_NAME: Database name (default: marine_db)
    DB_USER: Database user (default: marine_user)
    DB_PASSWORD: Database password (required for authentication)
    AODN_DATA_PATH: Path to AODN_data directory (default: /AODN_data)
"""

import os
import psycopg2
import netCDF4
from pathlib import Path
import logging
from typing import Dict, List, Tuple
from collections import defaultdict
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NetCDFEnricher:
    """Extract and enrich metadata from NetCDF file headers."""
    
    def __init__(self, db_config: dict, aodn_data_path: str):
        self.db_config = db_config
        self.aodn_data_path = Path(aodn_data_path)
        self.conn = None
        self.stats = {
            'files_found': 0,
            'files_processed': 0,
            'files_failed': 0,
            'variables_extracted': 0,
            'variables_validated': 0,
            'variables_invalid': 0,
            'mappings_updated': 0,
            'mappings_inserted': 0,
        }
        self.validation_issues = defaultdict(list)
    
    def connect(self):
        """Connect to PostgreSQL database."""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            logger.info(f"Connected to {self.db_config['database']} at {self.db_config['host']}:{self.db_config['port']}")
        except psycopg2.OperationalError as e:
            logger.error(f"Failed to connect to database: {e}")
            logger.error("Check your environment variables: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD")
            sys.exit(1)
    
    def disconnect(self):
        """Disconnect from database."""
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from database")
    
    def find_netcdf_files(self) -> Dict[str, List[Path]]:
        """Find all .nc files in dataset directories."""
        nc_files = {}
        
        if not self.aodn_data_path.exists():
            logger.error(f"AODN_DATA_PATH does not exist: {self.aodn_data_path}")
            return nc_files
        
        try:
            for nc_file in self.aodn_data_path.rglob('*.nc'):
                dataset_name = nc_file.parent.name
                if dataset_name not in nc_files:
                    nc_files[dataset_name] = []
                nc_files[dataset_name].append(nc_file)
        except Exception as e:
            logger.error(f"Error scanning for NetCDF files: {e}")
        
        self.stats['files_found'] = sum(len(v) for v in nc_files.values())
        logger.info(f"Found {self.stats['files_found']} NetCDF files across {len(nc_files)} datasets")
        return nc_files
    
    def extract_netcdf_attributes(self, nc_file: Path) -> Dict[str, Dict]:
        """Extract variable metadata from NetCDF file."""
        variables = {}
        
        try:
            ds = netCDF4.Dataset(nc_file, 'r')
            
            for var_name in ds.variables:
                var = ds.variables[var_name]
                
                attributes = {}
                
                # Extract standard CF attributes
                attr_names = ['long_name', 'standard_name', 'units', 'comment', 
                             'cf_role', 'instrument', 'valid_min', 'valid_max']
                
                for attr in attr_names:
                    if hasattr(var, attr):
                        try:
                            value = getattr(var, attr)
                            if value is not None:
                                attributes[attr] = str(value)
                        except Exception:
                            pass
                
                # Add data type and shape
                attributes['data_type'] = str(var.dtype.name)
                attributes['shape'] = str(var.shape)
                
                if attributes:
                    variables[var_name] = attributes
            
            ds.close()
            
        except Exception as e:
            logger.error(f"Error reading {nc_file}: {e}")
        
        return variables
    
    def validate_against_data(self, var_name: str, attributes: dict) -> Tuple[bool, str]:
        """Validate extracted attributes against actual data in measurements table."""
        cursor = self.conn.cursor()
        
        try:
            # Check if this parameter exists in measurements
            cursor.execute(
                "SELECT COUNT(*) FROM measurements WHERE parameter_code = %s",
                (var_name,)
            )
            count = cursor.fetchone()[0]
            
            if count == 0:
                return False, f"Parameter not found in measurements table"
            
            # Validate units by checking value ranges
            if 'units' in attributes:
                units = attributes['units']
                
                # Get actual min/max values
                cursor.execute(
                    "SELECT MIN(value), MAX(value) FROM measurements WHERE parameter_code = %s",
                    (var_name,)
                )
                min_val, max_val = cursor.fetchone()
                
                # Special case: wind_speed from satellite (often in cm/s instead of m/s)
                if 'wind' in var_name.lower() and 'cm' in units.lower():
                    if max_val and max_val > 50:
                        issue = f"Wind units likely wrong (cm/s declared but max={max_val})"
                        self.validation_issues[var_name].append(issue)
                        return False, issue
                
                # Flag pressure values that might be negative due to atmospheric offset
                if 'pressure' in var_name.lower() or 'pres' in var_name.lower():
                    cursor.execute(
                        "SELECT COUNT(*) FROM measurements WHERE parameter_code = %s AND value < 0",
                        (var_name,)
                    )
                    neg_count = cursor.fetchone()[0]
                    if neg_count > 0:
                        issue = f"Found {neg_count} negative pressure values"
                        self.validation_issues[var_name].append(issue)
            
            return True, "Valid"
        except Exception as e:
            logger.error(f"Validation error for {var_name}: {e}")
            return False, str(e)
        finally:
            cursor.close()
    
    def update_parameter_mappings(self, nc_vars: Dict[str, Dict]) -> Tuple[int, int]:
        """Update parameter_mappings table with NetCDF metadata."""
        cursor = self.conn.cursor()
        updated = 0
        inserted = 0
        
        for var_name, attributes in nc_vars.items():
            if not attributes:
                continue
            
            try:
                # Check if mapping exists
                cursor.execute(
                    "SELECT id FROM parameter_mappings WHERE raw_parameter_name = %s",
                    (var_name,)
                )
                mapping = cursor.fetchone()
                
                description = attributes.get('long_name') or attributes.get('standard_name')
                units = attributes.get('units')
                
                if mapping:
                    # Update existing mapping
                    update_fields = []
                    update_values = []
                    
                    if description:
                        update_fields.append('description = %s')
                        update_values.append(description[:500])
                    
                    if units:
                        update_fields.append('unit = %s')
                        update_values.append(units[:100])
                    
                    if update_fields:
                        update_values.append(mapping[0])
                        query = f"UPDATE parameter_mappings SET {', '.join(update_fields)} WHERE id = %s"
                        cursor.execute(query, update_values)
                        updated += 1
                
                else:
                    # Insert new mapping (conservative: only if we have strong CF metadata)
                    if 'standard_name' in attributes or 'long_name' in attributes:
                        cursor.execute(
                            """INSERT INTO parameter_mappings 
                            (raw_parameter_name, standard_code, namespace, unit, description, source)
                            VALUES (%s, %s, %s, %s, %s, %s)""",
                            (var_name, var_name, 'cf', units, description, 'netcdf_header')
                        )
                        inserted += 1
                
                self.conn.commit()
            except psycopg2.Error as e:
                logger.error(f"Database error for {var_name}: {e}")
                self.conn.rollback()
            except Exception as e:
                logger.error(f"Unexpected error for {var_name}: {e}")
                self.conn.rollback()
        
        cursor.close()
        return updated, inserted
    
    def run_enrichment(self):
        """Main enrichment workflow."""
        logger.info("Starting NetCDF metadata enrichment")
        self.connect()
        
        try:
            nc_files = self.find_netcdf_files()
            
            if not nc_files:
                logger.warning("No NetCDF files found to process")
                return
            
            for dataset_name, files in nc_files.items():
                for nc_file in files:
                    try:
                        logger.info(f"Processing {dataset_name}: {nc_file.name}")
                        variables = self.extract_netcdf_attributes(nc_file)
                        self.stats['variables_extracted'] += len(variables)
                        
                        # Validate each variable
                        valid_vars = {}
                        for var_name, attributes in variables.items():
                            is_valid, msg = self.validate_against_data(var_name, attributes)
                            if is_valid:
                                valid_vars[var_name] = attributes
                                self.stats['variables_validated'] += 1
                                logger.debug(f"  ✓ {var_name}: {msg}")
                            else:
                                self.stats['variables_invalid'] += 1
                                logger.warning(f"  ✗ {var_name}: {msg}")
                        
                        # Update mappings for valid variables
                        updated, inserted = self.update_parameter_mappings(valid_vars)
                        self.stats['mappings_updated'] += updated
                        self.stats['mappings_inserted'] += inserted
                        self.stats['files_processed'] += 1
                        
                    except Exception as e:
                        logger.error(f"Failed to process {nc_file}: {e}")
                        self.stats['files_failed'] += 1
        
        except Exception as e:
            logger.error(f"Fatal error during enrichment: {e}")
        finally:
            self.disconnect()
            self._print_summary()
    
    def _print_summary(self):
        """Print enrichment summary statistics."""
        logger.info("=" * 70)
        logger.info("NETCDF METADATA ENRICHMENT SUMMARY")
        logger.info("=" * 70)
        logger.info(f"NetCDF files found:      {self.stats['files_found']}")
        logger.info(f"Files processed:         {self.stats['files_processed']}")
        logger.info(f"Files failed:            {self.stats['files_failed']}")
        logger.info(f"Variables extracted:     {self.stats['variables_extracted']}")
        logger.info(f"Variables validated:     {self.stats['variables_validated']}")
        logger.info(f"Variables invalid:       {self.stats['variables_invalid']}")
        logger.info(f"Mappings updated:        {self.stats['mappings_updated']}")
        logger.info(f"Mappings inserted:       {self.stats['mappings_inserted']}")
        logger.info("=" * 70)
        
        if self.validation_issues:
            logger.info("\nVALIDATION ISSUES FOUND:")
            for var_name, issues in self.validation_issues.items():
                for issue in issues:
                    logger.info(f"  {var_name}: {issue}")


if __name__ == '__main__':
    # Build db_config from environment variables
    # Use correct defaults matching docker-compose.yml
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5433)),
        'database': os.getenv('DB_NAME', 'marine_db'),
        'user': os.getenv('DB_USER', 'marine_user'),
        'password': os.getenv('DB_PASSWORD'),  # No default - must be provided
    }
    
    # Validate required password
    if not db_config['password']:
        logger.error("DB_PASSWORD environment variable not set")
        logger.error("Set it with: export DB_PASSWORD=<your_password>")
        sys.exit(1)
    
    aodn_path = os.getenv('AODN_DATA_PATH', '/AODN_data')
    
    enricher = NetCDFEnricher(db_config, aodn_path)
    enricher.run_enrichment()
