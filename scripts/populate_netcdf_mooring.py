#!/usr/bin/env python3
"""
populate_netcdf_mooring.py

Ingest IMOS mooring NetCDF files into PostgreSQL with proper handling of:
- CF-1.6 time coordinates (days since 1950-01-01)
- IMOS quality flags
- Scalar spatial coordinates
- Time-series observations
"""

import netCDF4 as nc
import numpy as np
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Database connection parameters (from docker-compose.yml)
DB_CONFIG = {
    'dbname': 'marine_db',
    'user': 'marine_user',
    'password': 'marine_pass123',
    'host': 'localhost',
    'port': 5433
}

# IMOS QC flag interpretations
IMOS_QC_FLAGS = {
    0: 'no_qc_performed',
    1: 'good_data',
    2: 'probably_good_data',
    3: 'bad_data_potentially_correctable',
    4: 'bad_data',
    5: 'value_changed',
    9: 'missing_value'
}

def parse_cf_time(time_var):
    """Convert CF-compliant time variable to Python datetimes."""
    units = time_var.units
    calendar = getattr(time_var, 'calendar', 'gregorian')
    
    # Parse "days since 1950-01-01 00:00:00 UTC" format
    times = nc.num2date(time_var[:], units=units, calendar=calendar)
    
    # Convert to timezone-aware UTC datetimes
    return [t.replace(tzinfo=None) if hasattr(t, 'tzinfo') else t for t in times]

def extract_global_metadata(ds):
    """Extract key global attributes."""
    return {
        'project': getattr(ds, 'project', None),
        'title': getattr(ds, 'title', None),
        'institution': getattr(ds, 'institution', None),
        'site_code': getattr(ds, 'site_code', None),
        'site_name': getattr(ds, 'site', None),
        'instrument': getattr(ds, 'instrument', None),
        'instrument_serial': getattr(ds, 'instrument_serial_number', None),
        'principal_investigator': getattr(ds, 'principal_investigator', None),
        'time_coverage_start': getattr(ds, 'time_coverage_start', None),
        'time_coverage_end': getattr(ds, 'time_coverage_end', None),
        'data_centre': getattr(ds, 'data_centre', None),
        'license': getattr(ds, 'license', None),
        'conventions': getattr(ds, 'Conventions', None)
    }

def create_tables(conn):
    """Create database tables if they don't exist."""
    with conn.cursor() as cur:
        # Deployments table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mooring_deployments (
                deployment_id SERIAL PRIMARY KEY,
                site_code VARCHAR(50),
                site_name TEXT,
                instrument VARCHAR(100),
                instrument_serial VARCHAR(50),
                longitude DOUBLE PRECISION,
                latitude DOUBLE PRECISION,
                nominal_depth REAL,
                deployment_start TIMESTAMPTZ,
                deployment_end TIMESTAMPTZ,
                principal_investigator TEXT,
                institution TEXT,
                project TEXT,
                conventions TEXT,
                source_file TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        
        # Observations table (time-series data)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mooring_observations (
                observation_id BIGSERIAL PRIMARY KEY,
                deployment_id INTEGER REFERENCES mooring_deployments(deployment_id),
                observation_time TIMESTAMPTZ NOT NULL,
                pressure_dbar REAL,
                pressure_qc SMALLINT,
                temperature_c REAL,
                temperature_qc SMALLINT,
                salinity_psu REAL,
                salinity_qc SMALLINT,
                conductivity_sm REAL,
                conductivity_qc SMALLINT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        
        # Index for time-based queries
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_obs_time 
            ON mooring_observations(observation_time);
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_obs_deployment 
            ON mooring_observations(deployment_id);
        """)
        
        conn.commit()
        print("✓ Database tables created/verified")

def insert_deployment(conn, ds, filepath):
    """Insert deployment metadata and return deployment_id."""
    metadata = extract_global_metadata(ds)
    
    # Extract scalar coordinates
    lon = float(ds.variables['LONGITUDE'][:])
    lat = float(ds.variables['LATITUDE'][:])
    depth = float(ds.variables['NOMINAL_DEPTH'][:])
    
    # Parse time coverage
    start_time = datetime.fromisoformat(metadata['time_coverage_start'].replace('Z', '+00:00'))
    end_time = datetime.fromisoformat(metadata['time_coverage_end'].replace('Z', '+00:00'))
    
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO mooring_deployments (
                site_code, site_name, instrument, instrument_serial,
                longitude, latitude, nominal_depth,
                deployment_start, deployment_end,
                principal_investigator, institution, project,
                conventions, source_file
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING deployment_id
        """, (
            metadata['site_code'], metadata['site_name'],
            metadata['instrument'], str(metadata['instrument_serial']),
            lon, lat, depth,
            start_time, end_time,
            metadata['principal_investigator'], metadata['institution'],
            metadata['project'], metadata['conventions'],
            str(filepath)
        ))
        
        deployment_id = cur.fetchone()[0]
        conn.commit()
        
        print(f"✓ Inserted deployment #{deployment_id}: {metadata['site_name']}")
        return deployment_id

def insert_observations(conn, ds, deployment_id, batch_size=5000):
    """Insert time-series observations in batches."""
    
    # Parse time dimension
    times = parse_cf_time(ds.variables['TIME'])
    n_obs = len(times)
    
    # Prepare observation records
    observations = []
    
    # Get data variables and QC flags
    vars_to_read = {
        'PRES': ('pressure_dbar', 'pressure_qc'),
        'TEMP': ('temperature_c', 'temperature_qc'),
        'PSAL': ('salinity_psu', 'salinity_qc'),  # if present
        'CNDC': ('conductivity_sm', 'conductivity_qc')  # if present
    }
    
    # Load data arrays
    data = {}
    for var_name, (col_name, qc_col) in vars_to_read.items():
        if var_name in ds.variables:
            var_data = ds.variables[var_name][:]
            qc_data = ds.variables[f'{var_name}_quality_control'][:]
            
            # Replace fill values with None
            fill_value = getattr(ds.variables[var_name], '_FillValue', None)
            if fill_value is not None:
                var_data = np.ma.masked_equal(var_data, fill_value)
            
            data[col_name] = var_data
            data[qc_col] = qc_data
    
    # Build observation tuples
    for i in range(n_obs):
        obs = [deployment_id, times[i]]
        
        # Add each variable (or None if not present)
        for var_name, (col_name, qc_col) in vars_to_read.items():
            if col_name in data:
                val = data[col_name][i]
                qc = int(data[qc_col][i])
                
                # Convert masked values to None
                if isinstance(val, np.ma.core.MaskedConstant) or np.isnan(val):
                    obs.extend([None, qc])
                else:
                    obs.extend([float(val), qc])
            else:
                obs.extend([None, None])
        
        observations.append(tuple(obs))
    
    # Insert in batches
    with conn.cursor() as cur:
        execute_batch(cur, """
            INSERT INTO mooring_observations (
                deployment_id, observation_time,
                pressure_dbar, pressure_qc,
                temperature_c, temperature_qc,
                salinity_psu, salinity_qc,
                conductivity_sm, conductivity_qc
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, observations, page_size=batch_size)
        
        conn.commit()
    
    print(f"✓ Inserted {n_obs:,} observations in batches of {batch_size}")

def ingest_netcdf_file(filepath):
    """Main ingestion workflow for a single NetCDF file."""
    filepath = Path(filepath)
    
    if not filepath.exists():
        print(f"✗ File not found: {filepath}")
        return False
    
    print(f"\n{'='*70}")
    print(f"Processing: {filepath.name}")
    print(f"{'='*70}\n")
    
    try:
        # Open NetCDF file
        ds = nc.Dataset(filepath, 'r')
        
        # Connect to database
        conn = psycopg2.connect(**DB_CONFIG)
        
        # Create tables
        create_tables(conn)
        
        # Insert deployment metadata
        deployment_id = insert_deployment(conn, ds, filepath)
        
        # Insert observations
        insert_observations(conn, ds, deployment_id)
        
        # Clean up
        ds.close()
        conn.close()
        
        print(f"\n✓ Successfully ingested {filepath.name}")
        return True
        
    except Exception as e:
        print(f"\n✗ Error processing {filepath.name}:")
        print(f"  {type(e).__name__}: {e}")
        return False

def main():
    """Process command-line arguments and ingest NetCDF files."""
    if len(sys.argv) < 2:
        print("Usage: python populate_netcdf_mooring.py <netcdf_file> [<file2> ...]")
        print("\nExample:")
        print("  python populate_netcdf_mooring.py *.nc")
        sys.exit(1)
    
    files = sys.argv[1:]
    success_count = 0
    
    for filepath in files:
        if ingest_netcdf_file(filepath):
            success_count += 1
    
    print(f"\n{'='*70}")
    print(f"Summary: {success_count}/{len(files)} files ingested successfully")
    print(f"{'='*70}\n")

if __name__ == '__main__':
    main()
