
import os
import json
import pandas as pd
import xarray as xr
import psycopg2
from psycopg2.extras import execute_values
import numpy as np
from datetime import datetime
import warnings

# Suppress warnings
warnings.filterwarnings('ignore')

DB_CONFIG = {
    'dbname': 'marine_db',
    'user': 'marine_user',
    'password': 'marine_pass123',
    'host': 'localhost',
    'port': '5433'
}

LIMIT_ROWS_PER_FILE = 500  # Safety limit for demonstration

def load_mapping():
    """Load parameter mapping from JSON"""
    with open('AODN_data/marine_parameter_mapping.json', 'r') as f:
        mapping = json.load(f)
    
    # Flatten mapping for easy lookup: key -> {code, uom, ns}
    flat_map = {}
    for section in ['bodc_mapping', 'cf_mapping', 'custom_mapping', 'pigment_mapping', 'wave_mapping', 'quality_flags']:
        for key, info in mapping.get(section, {}).items():
            # Map the primary key
            flat_map[key.lower()] = info
            # Map alternate names
            for alt in info.get('alternate_names', []):
                flat_map[alt.lower()] = info
    return flat_map

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def process_chunk(conn, rows):
    """Bulk insert rows into measurements table"""
    if not rows:
        return
    
    sql = """
    INSERT INTO measurements (
        time, uuid, metadata_id, parameter_code, value, uom, namespace, depth_m, quality_flag
    ) VALUES %s
    ON CONFLICT DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()

def extract_csv(file_path, metadata, mapping):
    """Extract measurements from CSV"""
    extracted = []
    try:
        # Use comment='#' to skip header comments
        df = pd.read_csv(file_path, comment='#', on_bad_lines='skip', low_memory=False)
        
        # Identify Time Column
        time_col = next((c for c in df.columns if 'date' in c.lower() or 'time' in c.lower()), None)
        if not time_col:
            # Fallback for specific datasets
            if 'Year' in df.columns and 'Month' in df.columns:
                df['constructed_time'] = pd.to_datetime(df[['Year', 'Month']].assign(DAY=1), errors='coerce')
                time_col = 'constructed_time'
            else:
                return []

        # Identify Depth Column
        depth_col = next((c for c in df.columns if 'depth' in c.lower()), None)

        # Iterate through columns to find parameters
        for col in df.columns:
            col_lower = col.lower()
            if col_lower in mapping:
                param_info = mapping[col_lower]
                
                # Iterate rows (limited)
                for _, row in df.head(LIMIT_ROWS_PER_FILE).iterrows():
                    val = row[col]
                    
                    # Skip nulls or non-numeric
                    if pd.isna(val): continue
                    
                    try:
                        numeric_val = float(val)
                    except ValueError:
                        continue
                    
                    try:
                        ts = pd.to_datetime(row[time_col], errors='coerce')
                        if pd.isna(ts): continue # Skip invalid dates (NaT)
                    except:
                        continue
                        
                    depth_val = None
                    if depth_col and not pd.isna(row[depth_col]):
                        try:
                            depth_val = float(row[depth_col])
                        except ValueError:
                            pass

                    extracted.append((
                        ts,
                        metadata['uuid'],
                        metadata['id'],
                        param_info['code'],
                        numeric_val,
                        param_info['uom'],
                        param_info['ns'],
                        depth_val,
                        1 # Default quality
                    ))
    except Exception as e:
        print(f"    Error processing CSV {os.path.basename(file_path)}: {e}")
        
    return extracted

def extract_netcdf(file_path, metadata, mapping):
    """Extract measurements from NetCDF"""
    extracted = []
    try:
        try:
            ds = xr.open_dataset(file_path, decode_times=True, use_cftime=True)
        except Exception:
            # Fallback for weird time units
            ds = xr.open_dataset(file_path, decode_times=False)
            
            # Custom handler for 'months since'
            time_var_name = next((v for v in ds.coords if 'time' in v.lower()), None)
            if not time_var_name:
                 time_var_name = next((v for v in ds.data_vars if 'time' in v.lower()), None)
                 
            if time_var_name and time_var_name in ds:
                 try:
                     units = ds[time_var_name].attrs.get('units', '')
                     if 'months since' in units:
                          # Format: "months since YYYY-MM-DD HH:MM:SS"
                          base_str = units.replace('months since', '').strip()
                          from dateutil.parser import parse
                          from dateutil.relativedelta import relativedelta
                          
                          base_date = parse(base_str)
                          new_times = []
                          for val in ds[time_var_name].values:
                               if np.isnan(val):
                                   new_times.append(pd.NaT)
                                   continue
                               months = int(val)
                               frac = val - months
                               # Approx days from fraction
                               days = int(frac * 30.44)
                               dt = base_date + relativedelta(months=months) + relativedelta(days=days)
                               new_times.append(dt)
                          
                          ds[time_var_name] = ((ds[time_var_name].dims), new_times)
                          print(f"    DEBUG: Manually decoded 'months since' time units.")
                 except Exception as e:
                     print(f"    DEBUG: Failed manual time decode: {e}")
        
        # Try to find standard Time/Depth coordinates
        time_var = next((v for v in ds.coords if 'time' in v.lower()), None)
        # If no coordinate, maybe it's a data variable (ragged array)?
        if not time_var:
             time_var = next((v for v in ds.data_vars if 'time' in v.lower()), None)
             
        depth_var = next((v for v in ds.coords if 'depth' in v.lower()), None)
        
        # Check for 'TIME' specifically usually Upper Case in IMOS
        if not time_var and 'TIME' in ds: time_var = 'TIME'
        
        if not time_var:
            print(f"    DEBUG: No TIME variable found in {os.path.basename(file_path)}")
            return []

        # Iterate data variables
        for var_name in ds.data_vars:
            var_lower = var_name.lower()
            if var_lower in mapping:
                param_info = mapping[var_lower]
                print(f"    DEBUG: Matched variable {var_name}")
                
                da = ds[var_name]
                
                # Determine how to iterate
                # Standard grid: (time, lat, lon) or similar
                # Ragged: (obs) where TIME is also (obs)
                
                slice_dict = {}
                if time_var in da.dims:
                    # Increased limit to ensure we capture valid data in sparse/historical datasets
                    slice_dict[time_var] = slice(0, 2000)
                elif 'obs' in da.dims:
                    slice_dict['obs'] = slice(0, 500)
                # Fallback for profile data if 'profile' dim exists
                elif 'profile' in da.dims:
                    slice_dict['profile'] = slice(0, 50)
                else:
                    print(f"    DEBUG: No suitable dimension found to slice for {var_name}. Dims: {da.dims}")

                try:
                    da_slice = da.isel(**slice_dict) if slice_dict else da
                    
                    # Convert to dataset to hold both the var and time
                    try:
                        ds_subset = da_slice.to_dataset(name=var_name)
                    except ValueError as ve:
                        # Handle collision if var_name is same as dimension name
                         print(f"    DEBUG: ValueError in to_dataset: {ve}")
                         continue
                    
                    # Ensure time is included
                    if time_var in ds:
                        # Slice time same as data if it shares dims
                        time_da = ds[time_var]
                        # Intersect dimensions to see if we can slice
                        common_dims = set(time_da.dims).intersection(slice_dict.keys())
                        if common_dims:
                             # Slice time using the same dictionary
                             relevant_slice = {k: v for k, v in slice_dict.items() if k in time_da.dims}
                             ds_subset[time_var] = time_da.isel(**relevant_slice)
                        else:
                             # Maybe time is scalar or unconnected? Just add it
                             ds_subset[time_var] = time_da

                    df = ds_subset.to_dataframe().reset_index()
                    
                    # Ensure time_var is in columns
                    if time_var not in df.columns:
                        if time_var == df.index.name:
                            df.reset_index(inplace=True)
                        else:
                             # Try to get it from coords if not in df
                             if time_var in ds:
                                  continue
                             continue
                            
                    # Drop NaNs for the target variable to ensure we get valid data in our sample
                    df = df.dropna(subset=[var_name])

                    for _, row in df.head(LIMIT_ROWS_PER_FILE).iterrows():
                        # Handle Time
                        ts_raw = row[time_var]
                        try:
                            ts = pd.to_datetime(ts_raw)
                        except:
                            # Fallback for cftime objects
                            try:
                                ts = pd.to_datetime(str(ts_raw))
                            except:
                                continue
                        
                        if pd.isna(ts): continue
                            
                        val_raw = row[var_name]
                        if pd.isna(val_raw): continue
                        
                        extracted.append((
                            ts,
                            metadata['uuid'],
                            metadata['id'],
                            param_info['code'],
                            float(val_raw),
                            param_info['uom'],
                            param_info['ns'],
                            float(row[depth_var]) if depth_var and depth_var in row else None,
                            1
                        ))
                except MemoryError:
                    print("    DEBUG: MemoryError")
                    continue
                except Exception as ex:
                    print(f"    DEBUG: Exception in inner loop: {ex}")
                    continue

        ds.close()
    except Exception as e:
        print(f"    Error processing NetCDF {os.path.basename(file_path)}: {e}")
        
    return extracted

def main():
    print("Loading parameter mapping...")
    mapping = load_mapping()
    
    conn = get_db_connection()
    
    try:
        # Get Datasets
        with conn.cursor() as cur:
            cur.execute("""
                SELECT m.id, m.uuid, m.dataset_path, m.title 
                FROM metadata m
                LEFT JOIN measurements mes ON m.id = mes.metadata_id
                GROUP BY m.id
                HAVING COUNT(mes.data_id) = 0 AND m.dataset_path IS NOT NULL
            """)
            datasets = cur.fetchall() # List of tuples: (id, uuid, path, title)
            
        print(f"Found {len(datasets)} empty datasets to scan.")
        
        total_inserted = 0
        
        for ds_id, uuid, rel_path, title in datasets:
            # Construct full path
            # rel_path is "AODN_data/DatasetName"
            # Our script runs from tas_climate_data, so just use rel_path if it exists, or check validity
            
            full_path = os.path.abspath(rel_path)
            if not os.path.exists(full_path):
                # Try relative to CWD
                if os.path.exists(rel_path):
                    full_path = os.path.abspath(rel_path)
                else:
                    # Try prepending AODN_data if missing
                    # Actually DB has "AODN_data/..."
                    continue

            print(f"Scanning: {title[:50]}...")
            
            ds_c = 0
            
            for root, dirs, files in os.walk(full_path):
                if 'metadata' in root: continue
                
                for f in files:
                    file_path = os.path.join(root, f)
                    
                    rows = []
                    if f.endswith('.csv') and f != 'index.csv':
                        rows = extract_csv(file_path, {'id': ds_id, 'uuid': uuid}, mapping)
                    elif f.endswith('.nc'):
                        rows = extract_netcdf(file_path, {'id': ds_id, 'uuid': uuid}, mapping)
                    
                    if rows:
                        process_chunk(conn, rows)
                        total_inserted += len(rows)
                        ds_c += len(rows)
                        # Optional: limit per dataset for speed
                        if ds_c > 1000: break
                
                if ds_c > 1000: break
                        
    finally:
        conn.close()
        print(f"\nTotal measurements inserted: {total_inserted}")

if __name__ == "__main__":
    main()
