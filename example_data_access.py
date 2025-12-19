
# Marine Data Access Example
# This notebook demonstrates how to query the metadata database and load the corresponding data files.

import psycopg2
import pandas as pd
import xarray as xr
import os
import matplotlib.pyplot as plt
from psycopg2.extras import RealDictCursor
    
# Database connection
conn = psycopg2.connect(
    dbname='marine_db',
    user='marine_user',
    password='marine_pass123',
    host='localhost',
    port='5433'
)

# 1. Query Metadata from Database
print("Querying database for available datasets...")
query = """
SELECT id, title, dataset_name, dataset_path, time_start, time_end
FROM metadata
WHERE dataset_name LIKE '%Chlorophyll%'
   OR dataset_name LIKE '%Phytoplankton%'
ORDER BY title;
"""

with conn.cursor(cursor_factory=RealDictCursor) as cur:
    cur.execute(query)
    datasets = cur.fetchall()

df_meta = pd.DataFrame(datasets)
print(f"Found {len(df_meta)} datasets matching criteria.")
print(df_meta[['title', 'dataset_path']].head())

# 2. Load a CSV Dataset
# Example: Phytoplankton sampling
print("\n--- Loading CSV Dataset ---")
phytoplankton_ds = next((d for d in datasets if "Phytoplankton" in d['title']), None)

if phytoplankton_ds:
    base_dir = os.path.expanduser("~/tas_climate_data")
    dataset_path = os.path.join(base_dir, phytoplankton_ds['dataset_path'])
    
    # Find the CSV file
    csv_file = None
    for f in os.listdir(dataset_path):
        if f.endswith('.csv'):
            csv_file = os.path.join(dataset_path, f)
            break
            
    if csv_file:
        print(f"Loading {csv_file}...")
        df_phyto = pd.read_csv(csv_file)
        print("Columns:", df_phyto.columns.tolist())
        print(df_phyto.head())
        
        # Simple plot if relevant columns exist
        if 'Date' in df_phyto.columns and 'Abundance_cells_L' in df_phyto.columns:
             df_phyto['Date'] = pd.to_datetime(df_phyto['Date'])
             df_phyto.set_index('Date')['Abundance_cells_L'].plot(title="Phytoplankton Abundance")
             plt.show()

# 3. Load a NetCDF Dataset (Example handling)
# This part would search for .nc files similarly to how we searched for .csv
print("\n--- Searching for NetCDF Datasets in Query Results ---")
for d in datasets:
    path = os.path.join(base_dir, d['dataset_path'])
    nc_files = [f for f in os.listdir(path) if f.endswith('.nc')]
    
    if nc_files:
        print(f"Found NetCDF in {d['title']}: {nc_files[0]}")
        nc_path = os.path.join(path, nc_files[0])
        
        try:
            ds = xr.open_dataset(nc_path)
            print(ds)
            print("\nVariables:", list(ds.variables))
            ds.close()
            break # Just show one example
        except Exception as e:
            print(f"Error opening NetCDF: {e}")

conn.close()
