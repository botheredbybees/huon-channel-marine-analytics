# Data Ingestion Guide

Complete guide to downloading AODN/IMOS datasets and ingesting them into your marine analytics database.

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Downloading Data from AODN](#downloading-data-from-aodn)
4. [Metadata Extraction](#metadata-extraction)
5. [Data Ingestion Pipeline](#data-ingestion-pipeline)
6. [Verification](#verification)
7. [Troubleshooting](#troubleshooting)

---

## Overview

### Data Flow Architecture

```
AODN Portal              AODN_data/           Database
    │                        │                     │
    ├─ Download    →       ├─ Dataset1/          ├─ metadata
    ├─ Extract               │  ├─ *.csv            ├─ measurements
    ├─ Store                 │  ├─ *.nc             ├─ spatial_features
    └─ Metadata.xml          │  └─ metadata.xml    └─ species_observations
                             │
                             ├─ Dataset2/
                             └─ ...
                                 │
                          ETL Scripts
                          ├─ populate_measurements_v2.py
                          ├─ populate_spatial.py
                          └─ populate_biological.py
```

### Data Types Supported

| Type | File Format | ETL Script | Table Destination |
|------|-------------|------------|-------------------|
| **Time-series** | CSV, NetCDF | `populate_measurements_v2.py` | `measurements` |
| **Spatial** | Shapefiles, GPX | `populate_spatial.py` | `spatial_features` |
| **Biological** | CSV (species counts) | `populate_biological.py` | `species_observations` |
| **Metadata** | XML (ISO 19115) | Manual or script | `metadata` |

---

## Prerequisites

### System Requirements

- **Storage**: 50GB+ for AODN datasets
- **RAM**: 8GB minimum (16GB recommended)
- **Docker**: For PostgreSQL/TimescaleDB/PostGIS
- **Python**: 3.9+ with packages from `requirements.txt`

### Install Dependencies

```bash
# Install Python packages
pip install -r requirements.txt

# Verify installations
python -c "import pandas, netCDF4, geopandas; print('All packages installed')"

# Start database
docker-compose up -d timescaledb
```

### Database Initialization

If starting fresh:

```bash
# Initialize database schema
docker exec -i marine_timescaledb psql -U marine_user -d marine_db < init.sql

# Verify tables created
docker exec marine_timescaledb psql -U marine_user -d marine_db -c "\dt"
```

You should see tables including:
- `metadata`
- `measurements`
- `parameter_mappings` (new!)
- `spatial_features`
- `species_observations`
- `taxonomy`
- `locations`

---

## Downloading Data from AODN

### Method 1: AODN Portal (Manual Download)

#### Step 1: Search for Datasets

1. Visit **[AODN Portal](https://portal.aodn.org.au/)**
2. Search for your region:
   - Enter "Huon Estuary" or "D'Entrecasteaux Channel"
   - Or use coordinates: `-43.3, 147.0` (Huon region)
3. Filter by data type:
   - **Time Series**: Temperature, Salinity, Chlorophyll
   - **Gridded**: Satellite SST, Ocean Colour
   - **Profile**: CTD casts
   - **Biological**: Species surveys (RLS, IMAS)

#### Step 2: Download Dataset

1. **Click on dataset** → View Details
2. **Check spatial/temporal coverage**
3. **Download options**:
   - **NetCDF**: Time-series sensor data (preferred)
   - **CSV**: Tabular data, species observations
   - **Shapefile**: Spatial features (seagrass, kelp extent)
   - **Metadata (XML)**: ISO 19115 metadata (always download!)

#### Step 3: Organize Downloaded Files

Create this structure:

```
AODN_data/
├── Dataset_Name_1/
│   ├── *.nc              # NetCDF files
│   ├── *.csv             # CSV files
│   ├── metadata.xml      # ISO 19115 metadata
│   └── index.html        # Portal reference
├── Dataset_Name_2/
│   ├── *.shp, *.shx, *.dbf  # Shapefiles
│   └── metadata.xml
└── Dataset_Name_3/
    ├── *.csv
    └── metadata.xml
```

**Important**: Keep original folder names from AODN for traceability!

---

### Method 2: AODN API (Programmatic Download)

#### Using AODN THREDDS Server

For automated downloads of IMOS/AODN datasets:

```python
import requests
from pathlib import Path

# Example: Download IMOS Coastal Mooring Data
base_url = "https://thredds.aodn.org.au/thredds/fileServer/"
dataset_path = "IMOS/ANMN/TAS/NRSPHB/Temperature/"
file_name = "IMOS_ANMN-TAS_TZ_20230101_NRSPHB_FV01_TEMP-aggregated-timeseries_END-20231231.nc"

url = base_url + dataset_path + file_name
output_dir = Path("AODN_data/IMOS_Coastal_Moorings")
output_dir.mkdir(parents=True, exist_ok=True)

response = requests.get(url, stream=True)
with open(output_dir / file_name, 'wb') as f:
    for chunk in response.iter_content(chunk_size=8192):
        f.write(chunk)

print(f"Downloaded: {file_name}")
```

#### Bulk Download Script

For multiple datasets, create `download_aodn.py`:

```python
import requests
import json
from pathlib import Path

# Load dataset URLs from JSON
with open('aodn_datasets.json', 'r') as f:
    datasets = json.load(f)

for dataset in datasets:
    name = dataset['name']
    url = dataset['url']
    
    output_dir = Path(f"AODN_data/{name}")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Downloading {name}...")
    response = requests.get(url, stream=True)
    
    file_path = output_dir / url.split('/')[-1]
    with open(file_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    print(f"  → Saved to {file_path}")
```

Create `aodn_datasets.json`:

```json
[
  {
    "name": "IMOS_Wave_Buoys_Storm_Bay",
    "url": "https://thredds.aodn.org.au/thredds/fileServer/.../STORM-BAY_...nc"
  },
  {
    "name": "Chlorophyll_SE_Tasmania",
    "url": "https://portal.aodn.org.au/.../chlorophyll.csv"
  }
]
```

---

## Metadata Extraction

### Understanding AODN Metadata (ISO 19115 XML)

Every AODN dataset comes with an XML metadata file containing:

- **Spatial extent**: Bounding box (North, South, East, West)
- **Temporal extent**: Start/end dates
- **Parameters**: Variables measured
- **Keywords**: Themes, places, disciplines
- **Contact info**: Data custodians
- **License**: Usage constraints

### Automated Metadata Ingestion

**TODO: Create `populate_metadata.py`** (not yet implemented)

For now, manually insert metadata:

```sql
INSERT INTO metadata (
    uuid, title, abstract, west, east, south, north,
    time_start, time_end, dataset_name, dataset_path
) VALUES (
    'your-uuid-here',
    'Dataset Title',
    'Description from XML',
    147.0,  -- West longitude
    148.0,  -- East
    -43.5,  -- South latitude
    -42.5,  -- North
    '2010-01-01',
    '2023-12-31',
    'Dataset Folder Name',
    'AODN_data/Dataset_Folder_Name'
);
```

**Extract UUID from XML**:

```bash
xmlstarlet sel -t -v "//gmd:fileIdentifier/gco:CharacterString" -n metadata.xml
```

---

## Data Ingestion Pipeline

### Pipeline Overview

```
1. Run diagnostic         → Identify file formats and issues
2. Populate metadata      → Add dataset records (manual for now)
3. Ingest measurements    → Time-series CSV/NetCDF data
4. Ingest spatial         → Shapefiles for seagrass, kelp
5. Ingest biological      → Species observation CSVs
6. Verify                 → Check counts and data quality
```

---

### Step 1: Run Diagnostic

Scan all datasets to identify structure and potential issues:

```bash
python diagnostic_etl.py
```

**Output**: `diagnostic_report.json`

Review failure reasons:

```bash
jq '.summary.failure_reasons' diagnostic_report.json
```

Common issues:
- `ENCODING_ERROR` → Auto-handled by ETL v2
- `TIME_FORMAT_UNKNOWN` → Check time column format
- `MISSING_REQUIRED_COLUMNS` → Verify CSV structure

---

### Step 2: Populate Metadata Table

**Option A: Manual SQL Insert**

For each dataset folder:

```sql
INSERT INTO metadata (uuid, title, dataset_name, dataset_path, west, east, south, north)
VALUES (
    'f0904e61-2e15-4346-bb0e-638366b7e626',
    'Aerial surveys of giant kelp',
    'Aerial_surveys_giant_kelp_2019',
    'AODN_data/Aerial surveys of giant kelp (Macrocystis pyrifera) from Musselroe Bay to Southeast Cape, Tasmania, 2019',
    144.0, 148.0, -44.0, -40.0
);
```

**Option B: Bulk Import from CSV**

Create `metadata_import.csv`:

```csv
uuid,title,dataset_name,dataset_path,west,east,south,north
f0904e61-...,Aerial surveys...,Aerial_surveys_giant_kelp_2019,AODN_data/Aerial surveys...,144,148,-44,-40
```

Then import:

```bash
docker exec -i marine_timescaledb psql -U marine_user -d marine_db \
  -c "\COPY metadata(uuid,title,dataset_name,dataset_path,west,east,south,north) FROM STDIN CSV HEADER" < metadata_import.csv
```

---

### Step 3: Ingest Measurements (Time-Series Data)

Use **`populate_measurements_v2.py`** for CSV and NetCDF files:

```bash
# Full ingestion
python populate_measurements_v2.py

# Single dataset (testing)
python populate_measurements_v2.py --dataset "Chlorophyll"

# Limit rows for quick test
python populate_measurements_v2.py --limit 100
```

#### What It Does

1. **Scans** `AODN_data/` for CSV and NetCDF files
2. **Detects** time format automatically (7+ formats supported)
3. **Maps** parameters using `parameter_mappings` table
4. **Extracts** measurements with timestamps, values, depth
5. **Inserts** into `measurements` table in batches (1000 rows)

#### Monitor Progress

In another terminal:

```bash
# Watch measurement count grow
watch -n 5 'docker exec marine_timescaledb psql -U marine_user -d marine_db -c "SELECT COUNT(*) FROM measurements;"'
```

#### Expected Output

```
2025-12-20 08:00:00 - [INFO] Found 18 empty datasets
2025-12-20 08:00:01 - [INFO] Processing: National Outfall Database
2025-12-20 08:00:01 - [INFO]   Extracting CSV: National_Outfall_Database.csv
2025-12-20 08:00:08 - [INFO] Inserting 18793 measurements...
2025-12-20 08:00:09 - [INFO] Inserted 1000/1000 rows (total: 1000)
...
2025-12-20 08:00:30 - [INFO] Total inserted: 31501
```

---

### Step 4: Ingest Spatial Features (Shapefiles)

Use **`populate_spatial.py`** for seagrass, kelp extent, and other polygons:

```bash
python populate_spatial.py
```

#### Requirements

- **geopandas**: For shapefile reading
- **ogr2ogr**: System tool (install via `gdal-bin` on Ubuntu)

```bash
# Install GDAL tools
sudo apt-get install gdal-bin

# Verify
ogr2ogr --version
```

#### What It Does

1. Finds all `.shp` files in `AODN_data/`
2. Converts to GeoJSON
3. Inserts geometries + attributes into `spatial_features`
4. Properties stored as JSONB for flexible querying

#### Expected Output

```
Processing 'Australian Seagrass distribution'...
  → Inserted 1250 spatial features.
Processing 'CAMRIS Seagrass Dataset'...
  → Inserted 850 spatial features.
```

---

### Step 5: Ingest Biological Data (Species Observations)

Use **`populate_biological.py`** for RLS surveys, fish counts, invertebrate surveys:

```bash
python populate_biological.py
```

#### CSV Requirements

Biological CSVs must have:

- **Species column**: `SPECIES_NAME`, `SPECIES`, `SCIENTIFIC_NAME`
- **Location**: `SITE_CODE`, `LATITUDE`, `LONGITUDE`
- **Count**: `TOTAL_NUMBER`, `count_value`
- **Date**: `SURVEY_DATE`, `SIGHTING_DATE`

#### What It Does

1. Detects biological CSVs (checks for species-related columns)
2. Normalizes taxonomy into `taxonomy` table
3. Normalizes locations into `locations` table
4. Inserts observations into `species_observations`

#### Expected Output

```
Ingesting Condition_of_rocky_reef_communities_fish_surveys.csv...
  → 450 observations inserted
Ingesting Condition_of_rocky_reef_communities_algal_surveys.csv...
  → 320 observations inserted
```

---

## Verification

### Check Measurement Counts

```sql
-- Total measurements
SELECT COUNT(*) FROM measurements;

-- By dataset
SELECT uuid, COUNT(*) as count
FROM measurements
GROUP BY uuid
ORDER BY count DESC;

-- By parameter
SELECT parameter_code, COUNT(*) as count
FROM measurements
GROUP BY parameter_code
ORDER BY count DESC
LIMIT 20;
```

### Check Spatial Features

```sql
-- Total features
SELECT COUNT(*) FROM spatial_features;

-- By dataset
SELECT metadata_id, COUNT(*)
FROM spatial_features
GROUP BY metadata_id;

-- View properties
SELECT properties FROM spatial_features LIMIT 5;
```

### Check Species Observations

```sql
-- Total observations
SELECT COUNT(*) FROM species_observations;

-- Top species
SELECT t.species_name, COUNT(*) as sightings
FROM species_observations so
JOIN taxonomy t ON so.taxonomy_id = t.id
GROUP BY t.species_name
ORDER BY sightings DESC
LIMIT 10;
```

### Data Quality Checks

```sql
-- Time range
SELECT MIN(time) as earliest, MAX(time) as latest
FROM measurements;

-- Null values
SELECT 
    COUNT(*) as total,
    COUNT(*) FILTER (WHERE depth_m IS NULL) as missing_depth,
    COUNT(*) FILTER (WHERE quality_flag != 1) as questionable_quality
FROM measurements;

-- Parameter coverage
SELECT 
    namespace,
    COUNT(DISTINCT parameter_code) as unique_parameters,
    COUNT(*) as total_measurements
FROM measurements
GROUP BY namespace;
```

---

## Troubleshooting

### Issue: "No measurements extracted"

**Cause**: Dataset is likely spatial (shapefiles) or biological (species counts)

**Solution**: Run the appropriate script:
- Shapefiles → `populate_spatial.py`
- Species CSVs → `populate_biological.py`

---

### Issue: "Missing time or value column"

**Cause**: CSV doesn't match expected time-series format

**Solution**: Check CSV structure:

```bash
head -1 AODN_data/Dataset/file.csv
```

Required columns (case-insensitive):
- **Time**: `time`, `date`, `datetime`, `timestamp`
- **Value**: `value`, `concentration`, `measurement`

If columns have different names, manually inspect and add to parameter mapping.

---

### Issue: "ENCODING_ERROR"

**Cause**: File has non-UTF-8 encoding

**Solution**: ETL v2 auto-tries Latin-1 and ISO-8859-1. If still failing:

```bash
# Check encoding
file -i AODN_data/Dataset/file.csv

# Convert to UTF-8
iconv -f ISO-8859-1 -t UTF-8 file.csv > file_utf8.csv
```

---

### Issue: Time values look wrong (year 3000)

**Cause**: Wrong time reference in NetCDF

**Solution**: Check NetCDF time units:

```bash
ncdump -v time file.nc | grep "time:units"
```

If units are non-standard, convert manually:

```python
import netCDF4
import cftime

ds = netCDF4.Dataset('file.nc')
time_var = ds.variables['time']
time_units = time_var.units
calendar = time_var.calendar

dates = cftime.num2date(time_var[:], time_units, calendar=calendar)
print(dates[:5])  # Check first 5 timestamps
```

---

### Issue: "Batch insert failed: duplicate key"

**Cause**: Re-running ETL on already-ingested dataset

**Solution**: This is expected! ETL uses `ON CONFLICT DO NOTHING`. To force re-import:

```sql
-- Delete old data
DELETE FROM measurements WHERE metadata_id = <dataset_id>;

-- Re-run ETL
python populate_measurements_v2.py --dataset "<name>"
```

---

## Advanced Topics

### Custom Parameter Mappings

Add new mappings to database:

```sql
INSERT INTO parameter_mappings (raw_parameter_name, standard_code, namespace, unit, source)
VALUES ('CUSTOM_PARAM', 'MY_CODE', 'custom', 'units', 'user');
```

Update ETL to load from database (future enhancement).

---

### Batch Processing

For many datasets, process in parallel:

```bash
# Terminal 1
python populate_measurements_v2.py --dataset "Dataset1"

# Terminal 2  
python populate_measurements_v2.py --dataset "Dataset2"
```

---

### Incremental Updates

For datasets updated regularly:

```bash
# Download latest version
wget https://thredds.aodn.org.au/.../latest.nc

# Ingest only new data
python populate_measurements_v2.py --dataset "IMOS_Coastal_Moorings" --since "2023-12-01"
```

(Note: `--since` flag not yet implemented; manually filter in ETL script)

---

## Summary Checklist

- [ ] Database initialized (`init.sql` run successfully)
- [ ] Python dependencies installed
- [ ] AODN data downloaded to `AODN_data/`
- [ ] Metadata table populated
- [ ] Diagnostic run (`diagnostic_etl.py`)
- [ ] Measurements ingested (`populate_measurements_v2.py`)
- [ ] Spatial features ingested (`populate_spatial.py`)
- [ ] Biological data ingested (`populate_biological.py`)
- [ ] Verification queries run
- [ ] Data counts match expectations

---

## Next Steps

1. **Set up Grafana dashboards** → Visualize time-series data
2. **Spatial analysis in QGIS** → Load spatial features via PostGIS
3. **Custom queries** → Analyze species-environment relationships
4. **Automate downloads** → Schedule weekly AODN updates

---

## References

- **AODN Portal**: https://portal.aodn.org.au/
- **IMOS THREDDS**: https://thredds.aodn.org.au/
- **BODC Parameters**: https://www.bodc.ac.uk/data/parameters/
- **CF Conventions**: https://cfconventions.org/
- **ISO 19115 Metadata**: https://www.iso.org/standard/53798.html

---

**Questions?** See `ETL_GUIDE.md` for detailed ETL troubleshooting or check code docstrings.
