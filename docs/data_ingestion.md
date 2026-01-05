# Data Ingestion Guide

Complete guide to downloading AODN/IMOS datasets and ingesting them into your marine analytics database.

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Downloading Data from AODN](#downloading-data-from-aodn)
4. [Metadata Extraction](#metadata-extraction)
5. [Data Ingestion Pipeline](#data-ingestion-pipeline)
6. [Parameter Coverage Analysis](#parameter-coverage-analysis)
7. [Verification](#verification)
8. [Troubleshooting](#troubleshooting)

---

## Overview

### Data Flow Architecture

```
AODN Portal              AODN_data/           Database
    │                        │                     │
    ├─ Download    →       ├─ Dataset1/          ├─ metadata
    ├─ Extract               │  ├─ *.csv            ├─ parameters
    ├─ Store                 │  ├─ *.nc             ├─ parameter_mappings
    └─ Metadata.xml          │  └─ metadata.xml    ├─ measurements
                             │                      ├─ spatial_features
                             ├─ Dataset2/          └─ species_observations
                             └─ ...
                                 │
                          ETL Scripts
                          ├─ populate_metadata.py
                          ├─ populate_parameter_mappings.py
                          ├─ populate_parameters_from_measurements.py
                          ├─ populate_measurements.py
                          ├─ populate_spatial.py
                          └─ populate_biological.py
```

### Data Types Supported

| Type | File Format | ETL Script | Table Destination |
|------|-------------|------------|-------------------|
| **Time-series** | CSV, NetCDF | `populate_measurements.py` | `measurements` |
| **Spatial** | Shapefiles, GPX | `populate_spatial.py` | `spatial_features` |
| **Biological** | CSV (species counts) | `populate_biological.py` | `species_observations` |
| **Metadata** | XML (ISO 19115) | `populate_metadata.py` | `metadata` |
| **Parameters** | Inferred from data | `populate_parameters_from_measurements.py` | `parameters` |

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
- `parameters`
- `parameter_mappings`
- `measurements`
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

Use `populate_metadata.py` to automatically extract metadata from XML files:

```bash
python populate_metadata.py
```

This script extracts 30+ metadata fields including:
- Dataset UUID and parent relationships
- Spatial/temporal extents
- Distribution URLs (WFS, WMS, Portal)
- Data lineage and credits
- License information

For each dataset folder, the script:
1. Locates `metadata.xml` file
2. Parses ISO 19115-3 XML structure
3. Extracts comprehensive metadata
4. Inserts into `metadata` table

---

## Data Ingestion Pipeline

### Pipeline Overview

```
1. Run diagnostic         → Identify file formats and issues
2. Populate metadata      → Extract from XML files
3. Populate parameter mappings → Load standardized mappings
4. Populate parameters    → Create parameter records from measurements
5. Ingest measurements    → Time-series CSV/NetCDF data
6. Ingest spatial         → Shapefiles for seagrass, kelp
7. Ingest biological      → Species observation CSVs
8. Analyze coverage       → Check parameter coverage statistics
9. Verify                 → Check counts and data quality
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

Extract metadata from XML files:

```bash
python populate_metadata.py
```

This automatically:
- Scans `AODN_data/` directory
- Finds `metadata.xml` files
- Parses ISO 19115-3 XML
- Extracts 30+ metadata fields
- Populates `metadata` table

**Expected Output:**
```
✓ 38 datasets discovered
✓ 30+ fields extracted per dataset
✓ 26/38 datasets with parent_uuid (68%)
✓ 38/38 with metadata_revision_date (100%)
✓ 32/38 with WFS URLs (84%)
```

---

### Step 3: Populate Parameter Mappings

Load standardized parameter mappings from configuration:

```bash
python populate_parameter_mappings.py
```

This script:
- Reads `config_parameter_mapping.json`
- Maps raw parameter names to standard codes (BODC, CF)
- Defines units and namespaces
- Populates `parameter_mappings` table
- Creates indexed lookup for fast ETL operations

**Why this matters:** Enables consistent parameter standardization across 80+ different parameter naming conventions found in AODN datasets.

---

### Step 4: Populate Parameters Table ✨ NEW

After measurements are loaded, populate the parameters table:

```bash
python scripts/populate_parameters_from_measurements.py
```

This script:
- Extracts unique parameter codes from measurements
- Generates human-readable labels
- Infers units from parameter codes
- Creates parameter records with proper UUIDs
- Handles NULL metadata_id correctly

**Key Features:**
- Generates UUID for each parameter
- Uses `IS NULL` for proper NULL comparison in SQL
- Idempotent (safe to run multiple times)
- Links parameters to parameter_mappings when available

**Expected Output:**
```
Found 70 unique parameter codes
Inserted 70 parameters
✓ All parameter codes have corresponding parameter records
```

**Fixes Applied (v2):**
- Uses `metadata_id IS NULL` instead of `= NULL`
- Explicitly sets `metadata_id = NULL` in INSERT
- Proper handling of UNIQUE constraint on (parameter_code, metadata_id)

---

### Step 5: Ingest Measurements (Time-Series Data)

Use **`populate_measurements.py`** for CSV and NetCDF files:

```bash
# Full ingestion
python populate_measurements.py

# Single dataset (testing)
python populate_measurements.py --dataset "Chlorophyll"

# Limit rows for quick test
python populate_measurements.py --limit 100
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

---

### Step 6: Ingest Spatial Features (Shapefiles)

Use **`populate_spatial.py`** for seagrass, kelp extent, and other polygons:

```bash
python populate_spatial.py
```

#### Requirements

- **geopandas**: For shapefile reading
- **ogr2ogr**: System tool (install via `gdal-bin` on Ubuntu)

---

### Step 7: Ingest Biological Data (Species Observations)

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

---

## Parameter Coverage Analysis

### Analyzing Parameter Coverage ✨ NEW

After ingesting data, analyze which parameters have measurements:

```bash
python scripts/analyze_parameter_coverage.py
```

This comprehensive analysis script generates three CSV reports:

1. **parameter_coverage_YYYYMMDD_HHMMSS.csv**
   - All parameters from metadata
   - Measurement counts per parameter
   - Coverage status (measured/unmeasured)

2. **parameter_statistics_YYYYMMDD_HHMMSS.csv**
   - Summary statistics by dataset
   - Parameter counts and percentages
   - Top measured parameters

3. **unmeasured_parameters_YYYYMMDD_HHMMSS.csv**
   - Parameters without measurements
   - Grouped by dataset
   - Categorized by content type

### Typical Coverage Statistics

**Example Output:**
```
Overall Statistics:
- Total unique parameters: 361
- Parameters with measurements: 70 (19.4%)
- Parameters without measurements: 291 (80.6%)
- Total measurements: 7,000,000+

Top Measured Parameters:
1. TEMP - 2,500,000 measurements
2. PSAL - 2,300,000 measurements
3. CPHL - 1,800,000 measurements
```

### Understanding Low Coverage

The 19.4% parameter coverage is expected because:

1. **Metadata Comprehensiveness**: XML metadata lists ALL possible parameters the dataset *could* contain
2. **Subset Filtering**: Downloaded data may be filtered by region/time
3. **Different Data Types**:
   - **Physical measurements**: Temperature, salinity → High coverage
   - **Biological observations**: Species counts → Separate table (`species_observations`)
   - **Chemical analyses**: Often not included in time-series files

### Improving Coverage

Several enhancement issues have been created to improve coverage:

- **[Issue #5](https://github.com/botheredbybees/huon-channel-marine-analytics/issues/5)**: Create ETL for biological observations
  - Would increase coverage to ~50%
  - Extract species data to `species_observations` table

- **[Issue #6](https://github.com/botheredbybees/huon-channel-marine-analytics/issues/6)**: Add data quality checks
  - Range validation for oceanographic parameters
  - Consistency checks for timestamps

- **[Issue #7](https://github.com/botheredbybees/huon-channel-marine-analytics/issues/7)**: Implement fuzzy parameter matching
  - Intelligent string matching for parameter names
  - Reduce manual mapping effort

---

## Verification

### Check Measurement Counts

```sql
-- Total measurements
SELECT COUNT(*) FROM measurements;

-- By dataset
SELECT m.uuid, COUNT(*) as count
FROM measurements meas
JOIN metadata m ON meas.metadata_id = m.id
GROUP BY m.uuid
ORDER BY count DESC;

-- By parameter
SELECT parameter_code, COUNT(*) as count
FROM measurements
GROUP BY parameter_code
ORDER BY count DESC
LIMIT 20;
```

### Check Parameter Population

```sql
-- Total parameters
SELECT COUNT(*) FROM parameters;

-- Parameters with measurements
SELECT 
    p.parameter_code,
    p.parameter_label,
    p.unit_name,
    COUNT(m.data_id) as measurement_count
FROM parameters p
LEFT JOIN measurements m ON m.parameter_code = p.parameter_code
GROUP BY p.parameter_code, p.parameter_label, p.unit_name
ORDER BY measurement_count DESC
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

-- Parameter coverage analysis
SELECT 
    COUNT(DISTINCT p.parameter_code) as total_parameters,
    COUNT(DISTINCT m.parameter_code) as measured_parameters,
    ROUND(100.0 * COUNT(DISTINCT m.parameter_code) / COUNT(DISTINCT p.parameter_code), 1) as coverage_pct
FROM parameters p
LEFT JOIN measurements m ON m.parameter_code = p.parameter_code;
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

If columns have different names, add to parameter mapping configuration.

---

### Issue: "Parameter not found in parameters table"

**Cause**: Parameters table not populated from measurements

**Solution**: Run the parameter population script:

```bash
python scripts/populate_parameters_from_measurements.py
```

This creates parameter records for all unique parameter codes found in measurements.

---

### Issue: "Duplicate key violation on parameters"

**Cause**: Re-running parameter population script

**Solution**: This is expected! The script uses `ON CONFLICT DO NOTHING` for safety. To force re-population:

```sql
-- Delete old parameters
DELETE FROM parameters WHERE metadata_id IS NULL;

-- Re-run script
python scripts/populate_parameters_from_measurements.py
```

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

## Advanced Topics

### Custom Parameter Mappings

Add new mappings to configuration file and reload:

```json
{
  "parameter_mapping": {
    "CUSTOM_PARAM": ["MY_CODE", "custom", "units"]
  }
}
```

Then reload:

```bash
python populate_parameter_mappings.py
```

---

### Batch Processing

For many datasets, process in parallel:

```bash
# Terminal 1
python populate_measurements.py --dataset "Dataset1"

# Terminal 2  
python populate_measurements.py --dataset "Dataset2"
```

---

### Incremental Updates

For datasets updated regularly:

```bash
# Download latest version
wget https://thredds.aodn.org.au/.../latest.nc

# Ingest only new data
python populate_measurements.py --dataset "IMOS_Coastal_Moorings" --since "2023-12-01"
```

(Note: `--since` flag not yet implemented; manually filter in ETL script)

---

## Summary Checklist

- [ ] Database initialized (`init.sql` run successfully)
- [ ] Python dependencies installed
- [ ] AODN data downloaded to `AODN_data/`
- [ ] Metadata table populated (`populate_metadata.py`)
- [ ] Parameter mappings loaded (`populate_parameter_mappings.py`)
- [ ] Diagnostic run (`diagnostic_etl.py`)
- [ ] Measurements ingested (`populate_measurements.py`)
- [ ] Parameters table populated (`populate_parameters_from_measurements.py`)
- [ ] Parameter coverage analyzed (`analyze_parameter_coverage.py`)
- [ ] Spatial features ingested (`populate_spatial.py`)
- [ ] Biological data ingested (`populate_biological.py`)
- [ ] Verification queries run
- [ ] Data counts match expectations

---

## Next Steps

1. **Review parameter coverage** → Run `analyze_parameter_coverage.py`
2. **Set up Grafana dashboards** → Visualize time-series data
3. **Spatial analysis in QGIS** → Load spatial features via PostGIS
4. **Custom queries** → Analyze species-environment relationships
5. **Consider enhancements** → See GitHub issues #5, #6, #7
6. **Automate downloads** → Schedule weekly AODN updates

---

## References

- **AODN Portal**: https://portal.aodn.org.au/
- **IMOS THREDDS**: https://thredds.aodn.org.au/
- **BODC Parameters**: https://www.bodc.ac.uk/data/parameters/
- **CF Conventions**: https://cfconventions.org/
- **ISO 19115 Metadata**: https://www.iso.org/standard/53798.html
- **Project Issues**: https://github.com/botheredbybees/huon-channel-marine-analytics/issues

---

**Questions?** See `ETL_GUIDE.md` for detailed ETL troubleshooting or check code docstrings.

*Last Updated: January 5, 2026*