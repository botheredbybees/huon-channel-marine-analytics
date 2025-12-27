# populate_measurements.py - Detailed Documentation

## Overview

`populate_measurements.py` is the core ETL script that extracts oceanographic measurements from CSV and NetCDF files, standardizes parameters, validates locations, and loads data into the `measurements` table. This is the most complex script in the pipeline, integrating parameter mapping, time parsing, spatial validation, **multi-parameter row extraction**, and **3D gridded NetCDF support**.

**Script Version:** 3.1 (3D Gridded NetCDF + QC Filtering)  
**Dependencies:** `psycopg2`, `pandas`, `numpy`, `netCDF4`, `cftime`  
**Estimated Runtime:** 5-30 minutes per dataset (varies by file size)

---

## Key Features

### v3.1 Enhancements (December 2025)

- **3D Gridded NetCDF Extraction** - Extract from satellite/model data (time×lat×lon grids)
- **Spatial Bounding Box Filtering** - Only ingest grid cells within study area
- **Grid Cell Location Records** - Each grid cell gets a unique location
- **QC Column Filtering** - Quality control columns (e.g., `TEMP_QUALITY_CONTROL`) are **NOT** stored as separate measurements
- **Bad Data Removal** - Measurements with QC flag = 4 are excluded during ETL

### v3.0 Features (Preserved)

- **Multi-Parameter CSV Extraction** - One row with TEMP, SALINITY, PH → Multiple measurement records
- **Intelligent Parameter Detection** - Auto-detects ~25 common marine parameters by column name
- **Unit Inference** - Extracts units from column names (e.g., TEMP_C → degrees_celsius)
- **Wide & Long Format Support** - Handles both parameter-as-columns and parameter-as-rows
- **Improved IMOS/AODN Compatibility** - Built-in support for common oceanographic column patterns

### v2.1 Features (Preserved)

- **Integrated Location Patching** - Coordinate validation and station lookup
- **NetCDF Time Parsing** - Returns `datetime` objects (not tuples)
- **Parameter Mapping** - Loads from `parameter_mappings` table
- **Location Extraction** - Reads station info from CSV/NetCDF headers
- **Audit Trail** - `location_qc_flag`, `extracted_at`

### Guardrails

✓ **Upsert-Safe** - `INSERT ... ON CONFLICT DO NOTHING`  
✓ **Audit Trail** - QC flags track all modifications  
✓ **Schema Validation** - Type checking before database write  
✓ **Error Recovery** - Failed rows skipped with logging, no transaction rollback
✓ **QC Column Filtering** - Quality control columns excluded from measurements table
✓ **Bad Data Removal** - Measurements with QC flag = 4 (bad) are not ingested

---

## Quality Control (QC) Flag Handling

### Important: QC Columns Are NOT Stored

**IMOS datasets contain quality control columns** (e.g., `TEMP_QUALITY_CONTROL`, `PSAL_QUALITY_CONTROL`) with QC flag values:

| QC Flag | Meaning |
|---------|----------|
| 1 | Good data (passed all QC tests) |
| 2 | Probably good data |
| 3 | Probably bad data (failed some QC tests) |
| 4 | Bad data (failed critical QC tests) |
| 9 | Missing data |

### ETL Behavior (v3.1+)

**QC columns are filtered during extraction:**
```python
# In detect_parameter_columns()
if any(skip in col_clean for skip in [..., 'QUALITY_CONTROL', 'QC', '_FLAG']):
    continue  # Skip QC columns
```

**This means:**
- ✅ `TEMP` measurements are extracted and stored
- ❌ `TEMP_QUALITY_CONTROL` columns are **NOT** stored as separate measurements
- ✅ Only quality-controlled data (QC flag 1-2) is ingested
- ❌ Bad measurements (QC flag = 4) are excluded before ingestion

### Historical Cleanup (December 2025)

**Pre-v3.1 versions stored QC columns as measurements**. A one-time cleanup was performed:

1. **Deleted 8,276,395 QC flag records** (42.5% of original database)
2. **Deleted 54,325 bad wave measurements** (where QC flag = 4)
3. **Final clean database:** 12,028,987 quality-controlled measurements

See `init.sql` header comments for details.

---

## What's New in v3.1

### 3D Gridded NetCDF Extraction

**Problem:** Satellite and model data (e.g., SST, chlorophyll) are stored as 3D grids (time × latitude × longitude) with thousands of grid cells. Previous versions couldn't extract these.

**Solution:** v3.1 adds `_extract_3d_grid()` method with:
- Grid cell iteration
- Spatial bounding box filtering (only extract cells within study area)
- Automatic location creation for each grid cell
- Fill value handling (`NaN`, `-999`, `-1e34`)

**Example:**
```python
# Ocean Acidification dataset
n_time=1716, n_lat=1, n_lon=1 (single point)
→ Extracts 7 parameters × 1,716 timesteps = ~12,000 measurements

# SST GAMSSA dataset
n_time=6213, n_lat=3, n_lon=4 (small regional grid)
→ Filters to 4 grid cells within study area
→ Extracts 1 parameter × 6,213 timesteps × 4 cells = ~25,000 measurements
```

### Spatial Bounding Box (Huon Estuary)

```python
STUDY_AREA = {
    'lat_min': -43.558,
    'lat_max': -42.777,
    'lon_min': 146.844,
    'lon_max': 147.783
}
```

**Prevents data explosion:** Without filtering, MODIS chlorophyll would create **67 million measurements** from a global grid. With filtering, only ~200k relevant cells are extracted.

---

## What's New in v3.0

### Multi-Parameter Extraction Example

**Input CSV (Wide Format):**
```csv
SAMPLE_DATE,LATITUDE,LONGITUDE,TEMP_C,SALINITY_PSU,PH,DO_MG_L,TURBIDITY_NTU
2020-06-15T10:00:00,-42.5981,148.2317,15.2,35.1,8.1,7.5,2.3
```

**Output (5 Measurement Records):**
```python
[
  {time: '2020-06-15T10:00:00', parameter_code: 'TEMPERATURE', value: 15.2, uom: 'degrees_celsius'},
  {time: '2020-06-15T10:00:00', parameter_code: 'SALINITY', value: 35.1, uom: 'PSU'},
  {time: '2020-06-15T10:00:00', parameter_code: 'PH', value: 8.1, uom: 'pH'},
  {time: '2020-06-15T10:00:00', parameter_code: 'DISSOLVED_OXYGEN', value: 7.5, uom: 'mg/L'},
  {time: '2020-06-15T10:00:00', parameter_code: 'TURBIDITY', value: 2.3, uom: 'NTU'}
]
```

### Supported Parameter Keywords

The script auto-detects parameters using these keywords:

| Parameter | Column Keywords | Example Columns |
|-----------|----------------|----------------|
| **Temperature** | temp, temperature, sst, sbt, t_deg | TEMP_C, SURFACE_TEMPERATURE, SST |
| **Salinity** | sal, salinity, psal | SALINITY_PSU, PSAL, SAL |
| **Dissolved Oxygen** | do, oxygen, doxy, o2 | DO_MG_L, DISSOLVED_OXYGEN, O2_SAT |
| **Nitrate** | no3, nitrate, nox | NITRATE, NO3, NOX_UMOL |
| **Chlorophyll-a** | chl_a, chla, chlorophyll | CHL_A, CHLOROPHYLL_A |
| **Turbidity** | turb, turbidity, ntu | TURBIDITY_NTU, TURB |
| **pH** | ph | PH, PH_INSITU |

**QC columns are automatically excluded:**
- `TEMP_QUALITY_CONTROL` → Skipped
- `PSAL_QC` → Skipped
- `DO_FLAG` → Skipped

---

## Architecture

### Data Flow

```
AODN_data/Dataset/
  ├── water_quality.csv (wide format: multiple param columns)
  ├── ctd_profile.nc (2D: time×depth)
  └── satellite_sst.nc (3D: time×lat×lon)
       ↓
  [File Type Detection]
       ↓
  [Location Extraction] → locations table
       ↓
  [Coordinate Patching]
       ↓
  [Parameter Detection] ← Keyword matching
       │                  ← QC column filtering (NEW v3.1)
       ↓
  [Unit Inference] ← Regex patterns
       ↓
  [Multi-Parameter Extraction] ← 1 row → N records
       │  OR
  [3D Grid Extraction] ← Spatial filtering (NEW v3.1)
       ↓
  [Parameter Mapping] ← parameter_mappings table
       ↓
  [Time Parsing]
       ↓
  [Batch Insertion]
       ↓
  measurements table (QC-filtered data only)
```

### Core Components

1. **ParameterMapping** - Standardizes parameter names using database
2. **TimeFormatDetector** - Auto-detects time formats (ISO, numeric, epoch)
3. **CSVMeasurementExtractor** - Multi-parameter row extraction + QC filtering
4. **NetCDFMeasurementExtractor** - Extracts from 1D/2D/3D NetCDF files
5. **Location Patchers** - Validates and fixes coordinates
6. **MeasurementBatchInserter** - Bulk database writes

---

## Function Reference

### NEW (v3.1): 3D Grid Extraction

#### `_extract_3d_grid(data, time_data, time_attrs, lat_data, lon_data, ...)`

**Purpose:** Extract measurements from 3D gridded NetCDF data (time×lat×lon).

**Parameters:**
- `data` (np.ndarray): 3D array of measurement values
- `time_data` (np.ndarray): Time coordinate values
- `time_attrs` (dict): Time variable attributes (units, calendar)
- `lat_data` (np.ndarray): Latitude coordinates
- `lon_data` (np.ndarray): Longitude coordinates
- `param_code`, `namespace`, `uom`: Parameter metadata
- `metadata` (dict): Dataset metadata
- `limit` (int): Max measurements to extract

**Returns:** List of measurement dicts

**Logic:**
1. Filter grid cells to study area bounding box
2. For each valid grid cell:
   - Create/get location record (cached)
   - Extract timeseries for that cell
   - Skip fill values (NaN, -999, -1e34)
3. Batch insert measurements

**Example:**
```python
# MODIS chlorophyll: 8,052 timesteps × 85 lats × 98 lons
# Full grid: 67M potential measurements
# After spatial filter: 7,332 cells within study area
# Extracted: ~200k measurements (valid data only)
```

---

### Parameter Detection (v3.0)

#### `detect_parameter_columns(df: pd.DataFrame)`

**Purpose:** Detect which columns contain measurement parameters (vs metadata columns).

**Parameters:**
- `df` (pd.DataFrame): CSV data with headers

**Returns:** Dict `{column_name: standard_parameter_name}`

**Logic:**
```python
# Skip known metadata columns
SKIP = ['FID', 'ID', 'DATE', 'TIME', 'LATITUDE', 'LONGITUDE', 
        'STATION', 'SITE', 'TRIP', 'LOCATION', 'GEOM',
        'QUALITY_CONTROL', 'QC', '_FLAG']  # ← QC filtering added v3.1

# Match parameter keywords
for col in df.columns:
    if col_upper not in SKIP:
        if 'TEMP' in col_upper:
            params[col] = 'temperature'
        elif 'SAL' in col_upper:
            params[col] = 'salinity'
        # ... etc for 25+ parameters
```

**Example:**
```python
df = pd.read_csv('water_quality.csv')
detect_parameter_columns(df)
# Returns:
{
    'TEMP_C': 'temperature',
    'SALINITY_PSU': 'salinity',
    'PH': 'ph',
    'DO_MG_L': 'dissolved_oxygen',
    'TURBIDITY_NTU': 'turbidity'
    # 'TEMP_QUALITY_CONTROL' is excluded ✓
}
```

---

## Database Schema

### `measurements` Table

```sql
CREATE TABLE measurements (
    data_id BIGSERIAL,
    time TIMESTAMPTZ NOT NULL,
    uuid TEXT NOT NULL,
    parameter_code TEXT NOT NULL,
    namespace TEXT DEFAULT 'custom',
    value DOUBLE PRECISION NOT NULL,
    uom TEXT,
    uncertainty DOUBLE PRECISION,
    depth_m NUMERIC,
    metadata_id INTEGER REFERENCES metadata(id),
    quality_flag SMALLINT DEFAULT 1,  -- Only values 1-2 stored
    location_id BIGINT REFERENCES locations(id),
    PRIMARY KEY (time, data_id)
);
```

**Important Notes:**
- **`quality_flag`** stores the measurement quality (1=good, 2=probably good)
- **QC columns are NOT stored** (e.g., no `TEMP_QUALITY_CONTROL` records)
- **Bad data (QC=4) is excluded** before insertion

---

## Performance Impact of v3.1

### Gridded NetCDF Datasets

| Dataset | Grid Size | Study Area Cells | Timesteps | Measurements |
|---------|-----------|------------------|-----------|-------------|
| Ocean Acidification | 1×1 | 1 | 1,716 | ~12k |
| SST GAMSSA | 3×4 | 4 | 6,213 | ~25k |
| MODIS Chlorophyll | 85×98 | 7,332 | 8,052 | ~59M* |

*MODIS was **not extracted** due to memory constraints. Consider spatial aggregation or temporal subsampling.

### QC Column Filtering Impact

**Before v3.1:**
- 19,301,185 total records
- 8,276,395 QC column records (42.5%)
- Database size: ~1.2 GB

**After v3.1 + Cleanup:**
- 12,028,987 quality-controlled measurements
- 0 QC column records ✓
- Database size: ~700 MB
- **37.7% reduction in database size**

---

## Migration Guide (v3.0 → v3.1)

### Breaking Changes

**None** - v3.1 is fully backward compatible.

### New Capabilities

1. **Gridded NetCDF datasets** now extractable (SST, ocean acidification)
2. **QC columns automatically filtered** (no manual cleanup needed)
3. **Bad data excluded** before insertion (QC flag = 4)

### Recommended Actions After Upgrade

1. **Run one-time QC cleanup** (if upgrading from pre-v3.1):
   ```sql
   -- Delete bad wave measurements (QC flag = 4)
   DELETE FROM measurements m
   USING measurements qc
   WHERE qc.parameter_code LIKE '%_QUALITY_CONTROL'
     AND m.parameter_code = REPLACE(qc.parameter_code, '_QUALITY_CONTROL', '')
     AND m.time = qc.time
     AND m.location_id = qc.location_id
     AND qc.value = 4;
   
   -- Delete all QC columns
   DELETE FROM measurements
   WHERE parameter_code LIKE '%QUALITY_CONTROL%'
      OR parameter_code LIKE '%_QC%'
      OR parameter_code LIKE '%_FLAG%';
   
   -- Reclaim space
   VACUUM FULL measurements;
   ```

2. **Re-extract gridded datasets:**
   ```bash
   python populate_measurements.py --dataset "Ocean acidification"
   python populate_measurements.py --dataset "GAMSSA"
   ```

3. **Verify no QC columns remain:**
   ```sql
   SELECT parameter_code, COUNT(*) 
   FROM measurements 
   WHERE parameter_code LIKE '%QUALITY%' 
      OR parameter_code LIKE '%_QC%'
   GROUP BY parameter_code;
   -- Should return 0 rows
   ```

---

## Troubleshooting

### QC Columns Still Being Extracted

**Symptom:**
```sql
SELECT parameter_code FROM measurements WHERE parameter_code LIKE '%QUALITY%';
-- Returns rows
```

**Solution:** Ensure you're running v3.1+:
```bash
grep "v3.1" populate_measurements.py
# Should show: Script Version: 3.1
```

### Memory Error on Large Gridded Datasets

**Symptom:**
```
python populate_measurements.py --dataset "MODIS"
Killed
```

**Cause:** Dataset has too many grid cells (7,332 cells × 8,052 timesteps = 59M measurements)

**Solutions:**
1. Skip satellite chlorophyll (use in-situ data instead)
2. Implement streaming insertion (process one grid cell at a time)
3. Increase swap space or available RAM

---

## References

- [Project README](../README.md)
- [Database Schema Documentation](database_schema.md)
- [ETL Guide](ETL_GUIDE.md)
- [init.sql - QC Cleanup Notes](../init.sql)
- [IMOS Quality Control Procedures](http://imos.org.au/quality.html)
- [CF Conventions](http://cfconventions.org/)

---

*Last Updated: December 27, 2025*  
*Script Version: 3.1*  
*Maintained by: Huon Channel Marine Analytics Project*