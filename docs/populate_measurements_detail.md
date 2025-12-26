# populate_measurements.py - Detailed Documentation

## Overview

`populate_measurements.py` is the core ETL script that extracts oceanographic measurements from CSV and NetCDF files, standardizes parameters, validates locations, and loads data into the `measurements` table. This is the most complex script in the pipeline, integrating parameter mapping, time parsing, spatial validation, and **multi-parameter row extraction**.

**Script Version:** 3.0 (Multi-Parameter CSV Support)  
**Dependencies:** `psycopg2`, `pandas`, `numpy`, `netCDF4`, `cftime`  
**Estimated Runtime:** 5-30 minutes per dataset (varies by file size)

---

## Key Features

### v3.0 Enhancements (December 2025)

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
- **Zero Data Loss** - All rows logged, failures tracked

### Guardrails

✓ **Upsert-Safe** - `INSERT ... ON CONFLICT DO NOTHING`  
✓ **Audit Trail** - QC flags track all modifications  
✓ **Schema Validation** - Type checking before database write  
✓ **Error Recovery** - Failed rows skipped with logging, no transaction rollback

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
| **Nitrite** | no2, nitrite | NITRITE, NO2 |
| **Ammonia** | nh3, nh4, ammonia | AMMONIA, NH4, AMMONIUM |
| **Phosphate** | po4, phosphate, srp | SRP, PHOSPHATE, PO4 |
| **Silicate** | sio4, silicate, silica | SILICATE, SIO4 |
| **Chlorophyll-a** | chl_a, chla, chlorophyll | CHL_A, CHLOROPHYLL_A |
| **Turbidity** | turb, turbidity, ntu | TURBIDITY_NTU, TURB |
| **pH** | ph | PH, PH_INSITU |
| **Fluorescence** | fluor, fluorescence, chlf | FLUORESCENCE, CHLF |

### Unit Inference Patterns

Units are automatically extracted from column names using regex:

```python
TEMP_C → degrees_celsius
SALINITY_PSU → PSU
DO_SAT_% → percent
NITRATE_UMOL → umol/L
CHL_A_UG_L → ug/L
TURBIDITY_NTU → NTU
```

---

## Architecture

### Data Flow

```
AODN_data/Dataset/
  ├── water_quality.csv (wide format: multiple param columns)
  └── ctd_profile.nc (standard NetCDF)
       ↓
  [File Type Detection]
       ↓
  [Location Extraction] → locations table
       ↓
  [Coordinate Patching]
       ↓
  [Parameter Detection] ← Keyword matching
       ↓
  [Unit Inference] ← Regex patterns
       ↓
  [Multi-Parameter Extraction] ← 1 row → N records
       ↓
  [Parameter Mapping] ← parameter_mappings table
       ↓
  [Time Parsing]
       ↓
  [Batch Insertion]
       ↓
  measurements table
```

### Core Components

1. **ParameterMapping** - Standardizes parameter names using database
2. **TimeFormatDetector** - Auto-detects time formats (ISO, numeric, epoch)
3. **CSVMeasurementExtractor** - **NEW: Multi-parameter row extraction**
4. **NetCDFMeasurementExtractor** - Extracts from NetCDF files
5. **Location Patchers** - Validates and fixes coordinates
6. **MeasurementBatchInserter** - Bulk database writes

---

## Function Reference

### NEW: Parameter Detection

#### `detect_parameter_columns(df: pd.DataFrame)`

**Purpose:** Detect which columns contain measurement parameters (vs metadata columns).

**Parameters:**
- `df` (pd.DataFrame): CSV data with headers

**Returns:** Dict `{column_name: standard_parameter_name}`

**Logic:**
```python
# Skip known metadata columns
SKIP = ['FID', 'ID', 'DATE', 'TIME', 'LATITUDE', 'LONGITUDE', 
        'STATION', 'SITE', 'TRIP', 'LOCATION', 'GEOM']

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
}
```

---

#### `infer_unit_from_column_name(col_name: str)`

**Purpose:** Extract measurement unit from column name using regex patterns.

**Parameters:**
- `col_name` (str): Column name (e.g., "TEMP_C", "SALINITY_PSU")

**Returns:** Unit string (str)

**Patterns:**
```python
UNIT_PATTERNS = {
    r'(?i)temp.*(_c|celsius)': 'degrees_celsius',
    r'(?i)sal.*(_psu|psu)': 'PSU',
    r'(?i)(do|dissolved.*oxygen).*(_mg|mg/l)': 'mg/L',
    r'(?i)(nitrate|no3).*(_um|umol)': 'umol/L',
    r'(?i)chl.*(_a|a\b).*(_ug|ug/l)': 'ug/L',
    r'(?i)turb.*(_ntu|ntu)': 'NTU',
    r'(?i)ph': 'pH',
    # ... 20+ patterns
}
```

**Examples:**
```python
infer_unit_from_column_name('TEMP_C')
# Returns: 'degrees_celsius'

infer_unit_from_column_name('SALINITY_PSU')
# Returns: 'PSU'

infer_unit_from_column_name('DO_SAT_%')
# Returns: 'percent'

infer_unit_from_column_name('UNKNOWN_PARAM')
# Returns: 'unknown'
```

---

### ENHANCED: CSV Measurement Extractor

#### `class CSVMeasurementExtractor` (v3.0)

**Purpose:** Extracts measurements from CSV files with **multi-parameter row support**.

**Constructor:**
```python
extractor = CSVMeasurementExtractor(param_mapping)
```

##### `extract(file_path, metadata, limit=None)`

**Purpose:** Extracts measurement rows from CSV, **creating multiple records per row**.

**Parameters:**
- `file_path` (str): Path to CSV file
- `metadata` (dict): Dataset metadata (id, uuid)
- `limit` (int): Max rows to extract (optional)

**Returns:** List of measurement dictionaries

**NEW Behavior (v3.0):**

1. **Detect parameter columns** (not just value column)
2. **For each CSV row:**
   - Parse timestamp once
   - Extract depth once (if present)
   - **Loop through parameter columns**
   - Create separate measurement record for each parameter
3. **Infer units** from column names if not in mapping

**Output Schema:**
```python
# Single CSV row generates multiple measurement records:
[
    {
        'time': datetime(2020, 6, 15, 10, 30),
        'uuid': 'abc123...',
        'metadata_id': 47,
        'parameter_code': 'TEMPERATURE',
        'namespace': 'custom',
        'value': 15.2,
        'uom': 'degrees_celsius',
        'depth_m': 5.0,
        'quality_flag': 1,
        'location_id': None,
        'location_qc_flag': 'unknown'
    },
    {
        'time': datetime(2020, 6, 15, 10, 30),  # Same timestamp
        'uuid': 'abc123...',
        'metadata_id': 47,
        'parameter_code': 'SALINITY',  # Different parameter
        'namespace': 'custom',
        'value': 35.1,
        'uom': 'PSU',
        'depth_m': 5.0,
        'quality_flag': 1,
        'location_id': None,
        'location_qc_flag': 'unknown'
    },
    # ... one record per detected parameter column
]
```

**Error Handling:**
- Tries multiple encodings: `utf-8`, `latin1`, `iso-8859-1`
- Skips bad lines: `on_bad_lines='skip'`
- Skips NaN values in parameter columns
- Logs failed rows to `self.failed_count`
- Continues processing after individual row failures

---

### Location Patching Functions (Unchanged from v2.1)

#### `extract_station_info_from_file(file_path, dataset_title)`

**Purpose:** Extracts station name, latitude, longitude from data file headers.

**Parameters:**
- `file_path` (str): Path to CSV or NetCDF file
- `dataset_title` (str): Fallback station name

**Returns:** Tuple `(station_name, latitude, longitude)` or `(None, None, None)`

**Logic:**

**NetCDF Extraction:**
```python
# Station name priority
for attr in ['station_name', 'site_code', 'platform_code', 'title', 'id']:
    if hasattr(ds, attr):
        station = str(getattr(ds, attr))

# Latitude priority
for lat_name in ['LATITUDE', 'latitude', 'lat']:
    if lat_name in ds.variables:
        lat = float(ds.variables[lat_name][0])

# Longitude priority
for lon_name in ['LONGITUDE', 'longitude', 'lon']:
    if lon_name in ds.variables:
        lon = float(ds.variables[lon_name][0])
```

**CSV Extraction:**
```python
# Normalize columns to uppercase
df.columns = [c.upper().strip() for c in df.columns]

# Find LATITUDE column
lat_col = ['LATITUDE', 'LAT', 'START_LAT', 'DECIMAL_LAT']

# Find LONGITUDE column
lon_col = ['LONGITUDE', 'LON', 'LONG', 'START_LON', 'DECIMAL_LONG']

# Find STATION column (NEW: Added ESTUARY_SITE for v3.0)
station_col = ['STATION', 'SITE', 'SITE_CODE', 'STATION_NAME', 'TRIP_CODE', 'ESTUARY_SITE']
```

---

#### `patch_location_coordinates(lat, lon)`

**Purpose:** Applies location cleaning rules and validates coordinates.

**Parameters:**
- `lat` (float): Latitude value
- `lon` (float): Longitude value

**Returns:** Tuple `(patched_lat, patched_lon, qc_flag)`

**QC Flag Values:**

| Flag | Meaning |
|------|--------|
| `clean` | No modifications needed |
| `lat_sign_flipped` | Fixed positive latitude (Tasmania is southern hemisphere) |
| `lon_normalized` | Converted longitude to -180..180 range |
| `outside_tasmania` | Valid coordinates but outside Tasmania bounding box |
| `outlier_flagged` | Invalid coordinates (\|lat\| > 90 or \|lon\| > 180) |
| `missing_coordinates` | Lat or lon is None |

**Validation Rules:**

```python
# Fix positive latitudes (Tasmania should be negative)
if lat > 0 and lat < 90:
    lat = -lat
    qc_flag = 'lat_sign_flipped'

# Normalize longitude to -180..180
if lon > 180:
    lon = lon - 360
elif lon < -180:
    lon = lon + 360

# Tasmania bounds check
if not (-45 < lat < -40 and 144 < lon < 150):
    qc_flag = 'outside_tasmania'
```

---

#### `get_or_insert_location(conn, station, lat, lon)`

**Purpose:** Inserts location into `locations` table or retrieves existing ID.

**Parameters:**
- `conn` (psycopg2.connection): Database connection
- `station` (str): Station name
- `lat` (float): Latitude
- `lon` (float): Longitude

**Returns:** `location_id` (int) or `None` on failure

**SQL Logic:**
```sql
INSERT INTO locations (location_name, latitude, longitude, location_geom)
VALUES (%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
ON CONFLICT (latitude, longitude)
DO UPDATE SET location_name = EXCLUDED.location_name
RETURNING id;
```

---

### Parameter Mapping (Unchanged)

#### `class ParameterMapping`

**Purpose:** Loads and manages parameter standardization mappings.

**Constructor:**
```python
mapping = ParameterMapping(DB_CONFIG)
```

**Methods:**

##### `get_standard_param(raw_param)`

**Purpose:** Maps raw parameter name to standardized tuple.

**Parameters:**
- `raw_param` (str): Raw parameter name from data file

**Returns:** Tuple `(parameter_code, namespace, uom)`

**Behavior:**
- Checks database mappings first
- Falls back to `(raw_param, 'custom', 'unknown')` if not found
- **v3.0 Integration:** Called after parameter detection and unit inference

---

### Time Format Detection (Unchanged)

#### `class TimeFormatDetector`

**Purpose:** Automatically detects and converts various time formats.

##### `detect_and_convert(time_value)`

**Purpose:** Attempts multiple time format conversions.

**Supported Formats:**

| Format | Example | Detection Logic |
|--------|---------|----------------|
| ISO 8601 | `"2020-06-15T10:30:00"` | `pd.to_datetime()` |
| Decimal Year | `2020.5` | `1900 < val < 2100` with fractional part |
| Year Integer | `2020` | `1900 < val < 2100` without fraction |
| Months since 1900 | `1400` | `1000 < val < 2000` |
| Days since 1900 | `44000` | `40000 < val < 50000` |
| Days since 1970 | `18000` | `15000 < val < 25000` |
| Unix Timestamp | `1592216400` | `val > 1e8` |

---

### NetCDF Measurement Extractor (Unchanged)

#### `class NetCDFMeasurementExtractor`

**Purpose:** Extracts measurements from NetCDF files following CF conventions.

**Constructor:**
```python
extractor = NetCDFMeasurementExtractor(param_mapping)
```

##### `extract(file_path, metadata, limit=None)`

**Purpose:** Extracts measurement rows from NetCDF.

---

### Batch Insertion (Unchanged)

#### `class MeasurementBatchInserter`

**Purpose:** Handles bulk database insertion with batching.

**Configuration:**
```python
BATCH_SIZE = 1000  # Rows per transaction
```

**SQL:**
```sql
INSERT INTO measurements
(time, uuid, parameter_code, namespace, value, uom, 
 uncertainty, depth_m, metadata_id, quality_flag, location_id)
VALUES %s
ON CONFLICT DO NOTHING
```

---

## Main ETL Pipeline

### Command-Line Interface

```bash
python populate_measurements.py [--limit N] [--dataset "Title"]
```

**Options:**

| Flag | Description | Example |
|------|-------------|--------|
| `--limit N` | Max rows per dataset | `--limit 5000` |
| `--dataset "Title"` | Process specific dataset | `--dataset "Chlorophyll"` |

---

## Usage Examples

### Process All Empty Datasets

```bash
python populate_measurements.py
```

Processes all datasets with 0 measurements, extracting **all parameters** from each CSV row.

### Process Specific Water Quality Dataset

```bash
python populate_measurements.py --dataset "Estuarine Health in Tasmania"
```

Targets a specific multi-parameter water quality dataset.

### Test with Limited Records

```bash
python populate_measurements.py --dataset "Nearshore temperature" --limit 1000
```

Extracts max 1000 **CSV rows** (which may generate 1000-5000 measurement records depending on parameters per row).

---

## Logging

### Example Output (v3.0)

```
======================================================================
📊 Processing: Estuarine Health in Tasmania, status and indicators: water quality
======================================================================
  📍 Extracting location from: AODN_data/Estuarine_Health/.../data.csv
  ✓ Location: North West Bay - Site NTNW1 (-43.0239, 147.2709) [clean]
  ✓ Location ID: 12
  📄 Extracting CSV: water_quality.csv
  ✓ Detected 15 parameter columns: ['temperature', 'salinity', 'ph', 'dissolved_oxygen', 'turbidity']...
  ✓ Patched 2790 rows with location_id=12
  💾 Inserting 2790 measurements...
  Inserted 1000/1000 rows (total: 1000)
  Inserted 1000/1000 rows (total: 2000)
  Inserted 790/790 rows (total: 2790)

======================================================================
✅ ETL Complete
======================================================================
Total inserted:        385432
Total failed:          1247
CSV extracted:         343221 (1100 failed)
NetCDF extracted:      42211 (147 failed)
======================================================================
```

**Notice:** 186 CSV rows × 15 parameters = 2,790 measurement records

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
    quality_flag SMALLINT DEFAULT 1,
    location_id BIGINT REFERENCES locations(id),
    PRIMARY KEY (time, data_id)
);
```

---

## Performance Impact of v3.0

### Measurement Record Multiplication

| Dataset Type | CSV Rows | Params/Row | Output Records | Time |
|--------------|----------|-----------|---------------|------|
| Temperature only | 343,355 | 1-2 | ~350k | 5 min |
| Water quality (multi-param) | 186 | 15-20 | ~3k | <1 min |
| Oceanography (full profile) | 39,120 | 5-8 | ~200k | 8 min |

**Key Insight:** Multi-parameter datasets generate **more measurement records** but process **faster per CSV row** because they're typically smaller files.

---

## Migration Guide (v2.1 → v3.0)

### Breaking Changes

**None** - v3.0 is fully backward compatible:
- Single-parameter CSVs still work (detected as 1 parameter column)
- Long format CSVs (with parameter column) still work
- NetCDF extraction unchanged
- Database schema unchanged

### New Capabilities

1. **Water quality datasets** now fully supported (previously required manual parameter column)
2. **Unit inference** reduces dependency on `parameter_mappings` table
3. **IMOS/AODN column patterns** recognized automatically

### Recommended Actions

1. **Re-run on previously failed datasets:**
   ```sql
   -- Find datasets with 0 measurements
   SELECT m.title FROM metadata m
   LEFT JOIN measurements mes ON m.id = mes.metadata_id
   GROUP BY m.id HAVING COUNT(mes.data_id) = 0;
   ```

2. **Test on sample dataset:**
   ```bash
   python populate_measurements.py --dataset "Estuarine Health" --limit 10
   ```

3. **Verify parameter detection:**
   ```sql
   SELECT parameter_code, namespace, uom, COUNT(*) 
   FROM measurements 
   WHERE metadata_id = YOUR_DATASET_ID
   GROUP BY parameter_code, namespace, uom;
   ```

---

## Troubleshooting

### No Parameters Detected

**Symptom:**
```
⚠ No parameter columns detected in water_quality.csv
```

**Solutions:**
1. Check column names match keywords in `PARAMETER_KEYWORDS` dict
2. Ensure columns aren't in metadata skip list (FID, ID, TIME, etc.)
3. Add custom keywords to `PARAMETER_KEYWORDS` if needed

### Wrong Units Inferred

**Symptom:**
```sql
SELECT DISTINCT parameter_code, uom FROM measurements WHERE uom = 'unknown';
```

**Solutions:**
1. Add unit pattern to `UNIT_PATTERNS` dict
2. OR add explicit mapping to `parameter_mappings` table
3. Rerun ETL to update records

### Duplicate Measurement Records

**Symptom:**
```
INSERT conflict: (time, uuid, parameter_code, depth_m) already exists
```

**Cause:** Running ETL twice on same dataset

**Solution:** This is **expected behavior** - `ON CONFLICT DO NOTHING` silently skips duplicates.

---

## Extension Points

### Adding Custom Parameter Keywords

Edit `PARAMETER_KEYWORDS` dict:

```python
PARAMETER_KEYWORDS = {
    # ... existing keywords ...
    'custom_param': ['my_param', 'alt_param_name'],
}
```

### Adding Custom Unit Patterns

Edit `UNIT_PATTERNS` dict:

```python
UNIT_PATTERNS = {
    # ... existing patterns ...
    r'(?i)my_param.*(_custom|custom)': 'custom_unit',
}
```

### Disabling Multi-Parameter Extraction

If you need to revert to v2.1 behavior (single value column):

```python
# In CSVMeasurementExtractor.extract()
# Replace:
param_cols = detect_parameter_columns(df)

# With:
value_col = self._find_column(df, cols_upper, ['VALUE', 'CONCENTRATION'])
param_cols = {value_col: 'unknown_parameter'}
```

---

## Testing

### Unit Test Example (v3.0)

```python
import unittest
from populate_measurements import detect_parameter_columns, infer_unit_from_column_name

class TestMultiParameterExtraction(unittest.TestCase):
    def test_parameter_detection(self):
        df = pd.DataFrame(columns=['TIME', 'TEMP_C', 'SALINITY_PSU', 'LATITUDE'])
        params = detect_parameter_columns(df)
        self.assertIn('TEMP_C', params)
        self.assertIn('SALINITY_PSU', params)
        self.assertNotIn('LATITUDE', params)  # Metadata column
    
    def test_unit_inference(self):
        self.assertEqual(infer_unit_from_column_name('TEMP_C'), 'degrees_celsius')
        self.assertEqual(infer_unit_from_column_name('SALINITY_PSU'), 'PSU')
        self.assertEqual(infer_unit_from_column_name('UNKNOWN'), 'unknown')
```

---

## Integration with ETL Pipeline

### Pipeline Order

1. populate_metadata.py
2. populate_parameter_mappings.py
3. **populate_measurements.py** ← You are here (v3.0)
4. populate_spatial.py
5. populate_biological.py

### Data Dependencies

**Requires:**
- `metadata.id` - From populate_metadata.py
- `metadata.dataset_path` - Dataset file location
- `parameter_mappings` table - Parameter standardization (optional with v3.0)

**Produces:**
- `measurements.data_id` - Primary key for time series data
- `locations.id` - Spatial reference for measurements

**Consumed By:**
- Query API - Time series queries
- Visualization dashboards - Plotting and mapping
- Quality control scripts - Data validation

---

## References

- [Project README](../README.md)
- [Database Schema Documentation](database_schema.md)
- [ETL Guide](ETL_GUIDE.md)
- [Parameter Mapping Documentation](populate_parameter_mappings_detail.md)
- [CF Conventions](http://cfconventions.org/)
- [NetCDF4-Python Documentation](https://unidata.github.io/netcdf4-python/)
- [cftime Documentation](https://unidata.github.io/cftime/)

---

*Last Updated: December 27, 2025*  
*Script Version: 3.0*  
*Maintained by: Huon Channel Marine Analytics Project*