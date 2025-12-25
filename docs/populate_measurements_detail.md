# populate_measurements.py - Detailed Documentation

## Overview

`populate_measurements.py` is the core ETL script that extracts oceanographic measurements from CSV and NetCDF files, standardizes parameters, validates locations, and loads data into the `measurements` table. This is the most complex script in the pipeline, integrating parameter mapping, time parsing, and spatial validation.

**Script Version:** 2.1 (Enhanced with integrated location patching v4)  
**Dependencies:** `psycopg2`, `pandas`, `numpy`, `netCDF4`, `cftime`  
**Estimated Runtime:** 5-30 minutes per dataset (varies by file size)

---

## Key Features

### v2.1 Enhancements

- **Integrated Location Patching** - Merged patch_locations_v4.py logic directly into ETL
- **NetCDF Time Parsing** - Returns `datetime` objects (not tuples)
- **Parameter Mapping** - Loads from `parameter_mappings` table
- **Location Extraction** - Reads station info from CSV/NetCDF headers
- **Coordinate Validation** - Fixes hemisphere sign errors, normalizes longitude
- **Audit Trail** - `location_qc_flag`, `location_patch_flags`, `extracted_at`
- **Zero Data Loss** - Raw row JSON preserves 100% source data (future feature)

### Guardrails

✓ **Upsert-Safe** - `INSERT ... ON CONFLICT DO NOTHING`  
✓ **Audit Trail** - QC flags track all modifications  
✓ **Schema Validation** - Type checking before database write  
✓ **Error Recovery** - Failed rows skipped with logging, no transaction rollback

---

## Architecture

### Data Flow

```
AODN_data/Dataset/
  ├── data.csv
  └── data.nc
       ↓
  [File Type Detection]
       ↓
  [Location Extraction] → locations table
       ↓
  [Coordinate Patching]
       ↓
  [Parameter Mapping] ← parameter_mappings table
       ↓
  [Time Parsing]
       ↓
  [Measurement Extraction]
       ↓
  [Batch Insertion]
       ↓
  measurements table
```

### Core Components

1. **ParameterMapping** - Standardizes parameter names using database
2. **TimeFormatDetector** - Auto-detects time formats (ISO, numeric, epoch)
3. **CSVMeasurementExtractor** - Extracts from CSV files
4. **NetCDFMeasurementExtractor** - Extracts from NetCDF files
5. **Location Patchers** - Validates and fixes coordinates
6. **MeasurementBatchInserter** - Bulk database writes

---

## Function Reference

### Location Patching Functions

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

# Find STATION column
station_col = ['STATION', 'SITE', 'SITE_CODE', 'STATION_NAME', 'TRIP_CODE']
```

**Example Output:**
```python
extract_station_info_from_file("AODN_data/Dataset/data.nc", "Huon Dataset")
# Returns: ('Maria Island', -42.6, 148.2)
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
| `outlier_flagged` | Invalid coordinates (|lat| > 90 or |lon| > 180) |
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

**Examples:**
```python
patch_location_coordinates(42.6, 148.2)
# Returns: (-42.6, 148.2, 'lat_sign_flipped')

patch_location_coordinates(-42.6, 350.2)
# Returns: (-42.6, -9.8, 'lon_normalized')

patch_location_coordinates(-42.6, 148.2)
# Returns: (-42.6, 148.2, 'clean')
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

**Behavior:**
- Creates PostGIS geometry point (SRID 4326 = WGS84)
- Updates location name if coordinates already exist
- Returns location ID for foreign key linkage

---

### Parameter Mapping

#### `class ParameterMapping`

**Purpose:** Loads and manages parameter standardization mappings.

**Constructor:**
```python
mapping = ParameterMapping(DB_CONFIG)
```

**Methods:**

##### `load_from_database(db_config)`

**Purpose:** Loads parameter mappings from `parameter_mappings` table.

**Database Query:**
```sql
SELECT raw_parameter_name, standard_code, namespace, unit
FROM parameter_mappings
```

**Internal Structure:**
```python
self.mapping = {
    'TEMP': ('TEMP', 'CF', 'degree_C'),
    'PSAL': ('PSAL', 'CF', 'PSU'),
    'DOX2': ('DOX2', 'IMOS', 'umol/L'),
    # ... loaded from database
}
```

##### `get_standard_param(raw_param)`

**Purpose:** Maps raw parameter name to standardized tuple.

**Parameters:**
- `raw_param` (str): Raw parameter name from data file

**Returns:** Tuple `(parameter_code, namespace, uom)`

**Examples:**
```python
mapping.get_standard_param('TEMP')
# Returns: ('TEMP', 'CF', 'degree_C')

mapping.get_standard_param('Salinity')
# Returns: ('PSAL', 'CF', 'PSU')

mapping.get_standard_param('UnknownParam')
# Returns: ('UNKNOWNPARAM', 'custom', 'unknown')
```

---

### Time Format Detection

#### `class TimeFormatDetector`

**Purpose:** Automatically detects and converts various time formats.

##### `detect_and_convert(time_value)`

**Purpose:** Attempts multiple time format conversions.

**Parameters:**
- `time_value`: Single value (string, numeric, or datetime)

**Returns:** `datetime` object or `None`

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

**Example Conversions:**
```python
TimeFormatDetector.detect_and_convert("2020-06-15T10:30:00")
# Returns: datetime(2020, 6, 15, 10, 30, 0)

TimeFormatDetector.detect_and_convert(2020.5)
# Returns: datetime(2020, 7, 2)  # Mid-year

TimeFormatDetector.detect_and_convert(1592216400)  # Unix timestamp
# Returns: datetime(2020, 6, 15, 10, 0, 0)
```

---

### CSV Measurement Extractor

#### `class CSVMeasurementExtractor`

**Purpose:** Extracts measurements from CSV files.

**Constructor:**
```python
extractor = CSVMeasurementExtractor(param_mapping)
```

##### `extract(file_path, metadata, limit=None)`

**Purpose:** Extracts measurement rows from CSV.

**Parameters:**
- `file_path` (str): Path to CSV file
- `metadata` (dict): Dataset metadata (id, uuid)
- `limit` (int): Max rows to extract (optional)

**Returns:** List of measurement dictionaries

**Column Detection:**

```python
# Time column keywords (case-insensitive)
time_col = ['time', 'date', 'datetime', 'timestamp']

# Value column keywords
value_col = ['value', 'concentration', 'measurement', 'result']

# Parameter column keywords
param_col = ['parameter', 'variable', 'code']

# Depth column keywords
depth_col = ['depth', 'z', 'level']
```

**Output Schema:**
```python
{
    'time': datetime(2020, 6, 15, 10, 30),
    'uuid': 'abc123...',
    'metadata_id': 47,
    'parameter_code': 'TEMP',
    'namespace': 'CF',
    'value': 15.2,
    'uom': 'degree_C',
    'depth_m': 5.0,
    'quality_flag': 1,
    'location_id': None,  # Patched later
    'location_qc_flag': 'unknown'
}
```

**Error Handling:**
- Tries multiple encodings: `utf-8`, `latin1`, `iso-8859-1`
- Skips bad lines: `on_bad_lines='skip'`
- Logs failed rows to `self.failed_count`
- Continues processing after individual row failures

---

### NetCDF Measurement Extractor

#### `class NetCDFMeasurementExtractor`

**Purpose:** Extracts measurements from NetCDF files following CF conventions.

**Constructor:**
```python
extractor = NetCDFMeasurementExtractor(param_mapping)
```

##### `extract(file_path, metadata, limit=None)`

**Purpose:** Extracts measurement rows from NetCDF.

**Parameters:**
- `file_path` (str): Path to NetCDF file
- `metadata` (dict): Dataset metadata
- `limit` (int): Max rows to extract (optional)

**Returns:** List of measurement dictionaries

**Time Variable Detection:**
```python
time_vars = ['time', 'TIME', 'Time', 'datetime', 'DATETIME']
```

**Data Variable Filtering:**
```python
# Skip dimension variables
if var_name in ds.dimensions:
    continue

# Skip coordinate variables
if hasattr(var, 'axis'):
    continue

# Skip scalar variables
if len(var.dimensions) == 0:
    continue
```

**Dimensionality Handling:**

**1D Arrays (time series):**
```python
for t_idx in range(len(data)):
    ts = parse_netcdf_time(time_data[t_idx], time_attrs)
    value = float(data[t_idx])
    # Create measurement row
```

**2D Arrays (time × depth/station):**
```python
for t_idx in range(data.shape[0]):
    ts = parse_netcdf_time(time_data[t_idx], time_attrs)
    for s_idx in range(data.shape[1]):
        value = float(data[t_idx, s_idx])
        # Create measurement row
```

---

##### `_parse_netcdf_time(time_value, attrs)`

**Purpose:** Parse NetCDF time using CF units and calendar attributes.

**Parameters:**
- `time_value` (numeric): Time value from NetCDF variable
- `attrs` (dict): Variable attributes containing `units` and `calendar`

**Returns:** `datetime` object (NOT tuple)

**CF Time Parsing:**
```python
import cftime

units = attrs.get('units', '')  # e.g., "days since 1900-01-01"
calendar = attrs.get('calendar', 'standard')  # gregorian, proleptic_gregorian, etc.

cf_time = cftime.num2date(time_value, units, calendar=calendar)

# Convert cftime.DatetimeGregorian to datetime
if hasattr(cf_time, 'timetuple'):
    tt = cf_time.timetuple()
    return datetime(tt.tm_year, tt.tm_mon, tt.tm_mday,
                    tt.tm_hour, tt.tm_min, tt.tm_sec)
```

**Fallback:** If `cftime` fails, uses `TimeFormatDetector.detect_and_convert()`

---

### Batch Insertion

#### `class MeasurementBatchInserter`

**Purpose:** Handles bulk database insertion with batching.

**Constructor:**
```python
inserter = MeasurementBatchInserter(DB_CONFIG)
```

**Configuration:**
```python
BATCH_SIZE = 1000  # Rows per transaction
```

##### `insert_batch(rows)`

**Purpose:** Insert single batch of measurements.

**Parameters:**
- `rows` (List[Dict]): Measurement dictionaries

**Returns:** Number of rows inserted (int)

**SQL:**
```sql
INSERT INTO measurements
(time, uuid, parameter_code, namespace, value, uom, 
 uncertainty, depth_m, metadata_id, quality_flag, 
 location_id, location_qc_flag)
VALUES %s
ON CONFLICT DO NOTHING
```

**Conflict Resolution:**
- `ON CONFLICT DO NOTHING` - Skips duplicate rows silently
- Duplicates detected by composite unique constraint on `(time, uuid, parameter_code, depth_m)`

**Performance:**
- Uses `psycopg2.extras.execute_values` for efficient bulk inserts
- Single transaction per batch
- Commit per batch (not per row)

---

##### `process_batches(rows)`

**Purpose:** Process large row lists in batches.

**Parameters:**
- `rows` (List[Dict]): Full list of measurements

**Behavior:**
```python
for i in range(0, len(rows), BATCH_SIZE):
    batch = rows[i:i+BATCH_SIZE]
    inserted = insert_batch(batch)
    logger.info(f"Inserted {inserted}/{len(batch)} rows")
```

**Example Output:**
```
Inserted 1000/1000 rows (total: 1000)
Inserted 1000/1000 rows (total: 2000)
Inserted 543/543 rows (total: 2543)
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

### Execution Flow

```python
def main():
    # 1. Initialize components
    param_mapping = ParameterMapping(DB_CONFIG)
    csv_extractor = CSVMeasurementExtractor(param_mapping)
    nc_extractor = NetCDFMeasurementExtractor(param_mapping)
    inserter = MeasurementBatchInserter(DB_CONFIG)
    
    # 2. Find empty datasets
    datasets = query_empty_datasets()
    
    # 3. Process each dataset
    for dataset in datasets:
        # 3a. Extract location from first data file
        station, lat, lon = extract_station_info_from_file(...)
        
        # 3b. Patch coordinates
        patched_lat, patched_lon, qc_flag = patch_location_coordinates(lat, lon)
        
        # 3c. Insert/link location
        location_id = get_or_insert_location(conn, station, patched_lat, patched_lon)
        
        # 3d. Extract measurements from all files
        dataset_rows = []
        for file in dataset_files:
            if file.endswith('.csv'):
                rows = csv_extractor.extract(file, metadata)
            elif file.endswith('.nc'):
                rows = nc_extractor.extract(file, metadata)
            dataset_rows.extend(rows)
        
        # 3e. Apply location patch to all rows
        for row in dataset_rows:
            row['location_id'] = location_id
            row['location_qc_flag'] = qc_flag
        
        # 3f. Batch insert
        inserter.process_batches(dataset_rows)
```

---

## Database Schema

### `measurements` Table

```sql
CREATE TABLE measurements (
    data_id SERIAL PRIMARY KEY,
    time TIMESTAMP NOT NULL,
    uuid UUID NOT NULL,
    parameter_code VARCHAR(50) NOT NULL,
    namespace VARCHAR(20) DEFAULT 'custom',
    value REAL NOT NULL,
    uom VARCHAR(50),
    uncertainty REAL,
    depth_m REAL,
    metadata_id INTEGER REFERENCES metadata(id),
    quality_flag INTEGER DEFAULT 1,
    location_id INTEGER REFERENCES locations(id),
    location_qc_flag VARCHAR(50) DEFAULT 'unknown',
    extracted_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT measurements_unique UNIQUE (time, uuid, parameter_code, depth_m)
);
```

**Key Fields:**

| Field | Type | Purpose |
|-------|------|--------|
| `time` | TIMESTAMP | Measurement timestamp |
| `parameter_code` | VARCHAR(50) | Standardized parameter code |
| `namespace` | VARCHAR(20) | Vocabulary namespace (CF, IMOS, custom) |
| `value` | REAL | Measurement value |
| `uom` | VARCHAR(50) | Unit of measure |
| `location_id` | INTEGER | Foreign key to `locations` table |
| `location_qc_flag` | VARCHAR(50) | Location validation status |
| `quality_flag` | INTEGER | Data quality indicator (1=good) |

---

## Usage Examples

### Basic Usage

```bash
python populate_measurements.py
```

Processes all empty datasets (no limit).

### Limited Extraction

```bash
python populate_measurements.py --limit 5000
```

Extracts max 5000 measurements per dataset (for testing).

### Specific Dataset

```bash
python populate_measurements.py --dataset "Chlorophyll Database"
```

Processes only datasets with "Chlorophyll Database" in title.

### Combined Options

```bash
python populate_measurements.py --dataset "Phytoplankton" --limit 1000
```

---

## Logging

### Log Levels

- **INFO** - Processing progress, batch insertion, summary statistics
- **DEBUG** - Per-file extraction details, location patching
- **WARNING** - Missing columns, skipped files, validation warnings
- **ERROR** - Fatal errors, database failures

### Example Output

```
======================================================================
📊 Processing: Chlorophyll Database 1965-2017
======================================================================
  📍 Extracting location from: AODN_data/Chlorophyll/data.nc
  ✓ Location: Maria Island (-42.5981, 148.2317) [clean]
  ✓ Location ID: 12
  📄 Extracting CSV: phytoplankton_samples.csv
  📊 Extracting NetCDF: ctd_profiles.nc
  ✓ Patched 15432 rows with location_id=12
  💾 Inserting 15432 measurements...
  Inserted 1000/1000 rows (total: 1000)
  Inserted 1000/1000 rows (total: 2000)
  ...
  Inserted 432/432 rows (total: 15432)

======================================================================
✅ ETL Complete
======================================================================
Total inserted:        47832
Total failed:          512
CSV extracted:         23400 (200 failed)
NetCDF extracted:      24432 (312 failed)
======================================================================
```

---

## Error Handling

### File Read Failures

**Symptom:**
```
❌ NetCDF read failed: [Errno 2] No such file or directory
```

**Solutions:**
1. Check dataset path in `metadata.dataset_path`
2. Verify file permissions
3. Ensure NetCDF4 library installed: `pip install netCDF4`

---

### Time Parsing Failures

**Symptom:**
```
WARNING: cftime parsing failed: 'units' attribute not found
```

**Solutions:**
1. Check NetCDF time variable has `units` attribute
2. Ensure `cftime` library installed: `pip install cftime`
3. Fallback to `TimeFormatDetector` will attempt alternative parsing

---

### Location Validation Failures

**Symptom:**
```
⚠ Coordinates failed validation: 92.5, 350.2
```

**Solutions:**
1. Review `patch_location_coordinates()` rules
2. Check data file for metadata errors
3. Coordinates with `qc_flag='outlier_flagged'` are still inserted

---

### Database Connection Failures

**Symptom:**
```
ERROR: Batch insert failed: connection already closed
```

**Solutions:**
1. Check Docker containers: `docker-compose ps`
2. Verify database credentials in `DB_CONFIG`
3. Increase connection timeout in PostgreSQL config

---

## Performance Optimization

### Execution Time Estimates

| Dataset Size | Files | Measurements | Time |
|--------------|-------|--------------|------|
| Small | 1-5 | 1K-10K | < 1 min |
| Medium | 5-20 | 10K-100K | 2-5 min |
| Large | 20-50 | 100K-500K | 10-20 min |
| Very Large | 50+ | 500K+ | 30+ min |

### Optimization Tips

1. **Increase Batch Size**
   ```python
   BATCH_SIZE = 5000  # Default: 1000
   ```

2. **Use `--limit` for Testing**
   ```bash
   python populate_measurements.py --limit 1000
   ```

3. **Process Datasets in Parallel** (future enhancement)
   ```bash
   # Process datasets 1-10
   python populate_measurements.py --offset 0 --limit-datasets 10
   
   # Process datasets 11-20
   python populate_measurements.py --offset 10 --limit-datasets 10
   ```

4. **Database Indexing**
   ```sql
   CREATE INDEX idx_measurements_time ON measurements(time);
   CREATE INDEX idx_measurements_parameter ON measurements(parameter_code);
   CREATE INDEX idx_measurements_location ON measurements(location_id);
   ```

---

## Extension Points

### Adding Custom Parameter Mappings

Add entries to `parameter_mappings` table:

```sql
INSERT INTO parameter_mappings (raw_parameter_name, standard_code, namespace, unit)
VALUES 
    ('Chl-a', 'CPHL', 'IMOS', 'mg/m^3'),
    ('Turbidity', 'TURB', 'custom', 'NTU');
```

Rerun `populate_measurements.py` with `--force` to update existing records.

---

### Custom Time Formats

Extend `TimeFormatDetector._from_numeric()`:

```python
@staticmethod
def _from_numeric(val: float) -> Optional[datetime]:
    # Add custom format
    if 50000 < val < 60000:  # Days since 1800-01-01
        base = datetime(1800, 1, 1)
        return base + timedelta(days=val)
    
    # ... existing logic ...
```

---

### Adding Quality Control Rules

```python
def apply_qc_rules(value: float, param_code: str) -> int:
    """
    Returns quality_flag:
    1 = good
    2 = suspect
    3 = bad
    """
    if param_code == 'TEMP':
        if -5 < value < 35:
            return 1
        elif -10 < value < 40:
            return 2
        else:
            return 3
    
    return 1  # Default: good
```

Integrate into extractors:

```python
quality_flag = apply_qc_rules(value, param_code)
rows.append({'quality_flag': quality_flag, ...})
```

---

## Testing

### Unit Test Example

```python
import unittest
from populate_measurements import TimeFormatDetector, patch_location_coordinates

class TestMeasurementFunctions(unittest.TestCase):
    def test_time_conversion(self):
        result = TimeFormatDetector.detect_and_convert(2020.5)
        self.assertEqual(result.year, 2020)
        self.assertGreater(result.month, 6)
    
    def test_location_patching(self):
        lat, lon, flag = patch_location_coordinates(42.6, 148.2)
        self.assertLess(lat, 0)  # Should flip to negative
        self.assertEqual(flag, 'lat_sign_flipped')
```

### Integration Test

```bash
# Create test dataset
mkdir -p AODN_data/Test_Dataset
echo "time,parameter,value" > AODN_data/Test_Dataset/data.csv
echo "2020-06-15T10:00:00,TEMP,15.2" >> AODN_data/Test_Dataset/data.csv

# Add metadata
psql -h localhost -p 5433 -U marine_user -d marine_db -c "
INSERT INTO metadata (uuid, title, dataset_path)
VALUES ('test-uuid', 'Test Dataset', 'AODN_data/Test_Dataset');"

# Run ETL
python populate_measurements.py --dataset "Test Dataset"

# Verify
psql -h localhost -p 5433 -U marine_user -d marine_db -c "
SELECT * FROM measurements WHERE uuid='test-uuid';"
```

---

## Integration with ETL Pipeline

### Pipeline Order

1. populate_metadata.py
2. populate_parameter_mappings.py
3. **populate_measurements.py** ← You are here
4. populate_spatial.py
5. populate_biological.py

### Data Dependencies

**Requires:**
- `metadata.id` - From populate_metadata.py
- `metadata.dataset_path` - Dataset file location
- `parameter_mappings` table - Parameter standardization

**Produces:**
- `measurements.data_id` - Primary key for time series data
- `locations.id` - Spatial reference for measurements

**Consumed By:**
- Query API - Time series queries
- Visualization dashboards - Plotting and mapping
- Quality control scripts - Data validation

---

## Troubleshooting Checklist

- [ ] `metadata` table populated (run `populate_metadata.py` first)
- [ ] `parameter_mappings` table populated
- [ ] Dataset paths valid in `metadata.dataset_path`
- [ ] Python dependencies installed: `pip install -r requirements.txt`
- [ ] NetCDF4 library installed: `pip install netCDF4`
- [ ] cftime library installed: `pip install cftime`
- [ ] Database accepting connections (port 5433)
- [ ] PostGIS extension enabled: `CREATE EXTENSION IF NOT EXISTS postgis;`
- [ ] Sufficient disk space for measurement storage
- [ ] No file system permission issues

---

## References

- [Project README](../README.md)
- [Database Schema Documentation](database_schema.md)
- [ETL Guide](ETL_GUIDE.md)
- [Parameter Mapping Documentation](parameter_mappings.md)
- [CF Conventions](http://cfconventions.org/)
- [NetCDF4-Python Documentation](https://unidata.github.io/netcdf4-python/)
- [cftime Documentation](https://unidata.github.io/cftime/)

---

*Last Updated: December 25, 2025*  
*Script Version: 2.1*  
*Maintained by: Huon Channel Marine Analytics Project*