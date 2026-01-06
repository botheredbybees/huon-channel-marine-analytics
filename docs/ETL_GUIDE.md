# Enhanced ETL Guide (v2.0)

This guide walks you through the new diagnostic and improved ETL pipeline for ingesting AODN/IMOS marine data into your database.

## Quick Start

### Step 1: Run Diagnostic

First, scan all AODN datasets to identify issues:

```bash
python diagnostic_etl.py
```

This generates:
- **Console output**: Summary of ingestion status by dataset
- **diagnostic_report.json**: Detailed findings for each file

### Step 2: Review Failures

Check the detailed failure report:

```bash
cat diagnostic_report.json | jq '.summary.failure_reasons'
```

Common failure reasons:
- `ENCODING_ERROR` - File encoding issue (fix: specify encoding)
- `EMPTY_FILE` - Zero-byte files (skip these)
- `CSV_PARSE_ERROR` - Malformed CSV
- `NETCDF_READ_ERROR` - Corrupted NetCDF
- `TIME_FORMAT_UNKNOWN` - Couldn't parse time column
- `MISSING_REQUIRED_COLUMNS` - Missing time or value column

### Step 3: Run Improved ETL

Ingest all datasets with edge case handling:

```bash
python populate_measurements.py
```

For a specific dataset:

```bash
python populate_measurements.py --dataset "Chlorophyll sampling"
```

For testing (limit rows):

```bash
python populate_measurements.py --limit 100
```

With custom parameter mapping:

```bash
python populate_measurements.py --config config_parameter_mapping.json
```

## Architecture

### What's New in v2

```
File Input
    ↓
┌─────────────────────────────────────┐
│ TimeFormatDetector                  │
│ ─ Auto-detect time column format    │
│ ─ Handle months_since refs          │
│ ─ Parse compound dates (Y/M/D cols) │
│ ─ CF calendar support               │
└─────────────────────────────────────┘
    ↓
┌─────────────────────────────────────┐
│ ParameterMapping                    │
│ ─ Map raw vars to BODC/CF standard  │
│ ─ Load custom mappings from JSON    │
│ ─ Support unknown parameters        │
└─────────────────────────────────────┘
    ↓
┌─────────────────────────────────────┐
│ CSVMeasurementExtractor             │
│ ─ Flexible column detection         │
│ ─ Multi-encoding support            │
│ ─ Robust null handling              │
└─────────────────────────────────────┘
    ↓  OR  ↓
┌─────────────────────────────────────┐
│ NetCDFMeasurementExtractor          │
│ ─ Ragged array support              │
│ ─ Multi-dimensional data            │
│ ─ CF units parsing                  │
└─────────────────────────────────────┘
    ↓
┌─────────────────────────────────────┐
│ MeasurementBatchInserter            │
│ ─ 1000-row batches                  │
│ ─ Conflict handling                 │
│ ─ Progress reporting                │
└─────────────────────────────────────┘
    ↓
 PostgreSQL measurements hypertable
```

## Time Format Detection

The `TimeFormatDetector` automatically handles multiple time representations:

### Supported Formats

| Format | Example | Notes |
|--------|---------|-------|
| ISO 8601 | `2023-12-19T04:50:00Z` | Standard string format |
| Days since 1900 | `44250` (40000-50000) | IMOS historical standard |
| Days since 1970 | `19500` (15000-25000) | Unix epoch offset |
| Months since 1900 | `1487` (1000-2000) | CF conventions |
| Decimal year | `2023.5` | Year + fractional day |
| Year integer | `2023` | Just the year |
| Unix timestamp | `1640000000` | Seconds since 1970 |
| Compound columns | `year=2023, month=12, day=19` | Separate columns |

### Custom Time Column Examples

If your CSV has non-standard time columns, the ETL will attempt to auto-detect:

```csv
YEAR,MONTH,DAY,HOUR,TEMP
2023,12,19,04,15.3
```

Detection logic:
1. Look for columns: `time`, `date`, `datetime`, `timestamp` (case-insensitive)
2. Extract year/month/day/hour columns if time column not found
3. Parse ISO 8601 strings
4. Attempt numeric conversion (days/months/seconds since reference)

To override auto-detection, add a `time_format` hint in `config_parameter_mapping.json`.

## Parameter Mapping

### Default Mappings

The system includes 100+ pre-configured parameter mappings. Common examples:

```json
"TEMP": ["TEMP", "bodc", "Degrees Celsius"],
"CHLOROPHYLL_A": ["CPHL", "bodc", "mg/m3"],
"SEA_WATER_SALINITY": ["PSAL", "cf", "PSS-78"],
"DISSOLVED_OXYGEN": ["DOXY", "cf", "ml/l"]
```

Format: `"raw_name": ["standard_code", "namespace", "unit"]`

**Namespaces:**
- `bodc` - British Oceanographic Data Centre (IMOS standard)
- `cf` - Climate & Forecast conventions
- `custom` - Raw variable name (for unknowns)

### Adding Custom Mappings

Edit `config_parameter_mapping.json` to add mappings for your datasets:

```json
"MY_NEW_PARAMETER": ["MY_CODE", "custom", "units"]
```

Then use:

```bash
python populate_measurements.py --config config_parameter_mapping.json
```

### Unmapped Parameters

If a parameter isn't in the mapping, it's inserted with:
- `namespace = 'custom'`
- `parameter_code = raw_name`
- `uom = 'unknown'`

You can update these later in the database and adjust `config_parameter_mapping.json` for future runs.

## CSV Processing

### Expected Columns

The ETL looks for these columns (case-insensitive):

```
Required:
  - Time column: time, date, datetime, timestamp
  - Value column: value, concentration, measurement, result

Optional:
  - Parameter column: parameter, variable, code
  - Depth: depth, z, level
  - Spatial: latitude/lat, longitude/lon, x, y
```

### Example CSV Structure

```csv
DATE,PARAMETER,VALUE,DEPTH
2023-12-19T04:50:00Z,TEMP,15.3,0
2023-12-19T05:00:00Z,TEMP,15.4,0
2023-12-19T05:10:00Z,PSAL,35.2,0
```

### Encoding Issues

The ETL automatically tries:
1. UTF-8 (modern files)
2. Latin-1 (legacy Windows)
3. ISO-8859-1 (European)

If a file still fails, check for BOM or special characters:

```bash
file AODN_data/dataset/file.csv  # Shows encoding
hexdump -C AODN_data/dataset/file.csv | head  # Check first bytes
```

## NetCDF Processing

### NetCDF Supported Features

✅ **Supported:**
- Multi-dimensional data (time × station × depth)
- Ragged arrays (unlimited dimensions)
- CF calendar conventions
- Variable attributes (units, long_name)
- Coordinate variables (lat, lon, depth, time)

⚠️ **Known Issues:**
- Grouped datasets (`.groups`) not fully tested
- Some custom attributes may be ignored

### Example NetCDF Structure

```netcdf
netcdf IMOS_coastal_wave {
dimensions:
    time = UNLIMITED;
    station = 1;
variables:
    double time(time);
        time:units = "days since 1900-01-01";
        time:calendar = "gregorian";
    double latitude(station);
    double longitude(station);
    float SWVHT(time, station);
        SWVHT:long_name = "Significant Wave Height";
        SWVHT:units = "m";
```

The ETL will:
1. Parse `time` using CF units + calendar
2. Extract `SWVHT` as parameter `WAVE_HGT` (via mapping)
3. Create one row per time×station combination

### Debugging NetCDF Issues

```bash
# Inspect NetCDF structure
ncdump -h AODN_data/dataset/file.nc | head -50

# Check variable attributes
ncdump -v time AODN_data/dataset/file.nc | head -20
```

## Spatial Data (Shapefiles)

For shapefiles (seagrass, kelp extent), use `populate_spatial.py`:

```bash
python populate_spatial.py
```

Requires:
- `geopandas` and `ogr2ogr` (system tool)
- `.shp`, `.shx`, `.dbf` files present

## Biological Data (Species Observations)

For species surveys, use `populate_biological.py`:

```bash
python populate_biological.py
```

Expects CSV columns:
- Species name (scientific): `SPECIES_NAME`, `SPECIES`
- Location: `SITE_CODE`, `LATITUDE`, `LONGITUDE`
- Count: `TOTAL_NUMBER`, `count_value`
- Date: `SURVEY_DATE`, `SIGHTING_DATE`

## Troubleshooting

### Issue: "No time variable found"

**Cause:** Time column not detected

**Solution:**
1. Check column names: `ncdump -h file.nc` (NetCDF) or `head -1 file.csv` (CSV)
2. Ensure time column is named `time`, `date`, `datetime`, or `timestamp` (case-insensitive)
3. For compound dates, ensure columns are: `year`, `month`, `day`, `hour`, `minute`, `second`

### Issue: "Batch insert failed: ERROR: violating unique constraint"

**Cause:** Duplicate measurements already in database

**Solution:**
The ETL uses `ON CONFLICT DO NOTHING` to skip duplicates. This is expected behavior. Check:

```sql
SELECT COUNT(*) FROM measurements 
WHERE uuid = '<dataset-uuid>' AND time = '2023-12-19';
```

### Issue: "NETCDF4_NOT_INSTALLED"

**Solution:**

```bash
pip install netCDF4 xarray cftime
```

### Issue: "Encoding error on line X"

**Cause:** Mixed encodings in CSV

**Solution:**
1. Convert file to UTF-8:
   ```bash
   iconv -f ISO-8859-1 -t UTF-8 input.csv > output.csv
   ```
2. Or remove problematic characters:
   ```bash
   dos2unix file.csv  # Remove Windows line endings
   sed -i '/^$/d' file.csv  # Remove blank lines
   ```

### Issue: Time values look wrong (e.g., year 3000)

**Cause:** Wrong time reference in NetCDF or numeric offset

**Solution:**
1. Check NetCDF units:
   ```bash
   ncdump -v time file.nc | grep "time:units"
   ```
2. For numeric CSVs, check first few time values:
   ```bash
   head -10 file.csv | cut -d',' -f2  # 2nd column
   ```
3. Use TimeFormatDetector logic to infer correct reference

## Performance Tips

### Batch Processing

The ETL processes in 1000-row batches. For large datasets (>1M rows):

```bash
# Monitor insert rate
watch -n 5 'psql -c "SELECT COUNT(*) FROM measurements"'

# Adjust batch size in populate_measurements.py line 362:
BATCH_SIZE = 5000  # Increase for faster inserts
```

### Parallel Ingestion

You can run multiple ETL instances on different datasets:

```bash
# Terminal 1
python populate_measurements.py --dataset "Chlorophyll sampling"

# Terminal 2
python populate_measurements.py --dataset "Wave buoys"
```

### Index Optimization

After bulk inserts, analyze the table:

```sql
ANALYZE measurements;
REINDEX TABLE measurements;
```

## Output Reports

### diagnostic_report.json

Structure:

```json
{
  "timestamp": "2025-12-19T05:50:00",
  "summary": {
    "total_datasets": 38,
    "ingested": 28,
    "failed": 10,
    "file_format_distribution": {
      "csv": 15,
      "netcdf": 10,
      "shapefile": 8,
      "gpx": 5
    },
    "failure_reasons": {
      "ENCODING_ERROR": 3,
      "TIME_FORMAT_UNKNOWN": 2,
      "EMPTY_FILE": 5
    }
  },
  "datasets": {
    "AODN_data/Chlorophyll sampling/file.csv": {
      "status": "success",
      "file_format": "csv",
      "rows": 1250,
      "columns": ["date", "parameter", "value", "depth"],
      "time_format": "ISO_8601",
      "spatial_columns": ["latitude", "longitude"],
      "sample_data": [...]
    },
    "AODN_data/Wave buoys/file.nc": {
      "status": "success",
      "file_format": "netcdf",
      "dimensions": {"time": 8760, "station": 1},
      "variables": ["time", "latitude", "longitude", "SWVHT", "SWPD"],
      "time_variable": "time",
      "coordinate_variables": ["latitude", "longitude"],
      "time_format": {...}
    }
  }
}
```

## Next Steps

1. **Run diagnostic**: `python diagnostic_etl.py`
2. **Review failures**: Check `diagnostic_report.json`
3. **Add custom mappings**: Edit `config_parameter_mapping.json` if needed
4. **Run ETL v2**: `python populate_measurements.py`
5. **Verify**: Check `SELECT COUNT(*) FROM measurements`
6. **Spatial/Biological**: Run `populate_spatial.py` and `populate_biological.py` separately

## References

- **BODC Parameter Codes**: https://www.bodc.ac.uk/data/parameters/
- **CF Conventions**: https://cfconventions.org/
- **NetCDF4 Python**: https://unidata.github.io/netcdf4-python/
- **IMOS Standards**: https://imos.org.au/

## Questions?

See the code docstrings or open an issue on the GitHub repo.
