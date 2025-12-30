# ETL Scripts Reference

This document provides an overview of all Python ETL scripts in the Huon Channel Marine Analytics project. Each script handles a specific aspect of the data ingestion and processing pipeline.

## Quick Reference

For step-by-step instructions on running the ETL pipeline, see the [ETL Quick Reference](../ETL_QUICK_REFERENCE.md) in the project root.

## Core ETL Scripts

### 1. populate_metadata.py

**Purpose:** Scans the AODN data directory and extracts metadata from dataset files.

**Key Functions:**
- Discovers datasets in the AODN_data directory structure
- Extracts metadata from ISO 19115-3 XML files
- Parses spatial and temporal extents
- Stores dataset metadata in the `metadata` table
- Generates UUIDs for dataset identification

**Usage:**
```bash
python populate_metadata.py
```

**Output:** Populates the `metadata` table with dataset information including titles, abstracts, spatial/temporal bounds, and file paths.

[Detailed Documentation →](populate_metadata_detail.md)

---

### 2. populate_parameter_mappings.py

**Purpose:** Loads standardized parameter mappings from `config_parameter_mapping.json` into the database.

**Why Two Components?** This script works in conjunction with `config_parameter_mapping.json` to provide both:
- **Configuration Management** (JSON file): Human-editable, version-controlled parameter definitions
- **Performance Optimization** (Database table): Fast indexed lookups during ETL operations

The JSON file serves as the authoritative source for parameter mappings, while the database table enables millisecond-level lookups during data processing. This architecture separates configuration from execution, making mappings easy to maintain while keeping ETL operations performant.

**Key Functions:**
- Reads parameter mappings from `config_parameter_mapping.json`
- Maps raw parameter names to standardized codes (CF, BODC, custom)
- Defines measurement units and namespaces
- Creates indexed lookup table for measurement standardization
- Populates `parameter_mappings` database table
- Validates mapping structure and reports statistics
- Idempotent operation (safe to run multiple times)

**Configuration File:** `config_parameter_mapping.json`
- Contains 80+ parameter mappings
- Defines standard codes, namespaces, and units
- Includes time format hints and spatial column hints
- Version controlled with codebase
- Easy to extend with new mappings

**Usage:**
```bash
python populate_parameter_mappings.py
```

**When to Run:**
- After database initialization
- When adding new parameter mappings to config
- When setting up new environment
- Safe to re-run anytime (uses ON CONFLICT DO NOTHING)

**Example Mapping Flow:**
```
Raw Data:     "TEMPERATURE" → Config File → Database → ETL Scripts
Standardized: "TEMP" (BODC, Degrees Celsius)
```

[Detailed Documentation →](populate_parameter_mappings_detail.md)

---

### 3. populate_measurements.py

**Purpose:** Extracts time-series measurements from NetCDF and CSV files with multi-parameter support.

**Key Functions:**
- Processes NetCDF (`.nc`) and CSV (`.csv`) data files
- Detects multiple parameters per file (temperature, salinity, pressure, etc.)
- Extracts time-series measurements (time, value, parameter)
- Auto-detects time formats (ISO 8601, numeric timestamps, decimal years)
- Converts cftime objects to standard datetime
- Validates and creates location records using simple lat/lon matching
- **Uses pure PostgreSQL for location queries** (no PostGIS dependency)
- Links measurements to locations and metadata
- Handles large datasets with batch processing

**Database Configuration:**
```python
DB_CONFIG = {
    'dbname': 'marine_db',
    'user': 'marine_user',
    'password': 'marine_pass123',  # Updated
    'host': 'localhost',
    'port': '5433'  # Updated from 5432
}
```

**Usage:**
```bash
# Process all datasets
python populate_measurements.py

# Limit rows per dataset
python populate_measurements.py --limit 5000

# Process specific dataset
python populate_measurements.py --dataset "Chlorophyll"
```

**Features:**
- ✅ Multi-parameter detection from column names
- ✅ Time format auto-detection
- ✅ Parameter standardization via database mappings
- ✅ Location matching using coordinate proximity (0.0001 degree tolerance)
- ✅ Pure PostgreSQL queries (PostGIS removed December 2025)
- ✅ Quality control flags
- ✅ Batch insertion for performance

**Location Matching (Non-PostGIS Implementation):**
```python
# Find existing location within ~11 meters
SELECT id FROM locations 
WHERE ABS(latitude - %s) < 0.0001 
  AND ABS(longitude - %s) < 0.0001
LIMIT 1
```

[Detailed Documentation →](populate_measurements_detail.md)

---

### 4. populate_spatial.py

**Purpose:** Loads spatial reference data (marine regions, boundaries) from shapefiles.

**Important Note:** As of December 2025, PostGIS support has been removed from the database schema. This script requires modification or alternative approaches for spatial data.

**Original Functions (PostGIS-based):**
- Reads ESRI Shapefiles (`.shp`) using ogr2ogr conversion
- Extracts polygon geometries for marine regions
- Converts to PostGIS-compatible geometry format
- Populates `spatial_features` table with region boundaries

**Current Status:** Not compatible with PostGIS-free schema. Consider alternatives:
- Store geometries as GeoJSON in JSONB columns
- Use external GIS tools for spatial analysis
- Import pre-processed spatial boundaries as coordinate arrays

**Usage:**
```bash
python populate_spatial.py  # May require schema updates
```

[Detailed Documentation →](populate_spatial_detail.md)

---

### 5. populate_biological.py

**Purpose:** Processes species observation and biological survey data.

**Key Functions:**
- Extracts species occurrence records from CSV files
- Parses taxonomic information (scientific names, common names)
- Extracts observation counts and abundance data
- Links observations to locations and timestamps
- Handles Redmap sightings, larval fish surveys, phytoplankton data
- Normalizes taxonomic classifications
- Creates entries in `taxonomy` and `species_observations` tables

**Usage:**
```bash
python populate_biological.py
```

**Data Sources:**
- Redmap species sightings
- IMOS Larval Fish Database
- Phytoplankton sampling surveys
- Marine biodiversity observations

[Detailed Documentation →](populate_biological_detail.md)

---

## Utility Scripts

### diagnostic_etl.py

**Purpose:** Comprehensive diagnostic tool for analyzing data files and ETL processes.

**Key Functions:**
- Scans all data files and reports structure
- Identifies time columns and formats
- Detects parameter columns
- Reports location data availability
- Generates JSON diagnostic report
- Validates data quality

**Usage:**
```bash
python diagnostic_etl.py
```

**Output:** Creates `diagnostic_report.json` with detailed file analysis.

[Detailed Documentation →](diagnostic_etl_detail.md)

---

### example_data_access.py

**Purpose:** Demonstrates how to query the database after ETL completion.

**Key Functions:**
- Example queries for measurements
- Spatial filtering examples (using coordinate ranges, not PostGIS)
- Time-series data retrieval
- Parameter-based queries
- Joins across tables

**Usage:**
```bash
python example_data_access.py
```

[Detailed Documentation →](example_data_access_detail.md)

---

## ETL Pipeline Execution Order

For a complete data ingestion, run scripts in this order:

```bash
# 1. Initialize database (if not already done)
docker-compose up -d

# 2. Load metadata
python populate_metadata.py

# 3. Load parameter mappings (creates lookup table)
python populate_parameter_mappings.py

# 4. Load spatial features (optional - requires schema updates post-PostGIS removal)
# python populate_spatial.py

# 5. Load measurements (uses parameter_mappings for standardization)
python populate_measurements.py

# 6. Load biological observations
python populate_biological.py

# 7. Verify data
python example_data_access.py
```

**Important:** `populate_parameter_mappings.py` must run **before** `populate_measurements.py` because measurements script queries the `parameter_mappings` table for standardization.

## Common Command-Line Options

Most ETL scripts support the following options:

- `--help` - Display usage information
- `--limit N` - Process only N records (for testing)
- `--dataset "Name"` - Process only datasets matching name
- `--verbose` - Enable detailed logging
- `--dry-run` - Validate without writing to database

## Configuration Files

- **config_parameter_mapping.json** - Parameter standardization mappings (80+ definitions)
  - Raw parameter names → Standard codes
  - Namespace definitions (BODC, CF, custom)
  - Unit specifications
  - Time format hints
  - Quality flag definitions
- **init.sql** - Database schema initialization (PostGIS-free as of Dec 2025)
- **docker-compose.yml** - Database service configuration
- **.env** - Database credentials (not in repo)

## Database Connection

All scripts use these default connection parameters:

```python
DB_CONFIG = {
    'dbname': 'marine_db',
    'user': 'marine_user',
    'password': 'marine_pass123',  # Updated Dec 2025
    'host': 'localhost',
    'port': '5433'  # Updated from 5432
}
```

**Important Changes (December 2025):**
- Port changed from `5432` to `5433`
- Password changed from `marine_pass` to `marine_pass123`
- PostGIS plugin removed from schema
- Location queries use pure PostgreSQL (ABS comparisons instead of ST_DWithin)

Modify connection settings in each script or use environment variables.

## Logging

All scripts use Python's `logging` module:

- **INFO** level: Standard progress messages
- **WARNING** level: Skipped or problematic records
- **ERROR** level: Critical failures
- **DEBUG** level: Detailed processing information

Scripts typically write logs to `logs/` directory with timestamps.

## Error Handling

ETL scripts implement defensive programming:

- ✅ Graceful failure for individual records
- ✅ Transaction rollback on critical errors
- ✅ Detailed error logging with full tracebacks
- ✅ Progress tracking
- ✅ Resume capability (upsert patterns)

## Data Quality Flags

Scripts apply quality control flags:

- `quality_flag = 1` - Good data
- `quality_flag = 0` - Questionable data
- `location_qc_flag` - Location validation status:
  - `clean` - Passed all checks
  - `lat_sign_flipped` - Corrected hemisphere
  - `lon_normalized` - Normalized to -180..180
  - `outside_tasmania` - Outside expected region
  - `missing_coordinates` - No location data

## Performance Considerations

- **Batch Processing:** Scripts use batch inserts (default 1000 rows)
- **Indexing:** Database tables have appropriate indexes
- **Parameter Lookups:** Database queries (~10µs) vs. JSON file reads (~1ms)
- **Memory:** Large files processed incrementally
- **Parallelization:** Scripts can run independently after metadata and parameter mappings load
- **No PostGIS Overhead:** Pure SQL queries faster than geometry operations

## Troubleshooting

Common issues and solutions:

1. **Connection refused**
   - Check Docker containers: `docker-compose ps`
   - Verify port 5433 is available (not 5432)
   - Confirm password is `marine_pass123`

2. **File not found**
   - Ensure `AODN_data/` directory exists
   - Check file paths in error messages
   - Verify `config_parameter_mapping.json` exists

3. **Encoding errors**
   - CSV files tried with utf-8, latin1, iso-8859-1
   - NetCDF files require netCDF4 library

4. **Memory errors**
   - Use `--limit` flag for testing
   - Process datasets individually

5. **Parameter mapping errors**
   - Run `populate_parameter_mappings.py` first
   - Verify `parameter_mappings` table exists
   - Check `config_parameter_mapping.json` is valid JSON

6. **Database schema errors**
   - Column name mismatch: Use `id` not `metadata_id` in SELECT queries
   - PostGIS function errors: Script may need updating for pure SQL
   - Check init.sql matches current schema version

## Contributing

When modifying ETL scripts:

1. Maintain upsert-safe patterns (`ON CONFLICT DO NOTHING`)
2. Add comprehensive logging with timestamps
3. Update this documentation
4. Update `config_parameter_mapping.json` for new parameters
5. Test with diagnostic_etl.py
6. Verify data integrity with example_data_access.py
7. Use pure PostgreSQL (avoid PostGIS dependencies)
8. Include full traceback logging for errors

## Schema Migration Notes (December 2025)

**PostGIS Removal:**
- All `ST_*` functions replaced with standard SQL
- Location matching uses `ABS(latitude - lat) < 0.0001` proximity checks
- Geometry columns converted to latitude/longitude pairs
- Spatial queries use coordinate range filters

**Connection Updates:**
- Port: 5432 → 5433
- Password: `marine_pass` → `marine_pass123`
- Column: `metadata_id` → `id` (metadata table primary key)

## Additional Resources

- [Database Schema Documentation](database_schema.md)
- [Data Ingestion Guide](data_ingestion.md)
- [ETL Guide](ETL_GUIDE.md)
- [Project README](../README.md)
- [Parameter Mapping Configuration](../config_parameter_mapping.json)

---

*Last Updated: December 30, 2025*
*PostGIS removed, pure PostgreSQL implementation*
