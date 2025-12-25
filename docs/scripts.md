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

**Purpose:** Loads standardized parameter mappings from configuration files into the database.

**Key Functions:**
- Reads parameter mappings from `config_parameter_mapping.json`
- Maps raw parameter names to standardized codes
- Defines measurement units and namespaces (CF, NERC, IMOS)
- Creates lookup table for measurement standardization

**Usage:**
```bash
python populate_parameter_mappings.py
```

**Configuration:** Edit `config_parameter_mapping.json` to add new parameter mappings.

[Detailed Documentation →](populate_parameter_mappings_detail.md)

---

### 3. populate_measurements.py

**Purpose:** Extracts time-series measurements from NetCDF and CSV files with integrated location patching.

**Key Functions:**
- Processes NetCDF (`.nc`) and CSV (`.csv`) data files
- Extracts time-series measurements (time, value, parameter)
- Auto-detects time formats (ISO 8601, numeric timestamps, decimal years)
- Converts cftime objects to standard datetime
- Extracts and patches location data from file headers
- Applies coordinate validation and correction
- Links measurements to locations and metadata
- Handles large datasets with batch processing

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
- ✅ Integrated location patching (from patch_locations_v4.py)
- ✅ Time format auto-detection
- ✅ Parameter standardization via database mappings
- ✅ Quality control flags
- ✅ Upsert-safe (no duplicate insertions)

[Detailed Documentation →](populate_measurements_detail.md)

---

### 4. populate_spatial.py

**Purpose:** Loads spatial reference data (marine regions, boundaries) from shapefiles.

**Key Functions:**
- Reads ESRI Shapefiles (`.shp`) using ogr2ogr conversion
- Extracts polygon geometries for marine regions
- Converts to PostGIS-compatible geometry format
- Populates `spatial_features` table with region boundaries
- Supports Tasmania marine bioregions, MPAs, and other spatial features
- Preserves shapefile attributes as JSONB properties

**Usage:**
```bash
python populate_spatial.py
```

**Requirements:** 
- GDAL/OGR tools installed
- Shapefiles must be in `AODN_data/` subdirectories

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
- Spatial filtering examples
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

# 3. Load parameter mappings
python populate_parameter_mappings.py

# 4. Load spatial features (optional)
python populate_spatial.py

# 5. Load measurements (includes location patching)
python populate_measurements.py

# 6. Load biological observations
python populate_biological.py

# 7. Verify data
python example_data_access.py
```

## Common Command-Line Options

Most ETL scripts support the following options:

- `--help` - Display usage information
- `--limit N` - Process only N records (for testing)
- `--dataset "Name"` - Process only datasets matching name
- `--verbose` - Enable detailed logging
- `--dry-run` - Validate without writing to database

## Configuration Files

- **config_parameter_mapping.json** - Parameter standardization mappings
- **init.sql** - Database schema initialization
- **docker-compose.yml** - Database service configuration
- **.env** - Database credentials (not in repo)

## Database Connection

All scripts use these default connection parameters:

```python
DB_CONFIG = {
    'dbname': 'marine_db',
    'user': 'marine_user',
    'password': 'marine_pass123',
    'host': 'localhost',
    'port': '5433'
}
```

Modify connection settings in each script or use environment variables.

## Logging

All scripts use Python's `logging` module:

- **INFO** level: Standard progress messages
- **WARNING** level: Skipped or problematic records
- **ERROR** level: Critical failures
- **DEBUG** level: Detailed processing information

## Error Handling

ETL scripts implement defensive programming:

- ✅ Graceful failure for individual records
- ✅ Transaction rollback on critical errors
- ✅ Detailed error logging
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
- **Memory:** Large files processed incrementally
- **Parallelization:** Scripts can run independently after metadata load

## Troubleshooting

Common issues and solutions:

1. **Connection refused**
   - Check Docker containers: `docker-compose ps`
   - Verify port 5433 is available

2. **File not found**
   - Ensure `AODN_data/` directory exists
   - Check file paths in error messages

3. **Encoding errors**
   - CSV files tried with utf-8, latin1, iso-8859-1
   - NetCDF files require netCDF4 library

4. **Memory errors**
   - Use `--limit` flag for testing
   - Process datasets individually

## Contributing

When modifying ETL scripts:

1. Maintain upsert-safe patterns (`ON CONFLICT DO NOTHING`)
2. Add comprehensive logging
3. Update this documentation
4. Test with diagnostic_etl.py
5. Verify data integrity with example_data_access.py

## Additional Resources

- [Database Schema Documentation](database_schema.md)
- [Data Ingestion Guide](data_ingestion.md)
- [ETL Guide](ETL_GUIDE.md)
- [Project README](../README.md)

---

*Last Updated: December 25, 2025*