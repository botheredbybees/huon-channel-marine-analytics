# ETL Scripts Reference

This document provides an overview of all Python ETL scripts in the Huon Channel Marine Analytics project. Each script handles a specific aspect of the data ingestion and processing pipeline.

## Quick Reference

For step-by-step instructions on running the ETL pipeline, see the [ETL Quick Reference](../ETL_QUICK_REFERENCE.md) in the project root.

## Development Environment Setup

**Default Development Credentials** (from `docker-compose.yml`):
```bash
export DB_HOST=localhost
export DB_PORT=5433                  # Development port
export DB_NAME=marine_db
export DB_USER=marine_user
export DB_PASSWORD=marine_pass123
export AODN_DATA_PATH=/AODN_data
```

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

**Purpose:** Extracts time-series measurements from NetCDF and CSV files with integrated location patching.

**Key Functions:**
- Processes NetCDF (`.nc`) and CSV (`.csv`) data files
- Extracts time-series measurements (time, value, parameter)
- Auto-detects time formats (ISO 8601, numeric timestamps, decimal years)
- Converts cftime objects to standard datetime
- Extracts and patches location data from file headers
- Applies coordinate validation and correction
- **Uses parameter_mappings table for standardization** (fast database lookups)
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

**Purpose:** Extracts biological survey data from CSV files (species observations, habitat assessments).

**Key Functions:**
- Processes biological survey CSV files
- Maps species codes to standard taxonomies
- Extracts location, date, and observation metadata
- Handles abundance estimates and confidence levels
- Links to existing measurement locations
- Validates taxonomic information

**Usage:**
```bash
python populate_biological.py
```

**Output:** Populates the `biological_observations` and `species_mapping` tables.

[Detailed Documentation →](populate_biological_detail.md)

---

## Metadata Enrichment Scripts (Phase 1)

These NEW scripts enrich existing data with metadata extracted from XML files and NetCDF headers. **Non-destructive:** Only UPDATE NULL/empty fields.

### 6. enrich_metadata_from_xml.py ✨ NEW

**Purpose:** Extract ISO 19115-3 metadata from XML files and populate empty metadata fields.

**Problem Solved:**
- Empty metadata fields in `metadata` table (abstract, lineage, credits, dates)
- Missing spatial/temporal bounds
- Undocumented data lineage and processing history

**Key Functions:**
- Locates metadata.xml files in dataset directories
- Parses ISO 19115-3 XML with proper namespace handling
- Extracts geographic bounding boxes (W, E, S, N coordinates)
- Extracts temporal coverage (start and end dates)
- Extracts lineage (processing history)
- Extracts credits and acknowledgments
- Batch updates database with non-destructive UPDATEs

**Workflow:**
```
AODN_data/
└── <dataset_name>/
    └── <uuid>/
        └── metadata/
            └── metadata.xml  ← Parsed here
                ↓
            Extract abstract, spatial extent, dates, lineage
                ↓
            UPDATE metadata table WHERE field IS NULL
```

**Usage:**
```bash
python enrich_metadata_from_xml.py
```

**Expected Output:**
- ~30 metadata fields enriched for ~40 datasets
- Audit trail logged to console
- No data deleted or overwritten

**Safety Features:**
- Only updates NULL/empty fields
- Logs all changes for traceability
- Can be run multiple times safely (idempotent)

[Detailed Documentation →](enrich_metadata_from_xml_detail.md)

---

### 7. enrich_measurements_from_netcdf_headers.py ✨ NEW

**Purpose:** Extract parameter metadata from NetCDF files and update parameter mappings and units.

**Problem Solved:**
- Missing parameter descriptions (long_name, standard_name)
- Incorrect parameter mappings (e.g., "ph" misidentified as pH instead of phosphate)
- Unit conversion failures (wind_speed off by 100x)
- Quality flags undocumented

**Key Functions:**
- Discovers NetCDF files in dataset directories
- Extracts CF-compliant variable metadata
- Reads attributes: long_name, standard_name, units, comments, instrument info
- Cross-references with actual measurements in database
- Validates extracted units against observed data ranges
- Detects unit mismatches (e.g., cm/s vs m/s)
- Updates `parameter_mappings` table with discovered metadata
- Updates `measurements` table with extracted units

**Workflow:**
```
AODN_data/
└── <dataset>/
    └── <file>.nc  ← Read NetCDF variables
        ↓
    Extract long_name, standard_name, units
        ↓
    Validate against actual min/max values
        ↓
    INSERT/UPDATE parameter_mappings
    UPDATE measurements SET units
```

**Usage:**
```bash
python enrich_measurements_from_netcdf_headers.py
```

**Expected Output:**
- Parameter descriptions for 50+ parameters
- Detected unit issues (e.g., wind_speed cm/s → m/s)
- Quality flag documentation
- Instrument calibration metadata
- Updated `parameter_mappings` table

**Validation Examples:**
```python
# Wind speed validation
if var_name == 'wind_speed' and units == 'cm/s':
    max_observed = 1200  # cm/s = 12 m/s
    if max_observed > 50:
        issue = "Units should be m/s, not cm/s"

# Parameter name disambiguation
if var_name in ['ph', 'PH']:
    if value_range == (0.0, 33.0):
        actual_param = 'phosphate'  # Not pH!
```

[Detailed Documentation →](enrich_measurements_from_netcdf_headers_detail.md)

---

### 8. validate_and_fix_data_issues.py ✨ NEW

**Purpose:** Implement data quality fixes identified in enrichment phase.

**Fixes Applied:**
```sql
-- Fix 1: Rename phosphate parameters (ph/PH → phosphate)
UPDATE measurements 
SET parameter_code = 'PHOSPHATE'
WHERE parameter_code IN ('ph', 'PH')
  AND value BETWEEN 0.0 AND 33.0;

-- Fix 2: Convert wind_speed from cm/s to m/s
UPDATE measurements
SET value = value / 100
WHERE parameter_code = 'wind_speed'
  AND value > 50;

-- Fix 3: Flag negative pressure values
UPDATE measurements
SET quality_flag = 3  -- Bad data
WHERE parameter_code IN ('PRES', 'pressure')
  AND value < -10;

-- Fix 4: Remove obviously erroneous silicate values
UPDATE measurements
SET quality_flag = 4  -- Suspicious
WHERE parameter_code = 'SIO4'
  AND value > 500;
```

**Usage:**
```bash
# Review changes first (dry run)
python validate_and_fix_data_issues.py --dry-run

# Apply fixes
python validate_and_fix_data_issues.py --confirm
```

**Safety Features:**
- Dry-run mode shows what will change
- Backups recommendations before applying
- Logs all changes with affected row counts
- Can be rolled back using backup

[Detailed Documentation →](validate_and_fix_data_issues_detail.md)

---

## Execution Order & Dependencies

**Recommended ETL Pipeline Order:**

```
1. populate_metadata.py
   ↓
2. populate_parameter_mappings.py
   ↓
3. populate_measurements.py
   ↓
4. populate_spatial.py
   ↓
5. populate_biological.py
   ↓
6. enrich_metadata_from_xml.py      [NEW - Phase 1]
   ↓
7. enrich_measurements_from_netcdf_headers.py  [NEW - Phase 1]
   ↓
8. validate_and_fix_data_issues.py  [NEW - Phase 1]
```

**Parallel Execution:**
- Steps 4, 5 can run while 3 completes
- Steps 6, 7 can run in parallel (independent data sources)
- Step 8 should run after 6, 7 complete

---

## Performance Optimization

Key optimization techniques across all scripts:

- **Batch Processing:** Scripts use batch inserts (default 1000 rows)
- **Indexing:** Database tables have appropriate indexes
- **Parameter Lookups:** Database queries (~10µs) vs. JSON file reads (~1ms)
- **Memory:** Large files processed incrementally
- **Parallelization:** Scripts can run independently after metadata and parameter mappings load

---

## Troubleshooting

Common issues and solutions:

1. **Connection refused**
   - Check Docker containers: `docker-compose ps`
   - Verify port 5433 is available
   - Check credentials: `DB_USER=marine_user`, `DB_PASSWORD=marine_pass123`

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

6. **XML parsing errors** (enrich_metadata_from_xml.py)
   - Verify XML files are well-formed
   - Check namespace declarations match expected ISO 19115-3
   - Review logs for specific element paths

7. **NetCDF attribute errors** (enrich_measurements_from_netcdf_headers.py)
   - Verify NetCDF files are readable: `ncdump -h <file.nc>`
   - Check for CF conventions compliance
   - Review unit strings for non-standard formats

---

## Contributing

When modifying ETL scripts:

1. Maintain upsert-safe patterns (`ON CONFLICT DO NOTHING`)
2. Add comprehensive logging
3. Update this documentation
4. Update `config_parameter_mapping.json` for new parameters
5. Test with diagnostic_etl.py
6. Verify data integrity with example_data_access.py

---

## Additional Resources

- [Database Schema Documentation](database_schema.md)
- [Data Ingestion Guide](data_ingestion.md)
- [ETL Guide](ETL_GUIDE.md)
- [Project README](../README.md)
- [Parameter Mapping Configuration](../config_parameter_mapping.json)

---

*Last Updated: December 31, 2025*