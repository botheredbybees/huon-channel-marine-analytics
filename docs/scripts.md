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

### 1. populate_metadata.py ⭐ ENHANCED

**Purpose:** Scans the AODN data directory and extracts comprehensive metadata from ISO 19115-3 XML files.

**Key Functions:**
- Discovers datasets in the AODN_data directory structure
- Extracts **30+ metadata fields** from ISO 19115-3 XML files
- Parses spatial and temporal extents with **full bounding box support**
- Extracts **parent/child relationships** via `parent_uuid`
- Parses **distribution URLs** (WFS, WMS, Portal, Publications)
- Handles **multiple credits** (concatenated with `;` separator)
- Extracts metadata creation/revision dates
- Stores dataset metadata in the `metadata` table
- Generates UUIDs for dataset identification

**Enhanced Metadata Fields:**
- `parent_uuid` - Links child datasets to parent collections
- `metadata_creation_date` - When metadata record was created
- `metadata_revision_date` - Last metadata update timestamp
- `credit` - Data contributors/funders (multiple entries concatenated)
- `lineage` - Processing history and data provenance
- `status` - Dataset status (onGoing, completed, etc.)
- `distribution_wfs_url` - OGC Web Feature Service endpoint
- `distribution_wms_url` - OGC Web Map Service endpoint
- `distribution_portal_url` - Data portal URL
- `distribution_publication_url` - Associated publication DOI/URL
- `license_url` - Data license (Creative Commons, etc.)
- `language`, `character_set` - Metadata language and encoding
- **Full bounding box** (west, east, south, north)
- **Temporal extent** (time_start, time_end)
- **Vertical extent** (vertical_min, vertical_max, vertical_crs)

**XML Parsing Features:**
- ✅ ISO 19115-3 namespace handling
- ✅ Multiple XPath patterns for element discovery
- ✅ Fallback logic for incomplete metadata
- ✅ Robust error handling with detailed logging
- ✅ Multi-credit concatenation (e.g., "IMOS; CSIRO; UTAS")

**Usage:**
```bash
# Standard run (discovers and extracts all metadata)
python populate_metadata.py

# Process single dataset
python populate_metadata.py --dataset "Chlorophyll"

# Force re-extraction (updates existing records)
python populate_metadata.py --force
```

**Output Statistics (typical run):**
```
✓ 38 datasets discovered
✓ 30+ fields extracted per dataset
✓ 26/38 datasets with parent_uuid (68%)
✓ 38/38 with metadata_revision_date (100%)
✓ 32/38 with WFS URLs (84%)
✓ 35/38 with WMS URLs (92%)
✓ 19/38 with multiple credits
```

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

### 3. populate_parameters_from_measurements.py ✨ NEW

**Purpose:** Populates the `parameters` table by extracting unique parameter codes from existing measurements.

**Problem Solved:**
After measurements are loaded, the `parameters` table needs to be populated with records for each unique parameter code. This script creates those parameter records with proper metadata.

**Key Functions:**
- Extracts unique parameter codes from `measurements` table
- Generates human-readable parameter labels
- Infers appropriate units for each parameter
- Links to `parameter_mappings` for enriched metadata
- Creates UUID for each parameter
- Handles NULL metadata_id correctly (uses `IS NULL` not `= NULL`)
- Calculates measurement statistics per parameter
- Idempotent operation (safe to run multiple times)

**Features:**
- ✅ Proper NULL handling in SQL queries
- ✅ Generates deterministic UUIDs
- ✅ Maps ~25 common oceanographic parameters
- ✅ Infers units from parameter codes
- ✅ Statistics: mean, stddev, min/max dates

**Usage:**
```bash
# Run after measurements are loaded
python scripts/populate_parameters_from_measurements.py
```

**Expected Output:**
```
Found 70 unique parameter codes
Inserted 70 parameters
Skipped 0 (already existed)

✓ All parameter codes have corresponding parameter records

Top 10 parameters by measurement count:
  • TEMP            Temperature              - 2,500,000 measurements
  • PSAL            Salinity                 - 2,300,000 measurements
  • CPHL            Chlorophyll-a            - 1,800,000 measurements
```

**Critical Fixes (v2):**
- Uses `metadata_id IS NULL` instead of `= NULL` in WHERE clause
- Explicitly sets `metadata_id = NULL` in INSERT statement
- Proper handling of UNIQUE constraint on (parameter_code, metadata_id)

**When to Run:**
- After `populate_measurements.py` completes
- Before running queries that join measurements with parameters
- When measurements table has new parameter codes

---

### 4. analyze_parameter_coverage.py ✨ NEW

**Purpose:** Comprehensive analysis of parameter coverage across all datasets.

**Problem Solved:**
Understand which parameters from metadata actually have measurements, identify gaps in data coverage, and prioritize which datasets or parameters need attention.

**Key Functions:**
- Queries all parameters from metadata XML
- Counts measurements per parameter
- Calculates coverage statistics
- Identifies unmeasured parameters
- Groups results by dataset and content type
- Generates three detailed CSV reports

**Output Files:**

1. **parameter_coverage_YYYYMMDD_HHMMSS.csv**
   - All parameters with measurement counts
   - Coverage status (measured/unmeasured)
   - Organized by dataset

2. **parameter_statistics_YYYYMMDD_HHMMSS.csv**
   - Summary statistics per dataset
   - Parameter counts and percentages
   - Top measured parameters

3. **unmeasured_parameters_YYYYMMDD_HHMMSS.csv**
   - Parameters without any measurements
   - Categorized by content type
   - Potential data sources identified

**Usage:**
```bash
python scripts/analyze_parameter_coverage.py
```

**Expected Output:**
```
Parameter Coverage Analysis
===========================

Overall Statistics:
- Total unique parameters: 361
- Parameters with measurements: 70 (19.4%)
- Parameters without measurements: 291 (80.6%)
- Total measurements: 7,042,616

Top Measured Parameters:
1. TEMP - 2,500,000 measurements (35.5%)
2. PSAL - 2,300,000 measurements (32.7%)
3. CPHL - 1,800,000 measurements (25.6%)

Unmeasured Parameter Categories:
- Biological observations: 187 parameters (65.3% of unmeasured)
- Chemical analyses: 63 parameters (22.0%)
- Physical measurements: 41 parameters (14.3%)

Reports generated:
✓ parameter_coverage_20260105_103518.csv
✓ parameter_statistics_20260105_103518.csv
✓ unmeasured_parameters_20260105_103518.csv
```

**When to Run:**
- After measurements and parameters tables are populated
- Periodically to track data ingestion progress
- Before planning new data collection efforts
- When troubleshooting missing data

**Understanding Coverage:**
The typical 19.4% coverage is expected because:
1. XML metadata lists ALL possible parameters
2. Downloaded data may be regionally/temporally filtered
3. Biological data goes to `species_observations` table
4. Some parameters require specialized equipment

---

### 5. populate_measurements.py

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

### 6. populate_spatial.py ⭐ ENHANCED (v3.0)

**Purpose:** Loads spatial reference data (marine regions, boundaries) from shapefiles.

**Key Functions:**
- Reads ESRI Shapefiles (`.shp`) using ogr2ogr conversion
- Extracts centroid lat/lon coordinates from polygon/point geometries
- Converts to database-compatible format (no PostGIS required)
- Populates `spatial_features` table with region boundaries
- Supports Tasmania marine bioregions, MPAs, and other spatial features
- Preserves shapefile attributes as JSONB properties
- **Handles character encoding issues** (UTF-8, Windows-1252, Latin-1)
- **Force re-processing** with `--force` flag
- **Dataset filtering** with `--dataset` flag
- **Audit trail** via `extracted_at` timestamps

**New in v3.0:**
- ✅ `--force` flag to re-process existing datasets
- ✅ `--dataset` filter for selective processing
- ✅ Robust encoding handling (Windows-1252/CP1252 fallback)
- ✅ Enhanced logging with Unicode symbols (✓, ✗, ⚠️)
- ✅ Summary statistics at completion
- ✅ Timestamp tracking in metadata table
- ✅ Schema fix (removed uuid field)

**Usage:**
```bash
# Process datasets without spatial features
python populate_spatial.py

# Force re-process all datasets
python populate_spatial.py --force

# Process specific dataset
python populate_spatial.py --dataset "SeaMap Tasmania"

# Force re-process specific dataset
python populate_spatial.py --force --dataset "Living Shorelines"
```

**Example Output:**
```
Finding datasets...
Found 7 candidate dataset(s).

Processing 'SeaMap Tasmania benthic habitat map' (1 shapefile(s))...
  ✓ Inserted 1770 spatial features

Processing 'Living Shorelines Australia database' (2 shapefile(s))...
  ✓ Inserted 10 spatial features

============================================================
Processing Complete
============================================================
Datasets processed: 7
New features inserted: 2242
Total spatial features in database: 2242
```

**Requirements:** 
- GDAL/OGR tools installed (`sudo apt-get install gdal-bin`)
- Shapefiles must be in `AODN_data/` subdirectories
- No PostGIS required (uses lat/lon centroids)

**Encoding Fix:**
Previously failed on datasets with Windows-1252 encoded attributes (e.g., "Living Shorelines Australia").
Now handles automatically with fallback encoding strategies.

[Detailed Documentation →](populate_spatial_detail.md)

---

### 7. populate_biological.py

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

**Note:** This is targeted by [Issue #5](https://github.com/botheredbybees/huon-channel-marine-analytics/issues/5) for enhancement to handle 187 unmeasured biological parameters.

[Detailed Documentation →](populate_biological_detail.md)

---

## Metadata Enrichment Scripts (Phase 1)

These NEW scripts enrich existing data with metadata extracted from XML files and NetCDF headers. **Non-destructive:** Only UPDATE NULL/empty fields.

### 8. enrich_metadata_from_xml.py ✨ NEW

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

**Usage:**
```bash
python enrich_metadata_from_xml.py
```

**Safety Features:**
- Only updates NULL/empty fields
- Logs all changes for traceability
- Can be run multiple times safely (idempotent)

---

### 9. enrich_measurements_from_netcdf_headers.py ✨ NEW

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

**Usage:**
```bash
python enrich_measurements_from_netcdf_headers.py
```

---

### 10. validate_and_fix_data_issues.py ✨ NEW

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
```

**Usage:**
```bash
# Review changes first (dry run)
python validate_and_fix_data_issues.py --dry-run

# Apply fixes
python validate_and_fix_data_issues.py --confirm
```

---

## Execution Order & Dependencies

**Recommended ETL Pipeline Order:**

```
1. populate_metadata.py [ENHANCED - 30+ fields]
   ↓
2. populate_parameter_mappings.py
   ↓
3. populate_measurements.py
   ↓
4. populate_parameters_from_measurements.py  [NEW]
   ↓
5. analyze_parameter_coverage.py  [NEW - Optional but recommended]
   ↓
6. populate_spatial.py  [ENHANCED v3.0]
   ↓
7. populate_biological.py
   ↓
8. enrich_metadata_from_xml.py  [NEW - Phase 1]
   ↓
9. enrich_measurements_from_netcdf_headers.py  [NEW - Phase 1]
   ↓
10. validate_and_fix_data_issues.py  [NEW - Phase 1]
```

**Parallel Execution:**
- Steps 6, 7 can run while 3 completes
- Steps 8, 9 can run in parallel (independent data sources)
- Step 10 should run after 8, 9 complete
- Step 5 can run anytime after step 4

---

## Performance Optimization

Key optimization techniques across all scripts:

- **Batch Processing:** Scripts use batch inserts (default 1000 rows)
- **Indexing:** Database tables have appropriate indexes
- **Parameter Lookups:** Database queries (~10µs) vs. JSON file reads (~1ms)
- **Memory:** Large files processed incrementally
- **Parallelization:** Scripts can run independently after metadata and parameter mappings load
- **XML Caching:** Metadata parsed once, stored in database
- **NULL Handling:** Proper SQL NULL comparison (IS NULL not = NULL)
- **Encoding Detection:** Shapefile encoding auto-detected (CP1252, Latin-1 fallback)

---

## Troubleshooting

Common issues and solutions:

### 1. **Connection refused**
   - Check Docker containers: `docker-compose ps`
   - Verify port 5433 is available
   - Check credentials: `DB_USER=marine_user`, `DB_PASSWORD=marine_pass123`

### 2. **File not found**
   - Ensure `AODN_data/` directory exists
   - Check file paths in error messages
   - Verify `config_parameter_mapping.json` exists

### 3. **Encoding errors** (populate_spatial.py)
   - **FIXED in v3.0** - Script now handles automatically
   - If issues persist, check GDAL installation: `ogr2ogr --version`
   - Manually test conversion:
     ```bash
     ogr2ogr -f GeoJSON -t_srs EPSG:4326 \
       --config SHAPE_ENCODING CP1252 \
       test.json problem_file.shp
     ```

### 4. **Memory errors**
   - Use `--limit` flag for testing
   - Process datasets individually

### 5. **Parameter mapping errors**
   - Run `populate_parameter_mappings.py` first
   - Verify `parameter_mappings` table exists
   - Check `config_parameter_mapping.json` is valid JSON

### 6. **Parameter table population errors**
   - Ensure measurements table has data
   - Run `populate_parameters_from_measurements.py`
   - Check for SQL syntax errors with NULL handling

### 7. **Low parameter coverage (<20%)**
   - Expected behavior - metadata lists all possible parameters
   - Run `analyze_parameter_coverage.py` for detailed analysis
   - See [Issue #5](https://github.com/botheredbybees/huon-channel-marine-analytics/issues/5) for biological data ETL
   - See [Issue #7](https://github.com/botheredbybees/huon-channel-marine-analytics/issues/7) for fuzzy matching

### 8. **XML parsing errors** (populate_metadata.py)
   - Verify XML files are well-formed
   - Check namespace declarations match expected ISO 19115-3
   - Review logs for specific element paths
   - Some datasets may have incomplete metadata (logged as warnings)

### 9. **NetCDF attribute errors** (enrich_measurements_from_netcdf_headers.py)
   - Verify NetCDF files are readable: `ncdump -h <file.nc>`
   - Check for CF conventions compliance
   - Review unit strings for non-standard formats

### 10. **Duplicate key violations**
   - Check if running script multiple times (expected with ON CONFLICT DO NOTHING)
   - Review UNIQUE constraints in schema
   - Use proper NULL handling: `IS NULL` not `= NULL`

### 11. **Shapefile spatial_features table missing**
   - **FIXED** - Run this SQL to create table:
     ```sql
     CREATE TABLE IF NOT EXISTS spatial_features (
         id SERIAL PRIMARY KEY,
         metadata_id INTEGER REFERENCES metadata(id),
         latitude DOUBLE PRECISION,
         longitude DOUBLE PRECISION,
         properties JSONB
     );
     CREATE INDEX idx_spatial_features_lat_lon ON spatial_features (latitude, longitude);
     CREATE INDEX idx_spatial_features_metadata_id ON spatial_features(metadata_id);
     ```

---

## Planned Enhancements

Several enhancements have been proposed as GitHub issues:

- **[Issue #5](https://github.com/botheredbybees/huon-channel-marine-analytics/issues/5)**: Create ETL for biological observations
  - Extract 187 unmeasured biological parameters
  - Increase coverage from 19.4% to ~50%
  - Populate `species_observations` table

- **[Issue #6](https://github.com/botheredbybees/huon-channel-marine-analytics/issues/6)**: Add data quality checks
  - Range validation for oceanographic parameters
  - Consistency checks for timestamps
  - Completeness validation

- **[Issue #7](https://github.com/botheredbybees/huon-channel-marine-analytics/issues/7)**: Implement fuzzy parameter matching
  - Intelligent string matching for parameter names
  - Confidence scoring system
  - Reduce manual mapping effort

---

## Contributing

When modifying ETL scripts:

1. Maintain upsert-safe patterns (`ON CONFLICT DO NOTHING`)
2. Use proper NULL handling (`IS NULL` not `= NULL`)
3. Add comprehensive logging
4. Update this documentation
5. Update `config_parameter_mapping.json` for new parameters
6. Test with diagnostic_etl.py
7. Verify data integrity with example_data_access.py
8. Run parameter coverage analysis after changes
9. Handle character encoding issues (UTF-8, CP1252, Latin-1)
10. Add `--force` and `--dataset` arguments where appropriate

---

## Additional Resources

- [Database Schema Documentation](database_schema.md)
- [Data Ingestion Guide](data_ingestion.md)
- [ETL Guide](ETL_GUIDE.md)
- [Project README](../README.md)
- [Parameter Mapping Configuration](../config_parameter_mapping.json)
- [ISO 19115-3 Metadata Standard](https://www.iso.org/standard/32579.html)
- [GitHub Issues](https://github.com/botheredbybees/huon-channel-marine-analytics/issues)

---

*Last Updated: January 6, 2026*