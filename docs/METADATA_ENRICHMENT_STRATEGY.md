# Metadata Enrichment Strategy for AODN/IMOS Data

## Executive Summary

The ETL pipeline has been enhanced with three new metadata enrichment scripts that operate independently of the main ingestion process. These scripts address critical data quality issues by:

1. **Extracting metadata from ISO 19115-3 XML files** and populating empty metadata table fields
2. **Extracting parameter descriptions from NetCDF headers** and updating parameter mappings
3. **Validating and correcting known data quality issues** in the measurements table

This non-destructive, modular approach allows incremental improvements without risking existing data.

---

## Architecture Overview

### Why Three Separate Scripts?

Rather than modifying the existing ETL pipeline, we created focused enrichment scripts because:

- **Non-destructive**: UPDATE statements only, no DELETE/RECREATE operations
- **Parallel execution**: Runs alongside the main ETL pipeline
- **Clear audit trail**: Easy to track what changed and when
- **Reversible**: Can restore from backups if needed
- **Testable**: Can be validated on UAT before production deployment
- **Maintainable**: Each script has a single responsibility

### Script Dependencies

```
┌─────────────────────────────────────┐
│  Main ETL Pipeline (existing)       │
│  - populate_metadata.py             │
│  - populate_measurements.py         │
│  - populate_parameter_mappings.py   │
│  - populate_biological.py           │
└──────────┬──────────────────────────┘
           │ creates tables
           ↓
     ┌─────────────────────────────────┐
     │   Enrichment Phase (new)        │
     ├─────────────────────────────────┤
     │ 1. enrich_metadata_from_xml.py  │
     │    └→ fills: abstract, credit,  │
     │       spatial extent, dates     │
     ├─────────────────────────────────┤
     │ 2. enrich_measurements_from_    │
     │    netcdf.py                    │
     │    └→ fills: parameter desc,    │
     │       units, data types         │
     ├─────────────────────────────────┤
     │ 3. validate_and_fix_data_      │
     │    issues.py                    │
     │    └→ corrects: unit conversions│
     │       param misidentification   │
     │       flags: quality issues     │
     └─────────────────────────────────┘
```

---

## Script Details

### 1. `enrich_metadata_from_xml.py`

**Purpose**: Extract ISO 19115-3 metadata from XML files in dataset directories

**Source Data**:
```
AODN_data/
├── dataset_name_1/
│   └── {uuid}/
│       └── metadata/
│           └── metadata.xml  ← Parse these
├── dataset_name_2/
│   └── {uuid}/
│       └── metadata/
│           └── metadata.xml
...
```

**Extracted Fields**:
- `abstract` - Dataset description (from `gmd:abstract/gco:CharacterString`)
- `credit` - Acknowledgments and data providers (from `gmd:credit/gco:CharacterString`)
- `west`, `east`, `south`, `north` - Geographic bounding box
- `time_start`, `time_end` - Temporal coverage dates
- `lineage` - Processing history and data provenance
- `license_url` - Data usage constraints
- `supplemental_info` - Additional metadata

**Key Features**:
- **Non-destructive**: Only updates NULL fields in metadata table
- **Error handling**: Gracefully skips malformed XML files
- **Batch processing**: Processes all datasets in AODN_data directory
- **Statistics tracking**: Reports files found, processed, and rows updated

**Example Usage**:
```bash
# Set environment variables
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=marine_data
export DB_USER=postgres
export AODN_DATA_PATH=/AODN_data

# Run enrichment
python scripts/enrich_metadata_from_xml.py
```

**Expected Output**:
```
Found 40 metadata.xml files
Processing {uuid-1}: metadata.xml
Updated metadata for {uuid-1}: 1 rows affected
...
METADATA ENRICHMENT SUMMARY
XML files found:     40
Files processed:     40
Rows updated:        35
```

---

### 2. `enrich_measurements_from_netcdf.py`

**Purpose**: Extract parameter metadata from NetCDF file headers and update parameter_mappings table

**Source Data**:
```
AODN_data/
├── dataset_name_1/
│   ├── data_file_1.nc  ← Read NetCDF variables
│   ├── data_file_2.nc
│   └── ...
├── dataset_name_2/
│   ├── data_file_1.nc
│   └── ...
...
```

**Extracted Metadata**:
For each NetCDF variable, extracts CF-compliant attributes:
- `long_name` - Human-readable parameter name
- `standard_name` - CF standard name (if available)
- `units` - Physical units (e.g., 'K', 'mmol/m3', 'm/s')
- `comment` - Additional parameter information
- `valid_min`, `valid_max` - Acceptable value ranges
- `instrument` - Instrument/sensor that measured the parameter

**Validation Steps**:
1. Check if parameter exists in measurements table
2. Verify units make sense given actual data values
3. Flag potential unit conversion issues (e.g., wind_speed > 50 m/s)
4. Detect negative pressure values indicating atmospheric offset

**Key Features**:
- **Smart updating**: Only adds new mappings for CF-compliant parameters
- **Value range validation**: Cross-checks extracted metadata against actual data
- **Issue tracking**: Logs validation warnings for human review
- **Batch processing**: Scans all NetCDF files in dataset hierarchy

**Example Usage**:
```bash
python scripts/enrich_measurements_from_netcdf.py
```

**Expected Output**:
```
Found 156 NetCDF files across 8 datasets
Processing dataset_1: data_file.nc
  ✓ temperature: Valid
  ✓ salinity: Valid
  ✗ wind_speed: Wind units likely wrong (cm/s declared but max=125)
Variables extracted:     542
Variables validated:     520
Variables invalid:       22
Mappings updated:        45
Mappings inserted:       12
```

---

### 3. `validate_and_fix_data_issues.py`

**Purpose**: Detect and correct known data quality issues

**Issues Addressed**:

#### A. Phosphate Parameter Misidentification
- **Problem**: Some datasets labeled phosphate values as 'ph' or 'PH' (pH)
- **Detection**: Values between 0-33 mmol/m³ in specific datasets
- **Fix**: Rename parameter_code from 'ph'/'PH' to 'PHOSPHATE'
- **Affected datasets**: 11, 12, 16, 17, 24, 27, 30, 34

#### B. Wind Speed Unit Conversion
- **Problem**: Satellite altimetry wind speeds in cm/s, not m/s
- **Detection**: Wind speed values > 50 (impossible in m/s)
- **Fix**: Divide values by 100, update units to 'm/s'
- **Affected datasets**: 11 (satellite data)

#### C. Negative Pressure Values
- **Problem**: Some CTD pressure values negative due to atmospheric offset
- **Detection**: Pressure values < 0 dBar
- **Fix**: Flag with quality_flag=2 (questionable), add explanatory comment
- **Root cause**: Data processing applied atmospheric reference incorrectly

#### D. Extreme Silicate Outliers
- **Problem**: Silicate concentrations > 500 mmol/m³ (physically impossible)
- **Detection**: Values exceeding known maximum concentrations
- **Fix**: Flag with quality_flag=3 (bad data)
- **Note**: Suggests sensor error or data entry mistakes

**Quality Flags Used**:
- `1` (Good) - Default for valid data
- `2` (Questionable) - Data quality uncertain (negative pressure, etc.)
- `3` (Bad) - Data known to be invalid or corrected
- `4` (Missing) - Data not available

**Key Features**:
- **Validation phase**: Detects issues before attempting fixes
- **Dry-run mode**: Test corrections without committing to database
- **Audit trail**: Comments on measurements record what was fixed
- **Reversible**: Can be rolled back if issues are detected

**Example Usage**:
```bash
# Dry-run mode: see what would change
python scripts/validate_and_fix_data_issues.py --dry-run

# Production mode: apply corrections
python scripts/validate_and_fix_data_issues.py

# Or via environment variable
export DRY_RUN=1
python scripts/validate_and_fix_data_issues.py
```

**Expected Output**:
```
DRY RUN MODE - No database changes will be committed

VALIDATION PHASE: Detecting known data quality issues
Found 427 potentially misidentified phosphate values
Found 156 wind_speed values > 50 (likely cm/s not m/s)
Found 89 negative pressure values (likely atmospheric offset)
Found 34 silicate values > 500 (outliers)

Detected 4 issue type(s):
  - phosphate_misidentification
  - wind_speed_unit_conversion
  - negative_pressure_values
  - silicate_outliers

CORRECTION PHASE: Applying fixes
[DRY RUN] Fix phosphate parameter misidentification: 427 rows would be affected
[DRY RUN] Fix wind_speed unit conversion: 156 rows would be affected
[DRY RUN] Flag negative pressure values: 89 rows would be affected
[DRY RUN] Flag silicate outliers: 34 rows would be affected

DRY RUN MODE: No changes were committed to the database
```

---

## Implementation Timeline

### Phase 1: Week 1 - UAT Testing
**Goals**: Validate scripts in controlled environment

1. **Monday-Tuesday**: 
   - Deploy scripts to UAT database
   - Run `enrich_metadata_from_xml.py` on 2-3 test datasets
   - Verify metadata table updates are correct
   - Capture before/after statistics

2. **Wednesday-Thursday**:
   - Run `enrich_measurements_from_netcdf.py` on test datasets
   - Validate parameter_mappings updates
   - Review validation issues log

3. **Friday**:
   - Run `validate_and_fix_data_issues.py` in dry-run mode
   - Verify detected issues match expectations
   - Get approval for production deployment

### Phase 2: Week 2 - Production Deployment
**Goals**: Deploy to production with minimal risk

1. **Monday**: 
   - Create database backup
   - Run all three scripts on production in dry-run mode
   - Review output with team

2. **Tuesday-Wednesday**:
   - Deploy scripts to production (no execution yet)
   - Set up scheduled cron jobs for weekly enrichment
   - Create monitoring/alerting for script failures

3. **Thursday**:
   - Execute all three scripts in order
   - Monitor for errors/warnings
   - Verify data quality improvements

4. **Friday**:
   - Document results and lessons learned
   - Plan Phase 3 (integration into main ETL)

### Phase 3: Week 3-4 - Integration
**Goals**: Integrate enrichment into main ETL pipeline

1. Create orchestration script that runs all stages
2. Update ETL documentation
3. Schedule enrichment as Stage 4 of pipeline
4. Monitor for ongoing data quality issues

---

## Environment Variables

All scripts use these environment variables (with defaults):

```bash
# Database connection
DB_HOST=localhost          # PostgreSQL server hostname
DB_PORT=5432              # PostgreSQL port
DB_NAME=marine_data       # Database name
DB_USER=postgres          # Database user
DB_PASSWORD=              # Database password (empty by default)

# Data paths
AODN_DATA_PATH=/AODN_data # Root directory containing AODN datasets

# Script behavior
DRY_RUN=0                 # Set to 1 for validate_and_fix_data_issues.py
```

---

## Running the Scripts

### Option A: Individual scripts
```bash
cd /path/to/repo

# Enrich metadata from XML
python scripts/enrich_metadata_from_xml.py

# Enrich measurements from NetCDF
python scripts/enrich_measurements_from_netcdf.py

# Validate and fix issues (dry-run first!)
python scripts/validate_and_fix_data_issues.py --dry-run
python scripts/validate_and_fix_data_issues.py
```

### Option B: With Docker
```bash
# Using docker-compose
docker-compose exec app python scripts/enrich_metadata_from_xml.py
docker-compose exec app python scripts/enrich_measurements_from_netcdf.py
docker-compose exec app python scripts/validate_and_fix_data_issues.py
```

### Option C: Scheduled (cron job)
```bash
# Weekly enrichment (runs every Friday at 2 AM)
0 2 * * 5 cd /path/to/repo && python scripts/enrich_metadata_from_xml.py >> /var/log/enrichment.log 2>&1
5 2 * * 5 cd /path/to/repo && python scripts/enrich_measurements_from_netcdf.py >> /var/log/enrichment.log 2>&1
10 2 * * 5 cd /path/to/repo && python scripts/validate_and_fix_data_issues.py >> /var/log/enrichment.log 2>&1
```

---

## Monitoring and Troubleshooting

### Log Files
Each script outputs structured logs to stdout with timestamps and log levels:
- `INFO`: Normal operations
- `WARNING`: Potential issues (validation warnings, etc.)
- `ERROR`: Script failures that don't halt execution
- `DEBUG`: Detailed operation information

### Key Metrics to Monitor

**Metadata Enrichment**:
- Files found vs. processed (should be ~100%)
- Rows updated (should be > 0 if metadata was empty)
- Error rate (should be 0%)

**NetCDF Enrichment**:
- Variables extracted vs. validated (should be > 90%)
- Validation issues (review to identify data problems)
- Mappings inserted vs. updated (indicates new parameters discovered)

**Data Validation**:
- Issues detected vs. corrected (should be 100%)
- Rows affected by each type of fix
- Any errors during correction

### Common Issues

**Issue**: "No metadata.xml files found"
- **Cause**: `AODN_DATA_PATH` not set correctly
- **Fix**: Verify path exists and contains dataset directories

**Issue**: "permission denied" errors
- **Cause**: Script lacks read access to AODN_data directory
- **Fix**: Check file permissions, run with proper user/role

**Issue**: Database connection timeout
- **Cause**: PostgreSQL not running or network issue
- **Fix**: Verify database is accessible, check `DB_HOST` and `DB_PORT`

**Issue**: "ncdf4 module not found"
- **Cause**: netCDF4 Python library not installed
- **Fix**: Run `pip install netCDF4` or check requirements.txt

---

## Performance Considerations

### Execution Times (Approximate)
- **enrich_metadata_from_xml.py**: 2-5 minutes for 40 datasets
- **enrich_measurements_from_netcdf.py**: 5-15 minutes for 150 files
- **validate_and_fix_data_issues.py**: 1-3 minutes for all validations/corrections

### Database Impact
- Minimal during enrichment (indexed lookups)
- UPDATE statements are batched for efficiency
- Read-only validation phase has no locking impact

### Recommended Schedule
- Run enrichment scripts weekly (after new data ingestion)
- Run validation/fixes monthly (or on-demand)
- Archive logs for audit trail

---

## Next Steps

1. **Deploy to UAT**: Test scripts against copy of production data
2. **Gather feedback**: Review results with data quality team
3. **Refine metadata extraction**: Adjust XML/NetCDF parsing as needed
4. **Deploy to production**: Follow implementation timeline above
5. **Monitor quality metrics**: Track data quality improvements over time
6. **Document lessons learned**: Update this guide based on production experience

---

## References

- [ISO 19115-3 Geographic Information Metadata](https://www.iso.org/standard/67039.html)
- [CF Conventions - Climate and Forecast Metadata](https://cfconventions.org/)
- [NetCDF4 Python Documentation](https://unidata.github.io/netcdf4-python/)
- [AODN Data Portal](https://portal.aodn.org.au/)
- [IMOS Data Standards](https://data.aodn.org.au/)
