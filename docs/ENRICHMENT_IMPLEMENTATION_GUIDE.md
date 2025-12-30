# Metadata Enrichment Implementation Guide

## Quick Start

For the impatient: run these commands in order

```bash
# 1. Test in dry-run mode first
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=marine_data
export DB_USER=postgres
export AODN_DATA_PATH=/AODN_data

# Check what would happen
python scripts/enrich_metadata_from_xml.py
python scripts/enrich_measurements_from_netcdf.py
python scripts/validate_and_fix_data_issues.py --dry-run

# 2. If happy with results, apply corrections
python scripts/validate_and_fix_data_issues.py
```

---

## Prerequisites

### Software Requirements

1. **Python 3.7+**
   ```bash
   python --version
   ```

2. **Required Python packages** (install via pip)
   ```bash
   pip install psycopg2-binary netCDF4
   ```

3. **PostgreSQL client tools** (for database access)
   ```bash
   psql --version
   ```

### Data Requirements

1. **AODN Data Directory Structure**
   ```
   /AODN_data/
   ├── dataset_001/
   │   ├── {uuid-1}/
   │   │   ├── metadata/metadata.xml
   │   │   ├── data_file.nc
   │   │   └── data_file.csv
   │   ├── {uuid-2}/
   │   └── ...
   ├── dataset_002/
   └── ...
   ```

2. **Database must have been populated by main ETL pipeline**
   - `metadata` table with dataset records
   - `measurements` table with parameter values
   - `parameter_mappings` table (even if sparse)

### Access Requirements

1. **Read access to AODN_data directory**
   ```bash
   ls -la /AODN_data  # Should succeed
   ```

2. **Database write access** (UPDATE privileges on tables)
   ```bash
   # Test connection
   psql -h localhost -U postgres -d marine_data -c "SELECT 1"
   ```

---

## Step 1: Prepare Environment

### 1.1 Clone or Update Repository

```bash
cd /path/to/repo
git pull origin main
```

### 1.2 Set Environment Variables

```bash
# For bash/zsh - add to ~/.bashrc or ~/.zshrc
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=marine_data
export DB_USER=postgres
export DB_PASSWORD=your_password  # if needed
export AODN_DATA_PATH=/AODN_data

# Then reload
source ~/.bashrc
```

**Or set them inline**:
```bash
DB_HOST=localhost DB_PORT=5432 python scripts/enrich_metadata_from_xml.py
```

### 1.3 Verify Database Connection

```bash
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT 
  (SELECT COUNT(*) FROM metadata) as metadata_count,
  (SELECT COUNT(*) FROM measurements) as measurements_count,
  (SELECT COUNT(*) FROM parameter_mappings) as parameter_count;
"
```

Should output something like:
```
 metadata_count | measurements_count | parameter_count
---------------+--------------------+-----------------
         40    |      7250000       |       50
(1 row)
```

---

## Step 2: Create Database Backup

**CRITICAL: Always back up before running corrections!**

### Option A: Full Database Dump

```bash
# Single file backup
pg_dump -h $DB_HOST -U $DB_USER -d $DB_NAME > marine_data_backup_$(date +%Y%m%d).sql

# With compression
pg_dump -h $DB_HOST -U $DB_USER -d $DB_NAME | gzip > marine_data_backup_$(date +%Y%m%d).sql.gz
```

### Option B: Selective Table Backup

```bash
# Back up only the tables we'll modify
pg_dump -h $DB_HOST -U $DB_USER -d $DB_NAME -t metadata -t measurements -t parameter_mappings \
  > marine_data_tables_backup_$(date +%Y%m%d).sql
```

### Option C: Using Docker

```bash
# If using docker-compose
docker-compose exec -T db pg_dump -U postgres marine_data > backup_$(date +%Y%m%d).sql
```

**Verify backup succeeded**:
```bash
ls -lh marine_data_backup*.sql*
head -20 marine_data_backup_*.sql  # Should show SQL commands
```

---

## Step 3: Run Metadata Enrichment

### 3.1 Enrich from XML

```bash
echo "Starting metadata enrichment from XML files..."
python scripts/enrich_metadata_from_xml.py
```

**Expected output**:
```
2025-12-30 10:15:30,123 - __main__ - INFO - Connected to marine_data
2025-12-30 10:15:30,456 - __main__ - INFO - Found 40 metadata.xml files
2025-12-30 10:15:31,000 - __main__ - INFO - Processing {uuid-1}: metadata.xml
2025-12-30 10:15:31,500 - __main__ - INFO - Updated metadata for {uuid-1}: 1 rows affected
...
============================================================
METADATA ENRICHMENT SUMMARY
============================================================
XML files found:     40
Files processed:     40
Rows updated:        35
============================================================
```

**What to look for**:
- ✓ Files found > 0
- ✓ Rows updated > 0
- ✓ No ERROR messages
- ⚠ Some files may fail gracefully (fine if most succeed)

**Troubleshooting**:

If "Found 0 metadata.xml files":
```bash
# Check AODN_data path
find /AODN_data -name "metadata.xml" | head -5
echo "Found $(find /AODN_data -name 'metadata.xml' | wc -l) metadata.xml files"
```

If database errors:
```bash
# Test connection explicitly
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "UPDATE metadata SET abstract='test' WHERE id=1; ROLLBACK;"
```

### 3.2 Enrich from NetCDF Headers

```bash
echo "Starting parameter enrichment from NetCDF files..."
python scripts/enrich_measurements_from_netcdf.py
```

**Expected output**:
```
2025-12-30 10:20:00,000 - __main__ - INFO - Connected to marine_data
2025-12-30 10:20:01,000 - __main__ - INFO - Found 156 NetCDF files across 8 datasets
2025-12-30 10:20:05,000 - __main__ - INFO - Processing dataset_1: data_file.nc
2025-12-30 10:20:05,500 - __main__ - DEBUG -   ✓ temperature: Valid
2025-12-30 10:20:05,600 - __main__ - INFO -   ✗ wind_speed: Wind units likely wrong
...
======================================================================
NETCDF METADATA ENRICHMENT SUMMARY
======================================================================
NetCDF files found:      156
Files processed:         156
Files failed:            0
Variables extracted:     542
Variables validated:     520
Variables invalid:       22
Mappings updated:        45
Mappings inserted:       12
======================================================================

VALIDATION ISSUES FOUND:
  wind_speed: Wind units likely wrong (cm/s declared but max=125)
  pressure: Found 89 negative pressure values
```

**What to look for**:
- ✓ Files found > 0
- ✓ Variables validated > 0
- ✓ Mappings updated or inserted > 0
- ⚠ Some validation issues are expected (review them)
- ⚠ Some variables may fail validation (document why)

**Review validation issues**:
```bash
# If issues detected, investigate specific parameters
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT parameter_code, COUNT(*), MIN(value), MAX(value)
FROM measurements
WHERE parameter_code = 'wind_speed'
GROUP BY parameter_code;
"
```

---

## Step 4: Validate Data Issues (Dry Run)

**ALWAYS run dry-run first!**

```bash
echo "Running data validation in DRY RUN mode..."
python scripts/validate_and_fix_data_issues.py --dry-run
```

**Expected output**:
```
2025-12-30 10:25:00,000 - __main__ - INFO - *** DRY RUN MODE - No database changes will be committed ***

VALIDATION PHASE: Detecting known data quality issues
2025-12-30 10:25:05,000 - __main__ - INFO - Found 427 potentially misidentified phosphate values
2025-12-30 10:25:10,000 - __main__ - INFO - Found 156 wind_speed values > 50 (likely cm/s not m/s)
2025-12-30 10:25:15,000 - __main__ - INFO - Found 89 negative pressure values
2025-12-30 10:25:20,000 - __main__ - INFO - Found 34 silicate values > 500

Detected 4 issue type(s):
  - phosphate_misidentification
  - wind_speed_unit_conversion
  - negative_pressure_values
  - silicate_outliers

CORRECTION PHASE: Applying fixes
[DRY RUN] Fix phosphate parameter misidentification: 427 rows would be affected
[DRY RUN] Fix wind_speed unit conversion: 156 rows would be affected
[DRY RUN] Flag negative pressure values: 89 rows would be affected
[DRY RUN] Add comments to atmospheric offset pressures: 15 rows would be affected
[DRY RUN] Flag silicate outliers: 34 rows would be affected

DATA VALIDATION & CORRECTION SUMMARY
================================================================================
Issues detected:             4
Issues corrected:            0  (dry-run mode)
Phosphate values fixed:      427
Wind speed values fixed:     156
Pressure values flagged:     89
Silicate values flagged:     34

⚠ DRY RUN MODE: No changes were committed to the database
```

**Review the dry-run output**:

- Are the affected row counts reasonable?
- Are the issue types correct?
- Do you want to proceed with corrections?

**If satisfied**, proceed to Step 5. **If concerns**, investigate first:

```bash
# Example: Check phosphate values that would be renamed
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT DISTINCT parameter_code, COUNT(*), MIN(value), MAX(value), ARRAY_AGG(DISTINCT metadata_id)
FROM measurements
WHERE parameter_code IN ('ph', 'PH')
  AND value BETWEEN 0 AND 33
GROUP BY parameter_code;
"
```

---

## Step 5: Apply Corrections

```bash
echo "Applying data corrections..."
python scripts/validate_and_fix_data_issues.py
```

**Expected output**:
```
VALIDATION PHASE: Detecting known data quality issues
Found 427 potentially misidentified phosphate values
Found 156 wind_speed values > 50 (likely cm/s not m/s)
Found 89 negative pressure values
Found 34 silicate values > 500

Detected 4 issue type(s):
  - phosphate_misidentification
  - wind_speed_unit_conversion
  - negative_pressure_values
  - silicate_outliers

CORRECTION PHASE: Applying fixes
✓ Fix phosphate parameter misidentification: 427 rows affected
✓ Fix wind_speed unit conversion: 156 rows affected
✓ Flag negative pressure values: 89 rows affected
✓ Add comments to atmospheric offset pressures: 15 rows affected
✓ Flag silicate outliers: 34 rows affected

DATA VALIDATION & CORRECTION SUMMARY
================================================================================
Issues detected:             4
Issues corrected:            4
Phosphate values fixed:      427
Wind speed values fixed:     156
Pressure values flagged:     89
Silicate values flagged:     34
```

**If errors occur**:

1. **Check the error message**: Usually indicates database issue
2. **Restore from backup** if needed:
   ```bash
   psql -h $DB_HOST -U $DB_USER -d $DB_NAME < marine_data_backup_20251230.sql
   ```
3. **Contact database admin** if connection issues

---

## Step 6: Verify Results

### 6.1 Check Database Updates

```bash
# Verify metadata was enriched
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT 
  (SELECT COUNT(*) FROM metadata WHERE abstract IS NOT NULL) as metadata_with_abstract,
  (SELECT COUNT(*) FROM metadata WHERE west IS NOT NULL) as metadata_with_spatial,
  (SELECT COUNT(*) FROM metadata WHERE time_start IS NOT NULL) as metadata_with_temporal
;
"

# Verify parameter mappings were updated
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT 
  (SELECT COUNT(*) FROM parameter_mappings WHERE description IS NOT NULL) as mappings_with_desc,
  (SELECT COUNT(*) FROM parameter_mappings WHERE unit IS NOT NULL) as mappings_with_unit,
  (SELECT COUNT(*) FROM parameter_mappings WHERE source = 'netcdf_header') as new_netcdf_mappings
;
"

# Verify corrections were applied
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT parameter_code, COUNT(*) as count
FROM measurements
WHERE parameter_code IN ('PHOSPHATE', 'wind_speed', 'pressure')
GROUP BY parameter_code
ORDER BY count DESC;
"
```

### 6.2 Sample Data Quality Checks

```bash
# Check phosphate values were fixed
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT 'ph_old' as param, COUNT(*) FROM measurements WHERE parameter_code IN ('ph', 'PH')
UNION ALL
SELECT 'PHOSPHATE_new', COUNT(*) FROM measurements WHERE parameter_code = 'PHOSPHATE';
"

# Check wind_speed units
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT units, COUNT(*), MIN(value), MAX(value)
FROM measurements
WHERE parameter_code = 'wind_speed'
GROUP BY units;
"

# Check flagged data
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT quality_flag, parameter_code, COUNT(*)
FROM measurements
WHERE quality_flag > 1
GROUP BY quality_flag, parameter_code
ORDER BY quality_flag, COUNT(*) DESC;
"
```

---

## Step 7: Document Results

Create a log file with execution results:

```bash
cat > enrichment_log_$(date +%Y%m%d).txt << 'EOF'
METADATA ENRICHMENT EXECUTION LOG
Date: $(date)

Environment:
  DB_HOST: $DB_HOST
  DB_PORT: $DB_PORT
  DB_NAME: $DB_NAME
  AODN_DATA_PATH: $AODN_DATA_PATH

Scripts Run:
1. enrich_metadata_from_xml.py
2. enrich_measurements_from_netcdf.py
3. validate_and_fix_data_issues.py

Key Statistics:
$(psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT '  Metadata records enriched: ' || (SELECT COUNT(*) FROM metadata WHERE abstract IS NOT NULL);
")

Issues Fixed:
  - Phosphate misidentification: 427 records
  - Wind speed unit conversion: 156 records
  - Pressure value flagging: 89 records
  - Silicate outlier flagging: 34 records

Total records corrected: 706

Notes:
  - Database backed up before corrections
  - All corrections validated in dry-run mode before execution
  - No errors or warnings during execution
EOF

cat enrichment_log_*.txt
```

---

## Troubleshooting

### Issue: "psycopg2 not found"

```bash
pip install psycopg2-binary
# Or if building from source
pip install --upgrade pip
pip install psycopg2
```

### Issue: "netCDF4 not found"

```bash
# macOS/Linux
pip install netCDF4

# If that fails, may need dependencies
brew install netcdf  # macOS
apt-get install libnetcdf-dev  # Ubuntu/Debian
pip install netCDF4
```

### Issue: "Could not connect to database"

```bash
# Test connection parameters
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT 1"

# If fails, check:
echo "Trying localhost..."
psql -h localhost -U postgres -d marine_data

# Or check connection string
echo "DB_HOST=$DB_HOST DB_PORT=$DB_PORT DB_NAME=$DB_NAME DB_USER=$DB_USER"
```

### Issue: "Permission denied reading AODN_data"

```bash
# Check permissions
ls -la /AODN_data
find /AODN_data -type f -name "metadata.xml" 2>&1 | head

# If permission issue, may need to run as different user
sudo -u data_user python scripts/enrich_metadata_from_xml.py
```

### Issue: "No metadata.xml files found"

```bash
# Verify files exist
find /AODN_data -name "metadata.xml" | wc -l

# Check format of path in code
# Path should be: /AODN_data/<dataset>/<uuid>/metadata/metadata.xml
find /AODN_data -type d -name "metadata" | head -5
```

---

## Next Steps

1. **Schedule regular runs**: Add to cron for weekly execution
2. **Monitor for issues**: Check logs regularly
3. **Document findings**: Keep record of what was fixed
4. **Plan integration**: Merge into main ETL pipeline
5. **Continuous improvement**: Update scripts as new data patterns discovered

---

## Support

For issues or questions:

1. Check the troubleshooting section above
2. Review `METADATA_ENRICHMENT_STRATEGY.md` for detailed architecture
3. Check script logs for specific error messages
4. Contact the data engineering team
