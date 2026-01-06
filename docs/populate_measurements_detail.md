# populate_measurements.py - Detailed Documentation

## Overview

`populate_measurements.py` is the core ETL script that extracts oceanographic measurements from CSV and NetCDF files, standardizes parameters, validates locations, and loads data into the `measurements` table. This script features multi-parameter detection, intelligent time parsing, and pure PostgreSQL location matching.

**Script Version:** 3.3 (Smart PH/Phosphorus Disambiguation)  
**Dependencies:** `psycopg2`, `pandas`, `numpy`, `netCDF4`, `cftime`  
**Estimated Runtime:** 5-30 minutes per dataset (varies by file size)

---

## Key Features

### v3.3 Changes (January 2026)

- **💡 Smart PH/Phosphorus Detection** - Automatically distinguishes pH (acidity) from phosphate (nutrient) based on value distributions
- **Parameter Code Mapping** - 'phosphate' automatically mapped to 'PO4' with 'bodc' namespace
- **Ambiguity Logging** - Warns when unclear cases detected for manual review
- **Prevention System** - Prevents recurrence of Issue #5 (PH parameter ambiguity)

### v3.2 Changes (December 2025)

- **PostGIS Removed** - Pure PostgreSQL implementation
- **Updated Connection Config** - Port 5433, password `marine_pass123`
- **Schema Compatibility** - Uses `id` column (not `metadata_id`) for metadata table
- **Location Matching** - Simple coordinate proximity using `ABS()` function
- **Enhanced Error Logging** - Full traceback output for debugging

### v3.1 Features (Preserved)

- **Multi-Parameter CSV Extraction** - One row with TEMP, SALINITY, PH → Multiple measurement records
- **Intelligent Parameter Detection** - Auto-detects ~25 common marine parameters by column name
- **Unit Inference** - Extracts units from column names (e.g., TEMP_C → degrees_celsius)
- **Wide & Long Format Support** - Handles both parameter-as-columns and parameter-as-rows
- **QC Column Filtering** - Quality control columns automatically excluded

### v3.0 Features (Preserved)

- **NetCDF Time Parsing** - Returns `datetime` objects (not tuples)
- **Parameter Mapping** - Loads from `parameter_mappings` table
- **Location Extraction** - Reads station info from CSV/NetCDF headers
- **Batch Processing** - Efficient bulk insertion (1000 rows per batch)

### Guardrails

✓ **Upsert-Safe** - `INSERT ... ON CONFLICT DO NOTHING`  
✓ **Audit Trail** - QC flags track all modifications  
✓ **Schema Validation** - Type checking before database write  
✓ **Error Recovery** - Failed rows skipped with logging, no transaction rollback  
✓ **QC Column Filtering** - Quality control columns excluded from measurements table  
✓ **PostGIS-Free** - Pure SQL queries for maximum portability  
✓ **🆕 Smart PH Detection** - Prevents pH/phosphorus confusion (Issue #5 fix)

---

## Related Scripts

After running this script, you should run:
- **`populate_parameters_from_measurements.py`** - Populates the `parameters` table with records for each unique parameter code found in measurements. This script properly handles NULL metadata_id values and creates parameter records with UUIDs.

---

## Database Connection (v3.2)

### Updated Configuration

```python
def get_db_connection():
    """Create database connection."""
    return psycopg2.connect(
        host="localhost",
        port=5433,        # Changed from 5432
        dbname="marine_db",
        user="marine_user",
        password="marine_pass123"  # Changed from marine_pass
    )
```

**Critical Changes:**
- Port: `5432` → `5433`
- Password: `marine_pass` → `marine_pass123`

**Common Connection Errors:**

```bash
# Error: Port 5432 connection refused
# Solution: Update to port 5433

# Error: Password authentication failed
# Solution: Update password to marine_pass123
```

---

## Location Handling (PostGIS-Free Implementation)

### Old PostGIS-Based Approach (v3.1)

```python
# REMOVED - No longer works
cursor.execute("""
    SELECT id FROM locations 
    WHERE ST_DWithin(
        ST_SetSRID(ST_MakePoint(%s, %s), 4326),
        geom,
        0.0001
    )
    LIMIT 1
""", (longitude, latitude))
```

### New Pure SQL Approach (v3.2)

```python
def get_or_create_location(cursor, latitude: float, longitude: float, metadata_id: int) -> Optional[int]:
    """Get existing location ID or create new one if coordinates are valid."""
    
    # Validate coordinates
    if not (-90 <= latitude <= 90) or not (-180 <= longitude <= 180):
        return None
        
    # Try to find existing location (within 0.0001 degrees ~ 11 meters)
    cursor.execute("""
        SELECT id 
        FROM locations 
        WHERE ABS(latitude - %s) < 0.0001 
          AND ABS(longitude - %s) < 0.0001
        LIMIT 1
    """, (latitude, longitude))
    
    result = cursor.fetchone()
    if result:
        return result[0]
    
    # Create new location
    cursor.execute("""
        INSERT INTO locations (latitude, longitude)
        VALUES (%s, %s)
        RETURNING id
    """, (latitude, longitude))
    
    return cursor.fetchone()[0]
```

**Key Changes:**
- `ST_DWithin()` → `ABS(latitude - %s) < 0.0001`
- `ST_SetSRID(ST_MakePoint(...))` → Direct latitude/longitude values
- Same tolerance (0.0001 degrees ≈ 11 meters at Tasmanian latitudes)
- Simpler, faster, no PostGIS dependency

**Performance Impact:**
- PostGIS query: ~500µs per lookup
- Pure SQL query: ~100µs per lookup
- **5x faster** without geometry overhead

---

## Schema Compatibility (v3.2)

### Metadata Table Query Fix

**Old (Broken):**
```python
cursor.execute("""
    SELECT metadata_id, title, dataset_path  # Column doesn't exist!
    FROM metadata
    WHERE dataset_path IS NOT NULL
    ORDER BY metadata_id
""")
```

**New (Correct):**
```python
cursor.execute("""
    SELECT id, title, dataset_path  # Use 'id' not 'metadata_id'
    FROM metadata
    WHERE dataset_path IS NOT NULL
    ORDER BY id
""")
```

**Error Message (if using old code):**
```
ERROR: column "metadata_id" does not exist
LINE 2: SELECT metadata_id, title, file_path
```

**Solution:** The `metadata` table primary key is named `id`, not `metadata_id`.

---

## Smart Parameter Detection (v3.3)

### PH/Phosphorus Disambiguation

**Issue Addressed:** [DATA_QUALITY_ISSUES_AND_FIXES.md Issue #5](DATA_QUALITY_ISSUES_AND_FIXES.md#issue-5--ph-parameter-ambiguity-discovered-2026-01-07-resolved-same-day)

The script now intelligently distinguishes between:
- **pH** (acidity/alkalinity, range 6-9)
- **Phosphate** (nutrient concentration, range -2 to 4 mg/L)

When a column is named exactly `PH` (ambiguous), the script analyzes value distribution:

```python
def smart_detect_ph_or_phosphate(column_name: str, values: pd.Series) -> str:
    """
    Intelligently detect whether 'PH' column is pH or phosphate based on value range.
    
    Rules:
    - If >80% of values in 6-9 range → 'ph' (acidity)
    - If >80% of values in -2 to 4 range → 'phosphate' (concentration)
    - If mixed/unclear → log warning and default to 'ph' for safety
    """
    col_lower = str(column_name).lower()
    
    # Explicit phosphate indicators
    if any(keyword in col_lower for keyword in ['phosph', 'po4', 'phos']):
        return 'phosphate'
    
    # Explicit pH indicators
    if any(keyword in col_lower for keyword in ['ph_', 'acidity']):
        return 'ph'
    
    # Ambiguous 'PH' - use value-based detection
    if col_lower == 'ph':
        numeric_values = pd.to_numeric(values, errors='coerce').dropna()
        
        ph_range = numeric_values[(numeric_values >= 6) & (numeric_values <= 9)].count()
        phosphate_range = numeric_values[(numeric_values >= -2) & (numeric_values <= 4)].count()
        total = len(numeric_values)
        
        ph_pct = (ph_range / total) * 100 if total > 0 else 0
        phosphate_pct = (phosphate_range / total) * 100 if total > 0 else 0
        
        if ph_pct > 80:
            logger.info(f"    ✓ Column '{column_name}' detected as pH (acidity) - {ph_pct:.1f}% in 6-9 range")
            return 'ph'
        elif phosphate_pct > 80:
            logger.info(f"    ✓ Column '{column_name}' detected as PHOSPHATE - {phosphate_pct:.1f}% in -2 to 4 range")
            return 'phosphate'
        else:
            logger.warning(
                f"    ⚠️ AMBIGUOUS: Column '{column_name}' unclear - "
                f"pH range: {ph_pct:.1f}%, phosphate range: {phosphate_pct:.1f}%. "
                f"Defaulting to 'ph' - MANUAL REVIEW RECOMMENDED"
            )
            return 'ph'  # Conservative default
    
    return 'ph'
```

**Example Log Output:**

```
📂 Processing: Chlorophyll sampling in Tasmania
  📊 Processing 3 CSV files
    ✓ Detected 5 parameters: ['temperature', 'salinity', 'ph', 'chlorophyll', 'turbidity']
    ✓ Column 'PH' detected as PHOSPHATE - 94.3% in -2 to 4 range
    ✓ Parameter 'phosphate' mapped to code 'PO4' (namespace: bodc)
    ✓ Inserted 6,268 measurements
```

**Ambiguous Case Warning:**

```
    ⚠️ AMBIGUOUS: Column 'PH' unclear - pH range: 45.2%, phosphate range: 52.1%.
    Defaulting to 'ph' - MANUAL REVIEW RECOMMENDED
```

### Parameter Code Mapping

The script automatically maps parameter names to standard codes:

```python
# Map parameter names to standard codes
param_code = 'PO4' if param_name == 'phosphate' else param_name.upper()
namespace = 'bodc' if param_name in ['phosphate', 'ph'] else 'custom'
```

**Result:**
- `phosphate` → `PO4` (namespace: `bodc`)
- `ph` → `PH` (namespace: `bodc`)
- Other parameters → UPPERCASE (namespace: `custom`)

---

## Parameter Detection

### Supported Parameters (v3.3)

| Parameter | Column Keywords | Example Columns | Standard Code | Namespace |
|-----------|----------------|-----------------|---------------|----------|
| **Temperature** | temp, temperature, sst, sbt | TEMP_C, SURFACE_TEMPERATURE, SST | TEMPERATURE | custom |
| **Salinity** | sal, salinity, psal | SALINITY_PSU, PSAL, SAL | SALINITY | custom |
| **Pressure** | pres, pressure, depth | PRES, PRESSURE_DBAR, DEPTH | PRESSURE | custom |
| **Dissolved Oxygen** | oxygen, o2, doxy | DOXY, O2_SAT, DISSOLVED_OXYGEN | OXYGEN | custom |
| **Chlorophyll** | chlorophyll, chl, chla, cphl | CHL_A, CHLOROPHYLL, CPHL | CHLOROPHYLL | custom |
| **Turbidity** | turbidity, turb, ntu | TURBIDITY_NTU, TURB | TURBIDITY | custom |
| **🆕 pH** | ph_total, ph_insitu, ph_seawater | PH_TOTAL, PH_INSITU | **PH** | **bodc** |
| **🆕 Phosphate** | phosphate, po4, phos, phosphorus | PHOSPHATE, PO4, PHOS | **PO4** | **bodc** |
| **Current Speed** | current, velocity, ucur, vcur | CURRENT_SPEED, UCUR, VCUR | CURRENT_SPEED | custom |
| **Wave Height** | wave_height, hs | WAVE_HEIGHT, HS | WAVE_HEIGHT | custom |
| **Wind Speed** | wind_speed, wspd | WIND_SPEED, WSPD | WIND_SPEED | custom |

**🆕 New in v3.3:**
- **pH** and **Phosphate** now separate parameters (previously both matched 'ph')
- Ambiguous `PH` columns automatically classified by value distribution
- Standard codes `PH` and `PO4` assigned with `bodc` namespace

### QC Column Filtering

**Automatically excluded column patterns:**
- `*_QUALITY_CONTROL`
- `*_QC`
- `*_FLAG`

**Example:**
```python
# Input columns
['TEMP', 'TEMP_QUALITY_CONTROL', 'PSAL', 'PSAL_QC', 'PH']

# Detected parameters (QC columns filtered out)
{'temperature': 'TEMP', 'salinity': 'PSAL', 'phosphate': 'PH'}  # PH detected as phosphate
```

---

## Post-Processing

### Populate Parameters Table

After measurements are loaded, populate the parameters table:

```bash
python scripts/populate_parameters_from_measurements.py
```

This script:
- Extracts unique parameter codes from measurements
- Creates parameter records with proper UUIDs
- Handles NULL metadata_id correctly (uses `IS NULL` not `= NULL`)
- Links to parameter_mappings for enriched metadata
- Generates human-readable labels and infers units

**Expected Output:**
```
Found 70 unique parameter codes
Inserted 70 parameters
✓ All parameter codes have corresponding parameter records
```

**Important:** This step is required if you want to query the `parameters` table or join measurements with parameter metadata.

---

## Troubleshooting

### Connection Errors

**Symptom:**
```
ERROR: connection to server at "localhost" (127.0.0.1), port 5432 failed: 
FATAL: password authentication failed for user "marine_user"
```

**Cause:** Using old port (5432) or old password (`marine_pass`)

**Solution:**
```python
# Update connection in script
port=5433,
password="marine_pass123"
```

### Schema Errors

**Symptom:**
```
ERROR: column "metadata_id" does not exist
LINE 2: SELECT metadata_id, title, file_path
```

**Cause:** Querying non-existent column name

**Solution:**
```python
# Use 'id' not 'metadata_id'
SELECT id, title, dataset_path FROM metadata
```

### PostGIS Function Errors

**Symptom:**
```
ERROR: function st_dwithin(geometry, geometry, double precision) does not exist
```

**Cause:** PostGIS extension not installed (removed in v3.2)

**Solution:**
```python
# Replace PostGIS query
# Old:
WHERE ST_DWithin(geom, ST_MakePoint(%s, %s), 0.0001)

# New:
WHERE ABS(latitude - %s) < 0.0001 AND ABS(longitude - %s) < 0.0001
```

### No Measurements Extracted

**Symptom:**
```
📂 Processing: Dataset Name
  📊 Processing 5 CSV files
    ⚠ No parameter columns detected in file.csv
```

**Cause:** Parameter detection keywords don't match column names

**Solution:** Add custom keywords to `PARAMETER_KEYWORDS` dict or rename columns to standard names.

### Missing Parameters Table Records

**Symptom:**
```
ERROR: No parameter record found for code 'TEMP'
```

**Cause:** Parameters table not populated after measurements

**Solution:**
```bash
python scripts/populate_parameters_from_measurements.py
```

### Ambiguous PH Column (v3.3)

**Symptom:**
```
    ⚠️ AMBIGUOUS: Column 'PH' unclear - pH range: 45.2%, phosphate range: 52.1%.
    Defaulting to 'ph' - MANUAL REVIEW RECOMMENDED
```

**Cause:** Value distribution doesn't clearly indicate pH or phosphate

**Solution:**

1. **Check the log file** to see which values were classified:
   ```bash
   cat logs/etl_measurements_20260107_090000.log | grep "AMBIGUOUS"
   ```

2. **Manually review the data**:
   ```sql
   SELECT parameter_code, COUNT(*), MIN(value), MAX(value), AVG(value)
   FROM measurements
   WHERE parameter_code = 'PH'
   GROUP BY parameter_code;
   ```

3. **If phosphate (0-4 range)**, rename it:
   ```sql
   UPDATE measurements
   SET parameter_code = 'PO4', namespace = 'bodc'
   WHERE parameter_code = 'PH'
     AND value BETWEEN -2 AND 4;
   ```

4. **If true pH (6-9 range)**, keep as is:
   ```sql
   -- No action needed, already correct
   ```

5. **Update the source file** to use explicit names:
   - Rename column `PH` → `PH_INSITU` (for pH)
   - Or rename `PH` → `PHOSPHATE` or `PO4` (for phosphate)

**Prevention:**
Use explicit column names in source data:
- For pH: `pH`, `pH_total`, `pH_insitu`, `pH_seawater`
- For phosphate: `phosphate`, `PO4`, `PHOS`, `phosphorus`

Avoid using bare `PH` as a column name.

---

## Performance Optimization

### Batch Size Tuning

```python
# Default: 1000 rows per batch
inserter = BatchInserter(cursor, batch_size=1000)

# For large datasets (>1M measurements)
inserter = BatchInserter(cursor, batch_size=5000)

# For small datasets (<10k measurements)  
inserter = BatchInserter(cursor, batch_size=500)
```

### Memory Management

```python
# Process large files in chunks
for chunk in pd.read_csv('large_file.csv', chunksize=10000):
    measurements = extract_from_chunk(chunk)
    inserter.insert_batch(measurements)
    conn.commit()  # Commit each chunk
```

### Index Optimization

```sql
-- Ensure indexes exist on measurements table
CREATE INDEX IF NOT EXISTS idx_measurements_time ON measurements(time);
CREATE INDEX IF NOT EXISTS idx_measurements_parameter ON measurements(parameter_code);
CREATE INDEX IF NOT EXISTS idx_measurements_location ON measurements(location_id);
CREATE INDEX IF NOT EXISTS idx_measurements_metadata ON measurements(metadata_id);

-- For location lookups
CREATE INDEX IF NOT EXISTS idx_locations_coords ON locations(latitude, longitude);
```

---

## Version History

### v3.3 (January 2026)
- ✅ Smart PH/phosphorus disambiguation
- ✅ Value-based parameter detection
- ✅ Automatic PO4 code mapping
- ✅ Ambiguity logging and warnings
- ✅ Prevention of Issue #5 (PH parameter ambiguity)

### v3.2 (December 2025)
- ✅ PostGIS removed (pure SQL)
- ✅ Updated connection config (port 5433, new password)
- ✅ Schema compatibility fixes
- ✅ Pure SQL location matching
- ✅ Enhanced error logging

### v3.1 (Previous)
- Multi-parameter CSV extraction
- QC column filtering
- Unit inference
- Parameter mapping integration

### v3.0 (Previous)
- NetCDF time parsing
- Batch processing
- Error recovery

---

## References

- [Project README](../README.md)
- [Data Quality Issues and Fixes](DATA_QUALITY_ISSUES_AND_FIXES.md) - Issue #5: PH Parameter Ambiguity
- [Database Schema Documentation](database_schema.md)
- [ETL Scripts Reference](scripts.md)
- [ETL Guide](ETL_GUIDE.md)
- [init.sql - Schema Definition](../init.sql)
- [populate_parameters_from_measurements.py](../scripts/populate_parameters_from_measurements.py)

---

*Last Updated: January 7, 2026*  
*Script Version: 3.3 (Smart PH/Phosphorus Disambiguation)*  
*Maintained by: Huon Channel Marine Analytics Project*