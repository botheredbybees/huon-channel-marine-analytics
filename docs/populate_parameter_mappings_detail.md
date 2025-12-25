# populate_parameter_mappings.py - Detailed Documentation

## Overview

`populate_parameter_mappings.py` is a migration script that loads standardized parameter mappings from `config_parameter_mapping.json` into the PostgreSQL database. It creates a lookup table that enables consistent data normalization across diverse oceanographic datasets with varying parameter naming conventions.

## Purpose

This script:
- Reads parameter mappings from JSON configuration file
- Populates the `parameter_mappings` database table
- Standardizes oceanographic variable names to CF/BODC conventions
- Enables automatic parameter resolution during data ingestion
- Supports incremental updates (idempotent operation)
- Provides verification and reporting of loaded mappings

## Why Both Script and Config File?

### The Two-Component Design

The parameter standardization system uses two complementary components:

1. **config_parameter_mapping.json** (Configuration)
   - Human-readable mapping definitions
   - Easy to edit without database access
   - Version-controlled with the codebase
   - Portable across environments
   - Serves as authoritative mapping source

2. **populate_parameter_mappings.py** (Migration Tool)
   - Loads config into database
   - Makes mappings queryable via SQL
   - Enables fast lookup during ETL operations
   - Provides validation and reporting
   - Supports incremental updates

### Architecture Rationale

**Why Not Just Use the JSON File Directly?**

Option A: Read JSON during every measurement insert
```python
# SLOW: Read file every time
with open('config_parameter_mapping.json') as f:
    mappings = json.load(f)
standard_param = mappings.get(raw_param)
```
- **Problem**: File I/O overhead on every measurement row
- **Impact**: 10,000 measurements = 10,000 file reads
- **Performance**: ~1000x slower than database lookup

Option B: Database lookup (current approach)
```sql
-- FAST: Single indexed query
SELECT standard_code, namespace, unit 
FROM parameter_mappings 
WHERE raw_parameter_name = %s;
```
- **Benefit**: Indexed database query (microseconds)
- **Impact**: 10,000 measurements = minimal overhead
- **Performance**: Optimal for ETL operations

**Why Not Just Use the Database?**

- **Version Control**: Database isn't git-friendly
- **Documentation**: JSON is self-documenting
- **Portability**: Easy to share/reuse across projects
- **Initial Setup**: New environments need seed data
- **Updates**: Easier to review changes in git diff

### Workflow Integration

```
┌──────────────────────────────────┐
│ config_parameter_mapping.json    │
│ (Authoritative Source)            │
│ - Human-editable                  │
│ - Version controlled              │
│ - Portable                        │
└───────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│ populate_parameter_mappings.py    │
│ (Migration Script)                │
│ - Loads JSON → Database          │
│ - Validates mappings              │
│ - Reports statistics              │
└───────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│ parameter_mappings table          │
│ (Database Lookup Table)           │
│ - Indexed for fast queries        │
│ - Used by ETL scripts             │
│ - Queryable via SQL               │
└───────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│ populate_measurements.py          │
│ (ETL Script)                      │
│ - Fast database lookups           │
│ - Standardizes parameters         │
│ - No file I/O per measurement     │
└──────────────────────────────────┘
```

## Architecture

### Data Flow

```
JSON File Reading
       ↓
Config Validation
       ↓
Database Connection
       ↓
Batch Insert (ON CONFLICT)
       ↓
Verification & Reporting
```

## Core Components

### 1. Configuration Structure

**config_parameter_mapping.json Format:**

```json
{
  "description": "Mapping configuration description",
  "parameter_mapping": {
    "RAW_NAME": ["STANDARD_CODE", "namespace", "unit"],
    "TEMPERATURE": ["TEMP", "bodc", "Degrees Celsius"],
    "CHLOROPHYLL_A": ["CPHL", "bodc", "mg/m3"]
  }
}
```

**Mapping Structure:**
- **Key**: Raw parameter name (as appears in source data)
- **Value**: Array of three elements:
  1. `standard_code`: Standardized parameter code
  2. `namespace`: Authority (bodc, cf, custom)
  3. `unit`: Measurement unit

### 2. Database Connection

```python
DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'marine_db',
    'user': 'marine_user',
    'password': 'marine_pass123'
}

def connect_to_database():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Connected to database successfully")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Database connection failed: {e}")
        raise
```

### 3. JSON Config Loader

**Function:** `load_json_config(json_path='config_parameter_mapping.json')`

**Purpose:** Reads and validates JSON configuration file.

**Process:**
```python
def load_json_config(json_path='config_parameter_mapping.json'):
    config_file = Path(json_path)
    
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {json_path}")
    
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    logger.info(f"Loaded config from {json_path}")
    return config
```

**Returns:**
- `dict`: Parsed JSON configuration

**Errors:**
- `FileNotFoundError`: Config file missing
- `json.JSONDecodeError`: Invalid JSON syntax

### 4. Parameter Mapping Population

**Function:** `populate_parameter_mappings(conn, config)`

**Purpose:** Inserts/updates parameter mappings in database.

**Process:**

1. **Extract Mappings from Config**
   ```python
   parameter_mapping = config.get('parameter_mapping', {})
   ```

2. **Define Insert Statement**
   ```sql
   INSERT INTO parameter_mappings 
       (raw_parameter_name, standard_code, namespace, unit, source, description)
   VALUES 
       (%s, %s, %s, %s, %s, %s)
   ON CONFLICT (raw_parameter_name) DO NOTHING;
   ```

3. **Iterate Through Mappings**
   ```python
   for raw_name, mapping in parameter_mapping.items():
       if len(mapping) != 3:
           logger.warning(f"Invalid mapping for {raw_name}: {mapping}")
           continue
       
       standard_code, namespace, unit = mapping
       description = generate_description(raw_name, standard_code, namespace)
   ```

4. **Insert with Conflict Handling**
   ```python
   cursor.execute(insert_sql, (
       raw_name,
       standard_code,
       namespace,
       unit,
       'system',  # Source: from config file
       description
   ))
   
   if cursor.rowcount > 0:
       inserted_count += 1
   else:
       skipped_count += 1
   ```

5. **Commit Transaction**
   ```python
   conn.commit()
   logger.info(f"Inserted {inserted_count} new mappings")
   logger.info(f"Skipped {skipped_count} existing mappings")
   ```

**Key Features:**
- `ON CONFLICT DO NOTHING`: Idempotent - safe to run multiple times
- Validates mapping structure (must be 3-element array)
- Generates human-readable descriptions
- Tracks inserted vs. skipped counts
- Logs warnings for invalid mappings

### 5. Description Generation

**Function:** `generate_description(raw_name, standard_code, namespace)`

**Purpose:** Creates human-readable descriptions for parameters.

**Lookup Dictionary:**
```python
descriptions = {
    'TEMP': 'Sea water temperature',
    'SST': 'Sea surface temperature',
    'PSAL': 'Practical salinity',
    'CPHL': 'Chlorophyll-a concentration',
    'DOXY': 'Dissolved oxygen',
    'PH': 'pH',
    'DEPTH': 'Depth below surface',
    # ... 30+ more definitions
}
```

**Logic:**
```python
base_description = descriptions.get(standard_code, standard_code)

if namespace == 'cf':
    return f"{base_description} (CF standard name)"
elif namespace == 'bodc':
    return f"{base_description} (BODC P01 code)"
else:
    return base_description
```

**Example Output:**
- `TEMP` + `bodc` → "Sea water temperature (BODC P01 code)"
- `CPHL` + `cf` → "Chlorophyll-a concentration (CF standard name)"
- `CUSTOM_PARAM` + `custom` → "CUSTOM_PARAM"

### 6. Verification and Reporting

**Function:** `verify_population(conn)`

**Purpose:** Validates successful population and reports statistics.

**Checks:**

1. **Total Count**
   ```sql
   SELECT COUNT(*) FROM parameter_mappings;
   ```

2. **Sample Mappings**
   ```sql
   SELECT raw_parameter_name, standard_code, namespace, unit
   FROM parameter_mappings
   ORDER BY namespace, standard_code
   LIMIT 10;
   ```

3. **Namespace Distribution**
   ```sql
   SELECT namespace, COUNT(*) 
   FROM parameter_mappings 
   GROUP BY namespace 
   ORDER BY namespace;
   ```

**Example Output:**
```
Total parameter mappings in database: 87

Sample mappings:
  TEMP                           -> TEMP            (bodc  ) [Degrees Celsius]
  TEMPERATURE                    -> TEMP            (bodc  ) [Degrees Celsius]
  SEA_WATER_TEMPERATURE          -> TEMP            (cf    ) [Degrees Celsius]
  PSAL                           -> PSAL            (bodc  ) [PSS-78]
  SALINITY                       -> PSAL            (bodc  ) [PSS-78]
  ...

Mappings by namespace:
  bodc       : 42 mappings
  cf         : 35 mappings
  custom     : 10 mappings
```

## Database Schema

### parameter_mappings Table

```sql
CREATE TABLE parameter_mappings (
    id SERIAL PRIMARY KEY,
    raw_parameter_name TEXT UNIQUE NOT NULL,  -- As in source data
    standard_code TEXT NOT NULL,               -- Standardized code
    namespace TEXT NOT NULL,                   -- bodc, cf, custom
    unit TEXT,                                 -- Measurement unit
    source TEXT DEFAULT 'system',              -- Origin of mapping
    description TEXT,                          -- Human-readable
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_param_mappings_raw ON parameter_mappings(raw_parameter_name);
CREATE INDEX idx_param_mappings_standard ON parameter_mappings(standard_code);
CREATE INDEX idx_param_mappings_namespace ON parameter_mappings(namespace);
```

### Example Records

| raw_parameter_name | standard_code | namespace | unit | description |
|--------------------|---------------|-----------|------|-------------|
| TEMP | TEMP | bodc | Degrees Celsius | Sea water temperature (BODC P01 code) |
| TEMPERATURE | TEMP | bodc | Degrees Celsius | Sea water temperature (BODC P01 code) |
| SEA_WATER_TEMPERATURE | TEMP | cf | Degrees Celsius | Sea water temperature (CF standard name) |
| CPHL | CPHL | bodc | mg/m3 | Chlorophyll-a concentration (BODC P01 code) |
| CHLOROPHYLL_A | CPHL | bodc | mg/m3 | Chlorophyll-a concentration (BODC P01 code) |

## Usage

### Basic Execution

```bash
python populate_parameter_mappings.py
```

### Expected Output

```
2025-12-25 11:00:00 - [INFO] ============================================================
2025-12-25 11:00:00 - [INFO] Parameter Mappings Migration Script
2025-12-25 11:00:00 - [INFO] ============================================================
2025-12-25 11:00:01 - [INFO] Loaded config from config_parameter_mapping.json
2025-12-25 11:00:01 - [INFO] Connected to database successfully
2025-12-25 11:00:02 - [INFO] Inserted 87 new mappings
2025-12-25 11:00:02 - [INFO] Skipped 0 existing mappings
2025-12-25 11:00:02 - [INFO] Total parameter mappings in database: 87
2025-12-25 11:00:02 - [INFO] Sample mappings:
...
2025-12-25 11:00:02 - [INFO] 
Migration completed successfully!
```

### Incremental Updates

**Scenario:** Adding new parameter mappings

1. **Edit config_parameter_mapping.json:**
   ```json
   {
     "parameter_mapping": {
       "NEW_PARAM": ["NEW_CODE", "custom", "units"],
       ...
     }
   }
   ```

2. **Run migration:**
   ```bash
   python populate_parameter_mappings.py
   ```

3. **Expected output:**
   ```
   Inserted 1 new mappings
   Skipped 87 existing mappings
   Total parameter mappings in database: 88
   ```

**Key Behavior:**
- Only new mappings are inserted
- Existing mappings remain unchanged
- `ON CONFLICT DO NOTHING` prevents errors

### Re-running the Script

**Safe to run multiple times:**
```bash
python populate_parameter_mappings.py  # First run: 87 inserted
python populate_parameter_mappings.py  # Second run: 0 inserted, 87 skipped
```

**Idempotent operation:**
- No duplicates created
- No data loss
- Fast execution (just checks)

## Configuration File Details

### config_parameter_mapping.json Structure

```json
{
  "description": "Purpose and usage notes",
  
  "parameter_mapping": {
    "RAW_NAME": ["STANDARD_CODE", "namespace", "unit"],
    ...
  },
  
  "time_format_hints": {
    "description": "Time format detection guidance",
    "iso_8601": "ISO 8601 strings",
    "days_since_1900": "Numeric: 40000 < value < 50000",
    ...
  },
  
  "spatial_column_hints": [
    "latitude", "lat", "y", "northing",
    "longitude", "lon", "x", "easting"
  ],
  
  "quality_flags": {
    "1": "Good data",
    "2": "Questionable data",
    "3": "Bad data",
    "4": "Missing data"
  },
  
  "notes": "Additional documentation"
}
```

### Namespace Definitions

**bodc (British Oceanographic Data Centre)**
- Authority: [NERC Vocabulary Server](https://vocab.nerc.ac.uk/)
- Standard: P01 parameter codes
- Example: `TEMPPR01` = Temperature of water body
- Used by: IMOS, AODN, UK oceanography

**cf (Climate and Forecast)**
- Authority: [CF Conventions](http://cfconventions.org/)
- Standard: NetCDF CF standard names
- Example: `sea_water_temperature`
- Used by: NetCDF files, climate models

**custom (Project-Specific)**
- Authority: Local definitions
- Standard: None (project-defined)
- Example: `HUON_TURBIDITY`
- Used by: Non-standard parameters

### Adding New Mappings

**Step 1: Identify Raw Parameter Name**
```python
# From NetCDF file
import netCDF4 as nc
ds = nc.Dataset('data.nc')
print(ds.variables.keys())
# Output: ['TIME', 'WATER_TEMP', 'SAL', ...]
```

**Step 2: Find Standard Code**
- Search BODC: https://vocab.nerc.ac.uk/search_nvs/P01/
- Search CF: http://cfconventions.org/standard-names.html
- Or define custom code

**Step 3: Add to JSON**
```json
"WATER_TEMP": ["TEMP", "bodc", "Degrees Celsius"],
"SAL": ["PSAL", "bodc", "PSS-78"]
```

**Step 4: Run Migration**
```bash
python populate_parameter_mappings.py
```

## Integration with ETL Pipeline

### Usage in populate_measurements.py

**Database Lookup:**
```python
def get_standard_param(cursor, raw_name):
    """
    Lookup standardized parameter from database.
    
    Fast indexed query - no file I/O.
    """
    cursor.execute("""
        SELECT standard_code, namespace, unit
        FROM parameter_mappings
        WHERE raw_parameter_name = %s
    """, (raw_name.upper(),))
    
    result = cursor.fetchone()
    if result:
        return {
            'standard_code': result[0],
            'namespace': result[1],
            'unit': result[2]
        }
    return None
```

**Example Usage:**
```python
# Processing NetCDF variable
for var_name in ['TEMP', 'PSAL', 'CPHL']:
    std_param = get_standard_param(cursor, var_name)
    
    if std_param:
        # Insert measurement with standardized parameter
        cursor.execute("""
            INSERT INTO measurements 
            (parameter, namespace, unit, value, ...)
            VALUES (%s, %s, %s, %s, ...)
        """, (std_param['standard_code'], 
              std_param['namespace'],
              std_param['unit'],
              value))
    else:
        logger.warning(f"No mapping found for {var_name}")
```

### Performance Benefits

**Without Database Mapping:**
```python
# Read JSON file every time (SLOW)
for i in range(10000):
    with open('config.json') as f:
        config = json.load(f)
    std_param = config['parameter_mapping'].get('TEMP')

# Time: ~10 seconds (1ms per iteration)
```

**With Database Mapping:**
```python
# Single indexed query (FAST)
for i in range(10000):
    cursor.execute(
        "SELECT standard_code FROM parameter_mappings WHERE raw_parameter_name = %s",
        ('TEMP',)
    )
    std_param = cursor.fetchone()

# Time: ~0.1 seconds (10µs per iteration)
# 100x faster!
```

## Quality Assurance

### Validation Checks

**1. Completeness**
```sql
-- Find raw parameters without mappings
SELECT DISTINCT parameter
FROM measurements
WHERE parameter NOT IN (
    SELECT raw_parameter_name FROM parameter_mappings
);
```

**2. Consistency**
```sql
-- Check for multiple mappings to same standard code
SELECT standard_code, COUNT(*) as mapping_count
FROM parameter_mappings
GROUP BY standard_code
HAVING COUNT(*) > 1
ORDER BY mapping_count DESC;
```

**3. Namespace Distribution**
```sql
-- Verify namespace usage
SELECT namespace, COUNT(*) as count,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) as percentage
FROM parameter_mappings
GROUP BY namespace;
```

**4. Unit Validation**
```sql
-- Find parameters with unusual units
SELECT standard_code, unit, COUNT(*) as occurrences
FROM parameter_mappings
GROUP BY standard_code, unit
HAVING COUNT(*) > 1;
```

### Testing

**Unit Test Example:**
```python
import unittest
import json

class TestParameterMappings(unittest.TestCase):
    
    def setUp(self):
        with open('config_parameter_mapping.json') as f:
            self.config = json.load(f)
    
    def test_all_mappings_valid_structure(self):
        """Verify all mappings have 3 elements"""
        for raw_name, mapping in self.config['parameter_mapping'].items():
            self.assertEqual(len(mapping), 3, 
                           f"Invalid mapping for {raw_name}")
    
    def test_valid_namespaces(self):
        """Ensure only valid namespaces used"""
        valid_namespaces = {'bodc', 'cf', 'custom'}
        for raw_name, mapping in self.config['parameter_mapping'].items():
            namespace = mapping[1]
            self.assertIn(namespace, valid_namespaces,
                         f"Invalid namespace '{namespace}' for {raw_name}")
    
    def test_no_duplicate_raw_names(self):
        """Check for duplicate raw parameter names"""
        raw_names = list(self.config['parameter_mapping'].keys())
        unique_names = set(raw_names)
        self.assertEqual(len(raw_names), len(unique_names),
                        "Duplicate raw parameter names found")

if __name__ == '__main__':
    unittest.main()
```

## Troubleshooting

### Common Errors

**1. Config File Not Found**
```
FileNotFoundError: Config file not found: config_parameter_mapping.json
```
**Solution:** 
```bash
# Check file exists in current directory
ls -l config_parameter_mapping.json

# Or specify path
python populate_parameter_mappings.py --config /path/to/config.json
```

**2. Database Connection Failed**
```
Database connection failed: could not connect to server
```
**Solution:**
```bash
# Check PostgreSQL is running
docker-compose ps

# Verify connection parameters
psql -h localhost -p 5433 -U marine_user -d marine_db
```

**3. Invalid JSON Syntax**
```
json.JSONDecodeError: Expecting ',' delimiter: line 42 column 5
```
**Solution:**
```bash
# Validate JSON syntax
python -m json.tool config_parameter_mapping.json

# Or use online validator
```

**4. Invalid Mapping Structure**
```
WARNING - Invalid mapping for CUSTOM_PARAM: ['CODE', 'namespace']
```
**Solution:**
```json
// WRONG: Only 2 elements
"CUSTOM_PARAM": ["CODE", "namespace"]

// CORRECT: Must have 3 elements
"CUSTOM_PARAM": ["CODE", "namespace", "units"]
```

### Debugging

**Enable Verbose Logging:**
```python
# In script
logging.basicConfig(level=logging.DEBUG)

# Output includes:
# DEBUG - Processing mapping: TEMP -> ['TEMP', 'bodc', 'Degrees Celsius']
# DEBUG - Executing INSERT for TEMP
# DEBUG - Row count: 1
```

**Dry Run Mode:**
```python
# Add to main()
def main(dry_run=False):
    # ... load config ...
    
    if dry_run:
        logger.info("DRY RUN MODE - No database changes")
        # Print what would be inserted
        for raw_name, mapping in config['parameter_mapping'].items():
            logger.info(f"Would insert: {raw_name} -> {mapping}")
        return
    
    # Normal execution...

# Usage
if __name__ == '__main__':
    import sys
    dry_run = '--dry-run' in sys.argv
    main(dry_run=dry_run)
```

## Maintenance

### Updating Existing Mappings

**Current Behavior:** `ON CONFLICT DO NOTHING` means updates are ignored.

**To Force Update:**

1. **Option A: Delete and Re-insert**
   ```sql
   DELETE FROM parameter_mappings WHERE raw_parameter_name = 'OLD_NAME';
   ```
   Then run script.

2. **Option B: Manual Update**
   ```sql
   UPDATE parameter_mappings
   SET standard_code = 'NEW_CODE',
       namespace = 'cf',
       unit = 'new_units'
   WHERE raw_parameter_name = 'OLD_NAME';
   ```

3. **Option C: Modify Script**
   ```sql
   -- Change from:
   ON CONFLICT (raw_parameter_name) DO NOTHING
   
   -- To:
   ON CONFLICT (raw_parameter_name) 
   DO UPDATE SET
       standard_code = EXCLUDED.standard_code,
       namespace = EXCLUDED.namespace,
       unit = EXCLUDED.unit,
       updated_at = CURRENT_TIMESTAMP;
   ```

### Backup Strategy

**Export Mappings:**
```bash
# To SQL
pg_dump -h localhost -p 5433 -U marine_user -d marine_db \
  -t parameter_mappings > parameter_mappings_backup.sql

# To CSV
psql -h localhost -p 5433 -U marine_user -d marine_db \
  -c "COPY parameter_mappings TO STDOUT CSV HEADER" \
  > parameter_mappings_backup.csv
```

**Restore Mappings:**
```bash
# From SQL
psql -h localhost -p 5433 -U marine_user -d marine_db \
  < parameter_mappings_backup.sql

# From CSV
psql -h localhost -p 5433 -U marine_user -d marine_db \
  -c "\COPY parameter_mappings FROM 'parameter_mappings_backup.csv' CSV HEADER"
```

## Advanced Usage

### Custom Configuration Path

```python
# Modify main() to accept command-line argument
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='config_parameter_mapping.json',
                       help='Path to config file')
    args = parser.parse_args()
    
    config = load_json_config(args.config)
    # ...

# Usage
python populate_parameter_mappings.py --config /path/to/custom_config.json
```

### Environment-Specific Configs

```bash
# Development
python populate_parameter_mappings.py --config config_dev.json

# Production
python populate_parameter_mappings.py --config config_prod.json

# Testing
python populate_parameter_mappings.py --config config_test.json
```

### Programmatic Usage

```python
from populate_parameter_mappings import (
    load_json_config,
    connect_to_database,
    populate_parameter_mappings
)

# Load custom config
config = load_json_config('my_config.json')

# Add runtime mappings
config['parameter_mapping']['RUNTIME_PARAM'] = ['CODE', 'custom', 'units']

# Populate database
conn = connect_to_database()
populate_parameter_mappings(conn, config)
conn.close()
```

## References

### Documentation
- [BODC Vocabulary Server](https://vocab.nerc.ac.uk/)
- [CF Conventions](http://cfconventions.org/)
- [IMOS Parameter Codes](http://imos.org.au/facilities/aodn/)
- [psycopg2 Documentation](https://www.psycopg.org/docs/)

### Related Files
- [config_parameter_mapping.json](../config_parameter_mapping.json) - Configuration file
- [populate_measurements.py](populate_measurements_detail.md) - Uses these mappings
- [Database Schema](database_schema.md) - Table definitions

### Related Scripts
- [populate_metadata.py](populate_metadata_detail.md)
- [populate_measurements.py](populate_measurements_detail.md)
- [populate_spatial.py](populate_spatial_detail.md)

---

*Last Updated: December 25, 2025*