# populate_metadata.py - Detailed Documentation

## Overview

`populate_metadata.py` is the first script in the ETL pipeline that scans the `AODN_data/` directory structure and extracts dataset metadata. It creates catalog entries in the `metadata` table that subsequent ETL scripts use to locate and process data files.

**Script Version:** 1.0  
**Dependencies:** `psycopg2`, `pathlib`, `uuid`  
**Estimated Runtime:** < 1 minute for typical dataset collections

---

## Architecture

### Data Flow

```
AODN_data/
  ├── Dataset_A/
  ├── Dataset_B/
  └── Dataset_C/
       ↓
  [Directory Scan]
       ↓
  [Metadata Extraction]
       ↓
  [UUID Generation]
       ↓
  [Bounding Box Inference]
       ↓
  metadata table
```

### Core Components

1. **Directory Scanner** - Discovers dataset folders
2. **Metadata Extractor** - Extracts info from directory names
3. **UUID Generator** - Creates deterministic identifiers
4. **Bounding Box Detector** - Infers spatial extents
5. **Database Writer** - Populates metadata table

---

## Function Reference

### `connect_to_database()`

**Purpose:** Establishes connection to PostgreSQL database.

**Returns:** `psycopg2.connection` object

**Configuration:**
```python
DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'marine_db',
    'user': 'marine_user',
    'password': 'marine_pass123'
}
```

**Error Handling:**
- Logs connection failures
- Raises `psycopg2.Error` on failure

---

### `generate_uuid_from_path(dataset_path)`

**Purpose:** Generates deterministic UUID from dataset path.

**Algorithm:**
- Uses UUID5 (SHA-1 hash-based)
- Namespace: DNS namespace (`6ba7b810-9dad-11d1-80b4-00c04fd430c8`)
- Input: Full dataset path string

**Parameters:**
- `dataset_path` (str|Path): Absolute or relative path to dataset

**Returns:** UUID string (format: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`)

**Example:**
```python
uuid = generate_uuid_from_path("AODN_data/Chlorophyll_Database")
# Returns: "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
```

**Why Deterministic?**
- Same path always generates same UUID
- Enables idempotent re-runs
- Prevents duplicate records

---

### `extract_bounding_box_from_name(dataset_name)`

**Purpose:** Infers geographic bounding box from dataset name.

**Heuristics:**

| Pattern Detected | Bounding Box (W, E, S, N) |
|------------------|---------------------------|
| "huon", "d'entrecasteaux" | 146.8, 147.3, -43.5, -43.0 |
| "storm bay" | 147.0, 147.8, -43.5, -42.8 |
| "south east tasmania" | 147.0, 148.5, -43.5, -42.0 |
| "tasmania" (generic) | 144.0, 149.0, -44.0, -40.0 |
| **Default** | 144.0, 149.0, -44.0, -40.0 |

**Parameters:**
- `dataset_name` (str): Directory name or dataset title

**Returns:** Dictionary with keys: `west`, `east`, `south`, `north`

**Example:**
```python
bbox = extract_bounding_box_from_name("Huon_Estuary_Phytoplankton")
# Returns: {'west': 146.8, 'east': 147.3, 'south': -43.5, 'north': -43.0}
```

**Extensibility:**  
Add new patterns by modifying the function's conditional logic:

```python
# Add custom region
if 'my_region' in name_lower:
    return {'west': X, 'east': X, 'south': X, 'north': X}
```

---

### `clean_dataset_name(directory_name)`

**Purpose:** Sanitizes directory names for database storage.

**Transformations:**
1. Remove special characters (except `_`, `-`)
2. Replace spaces with underscores
3. Truncate to 100 characters

**Parameters:**
- `directory_name` (str): Raw directory name

**Returns:** Cleaned string suitable for database storage

**Examples:**
```python
clean_dataset_name("IMOS - Chlorophyll (1965-2017)")
# Returns: "IMOS_Chlorophyll_19652017"

clean_dataset_name("Dataset with Special!@#$%Characters")
# Returns: "Dataset_with_SpecialCharacters"
```

---

### `scan_aodn_directory(base_path='AODN_data')`

**Purpose:** Recursively scans AODN directory and builds dataset list.

**Parameters:**
- `base_path` (str): Path to AODN_data directory (default: `'AODN_data'`)

**Returns:** List of dataset dictionaries

**Dataset Dictionary Schema:**
```python
{
    'uuid': str,           # Deterministic UUID
    'title': str,          # Original directory name
    'dataset_name': str,   # Cleaned name
    'dataset_path': str,   # Absolute path to dataset
    'west': float,         # Western longitude
    'east': float,         # Eastern longitude
    'south': float,        # Southern latitude
    'north': float,        # Northern latitude
    'file_count': int      # Number of files in directory
}
```

**Behavior:**
- Skips hidden directories (starting with `.`)
- Only processes immediate subdirectories (not recursive)
- Logs debug message for each dataset found

**Example Output:**
```python
[
    {
        'uuid': 'abc123...',
        'title': 'Chlorophyll_Database',
        'dataset_name': 'Chlorophyll_Database',
        'dataset_path': '/path/to/AODN_data/Chlorophyll_Database',
        'west': 144.0, 'east': 149.0,
        'south': -44.0, 'north': -40.0,
        'file_count': 15
    },
    # ... more datasets
]
```

---

### `populate_metadata_table(conn, datasets, force=False)`

**Purpose:** Writes dataset metadata to database.

**Parameters:**
- `conn` (psycopg2.connection): Active database connection
- `datasets` (list): List of dataset dictionaries from `scan_aodn_directory()`
- `force` (bool): If True, update existing records; if False, skip duplicates

**SQL Logic:**

**Standard Mode (`force=False`):**
```sql
INSERT INTO metadata (...)
VALUES (...)
ON CONFLICT (uuid) DO NOTHING;
```
- Inserts new records only
- Skips existing UUIDs silently

**Force Mode (`force=True`):**
```sql
INSERT INTO metadata (...)
VALUES (...)
ON CONFLICT (uuid) DO UPDATE SET
    title = EXCLUDED.title,
    ...
```
- Updates existing records with new data
- Useful for correcting metadata

**Return Values:**
- Logs counts: `inserted_count`, `updated_count`, `skipped_count`
- Commits transaction on success

**Error Handling:**
- Catches `psycopg2.Error` per dataset
- Logs error and continues with next dataset
- Does not rollback entire transaction

---

### `verify_population(conn)`

**Purpose:** Validates metadata insertion and reports statistics.

**Queries Executed:**

1. **Total Record Count**
   ```sql
   SELECT COUNT(*) FROM metadata;
   ```

2. **Sample Records**
   ```sql
   SELECT title, dataset_name, west, east, south, north
   FROM metadata
   ORDER BY title
   LIMIT 10;
   ```

3. **Datasets Without Measurements**
   ```sql
   SELECT COUNT(*)
   FROM metadata m
   LEFT JOIN measurements meas ON m.id = meas.metadata_id
   WHERE meas.metadata_id IS NULL;
   ```

**Output:**
- Logs total metadata count
- Displays first 10 datasets
- Reports datasets needing measurement ingestion

**Example Output:**
```
Total metadata records in database: 47

Sample metadata records:
  Chlorophyll_Database_1965_2017          | Chlorophyll_Database          | bbox: [144.0, 149.0, -44.0, -40.0]
  Huon_Estuary_CTD_Profiles               | Huon_Estuary_CTD_Profiles     | bbox: [146.8, 147.3, -43.5, -43.0]
  ...

Datasets without measurements: 45

Run 'python populate_measurements.py' to ingest measurements for these datasets.
```

---

## Database Schema

### `metadata` Table

```sql
CREATE TABLE metadata (
    id SERIAL PRIMARY KEY,
    uuid UUID UNIQUE NOT NULL,
    title TEXT,
    dataset_name VARCHAR(100),
    abstract TEXT,
    dataset_path TEXT,
    west REAL,
    east REAL,
    south REAL,
    north REAL,
    time_start TIMESTAMP,
    time_end TIMESTAMP,
    extracted_at TIMESTAMP DEFAULT NOW()
);
```

**Fields Populated by This Script:**
- `uuid` - Deterministic identifier
- `title` - Original directory name
- `dataset_name` - Cleaned name
- `dataset_path` - Absolute path to dataset
- `west`, `east`, `south`, `north` - Bounding box
- `extracted_at` - Timestamp of metadata extraction

**Fields NOT Populated:**
- `abstract` - Requires XML parsing (future enhancement)
- `time_start`, `time_end` - Requires data file analysis

---

## Usage Examples

### Basic Usage

```bash
python populate_metadata.py
```

Scans `AODN_data/` and inserts new metadata records.

### Custom Data Directory

```bash
python populate_metadata.py --path /path/to/custom/data
```

### Force Update Existing Records

```bash
python populate_metadata.py --force
```

Updates existing metadata records with new directory scan results.

### Command-Line Options

```
usage: populate_metadata.py [-h] [--force] [--path PATH]

Populate metadata table from AODN_data directory

optional arguments:
  -h, --help   show this help message and exit
  --force      Update existing metadata records
  --path PATH  Path to AODN_data directory (default: AODN_data)
```

---

## Logging

### Log Levels

- **INFO** - Progress updates, summary statistics
- **DEBUG** - Per-dataset processing details
- **WARNING** - Non-critical issues (e.g., no datasets found)
- **ERROR** - Database errors, connection failures

### Log Format

```
2025-12-25 10:30:00 - [INFO] Found 47 datasets in AODN_data
2025-12-25 10:30:01 - [DEBUG] Inserted: Chlorophyll_Database
2025-12-25 10:30:05 - [INFO] Inserted 45 new metadata records
```

### Adjusting Log Level

Modify the script header:

```python
logging.basicConfig(
    level=logging.DEBUG,  # Change to DEBUG for verbose output
    format='%(asctime)s - [%(levelname)s] %(message)s'
)
```

---

## Error Handling

### Database Connection Failures

**Symptom:**
```
ERROR: Database connection failed: could not connect to server
```

**Solutions:**
1. Check Docker containers: `docker-compose ps`
2. Verify port 5433 is available: `netstat -an | grep 5433`
3. Check database credentials in `DB_CONFIG`

### Directory Not Found

**Symptom:**
```
WARNING: No datasets found. Exiting.
```

**Solutions:**
1. Ensure `AODN_data/` directory exists
2. Use `--path` to specify correct directory
3. Check directory permissions

### Duplicate UUID Conflicts

**Symptom:** (Rare, only if UUID generation changes)
```
ERROR: duplicate key value violates unique constraint "metadata_uuid_key"
```

**Solution:** Use `--force` to update existing records

---

## Performance Considerations

### Execution Time

| Datasets | Files | Time |
|----------|-------|------|
| 10       | 500   | < 5s |
| 50       | 2000  | < 10s |
| 100      | 5000  | < 20s |

### Optimization Tips

1. **Batch Processing** - Script already batches database writes
2. **Index Optimization** - UUID column is indexed automatically
3. **Network Latency** - Run script on same machine as database

---

## Extension Points

### Adding XML Metadata Parsing

To parse ISO 19115-3 XML files:

```python
import xml.etree.ElementTree as ET

def extract_xml_metadata(xml_path):
    """Extract abstract, temporal extent from XML."""
    tree = ET.parse(xml_path)
    root = tree.getroot()
    
    # Extract abstract
    abstract = root.find('.//gmd:abstract/gco:CharacterString', namespaces).text
    
    # Extract temporal extent
    time_start = root.find('.//gml:beginPosition', namespaces).text
    time_end = root.find('.//gml:endPosition', namespaces).text
    
    return {'abstract': abstract, 'time_start': time_start, 'time_end': time_end}
```

### Custom Bounding Box Rules

Add new geographic regions:

```python
def extract_bounding_box_from_name(dataset_name):
    name_lower = dataset_name.lower()
    
    # Add custom regions
    if 'bass strait' in name_lower:
        return {'west': 144.0, 'east': 148.5, 'south': -40.5, 'north': -38.5}
    
    # ... existing rules ...
```

### Database Connection Pooling

For high-volume scenarios:

```python
from psycopg2 import pool

connection_pool = pool.SimpleConnectionPool(1, 10, **DB_CONFIG)

def get_connection():
    return connection_pool.getconn()
```

---

## Testing

### Manual Testing

```bash
# Test with small dataset
mkdir -p AODN_data/Test_Dataset
touch AODN_data/Test_Dataset/data.csv
python populate_metadata.py

# Verify in database
psql -h localhost -p 5433 -U marine_user -d marine_db -c "SELECT * FROM metadata WHERE title='Test_Dataset';"
```

### Unit Test Example

```python
import unittest
from populate_metadata import clean_dataset_name, generate_uuid_from_path

class TestMetadataFunctions(unittest.TestCase):
    def test_clean_dataset_name(self):
        result = clean_dataset_name("Test!@#Dataset")
        self.assertEqual(result, "TestDataset")
    
    def test_uuid_determinism(self):
        uuid1 = generate_uuid_from_path("/path/to/dataset")
        uuid2 = generate_uuid_from_path("/path/to/dataset")
        self.assertEqual(uuid1, uuid2)
```

---

## Integration with ETL Pipeline

### Pipeline Order

1. **populate_metadata.py** ← You are here
2. populate_parameter_mappings.py
3. populate_measurements.py
4. populate_spatial.py
5. populate_biological.py

### Data Dependencies

**Outputs:**
- `metadata.id` - Primary key used by measurements table
- `metadata.uuid` - Foreign key for data lineage
- `metadata.dataset_path` - Used by measurement extractors

**Consumed By:**
- `populate_measurements.py` - Reads `metadata.id` and `dataset_path`
- `populate_biological.py` - Links observations to datasets

---

## Troubleshooting Checklist

- [ ] Docker containers running (`docker-compose ps`)
- [ ] Database accepting connections (port 5433)
- [ ] `AODN_data/` directory exists and contains subdirectories
- [ ] Database credentials correct in `DB_CONFIG`
- [ ] PostgreSQL user has INSERT permissions
- [ ] No file system permissions issues
- [ ] Python dependencies installed (`pip install -r requirements.txt`)

---

## References

- [Project README](../README.md)
- [Database Schema Documentation](database_schema.md)
- [ETL Guide](ETL_GUIDE.md)
- [ISO 19115-3 Metadata Standard](https://www.iso.org/standard/32579.html)
- [PostgreSQL UUID Functions](https://www.postgresql.org/docs/current/functions-uuid.html)

---

*Last Updated: December 25, 2025*  
*Script Version: 1.0*  
*Maintained by: Huon Channel Marine Analytics Project*