# populate_metadata.py - Detailed Documentation

## Overview

`populate_metadata.py` is the first script in the ETL pipeline that scans the `AODN_data/` directory structure and extracts comprehensive metadata from ISO 19115-3 XML files. It creates detailed catalog entries in the `metadata` table with **30+ fields** that subsequent ETL scripts use to locate and process data files.

**Script Version:** 4.0 ✨ **MAJOR UPDATE**  
**Dependencies:** `psycopg2`, `pathlib`, `uuid`, `xml.etree.ElementTree`, `logging`  
**Estimated Runtime:** 2-5 minutes for ~40 datasets (includes XML parsing)

---

## What's New in v4.0

### Enhanced Metadata Extraction

- **30+ fields** extracted from ISO 19115-3 XML files (vs. 8 fields in v1.0)
- **100% field population** for core metadata (title, bbox, dates)
- **68% datasets** have hierarchical parent relationships
- **84% datasets** have OGC WFS endpoints
- **50% datasets** have multiple data contributors

### New Fields Extracted

| Field | Description | Population Rate |
|-------|-------------|----------------|
| `parent_uuid` | Links child datasets to collections | 68% (26/38) |
| `metadata_revision_date` | Last metadata update | 100% (38/38) |
| `distribution_wfs_url` | OGC Web Feature Service | 84% (32/38) |
| `distribution_wms_url` | OGC Web Map Service | 92% (35/38) |
| `distribution_portal_url` | Data portal URL | 45% (17/38) |
| `distribution_publication_url` | DOI/publication | 37% (14/38) |
| `credit` (enhanced) | Multiple credits concatenated | 50% multi-credit |
| `lineage` (enhanced) | Full processing history | Varies by dataset |
| `license_url` | Creative Commons license | 100% AODN datasets |

---

## Architecture

### Data Flow

```
AODN_data/
  ├── Dataset_A/
  │   ├── metadata/metadata.xml  ← ISO 19115-3 XML
  │   └── data files
  ├── Dataset_B/
  └── Dataset_C/
       ↓
  [Directory Scan]
       ↓
  [Find metadata.xml]
       ↓
  [Parse XML (30+ fields)]
       ↓
  [Extract Parent UUID]
       ↓
  [Extract Distribution URLs]
       ↓
  [Concatenate Multiple Credits]
       ↓
  [UUID Generation]
       ↓
  [Bounding Box from XML]
       ↓
  metadata table (34 columns)
```

### Core Components

1. **Directory Scanner** - Discovers dataset folders
2. **XML Metadata Parser** ✨ **NEW** - Extracts ISO 19115-3 metadata
3. **Parent UUID Extractor** ✨ **NEW** - Links datasets to collections
4. **Distribution URL Extractor** ✨ **NEW** - Finds OGC/portal endpoints
5. **Credit Aggregator** ✨ **NEW** - Concatenates multiple contributors
6. **UUID Generator** - Creates deterministic identifiers
7. **Bounding Box Parser** - Extracts spatial extents from XML
8. **Database Writer** - Populates metadata table

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

### `find_metadata_xml(dataset_path)` ✨ **NEW**

**Purpose:** Locates ISO 19115-3 XML metadata file in dataset directory.

**Search Strategy:**
1. Look for `metadata/metadata.xml` (AODN standard location)
2. Look for `*/metadata/metadata.xml` (subdirectories)
3. Look for `metadata.xml` in root
4. Search recursively for any `.xml` file

**Parameters:**
- `dataset_path` (Path): Path to dataset directory

**Returns:** Path to metadata.xml or None

**Example:**
```python
xml_path = find_metadata_xml(Path("AODN_data/Chlorophyll_Database"))
# Returns: Path("AODN_data/Chlorophyll_Database/metadata/metadata.xml")
```

**Debug Logging:**
```
DEBUG: Searching for metadata.xml in Chlorophyll_Database
DEBUG: Found at abc123-uuid/metadata/metadata.xml
```

---

### `parse_xml_metadata(xml_path)` ✨ **NEW**

**Purpose:** Extracts 30+ metadata fields from ISO 19115-3 XML file.

**Parameters:**
- `xml_path` (Path): Path to metadata.xml file

**Returns:** Dictionary with extracted metadata fields

**Extracted Fields:**

```python
{
    # Identifiers
    'uuid': str,                      # Dataset UUID
    'parent_uuid': str | None,        # Parent collection UUID
    
    # Descriptive
    'title': str,                     # Dataset title
    'abstract': str,                  # Full description
    'credit': str,                    # Contributors (concatenated)
    
    # Status & Classification
    'status': str,                    # onGoing, completed, etc.
    'topic_category': str,            # ISO topic (oceans, etc.)
    
    # Temporal Metadata
    'metadata_creation_date': datetime,
    'metadata_revision_date': datetime,
    'citation_date': datetime,
    
    # Metadata Standards
    'language': str,                  # ISO 639-2 code
    'character_set': str,             # UTF-8, etc.
    
    # Spatial Extent (Bounding Box)
    'west': float,
    'east': float,
    'south': float,
    'north': float,
    
    # Temporal Extent
    'time_start': date,
    'time_end': date | None,          # NULL for ongoing
    
    # Vertical Extent
    'vertical_min': float | None,
    'vertical_max': float | None,
    'vertical_crs': str | None,
    
    # Data Provenance
    'lineage': str,                   # Processing history
    'supplemental_info': str | None,
    'use_limitation': str | None,
    
    # Distribution
    'license_url': str,
    'distribution_wfs_url': str | None,
    'distribution_wms_url': str | None,
    'distribution_portal_url': str | None,
    'distribution_publication_url': str | None
}
```

**XML Namespaces:**
```python
NAMESPACES = {
    'mdb': 'http://standards.iso.org/iso/19115/-3/mdb/2.0',
    'cit': 'http://standards.iso.org/iso/19115/-3/cit/2.0',
    'gco': 'http://standards.iso.org/iso/19115/-3/gco/1.0',
    'mri': 'http://standards.iso.org/iso/19115/-3/mri/1.0',
    'mrl': 'http://standards.iso.org/iso/19115/-3/mrl/2.0',
    'mrd': 'http://standards.iso.org/iso/19115/-3/mrd/1.0',
    'gml': 'http://www.opengis.net/gml/3.2',
    # ... more namespaces
}
```

**Extraction Logic (Simplified):**
```python
import xml.etree.ElementTree as ET

def parse_xml_metadata(xml_path):
    tree = ET.parse(xml_path)
    root = tree.getroot()
    
    # Extract UUID
    uuid_elem = root.find('.//mdb:metadataIdentifier//mcc:code/gco:CharacterString', NAMESPACES)
    uuid = uuid_elem.text if uuid_elem is not None else None
    
    # Extract parent UUID (hierarchical datasets)
    parent_elem = root.find('.//mdb:parentMetadata', NAMESPACES)
    parent_uuid = parent_elem.get('uuidref') if parent_elem is not None else None
    
    # Extract multiple credits and concatenate
    credit_elems = root.findall('.//mri:credit/gco:CharacterString', NAMESPACES)
    credits = [c.text for c in credit_elems if c.text]
    credit = '; '.join(credits) if credits else None
    
    # Extract distribution URLs
    wfs_elem = root.find(".//cit:linkage[../cit:protocol[contains(text(), 'OGC:WFS')]]/gco:CharacterString", NAMESPACES)
    wfs_url = wfs_elem.text if wfs_elem is not None else None
    
    # ... extract remaining 20+ fields
    
    return metadata_dict
```

**Error Handling:**
- Catches `ET.ParseError` for malformed XML
- Logs warnings for missing expected elements
- Continues with partial metadata if some fields missing

**Debug Output:**
```
INFO: Parsing XML metadata.xml
DEBUG: Root tag: {http://standards.iso.org/iso/19115/-3/mdb/2.0}MD_Metadata
DEBUG: UUID: abc123-uuid-here
DEBUG: PARENT_UUID: Found parent-uuid-here
DEBUG: CREDIT: Found 3 credit entries
DEBUG: CREDIT: Concatenated 3 credit entries
DEBUG: DISTRIBUTION: Extracted 4 distribution URLs
INFO: XML parsing completed: 17 fields extracted
```

---

### `extract_uuid_from_xml(root)` ✨ **NEW**

**Purpose:** Extracts dataset UUID from XML metadata.

**XPath Patterns Tried (in order):**
1. `mdb:metadataIdentifier/mcc:MD_Identifier/mcc:code/gco:CharacterString`
2. `fileIdentifier/gco:CharacterString` (legacy)
3. `MD_Metadata/fileIdentifier/gco:CharacterString`

**Parameters:**
- `root` (Element): Parsed XML root element

**Returns:** UUID string or None

**Example:**
```python
uuid = extract_uuid_from_xml(root)
# Returns: "3c42cb06-d153-450f-9e47-6a3ceaaf8d9b"
```

---

### `extract_parent_uuid(root)` ✨ **NEW**

**Purpose:** Extracts parent collection UUID to link child datasets.

**XPath Pattern:**
```python
# Look for mdb:parentMetadata element with uuidref attribute
parent = root.find('.//mdb:parentMetadata[@uuidref]', NAMESPACES)
if parent is not None:
    return parent.get('uuidref')
```

**Parameters:**
- `root` (Element): Parsed XML root element

**Returns:** Parent UUID string or None

**Example Use Case:**
```
Dataset: "IMOS - Ocean Colour - Chlorophyll 2020"
Parent:  "IMOS - Bio-Optical Database" (parent_uuid: "744ac2a9-689c...")

Enables queries like:
  SELECT * FROM metadata WHERE parent_uuid = '744ac2a9-689c...';
```

**Debug Output:**
```
DEBUG: PARENT_UUID: Found 744ac2a9-689c-40d3-b262-0df6863f0327
```

---

### `extract_distribution_urls(root)` ✨ **NEW**

**Purpose:** Extracts OGC service endpoints and data portals from XML.

**Protocols Detected:**

| Protocol Pattern | Field Populated |
|------------------|----------------|
| `OGC:WFS` or `OGCWFS` | `distribution_wfs_url` |
| `OGC:WMS` or `OGCWMS` | `distribution_wms_url` |
| `WWW:LINK-1.0-http--portal` | `distribution_portal_url` |
| `WWW:LINK-1.0-http--publication` | `distribution_publication_url` |

**XPath Logic:**
```python
def extract_distribution_urls(root):
    urls = {'wfs': None, 'wms': None, 'portal': None, 'publication': None}
    
    # Find all online resources
    resources = root.findall('.//mrd:onlineResource/cit:CI_OnlineResource', NAMESPACES)
    
    for resource in resources:
        protocol_elem = resource.find('.//cit:protocol/gco:CharacterString', NAMESPACES)
        linkage_elem = resource.find('.//cit:linkage/gco:CharacterString', NAMESPACES)
        
        if protocol_elem is not None and linkage_elem is not None:
            protocol = protocol_elem.text.upper()
            url = linkage_elem.text
            
            if 'WFS' in protocol:
                urls['wfs'] = url
            elif 'WMS' in protocol:
                urls['wms'] = url
            elif 'PORTAL' in protocol:
                urls['portal'] = url
            elif 'PUBLICATION' in protocol:
                urls['publication'] = url
    
    return urls
```

**Debug Output:**
```
DEBUG: DISTRIBUTION: Starting URL extraction
DEBUG: DISTRIBUTION: Found 12 CI_OnlineResource elements
DEBUG: DISTRIBUTION: Checking protocol: OGC:WMS-1.3.0-http-get-map
DEBUG: DISTRIBUTION: WMS URL: https://geoserver.imas.utas.edu.au/geoserver/wms
DEBUG: DISTRIBUTION: Checking protocol: OGC:WFS-1.0.0-http-get-capabilities
DEBUG: DISTRIBUTION: WFS URL: https://geoserver.imas.utas.edu.au/geoserver/wfs
DEBUG: DISTRIBUTION: Extracted 4 distribution URLs
```

**Returns:** Dictionary with keys: `wfs`, `wms`, `portal`, `publication`

---

### `extract_bounding_box_from_xml(root)` ✨ **ENHANCED**

**Purpose:** Extracts spatial extent from XML bounding box element.

**XPath Pattern:**
```xml
<gex:EX_GeographicBoundingBox>
  <gex:westBoundLongitude><gco:Decimal>146.90</gco:Decimal></gex:westBoundLongitude>
  <gex:eastBoundLongitude><gco:Decimal>147.10</gco:Decimal></gex:eastBoundLongitude>
  <gex:southBoundLatitude><gco:Decimal>-39.30</gco:Decimal></gex:southBoundLatitude>
  <gex:northBoundLatitude><gco:Decimal>-39.10</gco:Decimal></gex:northBoundLatitude>
</gex:EX_GeographicBoundingBox>
```

**Fallback Strategy:**
1. Try XML bounding box extraction
2. If incomplete, use `extract_bounding_box_from_name(dataset_name)`
3. If still incomplete, use default Tasmania bbox: `[144.0, 149.0, -44.0, -40.0]`

**Parameters:**
- `root` (Element): Parsed XML root element

**Returns:** Dictionary with keys: `west`, `east`, `south`, `north`

**Debug Output:**
```
DEBUG: Found bounding box element
INFO: Bounding box: 146.90, 147.10, -39.30, -39.10
```

or

```
WARNING: Incomplete bounding box, will use defaults
DEBUG: Estimating bbox for Dataset_Name
DEBUG: Using default Tasmania bounding box
```

---

### `extract_bounding_box_from_name(dataset_name)`

**Purpose:** Infers geographic bounding box from dataset name (fallback when XML is incomplete).

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

### `scan_aodn_directory(base_path='AODN_data')`

**Purpose:** Recursively scans AODN directory and builds dataset list with full metadata extraction.

**Parameters:**
- `base_path` (str): Path to AODN_data directory (default: `'AODN_data'`)

**Returns:** List of dataset dictionaries

**Dataset Dictionary Schema (34 fields):**
```python
{
    # Core identifiers
    'uuid': str,
    'parent_uuid': str | None,
    'title': str,
    'dataset_name': str,
    'dataset_path': str,
    
    # Descriptive
    'abstract': str,
    'credit': str,  # Concatenated credits
    'status': str,
    'topic_category': str,
    
    # Temporal metadata
    'metadata_creation_date': datetime,
    'metadata_revision_date': datetime,
    'citation_date': datetime,
    
    # Standards
    'language': str,
    'character_set': str,
    
    # Spatial extent
    'west': float, 'east': float,
    'south': float, 'north': float,
    
    # Temporal extent
    'time_start': date,
    'time_end': date | None,
    
    # Vertical extent
    'vertical_min': float | None,
    'vertical_max': float | None,
    'vertical_crs': str | None,
    
    # Provenance
    'lineage': str,
    'supplemental_info': str | None,
    'use_limitation': str | None,
    
    # Distribution
    'license_url': str,
    'distribution_wfs_url': str | None,
    'distribution_wms_url': str | None,
    'distribution_portal_url': str | None,
    'distribution_publication_url': str | None,
    
    # File stats
    'file_count': int
}
```

**Behavior:**
- Skips hidden directories (starting with `.`)
- Only processes immediate subdirectories (not recursive)
- Searches for `metadata.xml` in each dataset
- Logs debug message for each dataset found
- Logs info message with field extraction count

**Example Output:**
```
INFO: Processing: Nearshore temperature monitoring in Tasmanian coastal waters
DEBUG: Searching for metadata.xml in Nearshore temperature...
DEBUG: Found at 71fd0720-44a6-11dc-8cd0-00188b4c0af8/metadata/metadata.xml
INFO: Parsing XML metadata.xml
DEBUG: PARENT_UUID: Not found
DEBUG: CREDIT: No credit elements found
DEBUG: LINEAGE: Found lineage: Data from several sites...
DEBUG: DISTRIBUTION: Extracted 3 distribution URLs
INFO: XML parsing completed: 21 fields extracted
INFO: Using bounding box from XML
INFO: File count: 8
INFO: Dataset processed successfully
```

---

### `populate_metadata_table(conn, datasets, force=False)`

**Purpose:** Writes dataset metadata to database with all 34 columns.

**Parameters:**
- `conn` (psycopg2.connection): Active database connection
- `datasets` (list): List of dataset dictionaries from `scan_aodn_directory()`
- `force` (bool): If True, update existing records; if False, skip duplicates

**SQL Logic:**

**Standard Mode (`force=False`):**
```sql
INSERT INTO metadata (
    uuid, parent_uuid, title, abstract, credit, status,
    topic_category, metadata_creation_date, metadata_revision_date,
    citation_date, language, character_set,
    west, east, south, north,
    time_start, time_end,
    vertical_min, vertical_max, vertical_crs,
    lineage, supplemental_info, use_limitation,
    license_url, distribution_wfs_url, distribution_wms_url,
    distribution_portal_url, distribution_publication_url,
    dataset_name, dataset_path
)
VALUES (%s, %s, %s, ... 31 values ...)
ON CONFLICT (uuid) DO NOTHING;
```

**Force Mode (`force=True`):**
```sql
INSERT INTO metadata (...)
VALUES (...)
ON CONFLICT (uuid) DO UPDATE SET
    parent_uuid = EXCLUDED.parent_uuid,
    metadata_revision_date = EXCLUDED.metadata_revision_date,
    distribution_wfs_url = EXCLUDED.distribution_wfs_url,
    -- ... update all 30 fields
```

**Return Values:**
- Logs counts: `inserted_count`, `updated_count`, `skipped_count`
- Commits transaction on success

**Error Handling:**
- Catches `psycopg2.Error` per dataset
- Logs error and continues with next dataset
- Does not rollback entire transaction

---

### `verify_population(conn)` ✨ **ENHANCED**

**Purpose:** Validates metadata insertion and reports comprehensive statistics.

**Queries Executed:**

1. **Total Record Count**
   ```sql
   SELECT COUNT(*) FROM metadata;
   ```

2. **Field Population Statistics** ✨ **NEW**
   ```sql
   SELECT 
     COUNT(*) as total,
     COUNT(parent_uuid) as has_parent,
     COUNT(metadata_revision_date) as has_revision,
     COUNT(distribution_wfs_url) as has_wfs,
     COUNT(distribution_wms_url) as has_wms,
     COUNT(distribution_portal_url) as has_portal,
     ROUND(100.0 * COUNT(parent_uuid) / COUNT(*), 1) as pct_parent,
     ROUND(100.0 * COUNT(distribution_wfs_url) / COUNT(*), 1) as pct_wfs
   FROM metadata;
   ```

3. **Sample Records with New Fields**
   ```sql
   SELECT 
     title, 
     parent_uuid IS NOT NULL as has_parent,
     metadata_revision_date,
     distribution_wfs_url IS NOT NULL as has_wfs,
     distribution_portal_url IS NOT NULL as has_portal
   FROM metadata
   ORDER BY metadata_revision_date DESC
   LIMIT 10;
   ```

4. **Datasets Without Measurements**
   ```sql
   SELECT COUNT(*)
   FROM metadata m
   LEFT JOIN measurements meas ON m.id = meas.metadata_id
   WHERE meas.metadata_id IS NULL;
   ```

**Output Example:**
```
==================== VERIFICATION ====================

Field population statistics:
  • parent_uuid:                  26 / 38 ( 68.4%)
  • metadata_revision_date:       38 / 38 (100.0%)
  • distribution_wfs_url:         32 / 38 ( 84.2%)
  • distribution_wms_url:         35 / 38 ( 92.1%)
  • distribution_portal_url:      17 / 38 ( 44.7%)
  • distribution_publication_url: 14 / 38 ( 36.8%)

Credits with multiple entries (concatenated with '; '):
  Found 19 records with multiple credit entries

Sample metadata records (most recently updated):
  IMOS - Ocean Colour              | 2025-11-28 | WFS: Yes | Portal: No
  Chlorophyll Database              | 2025-10-31 | WFS: Yes | Portal: Yes
  ...

Datasets without measurements: 35

Run 'python populate_measurements.py' to ingest measurements.
```

---

## Database Schema

### `metadata` Table (v4.0)

```sql
CREATE TABLE metadata (
    id SERIAL PRIMARY KEY,
    uuid TEXT UNIQUE NOT NULL,
    parent_uuid TEXT,  -- ✨ NEW: Links to parent collection
    title TEXT NOT NULL,
    abstract TEXT,
    credit TEXT,  -- ✨ ENHANCED: Multiple credits concatenated
    status TEXT,
    topic_category TEXT,
    
    -- Temporal metadata
    metadata_creation_date TIMESTAMP,
    metadata_revision_date TIMESTAMP,  -- ✨ NEW: Last update
    citation_date TIMESTAMP,
    
    -- Standards
    language TEXT DEFAULT 'eng',
    character_set TEXT DEFAULT 'utf8',
    
    -- Spatial extent
    west DECIMAL(10,6),
    east DECIMAL(10,6),
    south DECIMAL(10,6),
    north DECIMAL(10,6),
    
    -- Temporal extent
    time_start DATE,
    time_end DATE,
    
    -- Vertical extent
    vertical_min DECIMAL(6,2),
    vertical_max DECIMAL(6,2),
    vertical_crs TEXT,
    
    -- Provenance
    lineage TEXT,  -- ✨ ENHANCED: Full processing history
    supplemental_info TEXT,
    use_limitation TEXT,
    
    -- Distribution URLs ✨ NEW
    license_url TEXT,
    distribution_wfs_url TEXT,
    distribution_wms_url TEXT,
    distribution_portal_url TEXT,
    distribution_publication_url TEXT,
    
    -- File paths
    dataset_name TEXT,
    dataset_path TEXT,
    
    -- Audit
    extracted_at TIMESTAMP DEFAULT NOW(),
    date_created DATE
);

CREATE INDEX idx_metadata_parent_uuid ON metadata(parent_uuid) WHERE parent_uuid IS NOT NULL;
```

**Total Columns:** 34  
**Fields Populated by v4.0:** 30+ (vs. 8 in v1.0)

---

## Usage Examples

### Basic Usage

```bash
python populate_metadata.py
```

Scans `AODN_data/`, parses all XML files, and inserts metadata.

### Custom Data Directory

```bash
python populate_metadata.py --path /path/to/custom/data
```

### Force Update Existing Records

```bash
python populate_metadata.py --force
```

Updates all 30+ fields for existing metadata records.

### Dry Run (No Database Writes)

```bash
python populate_metadata.py --dry-run
```

Parses XML and logs extracted fields without database insertion.

---

## Logging ✨ **ENHANCED**

### Log Levels

- **INFO** - Progress updates, summary statistics, field counts
- **DEBUG** - XML parsing details, XPath results, URL extraction
- **WARNING** - Missing XML files, incomplete metadata
- **ERROR** - XML parse errors, database failures

### Log Format

```
2026-01-04 09:33:52 - INFO - [scan_aodn_directory:831] 1/38: Processing Chlorophyll Database
2026-01-04 09:33:52 - DEBUG - [find_metadata_xml:158] Searching for metadata.xml
2026-01-04 09:33:52 - DEBUG - [find_metadata_xml:168] Found at abc123-uuid/metadata/metadata.xml
2026-01-04 09:33:52 - INFO - [parse_xml_metadata:408] Parsing XML metadata.xml
2026-01-04 09:33:52 - DEBUG - [extract_parent_uuid:310] PARENT_UUID: Found 744ac2a9-689c...
2026-01-04 09:33:52 - DEBUG - [parse_xml_metadata:480] CREDIT: Found 3 entries
2026-01-04 09:33:52 - DEBUG - [parse_xml_metadata:484] CREDIT: Concatenated 3 credit entries
2026-01-04 09:33:52 - DEBUG - [extract_distribution_urls:392] DISTRIBUTION: WMS URL found
2026-01-04 09:33:52 - DEBUG - [extract_distribution_urls:401] DISTRIBUTION: Extracted 4 URLs
2026-01-04 09:33:52 - INFO - [parse_xml_metadata:774] XML parsing completed: 21 fields extracted
2026-01-04 09:33:52 - INFO - [scan_aodn_directory:871] Dataset processed successfully
```

### Adjusting Log Level

Modify the script header:

```python
logging.basicConfig(
    level=logging.DEBUG,  # Change to DEBUG for verbose XML parsing
    format='%(asctime)s - [%(levelname)s] %(funcName)s:%(lineno)d - %(message)s'
)
```

---

## Error Handling

### XML Parse Errors

**Symptom:**
```
ERROR: XML parsing failed: not well-formed (invalid token): line 42, column 5
```

**Solutions:**
1. Validate XML: `xmllint --noout metadata.xml`
2. Check encoding: Must be UTF-8
3. Fix malformed elements

### Missing Metadata Files

**Symptom:**
```
WARNING: No metadata.xml found for Dataset_Name, using directory scan
```

**Behavior:**
- Falls back to directory-based metadata (v1.0 behavior)
- Only extracts: title, dataset_name, dataset_path, bbox (estimated)
- Logs warning and continues

### Database Connection Failures

**Symptom:**
```
ERROR: Database connection failed: could not connect to server
```

**Solutions:**
1. Check Docker containers: `docker-compose ps`
2. Verify port 5433: `netstat -an | grep 5433`
3. Check credentials in `DB_CONFIG`

---

## Performance Considerations

### Execution Time

| Datasets | Files | XML Parsing | Time (v4.0) |
|----------|-------|-------------|-------------|
| 10       | 500   | Yes         | ~30s        |
| 38       | 2000  | Yes         | ~2min       |
| 100      | 5000  | Yes         | ~5min       |

**v1.0 Comparison:** 5x slower due to XML parsing, but extracts 4x more fields.

### Optimization Tips

1. **Batch XML Parsing** - Already implemented
2. **Index on parent_uuid** - Created automatically (sparse index)
3. **Connection Pooling** - For high-volume scenarios

---

## Verification Queries

### Check Field Population Rates

```sql
SELECT 
  COUNT(*) as total_records,
  COUNT(parent_uuid) as has_parent_uuid,
  COUNT(metadata_revision_date) as has_revision_date,
  COUNT(distribution_wfs_url) as has_wfs,
  COUNT(distribution_wms_url) as has_wms,
  COUNT(distribution_portal_url) as has_portal,
  ROUND(100.0 * COUNT(parent_uuid) / COUNT(*), 1) as pct_parent,
  ROUND(100.0 * COUNT(metadata_revision_date) / COUNT(*), 1) as pct_revision,
  ROUND(100.0 * COUNT(distribution_wfs_url) / COUNT(*), 1) as pct_wfs
FROM metadata;
```

### View Hierarchical Dataset Relationships

```sql
SELECT 
  parent.title as parent_dataset,
  child.title as child_dataset,
  child.metadata_revision_date
FROM metadata child
INNER JOIN metadata parent ON child.parent_uuid = parent.uuid
ORDER BY parent.title, child.title;
```

### Find Datasets with Multiple Credits

```sql
SELECT title, credit
FROM metadata
WHERE credit LIKE '%;%'  -- Contains semicolon separator
ORDER BY title;
```

### List All Distribution Endpoints

```sql
SELECT 
  title,
  distribution_wfs_url,
  distribution_wms_url,
  distribution_portal_url
FROM metadata
WHERE distribution_wfs_url IS NOT NULL
   OR distribution_wms_url IS NOT NULL
ORDER BY title;
```

---

## Integration with ETL Pipeline

### Pipeline Order

1. **populate_metadata.py** ← You are here (v4.0 with XML extraction)
2. populate_parameter_mappings.py
3. populate_measurements.py
4. populate_spatial.py
5. populate_biological.py

### Data Dependencies

**Outputs (34 fields):**
- `metadata.id` - Primary key for measurements/observations
- `metadata.uuid` - Foreign key for data lineage
- `metadata.parent_uuid` - Dataset hierarchy
- `metadata.distribution_wfs_url` - OGC service endpoints
- `metadata.dataset_path` - File locations for ETL

**Consumed By:**
- `populate_measurements.py` - Reads `metadata.id`, `dataset_path`
- `populate_spatial.py` - Uses `distribution_wfs_url` for web services
- `populate_biological.py` - Links observations to datasets

---

## Troubleshooting Checklist

- [ ] Docker containers running (`docker-compose ps`)
- [ ] Database accepting connections (port 5433)
- [ ] `AODN_data/` directory exists with subdirectories
- [ ] XML files present: `*/metadata/metadata.xml`
- [ ] XML files well-formed: `xmllint --noout metadata.xml`
- [ ] Database credentials correct in `DB_CONFIG`
- [ ] PostgreSQL user has INSERT permissions
- [ ] Python dependencies installed: `pip install psycopg2-binary`
- [ ] Sufficient disk space for logs

---

## References

- [Project README](../README.md)
- [Database Schema Documentation](database_schema.md)
- [ISO 19115-3 Metadata Standard](https://www.iso.org/standard/32579.html)
- [AODN Metadata Guidelines](https://aodn.org.au/)
- [Python ElementTree](https://docs.python.org/3/library/xml.etree.elementtree.html)

---

*Last Updated: January 4, 2026*  
*Script Version: 4.0*  
*Maintained by: Huon Channel Marine Analytics Project*