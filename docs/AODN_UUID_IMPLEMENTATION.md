# AODN UUID Extraction & Deduplication Implementation Guide

## Overview

This document describes the implementation of AODN UUID extraction and automatic deduplication logic for the Huon Estuary marine data ETL pipeline.

**Purpose**: Prevent duplicate ingestion of AODN datasets by:
1. Extracting the official AODN UUID from ISO 19115-3 XML metadata
2. Checking if the AODN UUID already exists in the database
3. Skipping re-ingestion of existing AODN datasets
4. Maintaining an audit trail of deduplication actions

---

## Key Changes

### 1. Database Schema (init.sql)

Added `aodn_uuid` field to `metadata` table:

```sql
ALTER TABLE metadata 
ADD COLUMN IF NOT EXISTS aodn_uuid TEXT UNIQUE;

CREATE INDEX IF NOT EXISTS idx_metadata_aodn_uuid ON metadata(aodn_uuid)
  WHERE aodn_uuid IS NOT NULL;
```

**Field Details**:
- **Type**: TEXT UNIQUE (nullable)
- **Purpose**: Store official AODN identifier separate from internal UUID
- **NULL Handling**: Intentionally nullable for non-AODN datasets
- **Index**: Sparse index (only on non-NULL values) for performance

### 2. Updated Scripts

#### enrich_metadata_from_xml.py (Enhanced)

**New Features**:
- Extracts AODN UUID from ISO 19115-3 XML metadata
- Implements deduplication check before processing
- Populates `aodn_uuid` field for AODN-sourced datasets
- Logs all deduplication actions

**Key Methods**:

```python
def _extract_aodn_uuid(self, root: ET.Element) -> Optional[str]:
    """Extract AODN UUID from XML fileIdentifier element."""
    # Tries multiple XPath patterns:
    # - .//gmd:fileIdentifier/gco:CharacterString
    # - .//mdb:MD_Metadata/mdb:metadataIdentifier/mcc:MD_Identifier/mcc:code/gco:CharacterString
    # Returns: AODN UUID string or None

def check_aodn_uuid_exists(self, aodn_uuid: str) -> bool:
    """Check if AODN UUID already exists (deduplication)."""
    # Query: SELECT id FROM metadata WHERE aodn_uuid = %s
    # Returns: True if exists (skip), False if new (process)
```

**Enhanced Statistics**:
```
=== METADATA ENRICHMENT SUMMARY ===
XML files found:          10
Files processed:          8
Files deduplicated:       2    [NEW]
Files failed:             0
AODN UUIDs extracted:     8    [NEW]
Rows updated:             12
```

### 3. New Utility Module: aodn_deduplication.py

Reusable deduplication utility for all ETL scripts:

```python
from aodn_deduplication import AODNDeduplicator

dedup = AODNDeduplicator(db_connection)

# Single check
if dedup.aodn_uuid_exists(aodn_uuid):
    logger.info("Dataset already ingested")
else:
    # Process new dataset
    pass

# Batch check (more efficient)
results = dedup.batch_check_aodn_uuids([uuid1, uuid2, uuid3])
for uuid, exists in results.items():
    if not exists:
        process(uuid)

# Find duplicates in database
duplicates = dedup.get_duplicate_aodn_datasets()
if duplicates:
    logger.warning(f"Found {len(duplicates)} duplicate AODN UUIDs")

# Print statistics
dedup.print_stats()
```

**Available Methods**:

| Method | Purpose |
|--------|----------|
| `aodn_uuid_exists(uuid)` | Check if AODN UUID exists |
| `batch_check_aodn_uuids(uuids)` | Check multiple UUIDs efficiently |
| `log_skip(uuid, reason)` | Log deduplication events (optional) |
| `get_aodn_uuid_for_internal_uuid(uuid)` | Lookup AODN UUID by internal UUID |
| `get_duplicate_aodn_datasets()` | Find duplicates in database |
| `print_stats()` | Print deduplication statistics |
| `reset_stats()` | Reset statistics counters |

---

## Usage Examples

### Example 1: Run Metadata Enrichment with AODN UUID Extraction

```bash
# Set environment variables
export DB_HOST=localhost
export DB_PORT=5433
export DB_NAME=marine_db
export DB_USER=marine_user
export DB_PASSWORD=marine_pass123
export AODN_DATA_PATH=/AODN_data

# Run enrichment (with automatic AODN UUID extraction & deduplication)
python scripts/enrich_metadata_from_xml.py
```

**Expected Output**:
```
INFO - Found 10 metadata.xml files in /AODN_data
INFO - Processing 550e8400-e29b-41d4-a716-446655440000
INFO - Extracted AODN UUID: 550e8400-e29b-41d4-a716-446655440000
INFO - Updated 550e8400-e29b-41d4-a716-446655440000: 5 fields enriched
INFO -   AODN UUID: 550e8400-e29b-41d4-a716-446655440000
INFO - Processing a1b2c3d4-e5f6-47g8-h9i0-j1k2l3m4n5o6
WARNING - AODN dataset with UUID 550e8400-e29b-41d4-a716-446655440000 already exists. Skipping.
=== METADATA ENRICHMENT SUMMARY ===
XML files found:          2
Files processed:          1
Files deduplicated:       1
AODN UUIDs extracted:     2
Rows updated:             5
```

### Example 2: Using Deduplicator Utility in Custom Script

```python
#!/usr/bin/env python3
import psycopg2
import logging
from aodn_deduplication import AODNDeduplicator, create_dedup_log_table

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Connect to database
conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="marine_db",
    user="marine_user",
    password="marine_pass123"
)

# Create deduplicator
dedup = AODNDeduplicator(conn)

# Optional: Create audit log table on first run
create_dedup_log_table(conn)

# Check if datasets exist
aodn_uuids = [
    "550e8400-e29b-41d4-a716-446655440000",
    "a1b2c3d4-e5f6-47g8-h9i0-j1k2l3m4n5o6",
]

existing = dedup.batch_check_aodn_uuids(aodn_uuids)

for uuid, exists in existing.items():
    if exists:
        logger.info(f"Skipping {uuid} (already ingested)")
        dedup.log_skip(uuid, reason="duplicate", details="Already in system")
    else:
        logger.info(f"Processing new dataset {uuid}")
        # ... process dataset ...

# Print statistics
dedup.print_stats()
conn.close()
```

---

## Data Model

### Metadata Table

| Field | Type | Nullable | Purpose |
|-------|------|----------|----------|
| `id` | SERIAL | No | Internal primary key |
| `uuid` | TEXT | No | Internal system identifier |
| `aodn_uuid` | TEXT | **Yes** | Official AODN identifier (NEW) |
| ... other fields ... | | | |

### UUID Mapping Examples

**AODN Dataset**:
```
uuid:      550e8400-e29b-41d4-a716-446655440000  (internal)
aodn_uuid: 550e8400-e29b-41d4-a716-446655440000  (same, from XML)
```

**Non-AODN Dataset**:
```
uuid:      a1b2c3d4-e5f6-47g8-h9i0-j1k2l3m4n5o6  (internal, auto-generated)
aodn_uuid: NULL  (no AODN source)
```

### Optional Dedup Log Table

```sql
CREATE TABLE dedup_log (
    id SERIAL PRIMARY KEY,
    aodn_uuid TEXT NOT NULL,
    skip_reason TEXT,
    details TEXT,
    logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## Deduplication Flow

```
Parse XML Metadata
    ↓
Extract AODN UUID
    ↓
Check if AODN UUID exists in DB?
    ├─ YES → Log skip
    │       └─ Continue to next file
    └─ NO  → Extract other metadata
           └─ Update database
           └─ Log successful ingest
```

---

## Testing

### Test 1: AODN UUID Extraction

```bash
# Test data with UUID in XML
cat > /tmp/test_metadata.xml << 'EOF'
<?xml version="1.0"?>
<gmd:MD_Metadata xmlns:gmd="http://www.isotc211.org/2005/gmd"
                 xmlns:gco="http://www.isotc211.org/2005/gco">
    <gmd:fileIdentifier>
        <gco:CharacterString>550e8400-e29b-41d4-a716-446655440000</gco:CharacterString>
    </gmd:fileIdentifier>
</gmd:MD_Metadata>
EOF

# Script will extract UUID automatically
```

### Test 2: Deduplication

```bash
# First run: Processes dataset
python scripts/enrich_metadata_from_xml.py
# Output: "Updated ...: fields enriched"

# Second run: Skips duplicate
python scripts/enrich_metadata_from_xml.py
# Output: "already exists in database. Skipping."
```

### Test 3: Batch Deduplication Check

```python
from aodn_deduplication import AODNDeduplicator
import psycopg2

conn = psycopg2.connect(...)
dedup = AODNDeduplicator(conn)

# Test batch check
uuids = ["uuid1", "uuid2", "uuid3"]
results = dedup.batch_check_aodn_uuids(uuids)

assert len(results) == 3
assert all(isinstance(v, bool) for v in results.values())
print(results)  # {"uuid1": False, "uuid2": True, "uuid3": False}
```

---

## Troubleshooting

### Issue: AODN UUID Not Extracted

**Symptoms**: `aodn_uuid` column remains NULL

**Diagnosis**:
```python
# Check if UUID element exists in XML
import xml.etree.ElementTree as ET
tree = ET.parse('metadata.xml')
root = tree.getroot()

# Try XPath patterns
NS = {'gmd': 'http://www.isotc211.org/2005/gmd', 'gco': 'http://www.isotc211.org/2005/gco'}
for elem in root.findall('.//gmd:fileIdentifier/gco:CharacterString', NS):
    print(elem.text)
```

**Solutions**:
1. Verify XML uses correct ISO 19115-3 namespaces
2. Check XPath patterns in `_extract_aodn_uuid()`
3. Enable debug logging: `logging.basicConfig(level=logging.DEBUG)`

### Issue: Deduplication Not Working

**Symptoms**: Same dataset ingested multiple times

**Diagnosis**:
```sql
-- Check for duplicate AODN UUIDs
SELECT aodn_uuid, COUNT(*) as count FROM metadata 
WHERE aodn_uuid IS NOT NULL 
GROUP BY aodn_uuid 
HAVING COUNT(*) > 1;
```

**Solutions**:
1. Verify `aodn_uuid` column exists: `ALTER TABLE metadata ADD COLUMN aodn_uuid TEXT UNIQUE;`
2. Check for NULL values: `SELECT COUNT(*) FROM metadata WHERE aodn_uuid IS NULL;`
3. Verify dedup logic is enabled in script

### Issue: Database Constraint Violation

**Error**: `duplicate key value violates unique constraint 'metadata_aodn_uuid_key'`

**Cause**: Attempting to insert duplicate AODN UUID

**Solution**: Remove duplicate rows first

```sql
-- Identify duplicates
WITH dupes AS (
  SELECT aodn_uuid, COUNT(*) as cnt, array_agg(id) as ids
  FROM metadata
  WHERE aodn_uuid IS NOT NULL
  GROUP BY aodn_uuid
  HAVING COUNT(*) > 1
)
SELECT dupes.aodn_uuid, dupes.cnt, dupes.ids[2:] as to_delete
FROM dupes;

-- Delete duplicates (keep first occurrence)
DELETE FROM metadata
WHERE id IN (
  SELECT (array_agg(id))[2]
  FROM metadata
  GROUP BY aodn_uuid
  HAVING COUNT(*) > 1
);
```

---

## Migration Guide

### For Existing Databases

**Option 1: No Action** (Recommended for mixed-source data)
```sql
-- Add column (already nullable)
ALTER TABLE metadata 
ADD COLUMN IF NOT EXISTS aodn_uuid TEXT UNIQUE;

-- Leave NULL for existing AODN datasets
-- New ingestions will populate automatically
```

**Option 2: Backfill Known AODN UUIDs**
```sql
-- Only if all existing data is AODN-sourced
UPDATE metadata SET aodn_uuid = uuid WHERE aodn_uuid IS NULL;
```

**Option 3: Mark Non-AODN Data Explicitly**
```sql
-- Mark specific datasets as non-AODN
UPDATE metadata 
SET aodn_uuid = NULL 
WHERE dataset_name IN ('custom_dataset1', 'custom_dataset2');
```

### For New Deployments

1. Run `init.sql` (includes `aodn_uuid` field)
2. Deduplication works automatically
3. No additional setup needed

---

## Performance Considerations

### Deduplication Check Cost

- **Single UUID check**: ~1-5ms (indexed lookup)
- **Batch 100 UUIDs**: ~10-20ms (more efficient than 100 individual checks)
- **Index**: Sparse index only on non-NULL values (minimal overhead)

### Query Optimization

```sql
-- Fast: Uses index
SELECT id FROM metadata WHERE aodn_uuid = '550e8400-e29b-41d4-a716-446655440000';

-- Fast: Batch query with index
SELECT aodn_uuid FROM metadata WHERE aodn_uuid = ANY(ARRAY['uuid1', 'uuid2', 'uuid3']);

-- Fast: Find duplicates with grouping
SELECT aodn_uuid, COUNT(*) FROM metadata WHERE aodn_uuid IS NOT NULL GROUP BY aodn_uuid HAVING COUNT(*) > 1;
```

---

## Backward Compatibility

✅ **Fully backward compatible**
- New `aodn_uuid` field is optional (nullable)
- Existing queries unaffected
- Non-AODN datasets work normally
- No cascading changes required

---

## Future Enhancements

1. **Dedup Log Table**: Optional audit trail of all deduplication events
2. **Batch Ingest Mode**: Process multiple datasets with single dedup query
3. **AODN API Integration**: Auto-fetch metadata from AODN portal
4. **Conflict Resolution**: Handle UUID mismatches between systems
5. **Metrics Dashboard**: Track deduplication rates and success rates

---

## Support & Questions

For questions about AODN UUID implementation:
1. Check logs for deduplication messages
2. Run diagnostic queries to check UUID state
3. Use utility functions to analyze dedup behavior
4. See troubleshooting section above
