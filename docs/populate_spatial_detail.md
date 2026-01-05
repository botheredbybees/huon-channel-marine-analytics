# populate_spatial.py - Detailed Documentation

## Overview

`populate_spatial.py` extracts spatial features from ESRI Shapefiles and loads them into the PostGIS-free database. It focuses on marine regions, boundaries, and other geographic reference data that define the study area.

**Version:** 3.0 (Enhanced with --force flag and robust encoding handling)
**Last Updated:** January 6, 2026

## Purpose

This script:
- Identifies datasets containing shapefile (.shp) data
- Converts shapefiles to GeoJSON using GDAL/OGR tools
- Extracts centroid lat/lon coordinates from polygon/point geometries
- Loads spatial features into the `spatial_features` table
- Preserves attribute data (properties) from shapefiles as JSONB
- Handles character encoding issues (UTF-8, Windows-1252, Latin-1)
- Tracks extraction timestamps for audit trails
- Enables spatial queries and geographic analysis

## New Features (v3.0)

### 1. Force Re-processing

**Command-line flag:** `--force`

Allows re-processing of datasets that already have spatial features in the database.

**Usage:**
```bash
# Re-process all datasets
python populate_spatial.py --force

# Re-process specific dataset
python populate_spatial.py --force --dataset "SeaMap Tasmania"
```

**Behavior:**
- Deletes existing spatial features for the dataset
- Re-extracts from shapefiles
- Logs number of deleted and inserted features
- Updates `metadata.extracted_at` timestamp

### 2. Dataset Filtering

**Command-line flag:** `--dataset <pattern>`

Process only datasets matching a specific name pattern (case-insensitive).

**Usage:**
```bash
# Process only kelp-related datasets
python populate_spatial.py --dataset "kelp"

# Process Tasmania bioregion data
python populate_spatial.py --dataset "Tasmania benthic"
```

### 3. Enhanced Encoding Handling

Automatically handles shapefiles with various character encodings:

**Supported Encodings:**
1. UTF-8 (modern standard)
2. Windows-1252 / CP1252 (most common for GIS data)
3. ISO-8859-1 / Latin-1 (European characters)

**Fallback Strategy:**
1. Try Windows-1252 (most common)
2. If fails, convert all fields to strings and use error replacement
3. Logs encoding issues without stopping processing

**Fixed Issue:**
- Previously failed on "Living Shorelines Australia" dataset with `'utf-8' codec can't decode byte 0xa0`
- Now successfully processes with CP1252 encoding

### 4. Audit Trail & Logging

**Timestamp Tracking:**
- Updates `metadata.extracted_at` after successful processing
- Allows tracking when datasets were last processed

**Enhanced Output:**
```
Processing 'SeaMap Tasmania benthic habitat map' (1 shapefile(s))...
  ✓ Inserted 1770 spatial features

=====================================================
Processing Complete
=====================================================
Datasets processed: 6
New features inserted: 2242
Total spatial features in database: 2242
```

**Error Logging:**
- ⚠️ Warnings for missing paths
- ⚠️ Batch insert failures with row-by-row retry
- ✗ Individual feature failures with coordinates logged

## Architecture

### Data Flow

```
┌──────────────────┐
│ AODN Datasets    │
│ (Shapefiles)     │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Dataset Scanner  │
│ (--force filter) │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Shapefile        │
│ Discovery        │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ ogr2ogr          │
│ Conversion       │ → Temp GeoJSON (with encoding fallback)
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Centroid         │
│ Extraction       │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Batch Insert     │
│ (with retry)     │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ spatial_features │
│ Table            │
└──────────────────┘
```

## Core Components

### 1. Database Connection

```python
DB_CONFIG = {
    'dbname': 'marine_db',
    'user': 'marine_user',
    'password': 'marine_pass123',
    'host': 'localhost',
    'port': '5433'
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)
```

### 2. Shapefile Conversion (Enhanced)

**Function:** `convert_shp_to_geojson(shp_path)`

**Purpose:** Converts ESRI Shapefiles to GeoJSON format with robust encoding handling.

**Process:**
1. Creates temporary UUID-named JSON file in `/tmp/`
2. Tries Windows-1252 encoding first (most common for shapefiles)
3. Falls back to field type conversion if encoding fails
4. Loads JSON data into Python dict
5. Cleans up temporary file

**Primary Command:**
```bash
ogr2ogr -f GeoJSON -t_srs EPSG:4326 \
  --config SHAPE_ENCODING CP1252 \
  /tmp/{uuid}.json {shp_path}
```

**Fallback Command (encoding issues):**
```bash
ogr2ogr -f GeoJSON -t_srs EPSG:4326 \
  -fieldTypeToString All \
  --config SHAPE_ENCODING "" \
  /tmp/{uuid}.json {shp_path}
```

**Returns:** 
- `dict`: GeoJSON FeatureCollection
- `None`: On conversion failure

**Error Handling:**
- Catches subprocess errors
- Tries UTF-8 then Latin-1 for JSON reading
- Uses `errors='replace'` for undecodable characters
- Ensures temp file cleanup
- Suppresses ogr2ogr stderr output

### 3. Dataset Scanner (Enhanced)

**Normal Mode SQL:**
```sql
SELECT m.id, m.uuid, m.dataset_path, m.title
FROM metadata m
LEFT JOIN measurements mes ON m.id = mes.metadata_id
LEFT JOIN spatial_features sf ON m.id = sf.metadata_id
GROUP BY m.id
HAVING COUNT(mes.data_id) = 0
  AND COUNT(sf.id) = 0
  AND m.dataset_path IS NOT NULL
ORDER BY m.title
```

**Force Mode SQL:**
```sql
SELECT m.id, m.uuid, m.dataset_path, m.title
FROM metadata m
WHERE m.dataset_path IS NOT NULL
ORDER BY m.title
```

**With --dataset filter:**
```sql
... WHERE m.dataset_path IS NOT NULL
  AND LOWER(m.title) LIKE LOWER(%s)
```

**Logic:**
- Normal: Only processes datasets without measurements or spatial features
- Force: Processes all (or filtered) datasets regardless of existing data
- Deletes existing features in force mode before re-processing

### 4. Centroid Extraction

**Function:** `extract_centroid(geometry)`

**Supported Geometry Types:**
- Point → Returns coordinates directly
- Polygon → Calculates arithmetic mean of exterior ring coordinates
- MultiPolygon → Uses first polygon's exterior ring
- LineString → Returns midpoint

**Returns:** `(latitude, longitude)` or `(None, None)`

**Example:**
```python
# Polygon centroid calculation
coords = [[147.0, -43.0], [147.5, -43.0], [147.5, -43.5], [147.0, -43.5], [147.0, -43.0]]
lat_sum = sum(c[1] for c in coords)  # -43.0 + -43.0 + -43.5 + -43.5 + -43.0 = -215.0
lon_sum = sum(c[0] for c in coords)  # 147.0 + 147.5 + 147.5 + 147.0 + 147.0 = 736.0
count = 5
centroid = (lat_sum / count, lon_sum / count)  # (-43.0, 147.2)
```

### 5. Feature Extraction and Insertion

**Batch Processing:**
```python
features_to_insert = []
for feature in geojson['features']:
    if feature.get('geometry') is None:
        continue
    lat, lon = extract_centroid(feature['geometry'])
    if lat is None or lon is None:
        continue
    props = json.dumps(feature['properties'])
    features_to_insert.append((meta_id, lat, lon, props))
```

**Database Insert:**
```python
from psycopg2.extras import execute_values

sql = """
INSERT INTO spatial_features (metadata_id, latitude, longitude, properties)
VALUES %s
"""

template = "(%s, %s, %s, %s)"

execute_values(cur, sql, features_to_insert, template=template)
```

**Timestamp Update:**
```python
cur.execute("""
    UPDATE metadata 
    SET extracted_at = CURRENT_TIMESTAMP 
    WHERE id = %s
""", (meta_id,))
```

## Database Schema

### spatial_features Table (v3.0 - No UUID field)

```sql
CREATE TABLE IF NOT EXISTS spatial_features (
    id SERIAL PRIMARY KEY,
    metadata_id INTEGER REFERENCES metadata(id),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    properties JSONB
);

CREATE INDEX IF NOT EXISTS idx_spatial_features_lat_lon 
    ON spatial_features (latitude, longitude);

CREATE INDEX IF NOT EXISTS idx_spatial_features_metadata_id 
    ON spatial_features(metadata_id);
```

**Note:** The `uuid` field was removed in schema v3.2. The script now uses only `metadata_id` to link features to datasets.

### Example Queries

**Find features by name:**
```sql
SELECT id, properties->>'NAME' as region_name,
       latitude, longitude
FROM spatial_features
WHERE properties->>'NAME' ILIKE '%Huon%';
```

**Count features per dataset:**
```sql
SELECT m.title, COUNT(sf.id) as feature_count
FROM metadata m
JOIN spatial_features sf ON m.id = sf.metadata_id
GROUP BY m.title
ORDER BY feature_count DESC;
```

**Find features within bounding box:**
```sql
SELECT sf.id, sf.properties->>'NAME', sf.latitude, sf.longitude
FROM spatial_features sf
WHERE sf.latitude BETWEEN -43.5 AND -43.0
  AND sf.longitude BETWEEN 147.0 AND 147.5;
```

## Usage

### Basic Execution (Normal Mode)

```bash
python populate_spatial.py
```

**Behavior:** Processes only datasets without existing spatial features.

### Force Re-processing

```bash
# Re-process all datasets
python populate_spatial.py --force

# Re-process specific dataset
python populate_spatial.py --force --dataset "SeaMap"
```

**Behavior:** Deletes existing features and re-extracts from shapefiles.

### Dataset Filtering

```bash
# Process only kelp datasets
python populate_spatial.py --dataset "kelp"

# Case-insensitive partial match
python populate_spatial.py --dataset "Tasmania"
```

### Combined Arguments

```bash
# Force re-process only Living Shorelines dataset
python populate_spatial.py --force --dataset "Living Shorelines"
```

### Expected Output

```
Finding datasets...
Found 7 candidate dataset(s).

Processing 'Seagrass Presence Absence Australia (ACEAS)' (1 shapefile(s))...
  ✓ Inserted 83 spatial features

Processing 'SeaMap Tasmania benthic habitat map' (1 shapefile(s))...
  ✓ Inserted 1770 spatial features

Processing 'Aerial surveys of giant kelp (Macrocystis pyrifera)' (2 shapefile(s))...
  ✓ Inserted 324 spatial features

Processing 'Living Shorelines Australia database' (2 shapefile(s))...
  ✓ Inserted 10 spatial features

============================================================
Processing Complete
============================================================
Datasets processed: 7
New features inserted: 2242
Total spatial features in database: 2242
```

### Requirements

**System Dependencies:**
```bash
# Ubuntu/Debian
sudo apt-get install gdal-bin

# macOS
brew install gdal

# Test installation
ogr2ogr --version
```

**Python Libraries:**
```bash
pip install psycopg2-binary
```

**Database:**
- PostgreSQL 12+ (PostGIS NOT required)
- `spatial_features` table created
- Write permissions for script user

## Common Data Types

### Marine Bioregions

**Typical Properties:**
```json
{
  "NAME": "D'Entrecasteaux and Huon Bioregion",
  "AREA_KM2": 4250.3,
  "IMCRA_REG": "Tasmanian Shelf",
  "STATE": "Tasmania",
  "OBJECTID": 42
}
```

**Centroid:** Arithmetic mean of polygon coordinates

### Marine Protected Areas

**Typical Properties:**
```json
{
  "NAME": "Tinderbox Marine Reserve",
  "TYPE": "Marine Reserve",
  "IUCN_CAT": "Ia",
  "MANAGEMENT": "Parks and Wildlife Service",
  "ESTABLISH": "2009-03-15"
}
```

### Habitat Zones

**Typical Properties:**
```json
{
  "ZONE_NAME": "Reef Habitat",
  "DEPTH_MIN": 0,
  "DEPTH_MAX": 25,
  "SUBSTRATE": "Rocky reef"
}
```

## Performance Considerations

### Typical Execution Times

| Features | Shapefiles | Time |
|----------|------------|------|
| 10-50 | 1-2 | <10s |
| 100-500 | 3-5 | 30s-1m |
| 1000+ | 10+ | 2-5m |

**Bottlenecks:**
- ogr2ogr conversion (disk I/O)
- Complex polygon geometries
- Character encoding detection

### Optimization Strategies

**Batch Size:**
- Default: Process all features per shapefile
- Typical: 10-1000 features per shapefile
- Memory usage is minimal (centroids only, not full geometries)

**Indexing:**
```sql
CREATE INDEX idx_spatial_features_lat_lon 
  ON spatial_features (latitude, longitude);

CREATE INDEX idx_spatial_features_props 
  ON spatial_features USING GIN(properties);
```

## Error Handling

### Common Errors

**1. ogr2ogr Not Found**
```
FileNotFoundError: [Errno 2] No such file or directory: 'ogr2ogr'
```
**Solution:** Install GDAL
```bash
sudo apt-get install gdal-bin
```

**2. Encoding Errors (FIXED in v3.0)**
```
UnicodeDecodeError: 'utf-8' codec can't decode byte 0xa0
```
**Solution:** Script now handles automatically with CP1252/Latin-1 fallback

**3. Missing Projection**
```
ERROR: Unable to open EPSG support file gcs.csv
```
**Solution:** Ensure .prj file exists

**4. Permission Denied**
```
PermissionError: [Errno 13] Permission denied: '/tmp/uuid.json'
```
**Solution:** Check `/tmp` permissions
```bash
ls -ld /tmp
# Should show: drwxrwxrwt
```

### Debugging

**Enable Verbose Output:**
Remove `stdout/stderr` suppression in `convert_shp_to_geojson()`:
```python
subprocess.check_call(cmd)  # Shows ogr2ogr output
```

**Check Temp Files:**
```bash
ls -lh /tmp/*.json
```

**Manual Conversion Test:**
```bash
ogr2ogr -f GeoJSON -t_srs EPSG:4326 \
  --config SHAPE_ENCODING CP1252 \
  test.json problem_file.shp
cat test.json | jq '.'
```

**Check Database State:**
```sql
-- Count features per dataset
SELECT m.title, COUNT(sf.id)
FROM metadata m
LEFT JOIN spatial_features sf ON m.id = sf.metadata_id
GROUP BY m.title;

-- Check last extraction times
SELECT title, extracted_at
FROM metadata
WHERE extracted_at IS NOT NULL
ORDER BY extracted_at DESC;
```

## Integration Points

### Upstream Dependencies

1. **populate_metadata.py**
   - Must run first
   - Provides `metadata_id` foreign keys
   - Defines `dataset_path` for file discovery

### Downstream Usage

1. **Spatial Queries**
   - Filter measurements by region (lat/lon within bounds)
   - Calculate statistics per bioregion
   - Identify locations within MPAs

2. **Visualization**
   - Web mapping applications
   - QGIS desktop analysis
   - Dashboard widgets

3. **Analysis Scripts**
   - Habitat suitability models
   - Connectivity analysis
   - Marine spatial planning

## Quality Assurance

### Validation Checks

**1. Feature Count:**
```sql
SELECT m.title, COUNT(sf.id) as feature_count
FROM metadata m
JOIN spatial_features sf ON m.id = sf.metadata_id
GROUP BY m.title
ORDER BY feature_count DESC;
```

**2. Coordinate Range (Tasmania):**
```sql
SELECT MIN(latitude) as min_lat,
       MAX(latitude) as max_lat,
       MIN(longitude) as min_lon,
       MAX(longitude) as max_lon
FROM spatial_features;
-- Expected: lat ~(-44 to -40), lon ~(145 to 149)
```

**3. Property Completeness:**
```sql
SELECT id, metadata_id, latitude, longitude
FROM spatial_features
WHERE properties = '{}'::jsonb
   OR properties IS NULL;
```

**4. Extraction Timestamps:**
```sql
SELECT m.title, m.extracted_at,
       COUNT(sf.id) as feature_count
FROM metadata m
LEFT JOIN spatial_features sf ON m.id = sf.metadata_id
WHERE m.extracted_at IS NOT NULL
GROUP BY m.id
ORDER BY m.extracted_at DESC;
```

## Troubleshooting Checklist

- [ ] GDAL/OGR tools installed and in PATH
- [ ] `spatial_features` table exists with correct schema (no uuid field)
- [ ] Shapefiles include .shp, .shx, .dbf companion files
- [ ] Write permissions on /tmp directory
- [ ] Database connection parameters correct
- [ ] Sufficient disk space for temp files
- [ ] Character encoding handled (CP1252 fallback working)

## Migration from v2.0 to v3.0

### Schema Changes

**Removed field:** `uuid` column from `spatial_features` table

**Migration SQL:**
```sql
-- If you have old data with uuid field:
ALTER TABLE spatial_features DROP COLUMN IF EXISTS uuid;
```

### Script Changes

**What's New:**
- `--force` flag for re-processing
- `--dataset` filter
- Enhanced encoding handling (CP1252, Latin-1)
- Audit trail with `extracted_at` timestamps
- Summary statistics at end of run

**Breaking Changes:**
- None (backward compatible)

## References

### Documentation
- [GDAL/OGR Documentation](https://gdal.org/)
- [psycopg2 Documentation](https://www.psycopg.org/docs/)
- [GeoJSON Specification](https://geojson.org/)
- [Character Encoding Guide](https://docs.python.org/3/library/codecs.html)

### Related Scripts
- [populate_metadata.py](populate_metadata_detail.md)
- [populate_measurements.py](populate_measurements_detail.md)
- [Database Schema](database_schema.md)
- [Scripts Overview](scripts.md)

---

*Last Updated: January 6, 2026*
*Version: 3.0*