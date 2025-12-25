# populate_spatial.py - Detailed Documentation

## Overview

`populate_spatial.py` extracts spatial features from ESRI Shapefiles and loads them into the PostGIS-enabled database. It focuses on marine regions, boundaries, and other geographic reference data that define the study area.

## Purpose

This script:
- Identifies datasets containing shapefile (.shp) data
- Converts shapefiles to GeoJSON using GDAL/OGR tools
- Extracts polygon/multipolygon geometries for marine regions
- Loads spatial features into the `spatial_features` table
- Preserves attribute data (properties) from shapefiles
- Enables spatial queries and geographic analysis

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
│ (Empty Datasets) │
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
│ Conversion       │ → Temp GeoJSON
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Feature          │
│ Extraction       │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ PostGIS Insert   │
│ (ST_GeomFromJSON)│
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

### 2. Shapefile Conversion

**Function:** `convert_shp_to_geojson(shp_path)`

**Purpose:** Converts ESRI Shapefiles to GeoJSON format for easier parsing.

**Process:**
1. Creates temporary UUID-named JSON file in `/tmp/`
2. Executes `ogr2ogr` command:
   - Input: Shapefile (automatic format detection)
   - Output: GeoJSON
   - Transform: `-t_srs EPSG:4326` (ensures WGS84 lat/lon)
3. Loads JSON data into Python dict
4. Cleans up temporary file

**Command:**
```bash
ogr2ogr -f GeoJSON -t_srs EPSG:4326 /tmp/{uuid}.json {shp_path}
```

**Returns:** 
- `dict`: GeoJSON FeatureCollection
- `None`: On conversion failure

**Error Handling:**
- Catches subprocess errors
- Ensures temp file cleanup
- Suppresses ogr2ogr stderr output

**Example GeoJSON Structure:**
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": {
        "type": "Polygon",
        "coordinates": [[[147.0, -43.0], [147.5, -43.0], ...]]
      },
      "properties": {
        "NAME": "Huon Bioregion",
        "AREA_KM2": 1234.5,
        "TYPE": "Marine Park"
      }
    }
  ]
}
```

### 3. Dataset Scanner

**SQL Query:**
```sql
SELECT m.id, m.uuid, m.dataset_path, m.title
FROM metadata m
LEFT JOIN measurements mes ON m.id = mes.metadata_id
LEFT JOIN spatial_features sf ON m.id = sf.metadata_id
GROUP BY m.id
HAVING COUNT(mes.data_id) = 0
  AND COUNT(sf.id) = 0
  AND m.dataset_path IS NOT NULL
```

**Logic:**
- Identifies datasets with NO measurements OR spatial features
- Excludes datasets without file paths
- Targets "empty" datasets likely containing only shapefiles

**Rationale:**
- Avoids reprocessing datasets already loaded
- Focuses on spatial-only data packages
- Efficient for incremental updates

### 4. Shapefile Discovery

**Process:**
```python
for root, dirs, files in os.walk(path):
    for file in files:
        if file.lower().endswith('.shp'):
            shp_files.append(os.path.join(root, file))
```

**Features:**
- Recursive directory traversal
- Case-insensitive `.shp` detection
- Captures full file paths

**Typical Shapefile Structure:**
```
dataset_folder/
├── spatial/
│   ├── bioregions.shp
│   ├── bioregions.shx  (spatial index)
│   ├── bioregions.dbf  (attributes)
│   ├── bioregions.prj  (projection)
│   └── bioregions.cpg  (encoding)
```

**Note:** Only `.shp` files are targeted; companion files (.shx, .dbf, .prj) are automatically used by ogr2ogr.

### 5. Feature Extraction and Insertion

**Batch Processing:**
```python
features_to_insert = []
for feature in geojson['features']:
    if feature.get('geometry') is None:
        continue
    geom = json.dumps(feature['geometry'])
    props = json.dumps(feature['properties'])
    features_to_insert.append((meta_id, meta_uuid, geom, props))
```

**Database Insert:**
```python
from psycopg2.extras import execute_values

sql = """
INSERT INTO spatial_features (metadata_id, uuid, geom, properties)
VALUES %s
"""

template = "(%s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), %s)"

execute_values(cur, sql, features_to_insert, template=template)
```

**Key Functions:**
- `ST_GeomFromGeoJSON()`: Parses GeoJSON geometry string
- `ST_SetSRID(..., 4326)`: Sets spatial reference system to WGS84
- `execute_values()`: Efficient bulk insert

**Data Structure:**
| Column | Type | Description |
|--------|------|-------------|
| metadata_id | INTEGER | Links to metadata table |
| uuid | UUID | Dataset identifier |
| geom | GEOMETRY | PostGIS geometry (polygon/multipolygon) |
| properties | JSONB | Shapefile attributes |

### 6. Error Recovery

**Two-Phase Insert:**

**Phase 1: Batch Insert**
```python
try:
    execute_values(cur, sql, features_to_insert, template=template)
    inserted_count += len(features_to_insert)
except Exception as e:
    print(f" Batch error: {e}. Retrying row-by-row...")
    conn.rollback()
    # Proceed to Phase 2
```

**Phase 2: Row-by-Row Retry**
```python
for item in features_to_insert:
    try:
        with conn.cursor() as cur_single:
            execute_values(cur_single, sql, [item], template=template)
            conn.commit()
            inserted_count += 1
    except Exception as single_e:
        print(f" Failed Row Error: {single_e}")
        print(f" Bad Geom Snippet: {item[2][:100]}...")
        conn.rollback()
```

**Benefits:**
- Batch efficiency for valid data
- Isolates problematic geometries
- Logs specific geometry errors
- Continues processing after failures
- Maximizes data recovery

## Database Schema

### spatial_features Table

```sql
CREATE TABLE spatial_features (
    id SERIAL PRIMARY KEY,
    metadata_id INTEGER REFERENCES metadata(id),
    uuid UUID,
    geom GEOMETRY(GEOMETRY, 4326),  -- Supports any geometry type
    properties JSONB,  -- Flexible attribute storage
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_spatial_features_geom ON spatial_features USING GIST(geom);
CREATE INDEX idx_spatial_features_metadata ON spatial_features(metadata_id);
CREATE INDEX idx_spatial_features_props ON spatial_features USING GIN(properties);
```

### Example Queries

**Find features by name:**
```sql
SELECT id, properties->>'NAME' as region_name, 
       ST_Area(geom::geography)/1000000 as area_km2
FROM spatial_features
WHERE properties->>'NAME' ILIKE '%Huon%';
```

**Spatial intersection:**
```sql
SELECT sf.properties->>'NAME' as region,
       COUNT(l.id) as location_count
FROM spatial_features sf
JOIN locations l ON ST_Within(l.location_geom, sf.geom)
GROUP BY sf.properties->>'NAME';
```

**Buffer analysis:**
```sql
SELECT properties->>'NAME',
       ST_Area(ST_Buffer(geom::geography, 5000))/1000000 as buffer_5km_area
FROM spatial_features
WHERE properties->>'TYPE' = 'Marine Protected Area';
```

## Usage

### Basic Execution

```bash
python populate_spatial.py
```

### Expected Output

```
Finding empty datasets...
Found 3 candidate datasets.
Processing 'Tasmanian Marine Bioregions' (2 shapefiles)...
 -> Inserted 15 spatial features.
Processing 'Marine Protected Areas' (1 shapefiles)...
 -> Inserted 8 spatial features.
No biological CSVs found for 'Ocean Acidification Time Series'.
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
- PostgreSQL 12+ with PostGIS extension
- `spatial_features` table created
- Write permissions for script user

## Execution Flow

1. **Connect to Database**
   - Establish psycopg2 connection
   - Open transaction cursor

2. **Identify Empty Datasets**
   - Query metadata for unprocessed datasets
   - Filter for those with dataset_path

3. **For Each Dataset:**
   - Check if directory exists
   - Recursively find `.shp` files
   - Skip if no shapefiles found

4. **For Each Shapefile:**
   - Convert to GeoJSON with ogr2ogr
   - Parse GeoJSON features
   - Extract geometries and properties
   - Build batch insert list

5. **Batch Insert:**
   - Try bulk insert with execute_values
   - On error, retry row-by-row
   - Log failures and continue

6. **Commit and Report:**
   - Commit successful inserts
   - Report count per dataset
   - Close database connection

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

**Geometry:** MULTIPOLYGON

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

**Geometry:** POLYGON

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

**Geometry:** POLYGON

## Performance Considerations

### Optimization Strategies

**Batch Size:**
- Default: Process all features per shapefile
- Typical: 10-1000 features per shapefile
- Large datasets: Consider chunking if memory constrained

**Indexing:**
```sql
-- Essential for spatial queries
CREATE INDEX idx_spatial_features_geom 
  ON spatial_features USING GIST(geom);

-- For property searches
CREATE INDEX idx_spatial_features_props 
  ON spatial_features USING GIN(properties);
```

**Connection Pooling:**
- Script uses single connection
- Consider `psycopg2.pool` for concurrent processing

### Typical Execution Times

| Features | Shapefiles | Time |
|----------|------------|------|
| 10-50 | 1-2 | <10s |
| 100-500 | 3-5 | 30s-1m |
| 1000+ | 10+ | 2-5m |

**Bottlenecks:**
- ogr2ogr conversion (disk I/O)
- Complex polygon geometries
- GIST index updates

## Error Handling

### Common Errors

**1. ogr2ogr Not Found**
```
Error converting dataset.shp: [Errno 2] No such file or directory: 'ogr2ogr'
```
**Solution:** Install GDAL
```bash
sudo apt-get install gdal-bin
```

**2. Invalid Geometry**
```
Batch error: Geometry has self-intersections
Failed Row Error: invalid GeoJSON geometry
```
**Solution:** Fix geometry in source shapefile or use ST_MakeValid()
```sql
INSERT INTO spatial_features (...)
VALUES (..., ST_MakeValid(ST_GeomFromGeoJSON(%s)), ...)
```

**3. Missing Projection**
```
Error converting: Unable to open EPSG support file gcs.csv
```
**Solution:** Ensure .prj file exists or specify projection
```bash
ogr2ogr -s_srs EPSG:28355 -t_srs EPSG:4326 output.json input.shp
```

**4. Permission Denied**
```
Error: could not open file /tmp/uuid.json for writing
```
**Solution:** Check `/tmp` permissions or use alternative directory

### Debugging

**Enable Verbose Output:**
```python
# Remove subprocess output suppression
subprocess.check_call(cmd)  # Omit stdout/stderr params
```

**Check Temp Files:**
```bash
ls -lh /tmp/*.json
```

**Manual Conversion Test:**
```bash
ogr2ogr -f GeoJSON -t_srs EPSG:4326 test.json problem_file.shp
cat test.json | jq '.'  # Validate JSON
```

**Geometry Validation:**
```sql
SELECT id, ST_IsValid(geom), ST_IsValidReason(geom)
FROM spatial_features
WHERE NOT ST_IsValid(geom);
```

## Integration Points

### Upstream Dependencies

1. **populate_metadata.py**
   - Must run first
   - Provides `metadata_id` foreign keys
   - Defines `dataset_path` for file discovery

### Downstream Usage

1. **Spatial Queries**
   - Filter measurements by region
   - Calculate statistics per bioregion
   - Identify locations within MPAs

2. **Visualization**
   - GeoServer WMS/WFS services
   - Web mapping applications
   - QGIS desktop analysis

3. **Analysis Scripts**
   - Habitat suitability models
   - Connectivity analysis
   - Marine spatial planning

## Advanced Usage

### Custom Geometry Processing

**Simplify Geometries:**
```python
# Add to template
template = "(%s, %s, ST_SimplifyPreserveTopology(
            ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), 0.001), %s)"
```

**Calculate Centroids:**
```sql
ALTER TABLE spatial_features ADD COLUMN centroid GEOMETRY(POINT, 4326);

UPDATE spatial_features
SET centroid = ST_Centroid(geom);
```

### Property Extraction

**Index Specific Properties:**
```sql
CREATE INDEX idx_sf_name ON spatial_features ((properties->>'NAME'));
```

**Extract to Columns:**
```sql
ALTER TABLE spatial_features 
  ADD COLUMN region_name TEXT,
  ADD COLUMN area_km2 NUMERIC;

UPDATE spatial_features
SET region_name = properties->>'NAME',
    area_km2 = (properties->>'AREA_KM2')::numeric;
```

### Parallel Processing

**Process Multiple Datasets Concurrently:**
```python
from concurrent.futures import ThreadPoolExecutor

def process_dataset(ds):
    conn = get_db_connection()
    # Processing logic here
    conn.close()

with ThreadPoolExecutor(max_workers=4) as executor:
    executor.map(process_dataset, datasets)
```

## Quality Assurance

### Validation Checks

**1. Geometry Validity:**
```sql
SELECT COUNT(*) as invalid_geoms
FROM spatial_features
WHERE NOT ST_IsValid(geom);
```

**2. SRID Consistency:**
```sql
SELECT DISTINCT ST_SRID(geom) as srid, COUNT(*)
FROM spatial_features
GROUP BY ST_SRID(geom);
-- Should only show 4326
```

**3. Feature Count:**
```sql
SELECT m.title, COUNT(sf.id) as feature_count
FROM metadata m
JOIN spatial_features sf ON m.id = sf.metadata_id
GROUP BY m.title
ORDER BY feature_count DESC;
```

**4. Property Completeness:**
```sql
SELECT id, properties
FROM spatial_features
WHERE properties = '{}'::jsonb
   OR properties IS NULL;
```

### Data Integrity

**Check Metadata Links:**
```sql
SELECT sf.id
FROM spatial_features sf
LEFT JOIN metadata m ON sf.metadata_id = m.id
WHERE m.id IS NULL;
-- Should return no rows
```

**Spatial Extent Validation:**
```sql
-- Tasmania bounding box: ~145°E to 149°E, -44°S to -40°S
SELECT id, properties->>'NAME',
       ST_XMin(geom) as min_lon,
       ST_XMax(geom) as max_lon,
       ST_YMin(geom) as min_lat,
       ST_YMax(geom) as max_lat
FROM spatial_features
WHERE ST_XMin(geom) < 145 OR ST_XMax(geom) > 149
   OR ST_YMin(geom) < -45 OR ST_YMax(geom) > -39;
```

## Maintenance

### Updating Spatial Features

**Incremental Update:**
```sql
-- Add ON CONFLICT clause to template
template = """(%s, %s, 
  ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), %s)
  ON CONFLICT (metadata_id, uuid) 
  DO UPDATE SET geom = EXCLUDED.geom, 
                properties = EXCLUDED.properties"""
```

**Full Refresh:**
```sql
-- Clear existing features for a dataset
DELETE FROM spatial_features WHERE metadata_id = 123;

-- Rerun populate_spatial.py
```

### Backup Strategy

**Export Spatial Features:**
```bash
pg_dump -h localhost -p 5433 -U marine_user -d marine_db \
  -t spatial_features -F c -f spatial_features_backup.dump
```

**GeoPackage Export:**
```bash
ogr2ogr -f GPKG spatial_features.gpkg \
  "PG:host=localhost port=5433 dbname=marine_db user=marine_user" \
  spatial_features
```

## Troubleshooting Checklist

- [ ] GDAL/OGR tools installed and in PATH
- [ ] PostGIS extension enabled on database
- [ ] spatial_features table exists with correct schema
- [ ] Shapefiles include .shp, .shx, .dbf companion files
- [ ] Projection files (.prj) present or SRID specified
- [ ] Write permissions on /tmp directory
- [ ] Database connection parameters correct
- [ ] Sufficient disk space for temp files
- [ ] GIST index exists on geom column

## References

### Documentation
- [PostGIS Manual](https://postgis.net/docs/)
- [GDAL/OGR Documentation](https://gdal.org/)
- [psycopg2 Documentation](https://www.psycopg.org/docs/)
- [GeoJSON Specification](https://geojson.org/)

### Related Scripts
- [populate_metadata.py](populate_metadata_detail.md)
- [populate_measurements.py](populate_measurements_detail.md)
- [Database Schema](database_schema.md)

---

*Last Updated: December 25, 2025*