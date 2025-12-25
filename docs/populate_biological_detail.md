# populate_biological.py - Detailed Documentation

## Overview

`populate_biological.py` extracts species occurrence records and biological survey data from CSV files, normalizing taxonomic information and linking observations to locations. It populates three core tables: `taxonomy`, `species_observations`, and updates `locations`.

## Purpose

This script:
- Identifies datasets containing biological/ecological data
- Extracts species occurrence records from CSV files
- Normalizes taxonomic classifications (species, genus, family, etc.)
- Links observations to geographic locations
- Handles diverse data formats (Redmap, IMOS surveys, phytoplankton)
- Maintains data integrity across taxonomic and location tables

## Architecture

### Data Flow

```
┌──────────────────┐
│ AODN Datasets    │
│ (CSV Files)      │
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
│ CSV Header Check │
│ (Bio Keywords)   │
└────────┬─────────┘
         │
         ▼
┌──────────────────────────────┐
│ Row-by-Row Processing        │
├──────────────────────────────┤
│ ┌─────────┐   ┌──────────┐  │
│ │Location │   │Taxonomy  │  │
│ │Normalize│   │Normalize │  │
│ └────┬────┘   └────┬─────┘  │
│      │             │         │
│      ▼             ▼         │
│ ┌────────────────────────┐  │
│ │ Observation Creation   │  │
│ └────────┬───────────────┘  │
└──────────┼───────────────────┘
           │
           ▼
┌───────────────────────────┐
│ Database Tables           │
├───────────────────────────┤
│ • locations               │
│ • taxonomy                │
│ • species_observations    │
└───────────────────────────┘
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

### 2. Location Normalization

**Function:** `normalize_location(conn, cur, row, metadata_id)`

**Purpose:** Ensures location record exists, creates if needed, returns location ID.

**Process:**

1. **Extract Location Name**
   ```python
   name = row.get('SITE_CODE') or 
          row.get('site_code') or 
          row.get('SITE_DESCRIPTION') or 
          row.get('site_name') or 
          f"Site at {row.get('LATITUDE', 'Unknown')},{row.get('LONGITUDE', 'Unknown')}"
   ```

2. **Extract Coordinates**
   ```python
   lat = row.get('LATITUDE') or row.get('latitude') or row.get('lat')
   lon = row.get('LONGITUDE') or row.get('longitude') or row.get('lon')
   ```

3. **Build Geometry**
   - **Option A (WKT):** If `GEOM` column contains WKT string
     ```python
     if 'POINT' in geom_wkt:
         geom_sql = f"ST_GeomFromText('{geom_wkt}', 4326)"
     ```
   
   - **Option B (Coordinates):** Build from lat/lon
     ```python
     elif pd.notna(lat) and pd.notna(lon):
         geom_sql = f"ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)"
     ```
   
   - **Option C (None):** No location data
     ```python
     else:
         geom_sql = "NULL"
     ```

4. **Check Existing Location**
   ```python
   cur.execute("SELECT id FROM locations WHERE location_name = %s", (name,))
   res = cur.fetchone()
   if res:
       return res[0]  # Return existing ID
   ```

5. **Insert New Location**
   ```python
   sql = f"""
   INSERT INTO locations (location_name, description, location_geom, latitude, longitude)
   VALUES (%s, %s, {geom_sql}, %s, %s)
   ON CONFLICT (latitude, longitude) 
     DO UPDATE SET location_name = EXCLUDED.location_name
   RETURNING id
   """
   cur.execute(sql, (name, desc, lat, lon))
   return cur.fetchone()[0]
   ```

**Returns:** 
- `int`: Location ID (existing or new)
- `None`: On error (with rollback)

**Error Handling:**
- Catches insertion errors
- Rolls back transaction
- Returns None to skip observation

### 3. Taxonomy Normalization

**Function:** `normalize_taxonomy(conn, cur, row)`

**Purpose:** Ensures species record exists in taxonomy table, creates if needed.

**Process:**

1. **Extract Species Name**
   ```python
   sp_name = row.get('SPECIES_NAME') or 
             row.get('species_name') or 
             row.get('SPECIES')
   
   if pd.isna(sp_name) or str(sp_name).lower() == 'nan':
       return None  # Cannot proceed without species
   
   sp_name = str(sp_name).strip()
   ```

2. **Check Existing Taxonomy**
   ```python
   cur.execute("SELECT id FROM taxonomy WHERE species_name = %s", (sp_name,))
   res = cur.fetchone()
   if res:
       return res[0]
   ```

3. **Extract Taxonomic Hierarchy**
   ```python
   common = str(row.get('COMMON_NAME') or row.get('reporting_name') or "")
   family = str(row.get('FAMILY') or row.get('family') or "")
   phylum = str(row.get('PHYLUM') or row.get('phylum') or "")
   cls = str(row.get('CLASS') or row.get('class') or "")
   order = str(row.get('ORDER') or row.get('order') or "")
   genus = str(row.get('GENUS') or row.get('genus') or "")
   auth = str(row.get('AUTHORITY') or "")
   ```

4. **Insert New Taxonomy Record**
   ```python
   cur.execute("""
   INSERT INTO taxonomy (species_name, common_name, family, phylum, 
                         class, "order", genus, authority)
   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
   ON CONFLICT (species_name) DO NOTHING
   RETURNING id
   """, (sp_name, common, family, phylum, cls, order, genus, auth))
   ```

5. **Error Recovery**
   ```python
   except Exception as e:
       conn.rollback()
       # Retry SELECT to get ID if conflict occurred
       cur.execute("SELECT id FROM taxonomy WHERE species_name = %s", (sp_name,))
       return cur.fetchone()[0] if cur.fetchone() else None
   ```

**Returns:**
- `int`: Taxonomy ID
- `None`: If species name missing or invalid

**Note:** Uses `ON CONFLICT DO NOTHING` to handle concurrent inserts gracefully.

### 4. Dataset Identification

**Function:** `is_biological_csv(file_path)`

**Purpose:** Heuristic detection of biological data files.

**Process:**

1. **Read CSV Header** (first 5 rows)
   ```python
   df = pd.read_csv(file_path, encoding='utf-8', 
                    on_bad_lines='skip', comment='#', nrows=5)
   ```

2. **Check for Biological Keywords**
   ```python
   cols = [c.upper() for c in df.columns]
   bio_keywords = ['SPECIES', 'TAXON', 'SCIENTIFIC_NAME', 
                   'GENUS', 'PHYLUM', 'FAMILY']
   
   for k in bio_keywords:
       if any(k in c for c in cols):
           return True
   ```

3. **Return Classification**
   - `True`: File contains biological data
   - `False`: Not a biological CSV or unreadable

**Rationale:**
- Avoids processing non-biological CSVs
- Header-only check is fast
- Handles encoding errors gracefully

### 5. Dataset Ingestion

**Function:** `ingest_dataset(conn, file_path, metadata_id)`

**Purpose:** Main ETL function for processing a single CSV file.

**Process:**

1. **Load CSV with Encoding Detection**
   ```python
   try:
       df = pd.read_csv(file_path, encoding='utf-8', 
                        on_bad_lines='skip', comment='#')
   except:
       try:
           df = pd.read_csv(file_path, encoding='latin1', 
                            on_bad_lines='skip', comment='#')
       except TypeError:  # Older pandas
           df = pd.read_csv(file_path, encoding='latin1', 
                            error_bad_lines=False, comment='#')
   ```

2. **Row-by-Row Processing**
   ```python
   for idx, row in df.iterrows():
       loc_id = normalize_location(conn, cur, row, metadata_id)
       tax_id = normalize_taxonomy(conn, cur, row)
       
       if not tax_id:
           continue  # Skip rows without valid species
   ```

3. **Extract Observation Values**
   
   **Count/Abundance:**
   ```python
   count_val = row.get('TOTAL_NUMBER') or 
               row.get('total') or 
               row.get('count_code')
   
   # Try to parse as numeric
   try:
       numeric_count = float(str(count_val).replace('>','').replace('<',''))
   except:
       numeric_count = None
       count_cat = str(count_val)  # Store as category
   ```
   
   **Date:**
   ```python
   obs_date = row.get('SURVEY_DATE') or 
              row.get('survey_date') or 
              row.get('SIGHTING_DATE')
   ```
   
   **Depth:**
   ```python
   depth = row.get('DEPTH') or row.get('depth')
   ```
   
   **Geometry (Denormalized):**
   ```python
   lat = row.get('LATITUDE') or row.get('latitude')
   lon = row.get('LONGITUDE') or row.get('longitude')
   geom_sql = "NULL"
   if lat and lon:
       geom_sql = f"ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)"
   ```

4. **Insert Observation**
   ```python
   sql = f"""
   INSERT INTO species_observations
   (metadata_id, location_id, taxonomy_id, observation_date, 
    count_value, count_category, depth_m, geom)
   VALUES (%s, %s, %s, %s, %s, %s, %s, {geom_sql})
   """
   cur.execute(sql, (metadata_id, loc_id, tax_id, obs_date, 
                     numeric_count, count_cat, depth))
   ```

5. **Periodic Commits**
   ```python
   if idx % 1000 == 0:
       conn.commit()  # Commit every 1000 rows
   ```

6. **Final Commit**
   ```python
   conn.commit()
   print(f"Finished {file_path}")
   ```

**Error Handling:**
- File-level try/except catches all errors
- Rolls back on failure
- Logs error and continues to next file

### 6. Main Execution Loop

**Function:** `main()`

**Process:**

1. **Identify Empty Datasets**
   ```sql
   SELECT m.id, m.title, m.dataset_path
   FROM metadata m
   LEFT JOIN measurements mes ON m.id = mes.metadata_id
   LEFT JOIN spatial_features sf ON m.id = sf.metadata_id
   LEFT JOIN species_observations bio ON m.id = bio.metadata_id
   GROUP BY m.id
   HAVING COUNT(mes.data_id) = 0
     AND COUNT(sf.id) = 0
     AND COUNT(bio.id) = 0
     AND m.dataset_path IS NOT NULL
   ```

2. **Scan Dataset Directories**
   ```python
   for root, dirs, files in os.walk(path):
       for file in files:
           if file.lower().endswith('.csv'):
               fpath = os.path.join(root, file)
               if is_biological_csv(fpath):
                   ingest_dataset(conn, fpath, meta_id)
   ```

3. **Report Results**
   ```python
   if not found_bio:
       print(f" -> No biological CSVs found for '{title}'.")
   ```

## Database Schema

### taxonomy Table

```sql
CREATE TABLE taxonomy (
    id SERIAL PRIMARY KEY,
    species_name TEXT UNIQUE NOT NULL,
    common_name TEXT,
    family TEXT,
    phylum TEXT,
    class TEXT,
    "order" TEXT,  -- Reserved keyword, quoted
    genus TEXT,
    authority TEXT,  -- Taxonomic authority
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_taxonomy_species ON taxonomy(species_name);
CREATE INDEX idx_taxonomy_family ON taxonomy(family);
CREATE INDEX idx_taxonomy_genus ON taxonomy(genus);
```

### species_observations Table

```sql
CREATE TABLE species_observations (
    id SERIAL PRIMARY KEY,
    metadata_id INTEGER REFERENCES metadata(id),
    location_id INTEGER REFERENCES locations(id),
    taxonomy_id INTEGER REFERENCES taxonomy(id),
    observation_date DATE,
    count_value NUMERIC,  -- Numeric abundance
    count_category TEXT,  -- Categorical abundance (e.g., "1-10", "Rare")
    depth_m NUMERIC,
    geom GEOMETRY(POINT, 4326),  -- Denormalized for spatial queries
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_species_obs_metadata ON species_observations(metadata_id);
CREATE INDEX idx_species_obs_location ON species_observations(location_id);
CREATE INDEX idx_species_obs_taxonomy ON species_observations(taxonomy_id);
CREATE INDEX idx_species_obs_date ON species_observations(observation_date);
CREATE INDEX idx_species_obs_geom ON species_observations USING GIST(geom);
```

### locations Table Updates

```sql
-- Existing structure, populated by normalize_location()
CREATE TABLE locations (
    id SERIAL PRIMARY KEY,
    location_name TEXT,
    description TEXT,
    location_geom GEOMETRY(POINT, 4326),
    latitude NUMERIC,
    longitude NUMERIC,
    UNIQUE(latitude, longitude)  -- Prevents duplicate coordinates
);
```

## Usage

### Basic Execution

```bash
python populate_biological.py
```

### Expected Output

```
Finding empty datasets...
Found 5 candidate empty datasets.
Scanning 'Redmap - Range Shifting Species'...
 -> Found biological file: Redmap_sightings.csv
Ingesting Redmap_sightings.csv...
Finished /path/to/Redmap_sightings.csv
Scanning 'IMOS Larval Fish Database'...
 -> Found biological file: larval_fish_surveys.csv
Ingesting larval_fish_surveys.csv...
Finished /path/to/larval_fish_surveys.csv
 -> No biological CSVs found for 'Ocean Temperature Archive'.
```

### Requirements

**Python Libraries:**
```bash
pip install pandas psycopg2-binary numpy
```

**Database:**
- PostgreSQL 12+ with PostGIS
- Tables: `metadata`, `locations`, `taxonomy`, `species_observations`
- Write permissions

## Supported Data Formats

### 1. Redmap Species Sightings

**Typical Columns:**
```
SPECIES_NAME, COMMON_NAME, LATITUDE, LONGITUDE, 
SIGHTING_DATE, COUNT_DESCRIPTION, FAMILY, PHYLUM
```

**Example Row:**
```csv
Sphyraena novaehollandiae,Snook,43.1234S,147.5678E,2023-03-15,1-10,Sphyraenidae,Chordata
```

**Special Handling:**
- Count often categorical ("1-10", "Many")
- Southern hemisphere lat requires sign flip

### 2. IMOS Larval Fish Database

**Typical Columns:**
```
species_name, site_code, SURVEY_DATE, TOTAL_NUMBER, 
DEPTH, latitude, longitude, FAMILY, GENUS
```

**Example Row:**
```csv
Sardinops sagax,SITE_042,2022-11-20,45,12.5,-42.8765,147.3210,Clupeidae,Sardinops
```

**Special Handling:**
- Numeric counts common
- Site codes link to station metadata
- Depth in meters

### 3. Phytoplankton Surveys

**Typical Columns:**
```
SPECIES, SITE_DESCRIPTION, SURVEY_DATE, total, 
lat, lon, CLASS, PHYLUM, reporting_name
```

**Example Row:**
```csv
Chaetoceros socialis,Huon River Mouth,2023-06-10,1250,-43.0854,147.0432,Bacillariophyceae,Ochrophyta,Chaetoceros
```

**Special Handling:**
- Total = cell counts per liter
- reporting_name = simplified taxonomy
- Multiple species per site/date

## Column Mapping Strategy

### Flexible Column Detection

Script uses **OR chains** to handle naming variations:

**Species:**
```python
sp_name = row.get('SPECIES_NAME') or 
          row.get('species_name') or 
          row.get('SPECIES')
```

**Location:**
```python
name = row.get('SITE_CODE') or 
       row.get('site_code') or 
       row.get('SITE_DESCRIPTION') or 
       row.get('site_name')
```

**Coordinates:**
```python
lat = row.get('LATITUDE') or row.get('latitude') or row.get('lat')
lon = row.get('LONGITUDE') or row.get('longitude') or row.get('lon')
```

### Adding New Column Mappings

To support additional CSV formats:

1. **Identify column names** in new CSV
2. **Add to OR chain:**
   ```python
   count_val = row.get('TOTAL_NUMBER') or 
               row.get('total') or 
               row.get('count_code') or
               row.get('abundance')  # NEW
   ```

## Performance Considerations

### Optimization Strategies

**1. Batch Commits**
```python
if idx % 1000 == 0:
    conn.commit()
```
Reduces transaction overhead while maintaining recoverability.

**2. Index Strategy**
```sql
-- Essential indexes
CREATE INDEX idx_taxonomy_species ON taxonomy(species_name);
CREATE INDEX idx_locations_name ON locations(location_name);

-- For spatial queries
CREATE INDEX idx_species_obs_geom ON species_observations USING GIST(geom);
```

**3. Connection Reuse**
- Single connection per script execution
- Cursor reused for all queries
- Reduces connection overhead

### Typical Execution Times

| Rows | Files | Time |
|------|-------|------|
| 100-1K | 1 | <30s |
| 1K-10K | 2-3 | 1-3m |
| 10K-100K | 5-10 | 5-15m |
| 100K+ | 10+ | 15-60m |

**Bottlenecks:**
- CSV parsing (pandas read_csv)
- Individual INSERT statements (row-by-row)
- Geometry creation (ST_MakePoint)
- Location/taxonomy lookups

### Performance Tuning

**Bulk Inserts:**
```python
from psycopg2.extras import execute_values

observations = []
for idx, row in df.iterrows():
    # ... extract values ...
    observations.append((metadata_id, loc_id, tax_id, obs_date, 
                        numeric_count, count_cat, depth))
    
    if len(observations) >= 1000:
        execute_values(cur, sql, observations)
        observations = []

if observations:
    execute_values(cur, sql, observations)
```

**Prepared Statements:**
```python
cur.execute("PREPARE taxonomy_lookup AS 
             SELECT id FROM taxonomy WHERE species_name = $1")

for row in df.iterrows():
    cur.execute("EXECUTE taxonomy_lookup (%s)", (sp_name,))
```

## Error Handling

### Common Errors

**1. Missing Species Name**
```python
if pd.isna(sp_name) or str(sp_name).lower() == 'nan':
    return None  # Skip observation
```
**Impact:** Observation skipped, processing continues

**2. Encoding Issues**
```
UnicodeDecodeError: 'utf-8' codec can't decode byte
```
**Solution:** Fallback to latin1 encoding
```python
except:
    df = pd.read_csv(file_path, encoding='latin1', on_bad_lines='skip')
```

**3. Invalid Coordinates**
```
DataError: Latitude must be between -90 and 90
```
**Solution:** Add validation
```python
if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
    geom_sql = "NULL"
```

**4. Conflicting Species Names**
```
IntegrityError: duplicate key value violates unique constraint "taxonomy_species_name_key"
```
**Solution:** ON CONFLICT clause handles gracefully
```sql
ON CONFLICT (species_name) DO NOTHING
```

### Debugging

**Enable Verbose Logging:**
```python
import logging
logging.basicConfig(level=logging.DEBUG)

for idx, row in df.iterrows():
    logging.debug(f"Processing row {idx}: {row['SPECIES_NAME']}")
```

**Check CSV Structure:**
```python
df = pd.read_csv('problem_file.csv', nrows=5)
print(df.columns.tolist())
print(df.dtypes)
```

**Validate Taxonomic Data:**
```sql
-- Find species without observations
SELECT t.species_name, COUNT(so.id) as obs_count
FROM taxonomy t
LEFT JOIN species_observations so ON t.id = so.taxonomy_id
GROUP BY t.species_name
HAVING COUNT(so.id) = 0;
```

## Integration Points

### Upstream Dependencies

1. **populate_metadata.py**
   - Provides `metadata_id` foreign keys
   - Defines `dataset_path` for file discovery

### Downstream Usage

1. **Biodiversity Analysis**
   - Species richness by region
   - Temporal trends in abundance
   - Range shift detection

2. **Ecosystem Monitoring**
   - Indicator species tracking
   - Invasive species alerts
   - Habitat association studies

3. **Reporting**
   - SEEA Ecosystem Accounting
   - State of Environment reports
   - Marine park management

## Advanced Usage

### Custom Taxonomic Resolution

**Link to WoRMS (World Register of Marine Species):**
```python
import requests

def resolve_worms_id(species_name):
    url = f"https://www.marinespecies.org/rest/AphiaRecordsByName/{species_name}"
    response = requests.get(url)
    if response.ok:
        data = response.json()
        return data[0]['AphiaID'] if data else None
    return None

# Add to normalize_taxonomy()
worms_id = resolve_worms_id(sp_name)
cur.execute("UPDATE taxonomy SET worms_id = %s WHERE id = %s", 
            (worms_id, tax_id))
```

### Abundance Category Standardization

```python
ABUNDANCE_MAP = {
    'rare': 1, 'uncommon': 2, 'common': 3, 'abundant': 4,
    '1': 1, '1-10': 2, '10-100': 3, '>100': 4
}

def standardize_abundance(count_category):
    cat = str(count_category).lower().strip()
    return ABUNDANCE_MAP.get(cat, None)

# Use in ingest_dataset()
std_abundance = standardize_abundance(count_cat)
cur.execute(sql, (..., std_abundance, ...))
```

### Spatial Aggregation

**Count observations per bioregion:**
```sql
SELECT sf.properties->>'NAME' as bioregion,
       COUNT(DISTINCT so.taxonomy_id) as species_count,
       COUNT(so.id) as observation_count
FROM spatial_features sf
JOIN species_observations so ON ST_Within(so.geom, sf.geom)
GROUP BY sf.properties->>'NAME'
ORDER BY species_count DESC;
```

## Quality Assurance

### Validation Checks

**1. Taxonomic Completeness**
```sql
SELECT species_name, 
       CASE WHEN family IS NULL THEN 'Missing' ELSE 'Present' END as family_status,
       CASE WHEN genus IS NULL THEN 'Missing' ELSE 'Present' END as genus_status
FROM taxonomy
WHERE family IS NULL OR genus IS NULL;
```

**2. Observation Counts**
```sql
SELECT m.title, COUNT(so.id) as obs_count
FROM metadata m
JOIN species_observations so ON m.id = so.metadata_id
GROUP BY m.title
ORDER BY obs_count DESC;
```

**3. Location Linking**
```sql
SELECT COUNT(*) as observations_without_locations
FROM species_observations
WHERE location_id IS NULL;
```

**4. Date Range Validation**
```sql
SELECT MIN(observation_date) as earliest,
       MAX(observation_date) as latest,
       COUNT(*) as total_observations
FROM species_observations
WHERE observation_date IS NOT NULL;
```

**5. Spatial Validation**
```sql
-- Check observations within Tasmania bounding box
SELECT id, ST_X(geom) as lon, ST_Y(geom) as lat
FROM species_observations
WHERE geom IS NOT NULL
  AND (ST_X(geom) < 145 OR ST_X(geom) > 149
    OR ST_Y(geom) < -45 OR ST_Y(geom) > -39);
```

## Maintenance

### Updating Taxonomic Records

**Bulk Taxonomy Update:**
```sql
-- From external taxonomy file
CREATE TEMP TABLE taxonomy_updates (
    species_name TEXT,
    worms_id INTEGER,
    family TEXT
);

COPY taxonomy_updates FROM '/path/to/taxonomy.csv' CSV HEADER;

UPDATE taxonomy t
SET worms_id = tu.worms_id,
    family = COALESCE(t.family, tu.family)
FROM taxonomy_updates tu
WHERE t.species_name = tu.species_name;
```

### Data Cleanup

**Remove Duplicate Observations:**
```sql
DELETE FROM species_observations so1
USING species_observations so2
WHERE so1.id < so2.id
  AND so1.taxonomy_id = so2.taxonomy_id
  AND so1.location_id = so2.location_id
  AND so1.observation_date = so2.observation_date;
```

**Merge Similar Species:**
```sql
-- Standardize species name variants
UPDATE species_observations
SET taxonomy_id = (
    SELECT id FROM taxonomy WHERE species_name = 'Sardinops sagax'
)
WHERE taxonomy_id IN (
    SELECT id FROM taxonomy 
    WHERE species_name IN ('Sardinops neopilchardus', 'Sardinops sagax neopilchardus')
);

-- Remove old taxonomy entries
DELETE FROM taxonomy 
WHERE species_name IN ('Sardinops neopilchardus', 'Sardinops sagax neopilchardus')
  AND id NOT IN (SELECT DISTINCT taxonomy_id FROM species_observations);
```

## Troubleshooting Checklist

- [ ] pandas and psycopg2 installed
- [ ] Database tables exist (taxonomy, species_observations, locations)
- [ ] Foreign key constraints enabled
- [ ] CSV files are readable with valid encoding
- [ ] Species name column present in CSVs
- [ ] Coordinate columns present (or GEOM column)
- [ ] PostGIS extension enabled for geometry functions
- [ ] Sufficient disk space for database growth
- [ ] Indexes created on key columns

## References

### Documentation
- [pandas Documentation](https://pandas.pydata.org/docs/)
- [psycopg2 Documentation](https://www.psycopg.org/docs/)
- [World Register of Marine Species](https://www.marinespecies.org/)
- [OBIS Data Standards](https://obis.org/manual/)

### Related Scripts
- [populate_metadata.py](populate_metadata_detail.md)
- [populate_measurements.py](populate_measurements_detail.md)
- [populate_spatial.py](populate_spatial_detail.md)
- [Database Schema](database_schema.md)

---

*Last Updated: December 25, 2025*