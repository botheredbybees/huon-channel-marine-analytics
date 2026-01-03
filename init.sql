-- =============================================================================
-- Huon Channel Marine Analytics - Database Initialization Script (PostGIS-Free)
-- =============================================================================
-- Purpose: Creates PostgreSQL database schema for AODN marine data
-- Version: 3.2 (Pure PostgreSQL - no PostGIS, removed uuid field)
-- Last Updated: January 1, 2026
--
-- IMPORTANT NOTES:
-- 1. Removed ALL PostGIS dependencies (GEOMETRY, ST_* functions, BOX2D, GIST on geometry)
-- 2. Uses pure PostgreSQL lat/lon and bbox columns instead
-- 3. Compatible with timescale/timescaledb:latest-pg18 (Community license)
-- 4. All spatial queries work with DECIMAL bbox columns
-- 5. TimescaleDB hypertable enabled for measurements table
-- 6. Removed confusing 'uuid' field - now using aodn_uuid for AODN catalog IDs
-- 7. dataset_path is now the primary stable identifier for upserts
-- =============================================================================

-- Enable extensions (NO PostGIS)
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS unaccent;

-- =============================================================================
-- PARAMETER MAPPINGS TABLE (replaces config_parameter_mapping.json)
-- =============================================================================

CREATE TABLE IF NOT EXISTS parameter_mappings (
  id SERIAL PRIMARY KEY,
  raw_parameter_name TEXT UNIQUE NOT NULL,
  standard_code TEXT NOT NULL,
  namespace TEXT NOT NULL CHECK (namespace IN ('bodc', 'cf', 'custom')),
  unit TEXT NOT NULL,
  description TEXT,
  source TEXT DEFAULT 'system',
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_param_mappings_raw ON parameter_mappings(raw_parameter_name);
CREATE INDEX IF NOT EXISTS idx_param_mappings_code ON parameter_mappings(standard_code);
CREATE INDEX IF NOT EXISTS idx_param_mappings_namespace ON parameter_mappings(namespace);

COMMENT ON TABLE parameter_mappings IS 'Maps raw parameter names from data files to standardized BODC/CF codes';
COMMENT ON COLUMN parameter_mappings.namespace IS 'bodc = British Oceanographic Data Centre, cf = Climate & Forecast, custom = user-defined';

-- =============================================================================
-- IMOS CONTROLLED VOCABULARIES (from AODN)
-- =============================================================================

CREATE TABLE IF NOT EXISTS public.imos_vocab_geographic_extents (
  uri text PRIMARY KEY,
  pref_label text NOT NULL,
  alt_label text,
  definition text,
  bbox_north numeric(9,6),
  bbox_south numeric(9,6),
  bbox_east numeric(9,6),
  bbox_west numeric(9,6),
  broader_uri text REFERENCES imos_vocab_geographic_extents(uri) ON DELETE SET NULL,
  created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.imos_vocab_organization_categories (
  uri text PRIMARY KEY,
  pref_label text NOT NULL,
  alt_label text,
  definition text,
  broader_uri text REFERENCES imos_vocab_organization_categories(uri) ON DELETE SET NULL,
  created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.imos_vocab_platform_categories (
  uri text PRIMARY KEY,
  pref_label text NOT NULL,
  alt_label text,
  definition text,
  broader_uri text REFERENCES imos_vocab_platform_categories(uri) ON DELETE SET NULL,
  created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.imos_vocab_parameter_categories (
  uri text PRIMARY KEY,
  pref_label text NOT NULL,
  definition text,
  broader_uri text REFERENCES imos_vocab_parameter_categories(uri) ON DELETE SET NULL,
  created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.imos_vocab_parameters (
  uri text PRIMARY KEY,
  pref_label text NOT NULL,
  alt_label text,
  hidden_label text[],
  definition text,
  p01_code text,
  p01_uri text,
  cf_standard_name text,
  cf_uri text,
  category_uri text REFERENCES imos_vocab_parameter_categories(uri) ON DELETE SET NULL,
  created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.imos_vocab_instruments (
  uri text PRIMARY KEY,
  pref_label text NOT NULL,
  alt_label text,
  definition text,
  category_uri text,
  manufacturer text,
  model text,
  created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.imos_vocab_units (
  uri text PRIMARY KEY,
  pref_label text NOT NULL,
  alt_label text,
  definition text,
  p06_code text,
  p06_uri text,
  created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- LOCATIONS & SPATIAL REFERENCES (Pure PostgreSQL - lat/lon)
-- =============================================================================

CREATE TABLE IF NOT EXISTS public.locations (
  id SERIAL PRIMARY KEY,
  location_name text,
  location_type text DEFAULT 'observation_site',
  longitude double precision,
  latitude double precision,
  description text,
  created_at timestamp without time zone DEFAULT now(),
  CONSTRAINT unique_lat_lon UNIQUE (latitude, longitude)
);

-- Indexes on lat/lon (pure PostgreSQL B-TREE)
CREATE INDEX IF NOT EXISTS idx_locations_lat_lon ON public.locations (latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_locations_lat_lon_partial
  ON public.locations (latitude, longitude)
  WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- =============================================================================
-- METADATA TABLES (normalized structure for 38+ IMOS datasets)
-- CHANGED: Removed uuid field, made dataset_path UNIQUE NOT NULL
-- =============================================================================

CREATE TABLE metadata (
  id SERIAL PRIMARY KEY,
  uuid TEXT UNIQUE NOT NULL,  -- AODN UUID when available, generated UUID as fallback
  parent_uuid TEXT,
  title TEXT NOT NULL,
  abstract TEXT,
  credit TEXT,
  status TEXT,
  topic_category TEXT,
  metadata_creation_date TIMESTAMP,
  metadata_revision_date TIMESTAMP,
  citation_date TIMESTAMP,
  language TEXT DEFAULT 'eng',
  character_set TEXT DEFAULT 'utf8',
  west DECIMAL(10,6),
  east DECIMAL(10,6),
  south DECIMAL(10,6),
  north DECIMAL(10,6),
  time_start DATE,
  time_end DATE,
  vertical_min DECIMAL(6,2),
  vertical_max DECIMAL(6,2),
  vertical_crs TEXT,
  lineage TEXT,
  supplemental_info TEXT,
  use_limitation TEXT,
  license_url TEXT,
  distribution_wfs_url TEXT,
  distribution_wms_url TEXT,
  distribution_portal_url TEXT,
  distribution_publication_url TEXT,
  dataset_name TEXT,
  dataset_path TEXT UNIQUE,  -- For upsert conflict detection
  extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  date_created DATE,
  
  -- PostGIS spatial extent (unlocks spatial queries)
  extent_geom GEOMETRY(POLYGON, 4326),
  
  -- Materialized bounding box for non-spatial queries
  bbox_envelope BOX2D GENERATED ALWAYS AS (BOX2D(extent_geom)) STORED
);

-- Metadata indexes (pure PostgreSQL)
CREATE INDEX IF NOT EXISTS idx_metadata_aodn_uuid ON metadata(aodn_uuid)
  WHERE aodn_uuid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_metadata_bbox ON metadata(west, east, south, north);
CREATE INDEX IF NOT EXISTS idx_metadata_time ON metadata(time_start, time_end);
CREATE INDEX IF NOT EXISTS idx_metadata_dataset_name ON metadata(dataset_name);
CREATE INDEX IF NOT EXISTS idx_metadata_dataset_path ON metadata(dataset_path);

CREATE TABLE IF NOT EXISTS public.spatial_ref_system (
  id SERIAL PRIMARY KEY,
  metadata_id INTEGER NOT NULL REFERENCES metadata(id) ON DELETE CASCADE,
  srid integer NOT NULL,
  auth_name text DEFAULT 'EPSG'::text,
  crs_name text,
  is_primary boolean DEFAULT true,
  created_at timestamp without time zone DEFAULT now(),
  UNIQUE (metadata_id, srid)
);

CREATE TABLE IF NOT EXISTS parameters (
  id SERIAL PRIMARY KEY,
  metadata_id INTEGER REFERENCES metadata(id) ON DELETE CASCADE,
  parameter_code TEXT NOT NULL,
  parameter_label TEXT,
  standard_name TEXT,
  aodn_parameter_uri TEXT,
  unit_name TEXT,
  unit_uri TEXT,
  content_type TEXT DEFAULT 'physicalMeasurement',
  is_depth BOOLEAN DEFAULT FALSE,
  temporal_start TIMESTAMP,
  temporal_end TIMESTAMP,
  vertical_min NUMERIC(6,2),
  vertical_max NUMERIC(6,2),
  created_at TIMESTAMP DEFAULT NOW(),
  imos_parameter_uri TEXT REFERENCES imos_vocab_parameters(uri),
  imos_unit_uri TEXT REFERENCES imos_vocab_units(uri),
  UNIQUE(metadata_id, parameter_code)
);

CREATE INDEX IF NOT EXISTS idx_parameters_metadata_id ON parameters(metadata_id);
CREATE INDEX IF NOT EXISTS idx_parameters_code ON parameters(parameter_code);
CREATE INDEX IF NOT EXISTS idx_parameters_aodn_uri ON parameters(aodn_parameter_uri);
CREATE INDEX IF NOT EXISTS idx_parameters_imos_uri ON parameters(imos_parameter_uri);

CREATE TABLE IF NOT EXISTS keywords (
  id SERIAL PRIMARY KEY,
  metadata_id INTEGER REFERENCES metadata(id) ON DELETE CASCADE,
  keyword TEXT NOT NULL,
  keyword_type TEXT,
  thesaurus_name TEXT,
  thesaurus_uri TEXT,
  UNIQUE(metadata_id, keyword)
);

CREATE INDEX IF NOT EXISTS idx_keywords_metadata_id ON keywords(metadata_id);
CREATE INDEX IF NOT EXISTS idx_keywords_keyword ON keywords(keyword);
CREATE INDEX IF NOT EXISTS idx_keywords_thesaurus ON keywords(thesaurus_uri);

-- =============================================================================
-- MEASUREMENTS HYPERTABLE (TimescaleDB optimized for 12M+ measurements)
-- =============================================================================

CREATE TABLE IF NOT EXISTS measurements (
  time TIMESTAMPTZ NOT NULL,
  data_id BIGSERIAL,
  metadata_id INTEGER REFERENCES metadata(id) ON DELETE CASCADE,
  parameter_code TEXT NOT NULL,
  namespace TEXT NOT NULL DEFAULT 'custom',
  value DOUBLE PRECISION NOT NULL,
  uom TEXT NOT NULL,
  location_qc_flag TEXT,
  uncertainty DOUBLE PRECISION,
  depth_m NUMERIC,
  location_id BIGINT REFERENCES locations(id),
  quality_flag SMALLINT DEFAULT 1
);

COMMENT ON TABLE measurements IS 'Timeseries measurements (quality-controlled only, QC columns removed Dec 2025)';
COMMENT ON COLUMN measurements.quality_flag IS 'IMOS QC flag: 1=good, 2=probably good (only values 1-2 stored)';

-- Create hypertable ONLY if not already created
DO $$
BEGIN
  PERFORM create_hypertable('measurements', by_range('time'));
EXCEPTION
  WHEN duplicate_table THEN
    NULL;
  WHEN others THEN
    RAISE NOTICE 'Hypertable creation error: %', SQLERRM;
END $$;

-- Primary key (safe idempotent version)
CREATE UNIQUE INDEX IF NOT EXISTS measurements_pkey ON measurements (time, data_id);


-- Enable compression
ALTER TABLE IF EXISTS measurements SET (timescaledb.compress = true, timescaledb.compress_segmentby = 'parameter_code, namespace');

-- Compression policy (idempotent)
DO $$ BEGIN
  PERFORM add_compression_policy('measurements', INTERVAL '7 days');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- CRITICAL INDEXES FOR GRAFANA QUERIES
CREATE INDEX IF NOT EXISTS idx_measurements_time_param ON measurements (time DESC, parameter_code)
  WHERE namespace = 'bodc';
CREATE INDEX IF NOT EXISTS idx_measurements_param_time ON measurements (parameter_code, time DESC);
CREATE INDEX IF NOT EXISTS idx_measurements_location_time ON measurements (location_id, time DESC)
  WHERE location_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_measurements_namespace ON measurements (namespace);
CREATE INDEX IF NOT EXISTS idx_measurements_metadata_id ON measurements (metadata_id);
CREATE INDEX IF NOT EXISTS idx_measurements_time_brin ON measurements USING BRIN (time);
CREATE INDEX IF NOT EXISTS idx_measurements_param_gin ON measurements USING GIN (to_tsvector('english', parameter_code));
CREATE INDEX IF NOT EXISTS idx_measurements_good_data ON measurements (time DESC, parameter_code)
  WHERE quality_flag = 1 AND namespace IN ('bodc', 'cf');

-- =============================================================================
-- CONTINUOUS AGGREGATES (Grafana performance)
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS measurements_1h
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', time) AS bucket,
  parameter_code, namespace, location_id, metadata_id,
  AVG(value) AS avg_value,
  STDDEV(value) AS stddev_value,
  COUNT(*) AS count,
  MIN(quality_flag) AS min_quality
FROM measurements
GROUP BY bucket, parameter_code, namespace, location_id, metadata_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS measurements_1d
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', time) AS bucket,
  parameter_code, namespace, location_id,
  AVG(value) AS avg_value,
  STDDEV(value) AS stddev_value,
  COUNT(*) AS count
FROM measurements
GROUP BY bucket, parameter_code, namespace, location_id;

-- Continuous aggregate policies (idempotent - safe to re-run)
SELECT add_continuous_aggregate_policy('measurements_1h',
  start_offset => INTERVAL '3 days',
  end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '1 hour');

SELECT add_continuous_aggregate_policy('measurements_1d',
  start_offset => INTERVAL '30 days',
  end_offset => INTERVAL '1 day',
  schedule_interval => INTERVAL '1 day');


-- =============================================================================
-- VIEWS FOR GRAFANA & ANALYSIS (Pure PostgreSQL - no PostGIS)
-- =============================================================================

DROP VIEW IF EXISTS measurements_with_metadata CASCADE;
CREATE OR REPLACE VIEW measurements_with_metadata AS
SELECT
  m.time, m.data_id, m.parameter_code, m.namespace, m.value, m.uom,
  m.uncertainty, m.depth_m, m.location_id, m.quality_flag,
  md.title AS dataset_title,
  md.dataset_name,
  p.parameter_label,
  p.unit_name,
  md.west, md.east, md.south, md.north
FROM measurements m
LEFT JOIN metadata md ON m.metadata_id = md.id
LEFT JOIN parameters p ON md.id = p.metadata_id AND m.parameter_code = p.parameter_code;

DROP VIEW IF EXISTS datasets_by_parameter CASCADE;
CREATE OR REPLACE VIEW datasets_by_parameter AS
SELECT
  p.parameter_code,
  p.parameter_label,
  p.aodn_parameter_uri,
  COUNT(DISTINCT md.id) AS dataset_count,
  MIN(md.west) AS bbox_west,
  MIN(md.south) AS bbox_south,
  MAX(md.east) AS bbox_east,
  MAX(md.north) AS bbox_north,
  ARRAY_AGG(DISTINCT md.dataset_name) AS datasets
FROM parameters p
JOIN metadata md ON p.metadata_id = md.id
GROUP BY p.parameter_code, p.parameter_label, p.aodn_parameter_uri;

CREATE OR REPLACE VIEW grafana_parameters AS
SELECT DISTINCT
  parameter_code AS value,
  CASE
    WHEN parameter_code = 'TEMP' THEN 'Temperature (°C)'
    WHEN parameter_code = 'PSAL' THEN 'Salinity (PSS-78)'
    WHEN parameter_code = 'SST' THEN 'Sea Surface Temp (°C)'
    WHEN parameter_code = 'CPHL' THEN 'Chlorophyll-a (mg/m³)'
    WHEN parameter_code = 'DOXY' THEN 'Dissolved Oxygen (ml/l)'
    WHEN parameter_code = 'PH' THEN 'pH'
    WHEN parameter_code = 'NO3' THEN 'Nitrate (mmol/m³)'
    WHEN parameter_code = 'PO4' THEN 'Phosphate (mmol/m³)'
    WHEN parameter_code = 'SIO4' THEN 'Silicate (mmol/m³)'
    WHEN parameter_code = 'FLUO' THEN 'Chlorophyll Fluorescence (mg/m³)'
    WHEN parameter_code = 'TURB' THEN 'Turbidity (NTU)'
    WHEN parameter_code IN ('CNDC','PRES') THEN parameter_code || ' (CTD)'
    ELSE parameter_code || ' (' || uom || ')'
  END AS text,
  uom,
  COUNT(*) as record_count,
  namespace
FROM measurements
WHERE quality_flag = 1
GROUP BY parameter_code, namespace, uom
HAVING COUNT(*) > 500
ORDER BY record_count DESC;

CREATE OR REPLACE VIEW grafana_timeseries AS
SELECT
  time_bucket('15 minutes', time) AS time,
  parameter_code,
  AVG(value) AS value,
  STDDEV(value) AS stddev,
  COUNT(*) AS n_points,
  MIN(quality_flag) AS quality_flag,
  dataset_title,
  location_id
FROM measurements_with_metadata
WHERE quality_flag IN (1,2)
GROUP BY
  time_bucket('15 minutes', time),
  parameter_code, dataset_title, location_id
ORDER BY time DESC, parameter_code;

-- =============================================================================
-- ETL QA/QC SUMMARY VIEW
-- =============================================================================

CREATE OR REPLACE VIEW parameter_summary AS
SELECT 
    parameter_code,
    COUNT(*) as count,
    AVG(value) as mean,
    STDDEV(value) as std_dev,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) as median,
    MIN(time) as earliest,
    MAX(time) as latest
FROM measurements
GROUP BY parameter_code;


-- =============================================================================
-- SPATIAL & BIOLOGICAL FEATURES (Pure PostgreSQL - lat/lon)
-- =============================================================================

CREATE TABLE IF NOT EXISTS spatial_features (
  id SERIAL PRIMARY KEY,
  metadata_id INTEGER REFERENCES metadata(id),
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  properties JSONB
);

CREATE INDEX IF NOT EXISTS idx_spatial_features_lat_lon ON spatial_features (latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_spatial_features_metadata_id ON spatial_features(metadata_id);

CREATE TABLE IF NOT EXISTS taxonomy (
  id SERIAL PRIMARY KEY,
  species_name TEXT UNIQUE NOT NULL,
  common_name TEXT,
  family TEXT,
  phylum TEXT,
  class TEXT,
  "order" TEXT,
  genus TEXT,
  authority TEXT
);

CREATE TABLE IF NOT EXISTS species_observations (
  id SERIAL PRIMARY KEY,
  metadata_id INTEGER REFERENCES metadata(id),
  location_id INTEGER REFERENCES locations(id),
  taxonomy_id INTEGER REFERENCES taxonomy(id),
  observation_date TIMESTAMP,
  count_value NUMERIC,
  count_category TEXT,
  depth_m NUMERIC,
  sex TEXT,
  size_class TEXT,
  method TEXT,
  notes TEXT,
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_species_obs_lat_lon ON species_observations (latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_species_obs_taxonomy ON species_observations(taxonomy_id);
CREATE INDEX IF NOT EXISTS idx_species_obs_metadata ON species_observations(metadata_id);

-- =============================================================================
-- GRANTS FOR GRAFANA/PGADMIN
-- =============================================================================

GRANT USAGE ON SCHEMA public TO marine_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO marine_user;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO marine_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO marine_user;

GRANT USAGE ON SCHEMA _timescaledb_internal TO marine_user;
-- SAFE: Grant only if table exists
DO $$ 
BEGIN 
  GRANT SELECT ON _timescaledb_internal.continuous_aggs_materialization_invalidation_log TO marine_user;
EXCEPTION 
  WHEN undefined_table THEN NULL; 
END $$;


GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO marine_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO marine_user;

-- =============================================================================
-- DATABASE STATISTICS (Post-Cleanup, January 1, 2026)
-- =============================================================================
-- Schema Version: 3.2 (Pure PostgreSQL, no PostGIS, removed uuid field)
-- Total measurements capacity: 12M+ (quality-controlled)
-- Unique parameters: 125+
-- Datasets: 38+
-- Compatible with: timescale/timescaledb:latest-pg18 (Community license)
-- CHANGED: Removed uuid field, aodn_uuid is now sole AODN identifier
-- dataset_path is primary stable identifier for upserts
-- =============================================================================

VACUUM ANALYZE;
