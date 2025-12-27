-- =============================================================================
-- Huon Channel Marine Analytics - Database Initialization Script  
-- =============================================================================
-- Purpose: Creates PostgreSQL database schema for AODN marine data
-- Version: 2.2 (with QC cleanup documentation)
-- Last Updated: December 27, 2025
--
-- IMPORTANT NOTES:
-- 1. Quality Control (QC) columns are NOT stored as separate measurements
--    - QC flag columns (e.g., TEMP_QUALITY_CONTROL) are filtered during ETL
--    - Bad data flagged by QC=4 is removed before ingestion
--    - Only quality-controlled measurements (QC flag 1-2) are stored
-- 2. Database cleanup performed December 2025:
--    - Removed 8,276,395 QC flag records (42.5% of original data)
--    - Removed 54,325 bad wave measurements (where QC flag = 4)
--    - Final clean database: 12,028,987 quality-controlled measurements
-- 3. Coordinates use EPSG:4326 (WGS84)
-- 4. All timestamps are stored in UTC (timestamptz)
-- 5. TimescaleDB hypertable enabled for measurements table
-- =============================================================================

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS unaccent;

-- =============================================================================
-- PARAMETER MAPPINGS TABLE (replaces config_parameter_mapping.json)
-- =============================================================================

CREATE TABLE parameter_mappings (
    id SERIAL PRIMARY KEY,
    raw_parameter_name TEXT UNIQUE NOT NULL,
    standard_code TEXT NOT NULL,
    namespace TEXT NOT NULL CHECK (namespace IN ('bodc', 'cf', 'custom')),
    unit TEXT NOT NULL,
    description TEXT,
    source TEXT DEFAULT 'system',  -- 'system' or 'user'
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_param_mappings_raw ON parameter_mappings(raw_parameter_name);
CREATE INDEX idx_param_mappings_code ON parameter_mappings(standard_code);
CREATE INDEX idx_param_mappings_namespace ON parameter_mappings(namespace);

COMMENT ON TABLE parameter_mappings IS 'Maps raw parameter names from data files to standardized BODC/CF codes';
COMMENT ON COLUMN parameter_mappings.namespace IS 'bodc = British Oceanographic Data Centre, cf = Climate & Forecast, custom = user-defined';

-- =============================================================================
-- IMOS CONTROLLED VOCABULARIES (from AODN)
-- =============================================================================

CREATE TABLE public.imos_vocab_geographic_extents (
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

CREATE TABLE public.imos_vocab_organization_categories (
    uri text PRIMARY KEY,
    pref_label text NOT NULL,
    alt_label text,
    definition text,
    broader_uri text REFERENCES imos_vocab_organization_categories(uri) ON DELETE SET NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE public.imos_vocab_platform_categories (
    uri text PRIMARY KEY,
    pref_label text NOT NULL,
    alt_label text,
    definition text,
    broader_uri text REFERENCES imos_vocab_platform_categories(uri) ON DELETE SET NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE public.imos_vocab_parameter_categories (
    uri text PRIMARY KEY,
    pref_label text NOT NULL,
    definition text,
    broader_uri text REFERENCES imos_vocab_parameter_categories(uri) ON DELETE SET NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE public.imos_vocab_parameters (
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

CREATE TABLE public.imos_vocab_instruments (
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

CREATE TABLE public.imos_vocab_units (
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
-- LOCATIONS & SPATIAL REFERENCES
-- =============================================================================

CREATE TABLE public.locations (
    id SERIAL PRIMARY KEY,
    location_name text,
    location_type text DEFAULT 'observation_site',
    location_geom public.geometry(Point,4326),
    longitude double precision,
    latitude double precision,
    description text,
    created_at timestamp without time zone DEFAULT now(),
    
    -- Explicit UNIQUE constraint for ON CONFLICT support
    CONSTRAINT unique_lat_lon UNIQUE (latitude, longitude)
);

-- Performance index for spatial queries
CREATE INDEX idx_locations_geom ON public.locations USING gist (location_geom);

-- Partial index for non-NULL coordinates (additional performance optimization)
CREATE INDEX idx_locations_lat_lon_partial 
ON public.locations (latitude, longitude) 
WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- =============================================================================
-- METADATA TABLES (normalized structure for 38+ IMAS datasets)
-- =============================================================================

CREATE TABLE metadata (
    id SERIAL PRIMARY KEY,
    uuid TEXT UNIQUE NOT NULL,
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
    dataset_path TEXT,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_created DATE,
    
    -- PostGIS spatial extent (unlocks spatial queries)
    extent_geom GEOMETRY(POLYGON, 4326),
    
    -- Materialized bounding box for non-spatial queries
    bbox_envelope BOX2D GENERATED ALWAYS AS (BOX2D(extent_geom)) STORED
);

-- Metadata indexes (removed CONCURRENTLY for init script compatibility)
CREATE INDEX idx_metadata_uuid ON metadata(uuid);
CREATE INDEX idx_metadata_bbox ON metadata(west, east, south, north);
CREATE INDEX idx_metadata_time ON metadata(time_start, time_end);
CREATE INDEX idx_metadata_extent_geom ON metadata USING GIST(extent_geom);
CREATE INDEX idx_metadata_dataset_name ON metadata(dataset_name);
CREATE INDEX idx_metadata_dataset_path ON metadata(dataset_path) 
WHERE dataset_path IS NOT NULL;

CREATE TABLE public.spatial_ref_system (
    id SERIAL PRIMARY KEY,
    uuid text NOT NULL REFERENCES metadata(uuid) ON DELETE CASCADE,
    srid integer NOT NULL,
    auth_name text DEFAULT 'EPSG'::text,
    crs_name text,
    is_primary boolean DEFAULT true,
    created_at timestamp without time zone DEFAULT now(),
    UNIQUE (uuid, srid)
);

CREATE TABLE parameters (
    id SERIAL PRIMARY KEY,
    metadata_id INTEGER REFERENCES metadata(id) ON DELETE CASCADE,
    uuid TEXT NOT NULL REFERENCES metadata(uuid) ON DELETE CASCADE,
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
    
    -- Foreign keys to IMOS vocabularies
    imos_parameter_uri TEXT REFERENCES imos_vocab_parameters(uri),
    imos_unit_uri TEXT REFERENCES imos_vocab_units(uri),
    
    UNIQUE(uuid, parameter_code)
);

CREATE INDEX idx_parameters_metadata_id ON parameters(metadata_id);
CREATE INDEX idx_parameters_code ON parameters(parameter_code);
CREATE INDEX idx_parameters_aodn_uri ON parameters(aodn_parameter_uri);
CREATE INDEX idx_parameters_imos_uri ON parameters(imos_parameter_uri);

CREATE TABLE keywords (
    id SERIAL PRIMARY KEY,
    metadata_id INTEGER REFERENCES metadata(id) ON DELETE CASCADE,
    uuid TEXT REFERENCES metadata(uuid) ON DELETE CASCADE, 
    keyword TEXT NOT NULL,
    keyword_type TEXT,  -- theme, place, discipline, platform
    thesaurus_name TEXT,
    thesaurus_uri TEXT,
    UNIQUE(metadata_id, keyword)
);

CREATE INDEX idx_keywords_metadata_id ON keywords(metadata_id);
CREATE INDEX idx_keywords_keyword ON keywords(keyword);
CREATE INDEX idx_keywords_thesaurus ON keywords(thesaurus_uri);

-- =============================================================================
-- MEASUREMENTS HYPERTABLE (TimescaleDB optimized for 12M+ measurements)
-- =============================================================================
-- NOTE: Quality control columns (e.g., TEMP_QUALITY_CONTROL) are NOT stored.
--       Bad measurements (QC flag = 4) are removed during ETL.
--       Only quality-controlled data is ingested into this table.
-- =============================================================================

CREATE TABLE measurements (
    time TIMESTAMPTZ NOT NULL,
    data_id BIGSERIAL,
    uuid TEXT REFERENCES metadata(uuid) ON DELETE CASCADE,
    parameter_code TEXT NOT NULL,
    namespace TEXT NOT NULL DEFAULT 'custom',
    value DOUBLE PRECISION NOT NULL,
    uom TEXT NOT NULL,
    uncertainty DOUBLE PRECISION,
    depth_m NUMERIC,
    location_id BIGINT REFERENCES locations(id),
    metadata_id INTEGER REFERENCES metadata(id),
    quality_flag SMALLINT DEFAULT 1
);

COMMENT ON TABLE measurements IS 'Timeseries measurements (quality-controlled only, QC columns removed Dec 2025)';
COMMENT ON COLUMN measurements.quality_flag IS 'IMOS QC flag: 1=good, 2=probably good (only values 1-2 stored)';

-- Create hypertable BEFORE adding indexes
SELECT create_hypertable('measurements', by_range('time'));

-- Now add PRIMARY KEY as index (after hypertable creation)
CREATE UNIQUE INDEX measurements_pkey ON measurements (time, data_id);
ALTER TABLE measurements ADD CONSTRAINT measurements_pkey_constraint PRIMARY KEY USING INDEX measurements_pkey;

-- Enable compression with segmentation by parameter and namespace
ALTER TABLE measurements SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'parameter_code, namespace'
);

-- Add compression policy (compress data older than 7 days)
SELECT add_compression_policy('measurements', INTERVAL '7 days');

-- CRITICAL INDEXES FOR GRAFANA QUERIES
CREATE INDEX idx_measurements_time_param ON measurements (time DESC, parameter_code)
WHERE namespace = 'bodc';

CREATE INDEX idx_measurements_param_time ON measurements (parameter_code, time DESC);
CREATE INDEX idx_measurements_location_time ON measurements (location_id, time DESC)
WHERE location_id IS NOT NULL;

CREATE INDEX idx_measurements_namespace ON measurements (namespace);
CREATE INDEX idx_measurements_metadata_id ON measurements (metadata_id);
CREATE INDEX idx_measurements_uuid ON measurements (uuid);

-- BRIN indexes for massive time-range scans
CREATE INDEX idx_measurements_time_brin ON measurements USING BRIN (time);

-- GIN for parameter_code fuzzy search
CREATE INDEX idx_measurements_param_gin ON measurements USING GIN (to_tsvector('english', parameter_code));

-- Partial index for quality data only
CREATE INDEX idx_measurements_good_data ON measurements (time DESC, parameter_code) 
WHERE quality_flag = 1 AND namespace IN ('bodc', 'cf');

-- =============================================================================
-- CONTINUOUS AGGREGATES (Grafana performance)
-- =============================================================================

-- Hourly aggregates
CREATE MATERIALIZED VIEW measurements_1h
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', time) AS bucket,
       parameter_code, namespace, location_id, metadata_id,
       AVG(value) AS avg_value, 
       STDDEV(value) AS stddev_value,
       COUNT(*) AS count,
       MIN(quality_flag) AS min_quality
FROM measurements 
GROUP BY bucket, parameter_code, namespace, location_id, metadata_id;

-- Daily aggregates
CREATE MATERIALIZED VIEW measurements_1d
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', time) AS bucket,
       parameter_code, namespace, location_id,
       AVG(value) AS avg_value, 
       STDDEV(value) AS stddev_value,
       COUNT(*) AS count
FROM measurements 
GROUP BY bucket, parameter_code, namespace, location_id;

-- Real-time refresh policies
SELECT add_continuous_aggregate_policy('measurements_1h',
    start_offset => INTERVAL '3 days', 
    end_offset => INTERVAL '1 hour', 
    schedule_interval => INTERVAL '1 hour');

SELECT add_continuous_aggregate_policy('measurements_1d',
    start_offset => INTERVAL '30 days', 
    end_offset => INTERVAL '1 day', 
    schedule_interval => INTERVAL '1 day');

-- =============================================================================
-- VIEWS FOR GRAFANA & ANALYSIS
-- =============================================================================

-- View joining measurements with metadata
CREATE VIEW measurements_with_metadata AS
SELECT 
    m.time, m.data_id, m.parameter_code, m.namespace, m.value, m.uom,
    m.uncertainty, m.depth_m, m.location_id, m.quality_flag,
    md.title AS dataset_title,
    md.dataset_name,
    p.parameter_label,
    p.unit_name,
    md.extent_geom
FROM measurements m
LEFT JOIN metadata md ON m.metadata_id = md.id
LEFT JOIN parameters p ON md.id = p.metadata_id AND m.parameter_code = p.parameter_code;

-- Spatial view for datasets by parameter
CREATE VIEW datasets_by_parameter AS
SELECT 
    p.parameter_code,
    p.parameter_label,
    p.aodn_parameter_uri,
    COUNT(DISTINCT md.id) AS dataset_count,
    ST_Union(md.extent_geom) AS parameter_extent,
    ARRAY_AGG(DISTINCT md.dataset_name) AS datasets
FROM parameters p
JOIN metadata md ON p.metadata_id = md.id
GROUP BY p.parameter_code, p.parameter_label, p.aodn_parameter_uri;

-- =============================================================================
-- SPATIAL & BIOLOGICAL FEATURES
-- =============================================================================

-- Spatial Features Table
CREATE TABLE IF NOT EXISTS spatial_features (
    id SERIAL PRIMARY KEY,
    metadata_id INTEGER REFERENCES metadata(id),
    uuid TEXT,
    geom GEOMETRY(Geometry, 4326),
    properties JSONB
);
CREATE INDEX IF NOT EXISTS spatial_features_geom_idx ON spatial_features USING GIST (geom);
CREATE INDEX IF NOT EXISTS spatial_features_metadata_id_idx ON spatial_features(metadata_id);

-- Taxonomy Table
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

-- Species Observations Table
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
    geom GEOMETRY(Point, 4326)
);
CREATE INDEX IF NOT EXISTS idx_species_obs_geom ON species_observations USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_species_obs_tax ON species_observations(taxonomy_id);
CREATE INDEX IF NOT EXISTS idx_species_obs_meta ON species_observations(metadata_id);

-- =============================================================================
-- GRANTS FOR GRAFANA/PGADMIN
-- =============================================================================

GRANT USAGE ON SCHEMA public TO marine_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO marine_user;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO marine_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO marine_user;

-- Allow Grafana to refresh continuous aggregates
GRANT USAGE ON SCHEMA _timescaledb_internal TO marine_user;
GRANT SELECT ON _timescaledb_internal.continuous_aggs_materialization_invalidation_log TO marine_user;

-- Allow inserts for ETL scripts
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO marine_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO marine_user;

-- =============================================================================
-- DATABASE STATISTICS (Post-Cleanup, December 27, 2025)
-- =============================================================================
-- Total measurements:     12,028,987 (quality-controlled)
-- Unique parameters:      125
-- Datasets:               25  
-- Date range:             1900-2099 (some invalid dates to be cleaned)
-- QC records removed:     8,276,395 (December 2025 cleanup)
-- Bad wave data removed:  54,325 measurements (QC flag = 4)
-- =============================================================================

-- Vacuum analyze for optimal performance
VACUUM ANALYZE;