-- Complete init.sql for Marine Environmental Data Platform
-- TimescaleDB + PostGIS + Metadata normalization + Measurements hypertable

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
    location_type text,
    location_geom public.geometry(Point,4326),
    longitude double precision,
    latitude double precision,
    description text,
    created_at timestamp without time zone DEFAULT now(),
    UNIQUE (latitude, longitude)
);

CREATE INDEX idx_locations_geom ON public.locations USING gist (location_geom);

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

-- Metadata indexes
CREATE INDEX CONCURRENTLY idx_metadata_uuid ON metadata(uuid);
CREATE INDEX CONCURRENTLY idx_metadata_bbox ON metadata(west, east, south, north);
CREATE INDEX CONCURRENTLY idx_metadata_time ON metadata(time_start, time_end);
CREATE INDEX CONCURRENTLY idx_metadata_extent_geom ON metadata USING GIST(extent_geom);
CREATE INDEX CONCURRENTLY idx_metadata_dataset_name ON metadata(dataset_name);

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

CREATE INDEX CONCURRENTLY idx_parameters_metadata_id ON parameters(metadata_id);
CREATE INDEX CONCURRENTLY idx_parameters_code ON parameters(parameter_code);
CREATE INDEX CONCURRENTLY idx_parameters_aodn_uri ON parameters(aodn_parameter_uri);
CREATE INDEX CONCURRENTLY idx_parameters_imos_uri ON parameters(imos_parameter_uri);

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

CREATE INDEX CONCURRENTLY idx_keywords_metadata_id ON keywords(metadata_id);
CREATE INDEX CONCURRENTLY idx_keywords_keyword ON keywords(keyword);
CREATE INDEX CONCURRENTLY idx_keywords_thesaurus ON keywords(thesaurus_uri);

-- =============================================================================
-- MEASUREMENTS HYPERTABLE (your core table - TimescaleDB optimized)
-- =============================================================================

CREATE TABLE measurements (
    time TIMESTAMPTZ NOT NULL,
    data_id BIGSERIAL PRIMARY KEY,
    uuid TEXT REFERENCES metadata(uuid) ON DELETE CASCADE,
    parameter_code TEXT NOT NULL,
    namespace TEXT NOT NULL DEFAULT 'custom',
    value DOUBLE PRECISION NOT NULL,
    uom TEXT NOT NULL,
    uncertainty DOUBLE PRECISION,
    depth_m NUMERIC,
    location_id BIGINT REFERENCES locations(id),
    metadata_id INTEGER REFERENCES metadata(id),  -- FK to normalized metadata
    quality_flag SMALLINT DEFAULT 1
);

-- Convert to hypertable with COMPOSITE time + parameter_code partitioning
-- SELECT create_hypertable('measurements', by_range('time'),
--     partitioning_column => 'parameter_code',
--     number_partitions => 32);

-- CRITICAL INDEXES FOR GRAFANA QUERIES (your 942 parameters)
CREATE INDEX CONCURRENTLY idx_measurements_time_param ON measurements (time DESC, parameter_code)
WHERE namespace = 'bodc'; -- Fastest BODC queries (chl_a, sst, etc.)

CREATE INDEX CONCURRENTLY idx_measurements_param_time ON measurements (parameter_code, time DESC);
CREATE INDEX CONCURRENTLY idx_measurements_location_time ON measurements (location_id, time DESC)
WHERE location_id IS NOT NULL;

CREATE INDEX CONCURRENTLY idx_measurements_namespace ON measurements (namespace);
CREATE INDEX CONCURRENTLY idx_measurements_metadata_id ON measurements (metadata_id);
CREATE INDEX CONCURRENTLY idx_measurements_uuid ON measurements (uuid);

-- BRIN indexes for massive time-range scans (Grafana time picker)
CREATE INDEX idx_measurements_time_brin ON measurements USING BRIN (time);

-- GIN for parameter_code fuzzy search (Grafana variables)
CREATE INDEX idx_measurements_param_gin ON measurements USING GIN (to_tsvector('english', parameter_code));

-- Partial index for quality data only
CREATE INDEX idx_measurements_good_data ON measurements (time DESC, parameter_code) 
WHERE quality_flag = 1 AND namespace IN ('bodc', 'cf');

-- =============================================================================
-- COMPRESSION & CONTINUOUS AGGREGATES (Grafana performance)
-- =============================================================================

-- Add compression policy (90% space savings)
SELECT add_compression_policy('measurements', INTERVAL '7 days');

-- Continuous aggregates for Grafana (parameter-specific)
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

-- View joining measurements with metadata (Grafana-friendly)
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

-- Spatial Features Table for non-time-series data (Polygons, etc.)
CREATE TABLE IF NOT EXISTS spatial_features (
    id SERIAL PRIMARY KEY,
    metadata_id INTEGER REFERENCES metadata(id),
    uuid UUID,
    geom GEOMETRY(Geometry, 4326),
    properties JSONB
);
CREATE INDEX IF NOT EXISTS spatial_features_geom_idx ON spatial_features USING GIST (geom);
CREATE INDEX IF NOT EXISTS spatial_features_metadata_id_idx ON spatial_features(metadata_id);

-- Taxonomy Table (Species Info)
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
    count_category TEXT, -- For ranges like "2-5" or qualitative counts
    depth_m NUMERIC,
    sex TEXT,
    size_class TEXT,
    method TEXT,
    notes TEXT,
    geom GEOMETRY(Point, 4326) -- Denormalized for easy spatial heatmaps
);
CREATE INDEX IF NOT EXISTS idx_species_obs_geom ON species_observations USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_species_obs_tax ON species_observations(taxonomy_id);
CREATE INDEX IF NOT EXISTS idx_species_obs_meta ON species_observations(metadata_id);

-- =============================================================================
-- POPULATE DEFAULT PARAMETER MAPPINGS (from config_parameter_mapping.json)
-- =============================================================================

INSERT INTO parameter_mappings (raw_parameter_name, standard_code, namespace, unit, description) VALUES
-- Temperature variants
('TEMP', 'TEMP', 'bodc', 'Degrees Celsius', 'Sea water temperature'),
('TEMPERATURE', 'TEMP', 'bodc', 'Degrees Celsius', 'Sea water temperature'),
('SEA_WATER_TEMPERATURE', 'TEMP', 'cf', 'Degrees Celsius', 'CF standard name'),
('SST', 'SST', 'bodc', 'Degrees Celsius', 'Sea surface temperature'),
('SEA_SURFACE_TEMPERATURE', 'SST', 'cf', 'Degrees Celsius', 'CF SST'),
('SURFACE_TEMPERATURE', 'SST', 'custom', 'Degrees Celsius', 'Surface temperature'),

-- Salinity variants  
('PSAL', 'PSAL', 'bodc', 'PSS-78', 'Practical salinity'),
('SALINITY', 'PSAL', 'bodc', 'PSS-78', 'Practical salinity'),
('SEA_WATER_SALINITY', 'PSAL', 'cf', 'PSS-78', 'CF salinity'),
('PRACTICAL_SALINITY', 'PSAL', 'bodc', 'PSS-78', 'Practical salinity'),

-- Chlorophyll variants
('CPHL', 'CPHL', 'bodc', 'mg/m3', 'Chlorophyll-a concentration'),
('CHLOROPHYLL', 'CPHL', 'bodc', 'mg/m3', 'Chlorophyll-a'),
('CHLOROPHYLL_A', 'CPHL', 'bodc', 'mg/m3', 'Chlorophyll-a'),
('CHL_A', 'CPHL', 'bodc', 'mg/m3', 'Chlorophyll-a'),
('CHLOROPHYLL_CONCENTRATION', 'CPHL', 'cf', 'mg/m3', 'CF chlorophyll'),
('OCEAN_COLOUR_CHLOROPHYLL', 'CPHL', 'cf', 'mg/m3', 'Ocean colour chl'),
('PHYTOPLANKTON_CHLOROPHYLL', 'CPHL', 'cf', 'mg/m3', 'Phytoplankton chl'),

-- Oxygen
('DOXY', 'DOXY', 'bodc', 'ml/l', 'Dissolved oxygen'),
('DISSOLVED_OXYGEN', 'DOXY', 'cf', 'ml/l', 'Dissolved oxygen'),
('DO', 'DOXY', 'custom', 'ml/l', 'Dissolved oxygen'),
('OXYGEN_CONCENTRATION', 'DOXY', 'cf', 'ml/l', 'O2 concentration'),

-- pH
('PH', 'PH', 'bodc', 'unitless', 'pH'),
('PH_IN_SITU', 'PH', 'cf', 'unitless', 'In situ pH'),
('SEA_WATER_PH', 'PH', 'cf', 'unitless', 'Sea water pH'),

-- Depth/Altitude
('DEPTH', 'DEPTH', 'bodc', 'Meters', 'Depth below surface'),
('Z', 'DEPTH', 'cf', 'Meters', 'Vertical coordinate'),
('ALTITUDE', 'DEPTH', 'cf', 'Meters', 'Altitude'),
('HEIGHT', 'DEPTH', 'cf', 'Meters', 'Height'),

-- Pressure
('PRES', 'PRES', 'bodc', 'Decibars', 'Sea water pressure'),
('PRESSURE', 'PRES', 'bodc', 'Decibars', 'Pressure'),
('SEA_WATER_PRESSURE', 'PRES', 'cf', 'Decibars', 'CF pressure'),

-- Conductivity
('COND', 'COND', 'bodc', 'mS/cm', 'Electrical conductivity'),
('CONDUCTIVITY', 'COND', 'bodc', 'mS/cm', 'Conductivity'),
('SEA_WATER_ELECTRICAL_CONDUCTIVITY', 'COND', 'cf', 'mS/cm', 'CF conductivity'),

-- Turbidity
('TURB', 'TURB', 'bodc', 'NTU', 'Turbidity'),
('TURBIDITY', 'TURB', 'bodc', 'NTU', 'Turbidity'),
('TURBIDITY_COEFFICIENT', 'TURB', 'cf', 'NTU', 'Turbidity coefficient'),

-- Backscatter
('SCAT', 'SCAT', 'bodc', 'counts', 'Optical backscatter'),
('BACKSCATTER', 'SCAT', 'custom', 'counts', 'Backscatter'),
('OPTICAL_BACKSCATTER', 'SCAT', 'cf', 'counts', 'Optical backscatter'),

-- Fluorescence
('FLUO', 'FLUO', 'bodc', 'mg/m3', 'Fluorescence'),
('FLUORESCENCE', 'FLUO', 'custom', 'mg/m3', 'Fluorescence'),
('CHLOROPHYLL_FLUORESCENCE', 'FLUO', 'cf', 'mg/m3', 'Chl fluorescence'),

-- Velocity components
('VELOCITY_U', 'VELOCITY_U', 'cf', 'm/s', 'Eastward velocity'),
('VELOCITY_V', 'VELOCITY_V', 'cf', 'm/s', 'Northward velocity'),
('VELOCITY_W', 'VELOCITY_W', 'cf', 'm/s', 'Upward velocity'),
('EASTWARD_VELOCITY', 'VELOCITY_U', 'cf', 'm/s', 'U component'),
('NORTHWARD_VELOCITY', 'VELOCITY_V', 'cf', 'm/s', 'V component'),
('UPWARD_VELOCITY', 'VELOCITY_W', 'cf', 'm/s', 'W component'),

-- Waves
('WAVE_HEIGHT', 'WAVE_HGT', 'bodc', 'Meters', 'Wave height'),
('SIGNIFICANT_WAVE_HEIGHT', 'WAVE_HGT', 'cf', 'Meters', 'Significant wave height'),
('SWVHT', 'WAVE_HGT', 'custom', 'Meters', 'Significant wave height'),
('WAVE_PERIOD', 'WAVE_PER', 'bodc', 'Seconds', 'Wave period'),
('MEAN_WAVE_PERIOD', 'WAVE_PER', 'cf', 'Seconds', 'Mean wave period'),
('SWPD', 'WAVE_PER', 'custom', 'Seconds', 'Wave period'),

-- Wind
('WIND_SPEED', 'WIND_SPEED', 'bodc', 'm/s', 'Wind speed'),
('WIND_VELOCITY', 'WIND_SPEED', 'cf', 'm/s', 'Wind velocity'),
('WIND_U', 'WIND_U', 'cf', 'm/s', 'Eastward wind'),
('WIND_V', 'WIND_V', 'cf', 'm/s', 'Northward wind'),
('EASTWARD_WIND', 'WIND_U', 'cf', 'm/s', 'U wind component'),
('NORTHWARD_WIND', 'WIND_V', 'cf', 'm/s', 'V wind component'),

-- Currents
('CURRENT_SPEED', 'CURRENT_SPEED', 'custom', 'm/s', 'Current speed'),
('CURRENT_DIRECTION', 'CURRENT_DIR', 'custom', 'Degrees', 'Current direction'),

-- Biology
('ZOOPLANKTON_COUNT', 'ZOOP_COUNT', 'custom', 'count', 'Zooplankton count'),
('PHYTOPLANKTON_COUNT', 'PHYTO_COUNT', 'custom', 'count', 'Phytoplankton count'),
('ABUNDANCE', 'ABUNDANCE', 'custom', 'count', 'Species abundance'),
('BIOMASS', 'BIOMASS', 'custom', 'kg/m3', 'Biomass'),
('DENSITY', 'DENSITY', 'custom', 'individuals/m3', 'Population density'),

-- Nutrients
('NITRATE', 'NO3', 'custom', 'mmol/m3', 'Nitrate'),
('PHOSPHATE', 'PO4', 'custom', 'mmol/m3', 'Phosphate'),
('SILICATE', 'SIO4', 'custom', 'mmol/m3', 'Silicate'),
('AMMONIUM', 'NH4', 'custom', 'mmol/m3', 'Ammonium'),

-- Coordinates (for reference, not measurements)
('LATITUDE', 'LATITUDE', 'cf', 'Degrees', 'Latitude coordinate'),
('LONGITUDE', 'LONGITUDE', 'cf', 'Degrees', 'Longitude coordinate'),
('LAT', 'LATITUDE', 'cf', 'Degrees', 'Latitude'),
('LON', 'LONGITUDE', 'cf', 'Degrees', 'Longitude'),
('X', 'LONGITUDE', 'cf', 'Degrees', 'X coordinate'),
('Y', 'LATITUDE', 'cf', 'Degrees', 'Y coordinate')
ON CONFLICT (raw_parameter_name) DO NOTHING;

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

-- Vacuum analyze for optimal performance
VACUUM ANALYZE;
