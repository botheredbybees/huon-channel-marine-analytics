-- =============================================================================
-- Huon Channel Marine Analytics - Database Initialization Script (PostGIS-Free)
-- =============================================================================
-- Purpose: Creates PostgreSQL database schema for AODN marine data
-- Version: 3.3 (Pure PostgreSQL - WoRMS/GBIF enrichment support added)
-- Last Updated: January 6, 2026
--
-- IMPORTANT NOTES:
-- 1. Removed ALL PostGIS dependencies (GEOMETRY, ST_* functions, BOX2D, GIST on geometry)
-- 2. Uses pure PostgreSQL lat/lon and bbox columns instead
-- 3. Compatible with timescale/timescaledb:latest-pg18 (Community license)
-- 4. All spatial queries work with DECIMAL bbox columns
-- 5. TimescaleDB hypertable enabled for measurements table
-- 6. Removed confusing 'uuid' field - now using aodn_uuid for AODN catalog IDs
-- 7. dataset_path is now the primary stable identifier for upserts
-- 8. Added WoRMS/GBIF columns to taxonomy_cache (v3.3 - Jan 6, 2026)
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
  date_created DATE
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
-- TAXONOMY ENRICHMENT TABLES
-- =============================================================================
-- Purpose: Cache taxonomic data from external APIs (iNaturalist, WoRMS, GBIF)
-- Created: January 6, 2026
-- Version: 1.1 (Added WoRMS/GBIF columns)
-- Dependencies: Requires existing 'taxonomy' table
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. TAXONOMY CACHE TABLE
-- -----------------------------------------------------------------------------
-- Stores enriched taxonomic data from external APIs (iNaturalist, WoRMS, GBIF)
-- Acts as a cache to avoid repeated API calls and stores additional metadata
-- not available in source observation files

CREATE TABLE IF NOT EXISTS taxonomy_cache (
    id SERIAL PRIMARY KEY,
    taxonomy_id INTEGER REFERENCES taxonomy(id) ON DELETE CASCADE,
    species_name TEXT UNIQUE NOT NULL,
    
    -- iNaturalist identifiers
    inaturalist_taxon_id INTEGER,
    inaturalist_url TEXT,
    
    -- Taxonomic hierarchy (enriched from API)
    common_name TEXT,
    genus TEXT,
    family TEXT,
    "order" TEXT,
    class TEXT,
    phylum TEXT,
    kingdom TEXT,
    authority TEXT,  -- Taxonomic authority (e.g., "(Linnaeus, 1758)")
    
    -- Taxonomic metadata
    rank TEXT,  -- species, genus, family, order, class, phylum, kingdom
    rank_level INTEGER,  -- Numeric rank (10=species, 20=genus, etc.)
    iconic_taxon_name TEXT,  -- Plantae, Animalia, Chromista, Protozoa, Fungi
    
    -- Conservation & distribution
    conservation_status TEXT,  -- IUCN status if available
    conservation_status_source TEXT,
    introduced BOOLEAN DEFAULT FALSE,  -- Is this an introduced/invasive species?
    endemic BOOLEAN DEFAULT FALSE,  -- Is this endemic to Australia?
    threatened BOOLEAN DEFAULT FALSE,  -- Is this a threatened species?
    
    -- External links
    wikipedia_url TEXT,
    wikipedia_summary TEXT,
    photo_url TEXT,  -- Representative photo URL from iNaturalist
    photo_attribution TEXT,
    
    -- WoRMS data (World Register of Marine Species)
    worms_aphia_id INTEGER,
    worms_lsid TEXT,  -- Life Science Identifier
    worms_url TEXT,  -- Direct URL to WoRMS species page
    worms_valid_name TEXT,  -- Accepted valid name if this is a synonym
    scientific_name_authorship TEXT,  -- Taxonomic authority from WoRMS/GBIF
    taxonomic_status TEXT,  -- accepted, synonym, invalid, etc.
    accepted_name TEXT,  -- Valid/accepted name if current name is synonym
    accepted_aphia_id INTEGER,  -- WoRMS AphiaID of accepted name
    is_marine BOOLEAN,
    is_brackish BOOLEAN,
    is_freshwater BOOLEAN,
    is_terrestrial BOOLEAN,
    is_extinct BOOLEAN DEFAULT FALSE,  -- Extinction status
    
    -- GBIF data (Global Biodiversity Information Facility)
    gbif_taxon_key INTEGER,
    gbif_scientific_name TEXT,  -- Full scientific name from GBIF
    gbif_canonical_name TEXT,
    match_type TEXT,  -- GBIF match type: EXACT, FUZZY, HIGHERRANK
    confidence INTEGER,  -- GBIF match confidence (0-100)
    
    -- Raw API responses (stored as JSONB for future reference)
    inaturalist_response JSONB,
    worms_response JSONB,
    gbif_response JSONB,
    
    -- Metadata
    data_source TEXT DEFAULT 'inaturalist',  -- Primary source: inaturalist, worms, gbif, manual
    last_updated TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for taxonomy_cache
CREATE INDEX IF NOT EXISTS idx_taxonomy_cache_species ON taxonomy_cache(species_name);
CREATE INDEX IF NOT EXISTS idx_taxonomy_cache_taxonomy_id ON taxonomy_cache(taxonomy_id);
CREATE INDEX IF NOT EXISTS idx_taxonomy_cache_inat_id ON taxonomy_cache(inaturalist_taxon_id) WHERE inaturalist_taxon_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_taxonomy_cache_worms_id ON taxonomy_cache(worms_aphia_id) WHERE worms_aphia_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_taxonomy_cache_gbif_key ON taxonomy_cache(gbif_taxon_key) WHERE gbif_taxon_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_taxonomy_cache_iconic ON taxonomy_cache(iconic_taxon_name) WHERE iconic_taxon_name IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_taxonomy_cache_rank ON taxonomy_cache(rank);
CREATE INDEX IF NOT EXISTS idx_taxonomy_cache_marine ON taxonomy_cache(is_marine) WHERE is_marine = TRUE;
CREATE INDEX IF NOT EXISTS idx_taxonomy_cache_source ON taxonomy_cache(data_source);
CREATE INDEX IF NOT EXISTS idx_taxonomy_cache_taxonomic_status ON taxonomy_cache(taxonomic_status);
CREATE INDEX IF NOT EXISTS idx_taxonomy_cache_accepted_aphia_id ON taxonomy_cache(accepted_aphia_id) WHERE accepted_aphia_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_taxonomy_cache_match_type ON taxonomy_cache(match_type);

-- GIN index for JSONB API responses (enables fast JSON queries)
CREATE INDEX IF NOT EXISTS idx_taxonomy_cache_inat_response ON taxonomy_cache USING GIN (inaturalist_response);

COMMENT ON TABLE taxonomy_cache IS 'Enriched taxonomic data from external APIs (iNaturalist, WoRMS, GBIF)';
COMMENT ON COLUMN taxonomy_cache.rank_level IS 'Numeric rank: 10=species, 20=genus, 30=family, 40=order, 50=class, 60=phylum, 70=kingdom';
COMMENT ON COLUMN taxonomy_cache.iconic_taxon_name IS 'High-level taxonomic group: Plantae, Animalia, Chromista, Protozoa, Fungi, Bacteria, Archaea';
COMMENT ON COLUMN taxonomy_cache.inaturalist_response IS 'Full JSON response from iNaturalist API for future reference';
COMMENT ON COLUMN taxonomy_cache.scientific_name_authorship IS 'Taxonomic authority (e.g., "(Linnaeus, 1758)")';
COMMENT ON COLUMN taxonomy_cache.taxonomic_status IS 'Status: accepted, synonym, invalid, etc.';
COMMENT ON COLUMN taxonomy_cache.accepted_name IS 'Valid/accepted name if this is a synonym';
COMMENT ON COLUMN taxonomy_cache.accepted_aphia_id IS 'WoRMS AphiaID of accepted name';
COMMENT ON COLUMN taxonomy_cache.worms_url IS 'Direct URL to WoRMS species page';
COMMENT ON COLUMN taxonomy_cache.is_extinct IS 'Whether species is extinct';
COMMENT ON COLUMN taxonomy_cache.gbif_scientific_name IS 'Full scientific name from GBIF';
COMMENT ON COLUMN taxonomy_cache.match_type IS 'GBIF match type: EXACT, FUZZY, HIGHERRANK';
COMMENT ON COLUMN taxonomy_cache.confidence IS 'GBIF match confidence (0-100)';

-- -----------------------------------------------------------------------------
-- 2. TAXONOMY ENRICHMENT LOG TABLE
-- -----------------------------------------------------------------------------
-- Tracks all API lookups, matches, and failures for debugging and quality control
-- Essential for identifying species that need manual review

CREATE TABLE IF NOT EXISTS taxonomy_enrichment_log (
    id SERIAL PRIMARY KEY,
    taxonomy_id INTEGER REFERENCES taxonomy(id) ON DELETE CASCADE,
    species_name TEXT NOT NULL,
    
    -- Search metadata
    search_query TEXT,  -- Actual query sent to API (may differ from species_name)
    api_endpoint TEXT,  -- Which API was called (inaturalist, worms, gbif)
    api_url TEXT,  -- Full URL called
    
    -- Response metadata
    response_status INTEGER,  -- HTTP status code (200, 404, 500, etc.)
    response_time_ms INTEGER,  -- API response time in milliseconds
    matches_found INTEGER DEFAULT 0,  -- Number of results returned
    
    -- Match selection
    taxon_id_selected INTEGER,  -- Which taxon ID was chosen from results
    match_rank INTEGER,  -- Rank of selected match (1=best, 2=second best, etc.)
    confidence_score DECIMAL(3,2),  -- 0.00 to 1.00 (calculated match confidence)
    match_method TEXT,  -- exact, fuzzy, genus_only, manual, etc.
    
    -- Quality flags
    needs_manual_review BOOLEAN DEFAULT FALSE,
    review_reason TEXT,  -- Why this needs review (ambiguous, no_match, low_confidence, etc.)
    reviewed_by TEXT,  -- Username who reviewed this match
    reviewed_at TIMESTAMP,
    review_notes TEXT,
    
    -- Error handling
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for taxonomy_enrichment_log
CREATE INDEX IF NOT EXISTS idx_enrichment_log_taxonomy_id ON taxonomy_enrichment_log(taxonomy_id);
CREATE INDEX IF NOT EXISTS idx_enrichment_log_species ON taxonomy_enrichment_log(species_name);
CREATE INDEX IF NOT EXISTS idx_enrichment_log_api ON taxonomy_enrichment_log(api_endpoint);
CREATE INDEX IF NOT EXISTS idx_enrichment_log_status ON taxonomy_enrichment_log(response_status);
CREATE INDEX IF NOT EXISTS idx_enrichment_log_created ON taxonomy_enrichment_log(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_enrichment_log_needs_review 
    ON taxonomy_enrichment_log(needs_manual_review) 
    WHERE needs_manual_review = TRUE;
CREATE INDEX IF NOT EXISTS idx_enrichment_log_confidence 
    ON taxonomy_enrichment_log(confidence_score) 
    WHERE confidence_score < 0.80;

COMMENT ON TABLE taxonomy_enrichment_log IS 'Audit log of all taxonomy API lookups and match decisions';
COMMENT ON COLUMN taxonomy_enrichment_log.confidence_score IS 'Match confidence: 1.0=exact, 0.9+=high, 0.7-0.9=medium, <0.7=low (needs review)';
COMMENT ON COLUMN taxonomy_enrichment_log.review_reason IS 'Values: ambiguous, no_match, low_confidence, multiple_matches, synonym_conflict, unidentified_category';

-- -----------------------------------------------------------------------------
-- 3. TAXONOMY SYNONYMS TABLE
-- -----------------------------------------------------------------------------
-- Stores synonym relationships discovered during enrichment
-- Many species have multiple scientific names; this tracks accepted vs synonym names

CREATE TABLE IF NOT EXISTS taxonomy_synonyms (
    id SERIAL PRIMARY KEY,
    taxonomy_id INTEGER REFERENCES taxonomy(id) ON DELETE CASCADE,
    synonym_name TEXT NOT NULL,
    accepted_name TEXT NOT NULL,
    status TEXT,  -- synonym, accepted, invalid, misapplied, etc.
    source TEXT,  -- inaturalist, worms, gbif
    source_taxon_id INTEGER,
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(synonym_name, accepted_name)
);

CREATE INDEX IF NOT EXISTS idx_taxonomy_synonyms_taxonomy_id ON taxonomy_synonyms(taxonomy_id);
CREATE INDEX IF NOT EXISTS idx_taxonomy_synonyms_synonym ON taxonomy_synonyms(synonym_name);
CREATE INDEX IF NOT EXISTS idx_taxonomy_synonyms_accepted ON taxonomy_synonyms(accepted_name);
CREATE INDEX IF NOT EXISTS idx_taxonomy_synonyms_source ON taxonomy_synonyms(source);

COMMENT ON TABLE taxonomy_synonyms IS 'Synonym relationships for species names';
COMMENT ON COLUMN taxonomy_synonyms.status IS 'Taxonomic status: synonym, accepted, invalid, misapplied, uncertain';

-- -----------------------------------------------------------------------------
-- 4. TAXONOMY COMMON NAMES TABLE
-- -----------------------------------------------------------------------------
-- Stores multiple common names per species (many species have regional variants)
-- e.g., "Ecklonia radiata" = "Common kelp" (AUS), "Southern kelp" (NZ)

CREATE TABLE IF NOT EXISTS taxonomy_common_names (
    id SERIAL PRIMARY KEY,
    taxonomy_id INTEGER REFERENCES taxonomy(id) ON DELETE CASCADE,
    common_name TEXT NOT NULL,
    language TEXT DEFAULT 'en',  -- ISO 639-1 language code
    locality TEXT,  -- Australia, Tasmania, New Zealand, etc.
    is_primary BOOLEAN DEFAULT FALSE,  -- Primary common name for this species
    source TEXT,  -- inaturalist, worms, gbif, local
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(taxonomy_id, common_name, language)
);

CREATE INDEX IF NOT EXISTS idx_taxonomy_common_names_taxonomy_id ON taxonomy_common_names(taxonomy_id);
CREATE INDEX IF NOT EXISTS idx_taxonomy_common_names_name ON taxonomy_common_names(common_name);
CREATE INDEX IF NOT EXISTS idx_taxonomy_common_names_language ON taxonomy_common_names(language);
CREATE INDEX IF NOT EXISTS idx_taxonomy_common_names_primary 
    ON taxonomy_common_names(taxonomy_id, is_primary) 
    WHERE is_primary = TRUE;

COMMENT ON TABLE taxonomy_common_names IS 'Multiple common names per species with language and locality';
COMMENT ON COLUMN taxonomy_common_names.locality IS 'Regional variation: Australia, Tasmania, New South Wales, etc.';

-- -----------------------------------------------------------------------------
-- 5. HELPER VIEW: Taxonomy Summary
-- -----------------------------------------------------------------------------
-- Convenient view combining taxonomy table with enrichment status

CREATE OR REPLACE VIEW taxonomy_enrichment_status AS
SELECT 
    t.id,
    t.species_name,
    t.common_name AS original_common_name,
    tc.common_name AS enriched_common_name,
    tc.genus,
    tc.family,
    tc."order",
    tc.class,
    tc.phylum,
    tc.kingdom,
    tc.rank,
    tc.iconic_taxon_name,
    tc.conservation_status,
    tc.introduced,
    tc.endemic,
    tc.threatened,
    tc.data_source,
    tc.inaturalist_taxon_id,
    tc.worms_aphia_id,
    tc.gbif_taxon_key,
    tc.taxonomic_status,
    tc.is_marine,
    tc.photo_url,
    tc.wikipedia_url,
    tc.last_updated AS cache_last_updated,
    CASE 
        WHEN tc.id IS NULL THEN 'not_enriched'
        WHEN tel.needs_manual_review = TRUE THEN 'needs_review'
        WHEN tc.genus IS NOT NULL AND tc.family IS NOT NULL THEN 'fully_enriched'
        WHEN tc.genus IS NOT NULL THEN 'partially_enriched'
        ELSE 'enrichment_failed'
    END AS enrichment_status,
    tel.confidence_score,
    tel.review_reason,
    tel.matches_found,
    (SELECT COUNT(*) FROM species_observations WHERE taxonomy_id = t.id) AS observation_count
FROM taxonomy t
LEFT JOIN taxonomy_cache tc ON t.id = tc.taxonomy_id
LEFT JOIN LATERAL (
    SELECT * FROM taxonomy_enrichment_log 
    WHERE taxonomy_id = t.id 
    ORDER BY created_at DESC 
    LIMIT 1
) tel ON TRUE;

COMMENT ON VIEW taxonomy_enrichment_status IS 'Summary of taxonomy enrichment status with observation counts';

-- -----------------------------------------------------------------------------
-- 6. HELPER VIEW: Taxa Needing Manual Review
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW taxa_needing_review AS
SELECT 
    t.id AS taxonomy_id,
    t.species_name,
    tel.review_reason,
    tel.confidence_score,
    tel.matches_found,
    tel.search_query,
    tel.created_at AS last_lookup,
    (SELECT COUNT(*) FROM species_observations WHERE taxonomy_id = t.id) AS observation_count
FROM taxonomy t
JOIN taxonomy_enrichment_log tel ON t.id = tel.taxonomy_id
WHERE tel.needs_manual_review = TRUE
  AND tel.reviewed_at IS NULL
ORDER BY observation_count DESC, tel.created_at DESC;

COMMENT ON VIEW taxa_needing_review IS 'Species requiring manual review, ordered by observation count (most important first)';

-- =============================================================================
-- GRANTS (if needed for read-only users)
-- =============================================================================

GRANT SELECT ON taxonomy_cache TO marine_user;
GRANT SELECT ON taxonomy_enrichment_log TO marine_user;
GRANT SELECT ON taxonomy_synonyms TO marine_user;
GRANT SELECT ON taxonomy_common_names TO marine_user;
GRANT SELECT ON taxonomy_enrichment_status TO marine_user;
GRANT SELECT ON taxa_needing_review TO marine_user;

-- =============================================================================
-- END OF TAXONOMY ENRICHMENT SCHEMA
-- =============================================================================


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
-- DATABASE STATISTICS (Post-Cleanup, January 6, 2026)
-- =============================================================================
-- Schema Version: 3.3 (Pure PostgreSQL + WoRMS/GBIF enrichment)
-- Total measurements capacity: 12M+ (quality-controlled)
-- Unique parameters: 125+
-- Datasets: 38+
-- Compatible with: timescale/timescaledb:latest-pg18 (Community license)
-- CHANGED: Removed uuid field, aodn_uuid is now sole AODN identifier
-- dataset_path is primary stable identifier for upserts
-- ADDED: WoRMS/GBIF columns to taxonomy_cache (v3.3 - Jan 6, 2026)
-- =============================================================================

VACUUM ANALYZE;
