-- ============================================================================
-- TAXONOMY VIEWS & UTILITY FUNCTIONS
-- ============================================================================
-- 
-- Purpose: Comprehensive query layer for enriched taxonomy system
-- Strategy: Keep taxonomy + taxonomy_cache separate, use views for queries
-- Created: January 6, 2026
-- Version: 1.0
--
-- Architecture:
--   taxonomy (base) → taxonomy_cache (enriched) → views (convenience)
--
-- ============================================================================

-- ============================================================================
-- 1. MAIN TAXONOMY VIEW - Complete Denormalized Species Data
-- ============================================================================

CREATE OR REPLACE VIEW taxonomy_full AS
SELECT 
    -- Base taxonomy identifiers
    t.id,
    t.species_name,
    
    -- Enriched common names (prefer enriched, fallback to base)
    COALESCE(tc.common_name, t.common_name) AS common_name,
    
    -- Complete taxonomic hierarchy (from enrichment)
    tc.kingdom,
    tc.phylum,
    tc.class,
    tc."order",
    tc.family,
    tc.genus,
    tc.rank,
    tc.rank_level,
    tc.iconic_taxon_name,
    
    -- Taxonomic status & names
    tc.scientific_name_authorship AS authority,
    tc.taxonomic_status,
    tc.accepted_name,
    
    -- WoRMS data
    tc.worms_aphia_id,
    tc.worms_url,
    tc.worms_lsid,
    tc.accepted_aphia_id,
    
    -- GBIF data
    tc.gbif_taxon_key,
    tc.gbif_scientific_name,
    tc.match_type AS gbif_match_type,
    tc.confidence AS gbif_confidence,
    
    -- Habitat flags
    tc.is_marine,
    tc.is_brackish,
    tc.is_freshwater,
    tc.is_terrestrial,
    tc.is_extinct,
    
    -- Conservation & distribution
    tc.conservation_status,
    tc.introduced,
    tc.endemic,
    tc.threatened,
    
    -- Media & documentation
    tc.photo_url,
    tc.photo_attribution,
    tc.wikipedia_url,
    tc.wikipedia_summary,
    
    -- Data provenance
    tc.data_source,
    tc.inaturalist_taxon_id,
    tc.last_updated AS cache_updated,
    
    -- Observation count (performance: use with WHERE if possible)
    (SELECT COUNT(*) 
     FROM species_observations 
     WHERE taxonomy_id = t.id) AS observation_count,
     
    -- Enrichment status flag
    CASE 
        WHEN tc.id IS NULL THEN 'not_enriched'
        WHEN tc.worms_aphia_id IS NOT NULL THEN 'worms_enriched'
        WHEN tc.gbif_taxon_key IS NOT NULL THEN 'gbif_enriched'
        WHEN tc.inaturalist_taxon_id IS NOT NULL THEN 'inaturalist_only'
        ELSE 'partially_enriched'
    END AS enrichment_source
    
FROM taxonomy t
LEFT JOIN taxonomy_cache tc ON t.id = tc.taxonomy_id;

COMMENT ON VIEW taxonomy_full IS 'Complete denormalized species data with all enrichment fields';

-- ============================================================================
-- 2. TAXONOMY SUMMARY VIEW - High-Level Overview
-- ============================================================================

CREATE OR REPLACE VIEW taxonomy_summary AS
SELECT 
    t.id,
    t.species_name,
    COALESCE(tc.common_name, t.common_name) AS common_name,
    tc.family,
    tc.phylum,
    tc.iconic_taxon_name,
    tc.is_marine,
    tc.conservation_status,
    
    -- Enrichment sources (boolean flags)
    (tc.worms_aphia_id IS NOT NULL) AS has_worms,
    (tc.gbif_taxon_key IS NOT NULL) AS has_gbif,
    (tc.inaturalist_taxon_id IS NOT NULL) AS has_inaturalist,
    
    -- Quality indicators
    tc.taxonomic_status,
    CASE 
        WHEN tc.taxonomic_status = 'accepted' THEN TRUE
        WHEN tc.taxonomic_status IS NULL THEN NULL
        ELSE FALSE
    END AS is_accepted_name,
    
    -- Media availability
    (tc.photo_url IS NOT NULL) AS has_photo,
    (tc.wikipedia_url IS NOT NULL) AS has_wikipedia,
    
    -- Observation stats
    (SELECT COUNT(*) FROM species_observations WHERE taxonomy_id = t.id) AS obs_count,
    (SELECT MIN(observation_date) FROM species_observations WHERE taxonomy_id = t.id) AS first_observed,
    (SELECT MAX(observation_date) FROM species_observations WHERE taxonomy_id = t.id) AS last_observed
    
FROM taxonomy t
LEFT JOIN taxonomy_cache tc ON t.id = tc.taxonomy_id;

COMMENT ON VIEW taxonomy_summary IS 'Lightweight species overview with enrichment flags and observation counts';

-- ============================================================================
-- 3. MARINE SPECIES VIEW - WoRMS Marine Species Only
-- ============================================================================

CREATE OR REPLACE VIEW marine_species AS
SELECT 
    t.id,
    t.species_name,
    tc.common_name,
    tc.worms_aphia_id,
    tc.worms_url,
    tc.family,
    tc.phylum,
    tc.class,
    tc."order",
    tc.taxonomic_status,
    tc.accepted_name,
    tc.scientific_name_authorship AS authority,
    tc.is_marine,
    tc.is_brackish,
    tc.conservation_status,
    tc.endemic,
    tc.photo_url,
    (SELECT COUNT(*) FROM species_observations WHERE taxonomy_id = t.id) AS obs_count
FROM taxonomy t
INNER JOIN taxonomy_cache tc ON t.id = tc.taxonomy_id
WHERE tc.is_marine = TRUE
  AND tc.worms_aphia_id IS NOT NULL
ORDER BY tc.phylum, tc.family, t.species_name;

COMMENT ON VIEW marine_species IS 'Marine species with WoRMS enrichment data';

-- ============================================================================
-- 4. SPECIES BY HABITAT VIEW
-- ============================================================================

CREATE OR REPLACE VIEW species_by_habitat AS
SELECT 
    t.id,
    t.species_name,
    tc.common_name,
    tc.family,
    tc.phylum,
    
    -- Habitat classification
    CASE 
        WHEN tc.is_marine AND NOT tc.is_brackish THEN 'marine_only'
        WHEN tc.is_marine AND tc.is_brackish THEN 'marine_estuarine'
        WHEN tc.is_brackish AND NOT tc.is_marine THEN 'estuarine_only'
        WHEN tc.is_freshwater AND NOT tc.is_marine THEN 'freshwater'
        WHEN tc.is_terrestrial THEN 'terrestrial'
        ELSE 'unknown'
    END AS primary_habitat,
    
    -- Habitat breadth (number of habitats)
    (
        COALESCE(tc.is_marine::int, 0) + 
        COALESCE(tc.is_brackish::int, 0) + 
        COALESCE(tc.is_freshwater::int, 0) + 
        COALESCE(tc.is_terrestrial::int, 0)
    ) AS habitat_count,
    
    tc.is_marine,
    tc.is_brackish,
    tc.is_freshwater,
    tc.is_terrestrial,
    
    -- Enrichment source
    CASE 
        WHEN tc.worms_aphia_id IS NOT NULL THEN 'worms'
        WHEN tc.gbif_taxon_key IS NOT NULL THEN 'gbif'
        ELSE 'unknown'
    END AS habitat_source,
    
    (SELECT COUNT(*) FROM species_observations WHERE taxonomy_id = t.id) AS obs_count
    
FROM taxonomy t
LEFT JOIN taxonomy_cache tc ON t.id = tc.taxonomy_id
WHERE tc.id IS NOT NULL;  -- Only enriched species

COMMENT ON VIEW species_by_habitat IS 'Species classified by habitat type with breadth metrics';

-- ============================================================================
-- 5. TAXONOMY QUALITY METRICS VIEW
-- ============================================================================

CREATE OR REPLACE VIEW taxonomy_quality_metrics AS
SELECT 
    t.id,
    t.species_name,
    tc.common_name,
    
    -- Quality score (0-100)
    (
        COALESCE((tc.worms_aphia_id IS NOT NULL)::int * 30, 0) +
        COALESCE((tc.gbif_taxon_key IS NOT NULL)::int * 20, 0) +
        COALESCE((tc.family IS NOT NULL)::int * 15, 0) +
        COALESCE((tc.genus IS NOT NULL)::int * 10, 0) +
        COALESCE((tc.taxonomic_status = 'accepted')::int * 15, 0) +
        COALESCE((tc.photo_url IS NOT NULL)::int * 5, 0) +
        COALESCE((tc.wikipedia_url IS NOT NULL)::int * 5, 0)
    ) AS quality_score,
    
    -- Completeness flags
    (tc.worms_aphia_id IS NOT NULL OR tc.gbif_taxon_key IS NOT NULL) AS has_external_id,
    (tc.kingdom IS NOT NULL AND tc.phylum IS NOT NULL AND tc.class IS NOT NULL) AS has_full_hierarchy,
    (tc.taxonomic_status IS NOT NULL) AS has_status,
    
    -- Issues
    ARRAY_REMOVE(ARRAY[
        CASE WHEN tc.taxonomic_status = 'synonym' THEN 'synonym' END,
        CASE WHEN tc.taxonomic_status IN ('invalid', 'nomen dubium') THEN 'invalid_name' END,
        CASE WHEN tc.gbif_confidence < 80 THEN 'low_confidence' END,
        CASE WHEN tc.gbif_match_type = 'HIGHERRANK' THEN 'imprecise_match' END,
        CASE WHEN t.species_name ILIKE '%unidentified%' THEN 'unidentified' END,
        CASE WHEN t.species_name ILIKE '%spp.%' THEN 'genus_only' END
    ], NULL) AS quality_issues,
    
    -- Review status from log
    (SELECT needs_manual_review 
     FROM taxonomy_enrichment_log 
     WHERE taxonomy_id = t.id 
     ORDER BY created_at DESC 
     LIMIT 1) AS needs_review,
     
    tc.last_updated AS cache_updated
    
FROM taxonomy t
LEFT JOIN taxonomy_cache tc ON t.id = tc.taxonomy_id;

COMMENT ON VIEW taxonomy_quality_metrics IS 'Data quality assessment with scoring and issue flagging';

-- ============================================================================
-- 6. SPECIES FOR DISPLAY VIEW - User-Friendly Format
-- ============================================================================

CREATE OR REPLACE VIEW species_for_display AS
SELECT 
    t.id,
    t.species_name AS scientific_name,
    COALESCE(tc.common_name, t.common_name, 'No common name') AS common_name,
    tc.family,
    tc.phylum,
    
    -- Formatted authority
    CASE 
        WHEN tc.scientific_name_authorship IS NOT NULL 
        THEN t.species_name || ' ' || tc.scientific_name_authorship
        ELSE t.species_name
    END AS full_scientific_name,
    
    -- Status badge
    CASE 
        WHEN tc.taxonomic_status = 'accepted' THEN '✓ Accepted'
        WHEN tc.taxonomic_status = 'synonym' THEN '⟳ Synonym of ' || tc.accepted_name
        WHEN tc.taxonomic_status IS NULL THEN '? Unknown'
        ELSE tc.taxonomic_status
    END AS status_display,
    
    -- Habitat badge
    CASE 
        WHEN tc.is_marine THEN '🌊 Marine'
        WHEN tc.is_freshwater THEN '💧 Freshwater'
        WHEN tc.is_terrestrial THEN '🌳 Terrestrial'
        ELSE ''
    END AS habitat_display,
    
    -- Conservation badge
    CASE 
        WHEN tc.threatened THEN '⚠️ Threatened'
        WHEN tc.endemic THEN '🇦🇺 Endemic'
        WHEN tc.introduced THEN '🚀 Introduced'
        ELSE ''
    END AS conservation_display,
    
    -- External links
    tc.worms_url,
    tc.wikipedia_url,
    tc.photo_url,
    
    -- Observation summary
    (SELECT COUNT(*) FROM species_observations WHERE taxonomy_id = t.id) AS total_observations,
    (SELECT COUNT(DISTINCT location_id) FROM species_observations WHERE taxonomy_id = t.id) AS locations_count,
    
    -- Data quality indicator
    CASE 
        WHEN tc.worms_aphia_id IS NOT NULL THEN 'High (WoRMS)'
        WHEN tc.gbif_taxon_key IS NOT NULL AND tc.gbif_confidence >= 95 THEN 'High (GBIF)'
        WHEN tc.gbif_taxon_key IS NOT NULL THEN 'Medium (GBIF)'
        WHEN tc.inaturalist_taxon_id IS NOT NULL THEN 'Medium (iNaturalist)'
        ELSE 'Low'
    END AS data_quality
    
FROM taxonomy t
LEFT JOIN taxonomy_cache tc ON t.id = tc.taxonomy_id;

COMMENT ON VIEW species_for_display IS 'User-friendly species display with badges and formatted text';

-- ============================================================================
-- 7. ENRICHMENT GAPS VIEW - Species Needing Attention
-- ============================================================================

CREATE OR REPLACE VIEW enrichment_gaps AS
SELECT 
    t.id,
    t.species_name,
    
    -- Gap analysis
    (tc.id IS NULL) AS not_enriched,
    (tc.worms_aphia_id IS NULL AND tc.gbif_taxon_key IS NULL) AS no_external_ids,
    (tc.family IS NULL) AS missing_family,
    (tc.phylum IS NULL) AS missing_phylum,
    (tc.common_name IS NULL) AS missing_common_name,
    
    -- Priority score (higher = more urgent)
    (
        COALESCE((SELECT COUNT(*) FROM species_observations WHERE taxonomy_id = t.id), 0) * 10 +
        COALESCE((tc.id IS NULL)::int * 50, 0) +
        COALESCE((tc.family IS NULL)::int * 20, 0)
    ) AS priority_score,
    
    -- Last enrichment attempt
    (SELECT created_at 
     FROM taxonomy_enrichment_log 
     WHERE taxonomy_id = t.id 
     ORDER BY created_at DESC 
     LIMIT 1) AS last_attempt,
    
    (SELECT COUNT(*) FROM species_observations WHERE taxonomy_id = t.id) AS obs_count
    
FROM taxonomy t
LEFT JOIN taxonomy_cache tc ON t.id = tc.taxonomy_id
WHERE 
    tc.id IS NULL  -- Not enriched
    OR tc.worms_aphia_id IS NULL AND tc.gbif_taxon_key IS NULL  -- No external IDs
    OR tc.family IS NULL  -- Missing key taxonomy
ORDER BY priority_score DESC;

COMMENT ON VIEW enrichment_gaps IS 'Species with missing or incomplete enrichment data, prioritized by observation count';

-- ============================================================================
-- 8. WORMS VS GBIF COMPARISON VIEW
-- ============================================================================

CREATE OR REPLACE VIEW worms_gbif_comparison AS
SELECT 
    t.id,
    t.species_name,
    
    -- WoRMS data
    tc.worms_aphia_id,
    tc.worms_url,
    tc.taxonomic_status AS worms_status,
    tc.is_marine AS worms_marine,
    
    -- GBIF data
    tc.gbif_taxon_key,
    tc.gbif_scientific_name,
    tc.match_type AS gbif_match_type,
    tc.confidence AS gbif_confidence,
    
    -- Comparison flags
    (tc.worms_aphia_id IS NOT NULL AND tc.gbif_taxon_key IS NOT NULL) AS in_both,
    (tc.worms_aphia_id IS NOT NULL AND tc.gbif_taxon_key IS NULL) AS worms_only,
    (tc.worms_aphia_id IS NULL AND tc.gbif_taxon_key IS NOT NULL) AS gbif_only,
    
    -- Classification agreement
    CASE 
        WHEN tc.worms_aphia_id IS NOT NULL AND tc.gbif_taxon_key IS NOT NULL 
        THEN tc.family  -- Both sources should agree on family
        ELSE NULL
    END AS family_check,
    
    tc.data_source AS primary_source
    
FROM taxonomy t
INNER JOIN taxonomy_cache tc ON t.id = tc.taxonomy_id
WHERE tc.worms_aphia_id IS NOT NULL OR tc.gbif_taxon_key IS NOT NULL;

COMMENT ON VIEW worms_gbif_comparison IS 'Side-by-side comparison of WoRMS and GBIF enrichment data';

-- ============================================================================
-- UTILITY FUNCTIONS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- FUNCTION: Get complete species details
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION get_species_details(species_name_param TEXT)
RETURNS TABLE (
    id INTEGER,
    species_name TEXT,
    common_name TEXT,
    full_classification JSONB,
    external_ids JSONB,
    habitat_info JSONB,
    conservation_info JSONB,
    media_urls JSONB,
    observation_stats JSONB
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        t.id,
        t.species_name,
        COALESCE(tc.common_name, t.common_name),
        
        -- Full taxonomic hierarchy as JSON
        jsonb_build_object(
            'kingdom', tc.kingdom,
            'phylum', tc.phylum,
            'class', tc.class,
            'order', tc."order",
            'family', tc.family,
            'genus', tc.genus,
            'rank', tc.rank,
            'authority', tc.scientific_name_authorship,
            'taxonomic_status', tc.taxonomic_status
        ),
        
        -- External database IDs
        jsonb_build_object(
            'worms_aphia_id', tc.worms_aphia_id,
            'worms_url', tc.worms_url,
            'gbif_taxon_key', tc.gbif_taxon_key,
            'inaturalist_taxon_id', tc.inaturalist_taxon_id
        ),
        
        -- Habitat flags
        jsonb_build_object(
            'is_marine', tc.is_marine,
            'is_brackish', tc.is_brackish,
            'is_freshwater', tc.is_freshwater,
            'is_terrestrial', tc.is_terrestrial
        ),
        
        -- Conservation status
        jsonb_build_object(
            'conservation_status', tc.conservation_status,
            'threatened', tc.threatened,
            'endemic', tc.endemic,
            'introduced', tc.introduced,
            'is_extinct', tc.is_extinct
        ),
        
        -- Media URLs
        jsonb_build_object(
            'photo_url', tc.photo_url,
            'photo_attribution', tc.photo_attribution,
            'wikipedia_url', tc.wikipedia_url
        ),
        
        -- Observation statistics
        jsonb_build_object(
            'total_observations', (SELECT COUNT(*) FROM species_observations WHERE taxonomy_id = t.id),
            'distinct_locations', (SELECT COUNT(DISTINCT location_id) FROM species_observations WHERE taxonomy_id = t.id),
            'first_observed', (SELECT MIN(observation_date) FROM species_observations WHERE taxonomy_id = t.id),
            'last_observed', (SELECT MAX(observation_date) FROM species_observations WHERE taxonomy_id = t.id)
        )
        
    FROM taxonomy t
    LEFT JOIN taxonomy_cache tc ON t.id = tc.taxonomy_id
    WHERE t.species_name = species_name_param;
END;
$$;

COMMENT ON FUNCTION get_species_details IS 'Get complete species profile as structured JSON';

-- ----------------------------------------------------------------------------
-- FUNCTION: Search species (fuzzy matching)
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION search_species(search_query TEXT, limit_count INTEGER DEFAULT 20)
RETURNS TABLE (
    id INTEGER,
    species_name TEXT,
    common_name TEXT,
    family TEXT,
    match_type TEXT,
    similarity_score REAL
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        t.id,
        t.species_name,
        COALESCE(tc.common_name, t.common_name),
        tc.family,
        CASE 
            WHEN t.species_name ILIKE search_query THEN 'exact'
            WHEN t.species_name ILIKE search_query || '%' THEN 'starts_with'
            WHEN t.species_name ILIKE '%' || search_query || '%' THEN 'contains'
            WHEN COALESCE(tc.common_name, t.common_name) ILIKE '%' || search_query || '%' THEN 'common_name'
            ELSE 'fuzzy'
        END,
        similarity(t.species_name, search_query)
    FROM taxonomy t
    LEFT JOIN taxonomy_cache tc ON t.id = tc.taxonomy_id
    WHERE 
        t.species_name ILIKE '%' || search_query || '%'
        OR COALESCE(tc.common_name, t.common_name) ILIKE '%' || search_query || '%'
        OR tc.family ILIKE '%' || search_query || '%'
    ORDER BY 
        similarity(t.species_name, search_query) DESC,
        t.species_name
    LIMIT limit_count;
END;
$$;

COMMENT ON FUNCTION search_species IS 'Fuzzy search species by scientific name, common name, or family';

-- ----------------------------------------------------------------------------
-- FUNCTION: Get species by family
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION get_species_by_family(family_param TEXT)
RETURNS TABLE (
    id INTEGER,
    species_name TEXT,
    common_name TEXT,
    genus TEXT,
    is_marine BOOLEAN,
    observation_count BIGINT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        t.id,
        t.species_name,
        COALESCE(tc.common_name, t.common_name),
        tc.genus,
        tc.is_marine,
        (SELECT COUNT(*) FROM species_observations WHERE taxonomy_id = t.id)
    FROM taxonomy t
    INNER JOIN taxonomy_cache tc ON t.id = tc.taxonomy_id
    WHERE tc.family = family_param
    ORDER BY tc.genus, t.species_name;
END;
$$;

COMMENT ON FUNCTION get_species_by_family IS 'Get all species in a taxonomic family with observation counts';

-- ----------------------------------------------------------------------------
-- FUNCTION: Get habitat summary statistics
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION get_habitat_summary()
RETURNS TABLE (
    habitat TEXT,
    species_count BIGINT,
    observation_count BIGINT,
    worms_enriched BIGINT,
    gbif_enriched BIGINT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    WITH habitat_data AS (
        SELECT 
            CASE 
                WHEN tc.is_marine AND NOT COALESCE(tc.is_brackish, FALSE) THEN 'Marine'
                WHEN tc.is_marine AND tc.is_brackish THEN 'Marine/Estuarine'
                WHEN tc.is_brackish THEN 'Estuarine'
                WHEN tc.is_freshwater THEN 'Freshwater'
                WHEN tc.is_terrestrial THEN 'Terrestrial'
                ELSE 'Unknown'
            END AS habitat_type,
            t.id,
            tc.worms_aphia_id,
            tc.gbif_taxon_key
        FROM taxonomy t
        LEFT JOIN taxonomy_cache tc ON t.id = tc.taxonomy_id
    )
    SELECT 
        hd.habitat_type,
        COUNT(DISTINCT hd.id),
        COALESCE(SUM((SELECT COUNT(*) FROM species_observations WHERE taxonomy_id = hd.id)), 0),
        SUM((hd.worms_aphia_id IS NOT NULL)::int),
        SUM((hd.gbif_taxon_key IS NOT NULL)::int)
    FROM habitat_data hd
    GROUP BY hd.habitat_type
    ORDER BY COUNT(DISTINCT hd.id) DESC;
END;
$$;

COMMENT ON FUNCTION get_habitat_summary IS 'Summary statistics of species counts by habitat type';

-- ----------------------------------------------------------------------------
-- FUNCTION: Flag species for manual review
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION flag_species_for_review(
    taxonomy_id_param INTEGER,
    review_reason_param TEXT,
    reviewed_by_param TEXT DEFAULT NULL
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    -- Update most recent enrichment log entry
    UPDATE taxonomy_enrichment_log
    SET 
        needs_manual_review = TRUE,
        review_reason = review_reason_param,
        reviewed_by = reviewed_by_param,
        reviewed_at = NOW()
    WHERE taxonomy_id = taxonomy_id_param
      AND created_at = (
          SELECT MAX(created_at) 
          FROM taxonomy_enrichment_log 
          WHERE taxonomy_id = taxonomy_id_param
      );
      
    -- If no log entry exists, create one
    IF NOT FOUND THEN
        INSERT INTO taxonomy_enrichment_log (
            taxonomy_id,
            species_name,
            needs_manual_review,
            review_reason,
            reviewed_by,
            reviewed_at
        )
        SELECT 
            id,
            species_name,
            TRUE,
            review_reason_param,
            reviewed_by_param,
            NOW()
        FROM taxonomy
        WHERE id = taxonomy_id_param;
    END IF;
END;
$$;

COMMENT ON FUNCTION flag_species_for_review IS 'Mark species for manual review with reason';

-- ============================================================================
-- EXAMPLE QUERIES
-- ============================================================================

/*
-- Get complete profile for a species
SELECT * FROM get_species_details('Ecklonia radiata');

-- Search for kelp species
SELECT * FROM search_species('kelp');

-- All species in the kelp family
SELECT * FROM get_species_by_family('Lessoniaceae');

-- Habitat breakdown
SELECT * FROM get_habitat_summary();

-- Marine species with photos
SELECT species_name, common_name, photo_url, obs_count
FROM marine_species
WHERE photo_url IS NOT NULL
ORDER BY obs_count DESC
LIMIT 20;

-- Species needing review (high priority)
SELECT species_name, obs_count, last_attempt
FROM enrichment_gaps
WHERE priority_score > 100
ORDER BY priority_score DESC;

-- Data quality dashboard
SELECT 
    CASE 
        WHEN quality_score >= 80 THEN 'High'
        WHEN quality_score >= 50 THEN 'Medium'
        ELSE 'Low'
    END AS quality_tier,
    COUNT(*) as species_count
FROM taxonomy_quality_metrics
GROUP BY quality_tier;

-- Flag a species for review
SELECT flag_species_for_review(42, 'Ambiguous taxonomy - multiple matches', 'researcher_1');
*/

-- ============================================================================
-- INDEXES FOR VIEW PERFORMANCE
-- ============================================================================

-- Already created in init.sql, but listed here for reference:
-- CREATE INDEX IF NOT EXISTS idx_taxonomy_cache_taxonomy_id ON taxonomy_cache(taxonomy_id);
-- CREATE INDEX IF NOT EXISTS idx_taxonomy_cache_worms_id ON taxonomy_cache(worms_aphia_id) WHERE worms_aphia_id IS NOT NULL;
-- CREATE INDEX IF NOT EXISTS idx_taxonomy_cache_gbif_key ON taxonomy_cache(gbif_taxon_key) WHERE gbif_taxon_key IS NOT NULL;
-- CREATE INDEX IF NOT EXISTS idx_taxonomy_cache_marine ON taxonomy_cache(is_marine) WHERE is_marine = TRUE;
-- CREATE INDEX IF NOT EXISTS idx_species_obs_taxonomy ON species_observations(taxonomy_id);

-- ============================================================================
-- END OF TAXONOMY VIEWS & FUNCTIONS
-- ============================================================================
