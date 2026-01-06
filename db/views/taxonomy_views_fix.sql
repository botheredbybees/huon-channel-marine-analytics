-- ============================================================================
-- TAXONOMY VIEWS HOTFIX
-- ============================================================================
-- Fixes column name mismatches and type errors from initial installation
-- Run this after apply_taxonomy_views.sh if you encounter errors
-- ============================================================================

-- Drop broken views first
DROP VIEW IF EXISTS taxonomy_quality_metrics CASCADE;
DROP VIEW IF EXISTS species_for_display CASCADE;
DROP FUNCTION IF EXISTS get_habitat_summary();

-- ============================================================================
-- 5. TAXONOMY QUALITY METRICS VIEW - FIXED
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
    
    -- Issues (FIXED: confidence not gbif_confidence)
    ARRAY_REMOVE(ARRAY[
        CASE WHEN tc.taxonomic_status = 'synonym' THEN 'synonym' END,
        CASE WHEN tc.taxonomic_status IN ('invalid', 'nomen dubium') THEN 'invalid_name' END,
        CASE WHEN tc.confidence < 80 THEN 'low_confidence' END,
        CASE WHEN tc.match_type = 'HIGHERRANK' THEN 'imprecise_match' END,
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
-- 6. SPECIES FOR DISPLAY VIEW - FIXED
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
    
    -- Data quality indicator (FIXED: confidence not gbif_confidence)
    CASE 
        WHEN tc.worms_aphia_id IS NOT NULL THEN 'High (WoRMS)'
        WHEN tc.gbif_taxon_key IS NOT NULL AND tc.confidence >= 95 THEN 'High (GBIF)'
        WHEN tc.gbif_taxon_key IS NOT NULL THEN 'Medium (GBIF)'
        WHEN tc.inaturalist_taxon_id IS NOT NULL THEN 'Medium (iNaturalist)'
        ELSE 'Low'
    END AS data_quality
    
FROM taxonomy t
LEFT JOIN taxonomy_cache tc ON t.id = tc.taxonomy_id;

COMMENT ON VIEW species_for_display IS 'User-friendly species display with badges and formatted text';

-- ============================================================================
-- FUNCTION: Get habitat summary statistics - FIXED
-- ============================================================================

CREATE OR REPLACE FUNCTION get_habitat_summary()
RETURNS TABLE (
    habitat TEXT,
    species_count BIGINT,
    observation_count NUMERIC,  -- FIXED: was BIGINT, should be NUMERIC
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

-- ============================================================================
-- VALIDATION
-- ============================================================================

-- Test queries
SELECT 'Testing taxonomy_quality_metrics...' AS test;
SELECT COUNT(*) as row_count FROM taxonomy_quality_metrics;

SELECT 'Testing species_for_display...' AS test;
SELECT COUNT(*) as row_count FROM species_for_display;

SELECT 'Testing get_habitat_summary()...' AS test;
SELECT * FROM get_habitat_summary();

SELECT '✓ All views and functions fixed!' AS status;
