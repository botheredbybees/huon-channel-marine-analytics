[Previous content from database_schema.md up to '### Example Enrichment Queries', then add this new section after the SQL block ending with 'GROUP BY api_endpoint;']

---

## 6. Taxonomy Query Layer (Views & Functions) 🆕

Pre-built views and functions that simplify querying enriched taxonomy data. These eliminate the need for complex JOINs and provide convenient access patterns for common tasks.

### Installation

```bash
# Apply all views and functions
./db/apply_taxonomy_views.sh

# Or apply SQL directly
psql -h localhost -p 5433 -U marine_user -d marine_db -f db/views/taxonomy_views.sql

# If column name errors occur, apply hotfix
psql -h localhost -p 5433 -U marine_user -d marine_db -f db/views/taxonomy_views_fix.sql
```

### Views

#### 1. `taxonomy_full` - Complete Denormalized Data

**Purpose:** All enrichment fields in one place for API responses, exports, and comprehensive queries.

```sql
CREATE OR REPLACE VIEW taxonomy_full AS
SELECT 
    t.id,
    t.species_name,
    COALESCE(tc.common_name, t.common_name) AS common_name,
    
    -- Taxonomic hierarchy
    tc.genus,
    tc.family,
    tc."order",
    tc.class,
    tc.phylum,
    tc.kingdom,
    tc.rank,
    tc.iconic_taxon_name,
    
    -- Classification metadata
    tc.scientific_name_authorship AS authority,
    tc.taxonomic_status,
    tc.accepted_name,
    
    -- External IDs
    tc.worms_aphia_id,
    tc.worms_url,
    tc.gbif_taxon_key,
    tc.inaturalist_taxon_id,
    
    -- Habitat flags
    tc.is_marine,
    tc.is_brackish,
    tc.is_freshwater,
    tc.is_terrestrial,
    
    -- Conservation
    tc.conservation_status,
    tc.introduced,
    tc.endemic,
    tc.threatened,
    tc.is_extinct,
    
    -- Media
    tc.photo_url,
    tc.wikipedia_url,
    
    -- GBIF match quality
    tc.match_type,
    tc.confidence AS gbif_confidence,
    
    -- Enrichment source (computed)
    CASE 
        WHEN tc.worms_aphia_id IS NOT NULL THEN 'worms_enriched'
        WHEN tc.gbif_taxon_key IS NOT NULL THEN 'gbif_enriched'
        WHEN tc.inaturalist_taxon_id IS NOT NULL THEN 'inaturalist_enriched'
        ELSE 'not_enriched'
    END AS enrichment_source,
    
    -- Observation count (cached)
    (SELECT COUNT(*) FROM species_observations WHERE taxonomy_id = t.id) AS observation_count,
    
    -- Metadata timestamps
    tc.last_updated AS cache_updated,
    tc.created_at AS cache_created
FROM taxonomy t
LEFT JOIN taxonomy_cache tc ON t.id = tc.taxonomy_id;

COMMENT ON VIEW taxonomy_full IS 'Complete denormalized taxonomy data with all enrichment fields';
```

**Usage:**
```sql
-- Get Ecklonia radiata with all details
SELECT * FROM taxonomy_full WHERE species_name = 'Ecklonia radiata';

-- Export all marine species
COPY (SELECT * FROM taxonomy_full WHERE is_marine = TRUE) 
TO '/tmp/marine_species.csv' CSV HEADER;
```

#### 2. `taxonomy_summary` - Lightweight Overview

**Purpose:** Quick lookups and species lists for dropdowns.

```sql
CREATE OR REPLACE VIEW taxonomy_summary AS
SELECT 
    t.id,
    t.species_name,
    tc.common_name,
    tc.family,
    tc.phylum,
    tc.rank,
    
    -- Boolean flags
    (tc.worms_aphia_id IS NOT NULL) AS has_worms,
    (tc.gbif_taxon_key IS NOT NULL) AS has_gbif,
    (tc.inaturalist_taxon_id IS NOT NULL) AS has_inaturalist,
    (tc.photo_url IS NOT NULL) AS has_photo,
    (tc.wikipedia_url IS NOT NULL) AS has_wikipedia,
    (tc.taxonomic_status = 'accepted') AS is_accepted_name,
    
    -- Observation count
    (SELECT COUNT(*) FROM species_observations WHERE taxonomy_id = t.id) AS obs_count
FROM taxonomy t
LEFT JOIN taxonomy_cache tc ON t.id = tc.taxonomy_id;

COMMENT ON VIEW taxonomy_summary IS 'Lightweight species overview with enrichment flags';
```

**Usage:**
```sql
-- Species list for dropdown
SELECT species_name, common_name, family, obs_count
FROM taxonomy_summary
WHERE obs_count > 10
ORDER BY obs_count DESC;

-- Find species with photos
SELECT species_name, family
FROM taxonomy_summary
WHERE has_photo = TRUE;
```

#### 3. `marine_species` - WoRMS Marine Species Only

**Purpose:** Filter to verified marine species for marine ecology studies.

```sql
CREATE OR REPLACE VIEW marine_species AS
SELECT 
    t.id,
    t.species_name,
    tc.common_name,
    tc.family,
    tc.phylum,
    tc.worms_aphia_id,
    tc.worms_url,
    tc.taxonomic_status,
    tc.is_brackish,
    tc.photo_url,
    (SELECT COUNT(*) FROM species_observations WHERE taxonomy_id = t.id) AS obs_count
FROM taxonomy t
JOIN taxonomy_cache tc ON t.id = tc.taxonomy_id
WHERE tc.is_marine = TRUE
  AND tc.worms_aphia_id IS NOT NULL;

COMMENT ON VIEW marine_species IS 'Verified marine species with WoRMS data only';
```

**Usage:**
```sql
-- Top marine species by observations
SELECT species_name, family, obs_count
FROM marine_species
ORDER BY obs_count DESC
LIMIT 20;

-- Marine families
SELECT family, COUNT(*) as species_count
FROM marine_species
GROUP BY family
ORDER BY species_count DESC;
```

#### 4. `species_by_habitat` - Habitat Classification

**Purpose:** Group species by ecosystem type for ecological niche analysis.

```sql
CREATE OR REPLACE VIEW species_by_habitat AS
SELECT 
    t.id,
    t.species_name,
    tc.family,
    
    -- Primary habitat (most specific)
    CASE 
        WHEN tc.is_marine AND NOT COALESCE(tc.is_brackish, FALSE) AND NOT COALESCE(tc.is_freshwater, FALSE) THEN 'marine_only'
        WHEN tc.is_marine AND tc.is_brackish THEN 'marine_estuarine'
        WHEN tc.is_brackish AND NOT COALESCE(tc.is_marine, FALSE) THEN 'estuarine_only'
        WHEN tc.is_freshwater THEN 'freshwater'
        WHEN tc.is_terrestrial THEN 'terrestrial'
        ELSE 'unknown'
    END AS primary_habitat,
    
    -- Habitat breadth (versatility metric)
    (
        COALESCE(tc.is_marine::int, 0) +
        COALESCE(tc.is_brackish::int, 0) +
        COALESCE(tc.is_freshwater::int, 0) +
        COALESCE(tc.is_terrestrial::int, 0)
    ) AS habitat_count,
    
    -- Individual flags
    tc.is_marine,
    tc.is_brackish,
    tc.is_freshwater,
    tc.is_terrestrial,
    
    (SELECT COUNT(*) FROM species_observations WHERE taxonomy_id = t.id) AS obs_count
FROM taxonomy t
LEFT JOIN taxonomy_cache tc ON t.id = tc.taxonomy_id;

COMMENT ON VIEW species_by_habitat IS 'Species classified by primary habitat type';
```

**Usage:**
```sql
-- Habitat distribution
SELECT primary_habitat, COUNT(*) as species_count
FROM species_by_habitat
GROUP BY primary_habitat;

-- Versatile species (3+ habitats)
SELECT species_name, habitat_count, family
FROM species_by_habitat
WHERE habitat_count >= 3
ORDER BY habitat_count DESC;
```

#### 5. `taxonomy_quality_metrics` - Data Quality Dashboard

**Purpose:** Assess enrichment completeness and accuracy for QC workflows.

```sql
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
    
    -- Quality issues array
    ARRAY_REMOVE(ARRAY[
        CASE WHEN tc.taxonomic_status = 'synonym' THEN 'synonym' END,
        CASE WHEN tc.taxonomic_status IN ('invalid', 'nomen dubium') THEN 'invalid_name' END,
        CASE WHEN tc.confidence < 80 THEN 'low_confidence' END,
        CASE WHEN tc.match_type = 'HIGHERRANK' THEN 'imprecise_match' END,
        CASE WHEN t.species_name ILIKE '%unidentified%' THEN 'unidentified' END,
        CASE WHEN t.species_name ILIKE '%spp.%' THEN 'genus_only' END
    ], NULL) AS quality_issues,
    
    -- Review flag from log
    (SELECT needs_manual_review 
     FROM taxonomy_enrichment_log 
     WHERE taxonomy_id = t.id 
     ORDER BY created_at DESC 
     LIMIT 1) AS needs_review,
     
    tc.last_updated AS cache_updated
FROM taxonomy t
LEFT JOIN taxonomy_cache tc ON t.id = tc.taxonomy_id;

COMMENT ON VIEW taxonomy_quality_metrics IS 'Data quality assessment with scoring and issue flagging';
```

**Usage:**
```sql
-- Quality distribution
SELECT 
    CASE 
        WHEN quality_score >= 80 THEN 'High'
        WHEN quality_score >= 50 THEN 'Medium'
        ELSE 'Low'
    END AS tier,
    COUNT(*) as count
FROM taxonomy_quality_metrics
GROUP BY tier;

-- Species with issues
SELECT species_name, quality_score, quality_issues
FROM taxonomy_quality_metrics
WHERE array_length(quality_issues, 1) > 0
ORDER BY quality_score;
```

#### 6. `species_for_display` - User-Friendly Format

**Purpose:** Formatted text for web displays and reports with Unicode badges.

```sql
CREATE OR REPLACE VIEW species_for_display AS
SELECT 
    t.id,
    t.species_name AS scientific_name,
    COALESCE(tc.common_name, t.common_name, 'No common name') AS common_name,
    tc.family,
    
    -- Status badge
    CASE 
        WHEN tc.taxonomic_status = 'accepted' THEN '✓ Accepted'
        WHEN tc.taxonomic_status = 'synonym' THEN '⟳ Synonym of ' || tc.accepted_name
        ELSE '? Unknown'
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
    
    tc.photo_url,
    tc.wikipedia_url,
    
    (SELECT COUNT(*) FROM species_observations WHERE taxonomy_id = t.id) AS total_observations,
    
    -- Data quality indicator
    CASE 
        WHEN tc.worms_aphia_id IS NOT NULL THEN 'High (WoRMS)'
        WHEN tc.gbif_taxon_key IS NOT NULL AND tc.confidence >= 95 THEN 'High (GBIF)'
        WHEN tc.gbif_taxon_key IS NOT NULL THEN 'Medium (GBIF)'
        ELSE 'Low'
    END AS data_quality
FROM taxonomy t
LEFT JOIN taxonomy_cache tc ON t.id = tc.taxonomy_id;

COMMENT ON VIEW species_for_display IS 'User-friendly species display with badges and formatted text';
```

**Usage:**
```sql
-- Species list for website
SELECT 
    scientific_name,
    common_name,
    status_display,
    habitat_display,
    conservation_display,
    data_quality
FROM species_for_display
WHERE total_observations > 5
ORDER BY total_observations DESC;
```

#### 7. `enrichment_gaps` - Species Needing Attention

**Purpose:** Identify and prioritize enrichment work for data stewardship.

```sql
CREATE OR REPLACE VIEW enrichment_gaps AS
SELECT 
    t.id,
    t.species_name,
    
    -- Priority score (higher = more important)
    (
        COALESCE((SELECT COUNT(*) FROM species_observations WHERE taxonomy_id = t.id), 0) * 10 +
        CASE WHEN tc.id IS NULL THEN 50 ELSE 0 END +
        CASE WHEN tc.family IS NULL THEN 20 ELSE 0 END
    ) AS priority_score,
    
    (SELECT COUNT(*) FROM species_observations WHERE taxonomy_id = t.id) AS obs_count,
    
    CASE 
        WHEN tc.id IS NULL THEN 'not_enriched'
        WHEN tc.family IS NULL THEN 'missing_family'
        WHEN tc.taxonomic_status IS NULL THEN 'missing_status'
        ELSE 'partial_data'
    END AS gap_type,
    
    (SELECT MAX(created_at) FROM taxonomy_enrichment_log WHERE taxonomy_id = t.id) AS last_attempt
FROM taxonomy t
LEFT JOIN taxonomy_cache tc ON t.id = tc.taxonomy_id
WHERE tc.id IS NULL OR tc.family IS NULL OR tc.taxonomic_status IS NULL
ORDER BY priority_score DESC;

COMMENT ON VIEW enrichment_gaps IS 'Species needing enrichment work, prioritized by observation count';
```

**Usage:**
```sql
-- Top 20 priorities
SELECT species_name, priority_score, gap_type, obs_count
FROM enrichment_gaps
LIMIT 20;

-- Species never attempted
SELECT species_name, obs_count
FROM enrichment_gaps
WHERE last_attempt IS NULL;
```

#### 8. `worms_gbif_comparison` - API Source Comparison

**Purpose:** Compare WoRMS vs GBIF data for validation.

```sql
CREATE OR REPLACE VIEW worms_gbif_comparison AS
SELECT 
    t.species_name,
    (tc.worms_aphia_id IS NOT NULL) AS in_worms,
    (tc.gbif_taxon_key IS NOT NULL) AS in_gbif,
    (tc.worms_aphia_id IS NOT NULL AND tc.gbif_taxon_key IS NOT NULL) AS in_both,
    (tc.worms_aphia_id IS NOT NULL AND tc.gbif_taxon_key IS NULL) AS worms_only,
    (tc.worms_aphia_id IS NULL AND tc.gbif_taxon_key IS NOT NULL) AS gbif_only,
    tc.worms_aphia_id,
    tc.gbif_taxon_key,
    tc.family,
    tc.confidence AS gbif_confidence
FROM taxonomy t
JOIN taxonomy_cache tc ON t.id = tc.taxonomy_id;

COMMENT ON VIEW worms_gbif_comparison IS 'Comparison of WoRMS vs GBIF coverage';
```

**Usage:**
```sql
-- Coverage summary
SELECT 
    COUNT(*) FILTER (WHERE in_worms) as worms_count,
    COUNT(*) FILTER (WHERE in_gbif) as gbif_count,
    COUNT(*) FILTER (WHERE in_both) as both_count
FROM worms_gbif_comparison;

-- Species in both databases
SELECT species_name, family, gbif_confidence
FROM worms_gbif_comparison
WHERE in_both = TRUE;
```

### Functions

#### 1. `get_species_details(species_name TEXT)`

**Returns:** Complete species profile as structured JSONB.

```sql
CREATE OR REPLACE FUNCTION get_species_details(p_species_name TEXT)
RETURNS TABLE (
    id INTEGER,
    species_name TEXT,
    common_name TEXT,
    full_classification JSONB,
    external_ids JSONB,
    habitat_info JSONB,
    conservation_info JSONB,
    media JSONB,
    observation_stats JSONB
)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT 
        t.id,
        t.species_name,
        tc.common_name,
        
        -- Classification as JSON
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
        ) AS full_classification,
        
        -- External IDs as JSON
        jsonb_build_object(
            'worms_aphia_id', tc.worms_aphia_id,
            'worms_url', tc.worms_url,
            'gbif_taxon_key', tc.gbif_taxon_key,
            'inaturalist_taxon_id', tc.inaturalist_taxon_id
        ) AS external_ids,
        
        -- Habitat as JSON
        jsonb_build_object(
            'is_marine', tc.is_marine,
            'is_brackish', tc.is_brackish,
            'is_freshwater', tc.is_freshwater,
            'is_terrestrial', tc.is_terrestrial
        ) AS habitat_info,
        
        -- Conservation as JSON
        jsonb_build_object(
            'conservation_status', tc.conservation_status,
            'threatened', tc.threatened,
            'endemic', tc.endemic,
            'introduced', tc.introduced,
            'is_extinct', tc.is_extinct
        ) AS conservation_info,
        
        -- Media as JSON
        jsonb_build_object(
            'photo_url', tc.photo_url,
            'photo_attribution', tc.photo_attribution,
            'wikipedia_url', tc.wikipedia_url
        ) AS media,
        
        -- Observations as JSON
        jsonb_build_object(
            'total_observations', (SELECT COUNT(*) FROM species_observations WHERE taxonomy_id = t.id),
            'distinct_locations', (SELECT COUNT(DISTINCT location_id) FROM species_observations WHERE taxonomy_id = t.id),
            'first_observed', (SELECT MIN(observation_date) FROM species_observations WHERE taxonomy_id = t.id),
            'last_observed', (SELECT MAX(observation_date) FROM species_observations WHERE taxonomy_id = t.id)
        ) AS observation_stats
    FROM taxonomy t
    LEFT JOIN taxonomy_cache tc ON t.id = tc.taxonomy_id
    WHERE t.species_name = p_species_name;
END;
$$;

COMMENT ON FUNCTION get_species_details IS 'Complete species profile as structured JSON';
```

**Usage:**
```sql
SELECT * FROM get_species_details('Ecklonia radiata');
```

#### 2. `search_species(query TEXT, limit INT)`

**Purpose:** Fuzzy species search with similarity ranking.

```sql
CREATE OR REPLACE FUNCTION search_species(p_query TEXT, p_limit INT DEFAULT 20)
RETURNS TABLE (
    id INTEGER,
    species_name TEXT,
    common_name TEXT,
    family TEXT,
    match_type TEXT,
    similarity FLOAT
)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT 
        t.id,
        t.species_name,
        tc.common_name,
        tc.family,
        CASE 
            WHEN t.species_name = p_query THEN 'exact'
            WHEN t.species_name ILIKE p_query || '%' THEN 'starts_with'
            WHEN t.species_name ILIKE '%' || p_query || '%' THEN 'contains'
            WHEN tc.common_name ILIKE '%' || p_query || '%' THEN 'common_name'
            ELSE 'fuzzy'
        END AS match_type,
        similarity(t.species_name, p_query) AS similarity
    FROM taxonomy t
    LEFT JOIN taxonomy_cache tc ON t.id = tc.taxonomy_id
    WHERE t.species_name ILIKE '%' || p_query || '%'
       OR tc.common_name ILIKE '%' || p_query || '%'
       OR tc.family ILIKE '%' || p_query || '%'
    ORDER BY match_type, similarity DESC
    LIMIT p_limit;
END;
$$;

COMMENT ON FUNCTION search_species IS 'Fuzzy species search with similarity ranking';
```

**Usage:**
```sql
SELECT * FROM search_species('kelp', 10);
SELECT * FROM search_species('Ecklonia');
```

#### 3. `get_species_by_family(family_name TEXT)`

**Purpose:** List all species in a taxonomic family.

```sql
CREATE OR REPLACE FUNCTION get_species_by_family(p_family TEXT)
RETURNS TABLE (
    id INTEGER,
    species_name TEXT,
    common_name TEXT,
    genus TEXT,
    is_marine BOOLEAN,
    obs_count BIGINT
)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT 
        t.id,
        t.species_name,
        tc.common_name,
        tc.genus,
        tc.is_marine,
        (SELECT COUNT(*) FROM species_observations WHERE taxonomy_id = t.id) AS obs_count
    FROM taxonomy t
    JOIN taxonomy_cache tc ON t.id = tc.taxonomy_id
    WHERE tc.family = p_family
    ORDER BY obs_count DESC;
END;
$$;

COMMENT ON FUNCTION get_species_by_family IS 'List all species in a taxonomic family';
```

**Usage:**
```sql
SELECT * FROM get_species_by_family('Lessoniaceae');
```

#### 4. `get_habitat_summary()`

**Purpose:** Ecosystem-level statistics.

```sql
CREATE OR REPLACE FUNCTION get_habitat_summary()
RETURNS TABLE (
    habitat TEXT,
    species_count BIGINT,
    observation_count NUMERIC,
    worms_enriched BIGINT,
    gbif_enriched BIGINT
)
LANGUAGE plpgsql AS $$
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

COMMENT ON FUNCTION get_habitat_summary IS 'Summary statistics by habitat type';
```

**Usage:**
```sql
SELECT * FROM get_habitat_summary();
```

#### 5. `flag_species_for_review(taxonomy_id INT, reason TEXT, reviewer TEXT)`

**Purpose:** Mark species for manual quality control.

```sql
CREATE OR REPLACE FUNCTION flag_species_for_review(
    p_taxonomy_id INTEGER,
    p_reason TEXT,
    p_reviewer TEXT
)
RETURNS VOID
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE taxonomy_enrichment_log
    SET 
        needs_manual_review = TRUE,
        review_reason = p_reason,
        reviewed_by = p_reviewer,
        reviewed_at = NOW()
    WHERE taxonomy_id = p_taxonomy_id
      AND id = (SELECT id FROM taxonomy_enrichment_log WHERE taxonomy_id = p_taxonomy_id ORDER BY created_at DESC LIMIT 1);
    
    IF NOT FOUND THEN
        RAISE NOTICE 'No enrichment log found for taxonomy_id=%', p_taxonomy_id;
    END IF;
END;
$$;

COMMENT ON FUNCTION flag_species_for_review IS 'Mark species for manual quality control review';
```

**Usage:**
```sql
-- Flag a species
SELECT flag_species_for_review(42, 'Multiple GBIF matches - needs taxonomist review', 'peter.shanks@example.com');

-- View flagged species
SELECT * FROM taxa_needing_review;
```

### Performance Considerations

**View Performance:**
- Views compute on-demand (no storage overhead)
- For dashboards, create materialized views:
  ```sql
  CREATE MATERIALIZED VIEW taxonomy_summary_cached AS
  SELECT * FROM taxonomy_summary;
  
  REFRESH MATERIALIZED VIEW taxonomy_summary_cached;
  ```

**Function Performance:**
- `get_species_details()`: Fast for single species (<10ms)
- `search_species()`: Uses ILIKE and similarity() (add trigram index for better performance)
- `get_habitat_summary()`: Aggregates all species (~100ms for 564 species)

**Optimization Tips:**
- Filter before aggregating
- Use views for reporting, tables for transactions
- Cache observation counts in separate table if performance degrades
- Add composite indexes for common query patterns

[Continue with existing content from database_schema.md]

---

*Last Updated: January 6, 2026*  
*Schema Version: 3.3 (WoRMS/GBIF Enrichment + Views)*  
*Contributors: Huon Channel Marine Analytics Project*