# Taxonomy System Usage Guide

## 🌊 Huon Estuary & D'Entrecasteaux Channel Marine Analytics

**Version:** 1.0  
**Last Updated:** January 6, 2026  
**Enrichment Status:** 75.5% success (426/564 species)  

---

## 📋 Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Enrichment Results](#enrichment-results)
3. [Database Views](#database-views)
4. [Utility Functions](#utility-functions)
5. [Common Query Patterns](#common-query-patterns)
6. [Data Quality Monitoring](#data-quality-monitoring)
7. [Integration Examples](#integration-examples)
8. [Performance Tips](#performance-tips)

---

## 🏛️ Architecture Overview

### Three-Layer Design

```
taxonomy (base table)
    ↓
    |───────────────────────────> Species ID & original name
    |
taxonomy_cache (enrichment data)
    ↓
    |───────────────────────────> WoRMS, GBIF, iNaturalist data
    |
VIEWS (query layer)
    ↓
    |───────────────────────────> Pre-built queries for common tasks
```

### Key Tables

| Table | Purpose | Records |
|-------|---------|--------|
| `taxonomy` | Base species registry | 564 |
| `taxonomy_cache` | Enriched taxonomic data | 426 |
| `taxonomy_enrichment_log` | API call history & QC | 564 |
| `species_observations` | Field observation data | Varies |

---

## 🎓 Enrichment Results

### Summary Statistics

```
Total Species Processed:  564
✅ WoRMS Successful:       48  (8.5%)
✅ GBIF Successful:        378 (67.0%)
❌ Failed:                138 (24.5%)
⚠️  Needs Review:          140 (24.8%)

API Calls:
  - WoRMS:  79
  - GBIF:   516
  
Processing Time: 497.6 seconds (~8.3 minutes)
```

### Data Sources Breakdown

| Source | Count | Use Case |
|--------|-------|----------|
| WoRMS (marine) | 48 | Authoritative marine taxonomy |
| GBIF (general) | 378 | Broad taxonomic coverage |
| iNaturalist | TBD | Citizen science observations |
| No enrichment | 138 | "Unidentified" or invalid names |

### Common Enrichment Failures

- **"Unidentified" names** (e.g., "Unidentified algae turf")
- **Substrate categories** (e.g., "Sand", "Bare rock", "Gravel")
- **Morphological groups** (e.g., "Crustacean nauplii", "Larval fish")
- **Genus + "spp."** without exact match

---

## 🔍 Database Views

### 1. `taxonomy_full` – Complete Denormalized Data

**Purpose:** All enrichment fields in one place  
**Use Case:** API responses, data exports, comprehensive queries

```sql
SELECT * FROM taxonomy_full
WHERE species_name = 'Ecklonia radiata';
```

**Key Fields:**
- `enrichment_source`: `worms_enriched`, `gbif_enriched`, `not_enriched`
- `observation_count`: Total field observations (performance note: cached count)
- All taxonomic hierarchy: kingdom → phylum → class → order → family → genus

---

### 2. `taxonomy_summary` – High-Level Overview

**Purpose:** Lightweight species list with enrichment flags  
**Use Case:** Quick lookups, species lists for dropdowns

```sql
SELECT species_name, common_name, family, has_worms, has_gbif, obs_count
FROM taxonomy_summary
WHERE obs_count > 10
ORDER BY obs_count DESC;
```

**Boolean Flags:**
- `has_worms`, `has_gbif`, `has_inaturalist`
- `has_photo`, `has_wikipedia`
- `is_accepted_name`

---

### 3. `marine_species` – WoRMS Marine Species

**Purpose:** Filter to verified marine species only  
**Use Case:** Marine ecology studies, WoRMS-only data

```sql
SELECT family, COUNT(*) as species_count
FROM marine_species
GROUP BY family
ORDER BY species_count DESC;
```

**Filters Applied:**
- `is_marine = TRUE`
- `worms_aphia_id IS NOT NULL`

---

### 4. `species_by_habitat` – Habitat Classification

**Purpose:** Group species by ecosystem type  
**Use Case:** Ecological niche analysis, habitat surveys

```sql
SELECT primary_habitat, COUNT(*) as species_count
FROM species_by_habitat
GROUP BY primary_habitat;
```

**Habitat Types:**
- `marine_only`
- `marine_estuarine`
- `estuarine_only`
- `freshwater`
- `terrestrial`
- `unknown`

**Breadth Metric:** `habitat_count` (1-4) for species versatility

---

### 5. `taxonomy_quality_metrics` – Data Quality Dashboard

**Purpose:** Assess enrichment completeness and accuracy  
**Use Case:** QC workflows, identifying issues

```sql
SELECT 
    species_name,
    quality_score,
    quality_issues,
    needs_review
FROM taxonomy_quality_metrics
WHERE quality_score < 50
ORDER BY quality_score;
```

**Quality Score (0-100):**
- WoRMS match: +30 points
- GBIF match: +20 points
- Family present: +15 points
- Genus present: +10 points
- Accepted status: +15 points
- Has photo: +5 points
- Has Wikipedia: +5 points

**Quality Issues Array:**
- `synonym`
- `invalid_name`
- `low_confidence`
- `imprecise_match`
- `unidentified`
- `genus_only`

---

### 6. `species_for_display` – User-Friendly Format

**Purpose:** Formatted text for web displays and reports  
**Use Case:** Public-facing websites, field guides

```sql
SELECT 
    scientific_name,
    common_name,
    status_display,
    habitat_display,
    conservation_display,
    data_quality
FROM species_for_display
WHERE total_observations > 5;
```

**Display Badges:**
- Status: `✓ Accepted`, `⟳ Synonym of...`, `? Unknown`
- Habitat: `🌊 Marine`, `💧 Freshwater`, `🌳 Terrestrial`
- Conservation: `⚠️ Threatened`, `🇦🇺 Endemic`, `🚀 Introduced`

---

### 7. `enrichment_gaps` – Species Needing Attention

**Purpose:** Identify and prioritize enrichment work  
**Use Case:** Data stewardship, targeted API re-queries

```sql
SELECT species_name, priority_score, obs_count, last_attempt
FROM enrichment_gaps
WHERE priority_score > 100
ORDER BY priority_score DESC
LIMIT 20;
```

**Priority Scoring:**
```
priority_score = (obs_count * 10) + 
                 (not_enriched * 50) + 
                 (missing_family * 20)
```

---

### 8. `worms_gbif_comparison` – API Cross-Check

**Purpose:** Compare WoRMS vs GBIF data for same species  
**Use Case:** Data validation, source preference decisions

```sql
SELECT species_name, in_both, worms_only, gbif_only, gbif_confidence
FROM worms_gbif_comparison
WHERE in_both = TRUE;
```

---

## ⚙️ Utility Functions

### 1. `get_species_details(species_name)`

**Returns:** Complete species profile as structured JSON

```sql
SELECT * FROM get_species_details('Ecklonia radiata');
```

**Output Structure:**
```json
{
  "id": 1,
  "species_name": "Ecklonia radiata",
  "common_name": "Golden Kelp",
  "full_classification": {
    "kingdom": "Chromista",
    "phylum": "Ochrophyta",
    "class": "Phaeophyceae",
    "order": "Laminariales",
    "family": "Lessoniaceae",
    "genus": "Ecklonia"
  },
  "external_ids": {
    "worms_aphia_id": 144181,
    "gbif_taxon_key": 2923742
  },
  "habitat_info": {
    "is_marine": true,
    "is_brackish": false
  },
  "observation_stats": {
    "total_observations": 127,
    "distinct_locations": 8,
    "first_observed": "2020-03-15",
    "last_observed": "2025-11-20"
  }
}
```

---

### 2. `search_species(query, limit)`

**Purpose:** Fuzzy search with similarity ranking

```sql
-- Find all kelp species
SELECT * FROM search_species('kelp', 10);

-- Search by family
SELECT * FROM search_species('Lessoniaceae');
```

**Match Types:**
- `exact`: Perfect match
- `starts_with`: Prefix match
- `contains`: Substring match
- `common_name`: Common name match
- `fuzzy`: Similarity-based

---

### 3. `get_species_by_family(family_name)`

**Purpose:** List all species in a taxonomic family

```sql
SELECT * FROM get_species_by_family('Lessoniaceae');
```

**Output:**
```
 id | species_name        | common_name  | genus     | is_marine | obs_count
----+---------------------+--------------+-----------+-----------+-----------
  1 | Ecklonia radiata    | Golden Kelp  | Ecklonia  | true      | 127
 23 | Lessonia corrugata  | NULL         | Lessonia  | true      | 34
 57 | Macrocystis pyrifera| Giant Kelp   | Macrocystis| true     | 89
```

---

### 4. `get_habitat_summary()`

**Purpose:** Ecosystem-level statistics

```sql
SELECT * FROM get_habitat_summary();
```

**Output:**
```
 habitat           | species_count | observation_count | worms_enriched | gbif_enriched
------------------+---------------+-------------------+----------------+---------------
 Marine            | 312           | 4,523             | 42             | 289
 Marine/Estuarine  | 67            | 891               | 6              | 54
 Freshwater        | 12            | 102               | 0              | 12
 Terrestrial       | 8             | 34                | 0              | 8
 Unknown           | 165           | 0                 | 0              | 15
```

---

### 5. `flag_species_for_review(taxonomy_id, reason, reviewer)`

**Purpose:** Mark species for manual quality control

```sql
-- Flag a species with ambiguous taxonomy
SELECT flag_species_for_review(
    42, 
    'Multiple matches in GBIF - needs taxonomist review',
    'peter.shanks@example.com'
);
```

**Use Cases:**
- Conflicting data between WoRMS and GBIF
- Low confidence matches (<80%)
- Synonyms requiring resolution
- Field observations not matching enriched data

---

## 📊 Common Query Patterns

### 1. Marine Species with Photos (Top 20)

```sql
SELECT 
    species_name,
    common_name,
    family,
    photo_url,
    obs_count
FROM marine_species
WHERE photo_url IS NOT NULL
ORDER BY obs_count DESC
LIMIT 20;
```

---

### 2. High-Priority Species Needing Enrichment

```sql
SELECT 
    species_name,
    obs_count,
    priority_score,
    last_attempt
FROM enrichment_gaps
WHERE priority_score > 100
ORDER BY priority_score DESC;
```

---

### 3. Species by Phylum with Counts

```sql
SELECT 
    phylum,
    COUNT(*) as species_count,
    SUM(observation_count) as total_observations,
    COUNT(*) FILTER (WHERE enrichment_source = 'worms_enriched') as worms_count
FROM taxonomy_full
WHERE phylum IS NOT NULL
GROUP BY phylum
ORDER BY species_count DESC;
```

---

### 4. Synonyms and Accepted Names

```sql
SELECT 
    species_name as synonym,
    accepted_name,
    worms_url
FROM taxonomy_full
WHERE taxonomic_status = 'synonym'
ORDER BY accepted_name;
```

---

### 5. Endemic Tasmanian Species

```sql
SELECT 
    species_name,
    common_name,
    family,
    conservation_status
FROM taxonomy_full
WHERE endemic = TRUE
ORDER BY family, species_name;
```

---

### 6. Data Quality Breakdown by Tier

```sql
SELECT 
    CASE 
        WHEN quality_score >= 80 THEN 'High'
        WHEN quality_score >= 50 THEN 'Medium'
        ELSE 'Low'
    END AS quality_tier,
    COUNT(*) as species_count,
    ROUND(AVG(quality_score), 1) as avg_score
FROM taxonomy_quality_metrics
GROUP BY quality_tier
ORDER BY avg_score DESC;
```

---

### 7. Species with Conflicting Habitat Data

```sql
SELECT 
    species_name,
    is_marine,
    is_freshwater,
    is_terrestrial,
    habitat_source,
    gbif_confidence
FROM species_by_habitat
WHERE habitat_count > 2  -- Species in 3+ habitats
ORDER BY habitat_count DESC;
```

---

### 8. Recent Enrichment Activity

```sql
SELECT 
    species_name,
    data_source,
    match_confidence,
    created_at
FROM taxonomy_enrichment_log
WHERE created_at > NOW() - INTERVAL '7 days'
ORDER BY created_at DESC;
```

---

## ✅ Data Quality Monitoring

### QC Dashboard Query

```sql
WITH quality_stats AS (
    SELECT 
        COUNT(*) as total_species,
        SUM((worms_aphia_id IS NOT NULL)::int) as worms_count,
        SUM((gbif_taxon_key IS NOT NULL)::int) as gbif_count,
        SUM((worms_aphia_id IS NULL AND gbif_taxon_key IS NULL)::int) as not_enriched,
        AVG(quality_score) as avg_quality,
        SUM((needs_review)::int) as review_needed
    FROM taxonomy_quality_metrics
)
SELECT 
    total_species,
    worms_count,
    ROUND(100.0 * worms_count / total_species, 1) as worms_pct,
    gbif_count,
    ROUND(100.0 * gbif_count / total_species, 1) as gbif_pct,
    not_enriched,
    ROUND(100.0 * not_enriched / total_species, 1) as not_enriched_pct,
    ROUND(avg_quality, 1) as avg_quality_score,
    review_needed
FROM quality_stats;
```

---

### Species with Issues

```sql
SELECT 
    species_name,
    quality_score,
    UNNEST(quality_issues) as issue,
    needs_review,
    obs_count
FROM taxonomy_quality_metrics
WHERE array_length(quality_issues, 1) > 0
ORDER BY obs_count DESC;
```

---

## 🔗 Integration Examples

### REST API Endpoint (Flask/FastAPI)

```python
from flask import Flask, jsonify
import psycopg2

app = Flask(__name__)

@app.route('/api/species/<species_name>')
def get_species(species_name):
    conn = psycopg2.connect("dbname=marine_data")
    cur = conn.cursor()
    
    cur.execute(
        "SELECT * FROM get_species_details(%s)",
        (species_name,)
    )
    
    result = cur.fetchone()
    cur.close()
    conn.close()
    
    if result:
        return jsonify({
            'id': result[0],
            'species_name': result[1],
            'common_name': result[2],
            'classification': result[3],
            'external_ids': result[4],
            'habitat': result[5],
            'conservation': result[6],
            'media': result[7],
            'observations': result[8]
        })
    else:
        return jsonify({'error': 'Species not found'}), 404

@app.route('/api/search/<query>')
def search_species(query):
    conn = psycopg2.connect("dbname=marine_data")
    cur = conn.cursor()
    
    cur.execute(
        "SELECT * FROM search_species(%s, 20)",
        (query,)
    )
    
    results = cur.fetchall()
    cur.close()
    conn.close()
    
    return jsonify([{
        'id': r[0],
        'species_name': r[1],
        'common_name': r[2],
        'family': r[3],
        'match_type': r[4],
        'similarity': r[5]
    } for r in results])
```

---

### Grafana Dashboard Panel

**Species Richness by Phylum (Pie Chart)**

```sql
SELECT 
    COALESCE(phylum, 'Unknown') as metric,
    COUNT(*) as value
FROM taxonomy_full
WHERE observation_count > 0
GROUP BY phylum
ORDER BY value DESC;
```

**Enrichment Progress (Time Series)**

```sql
SELECT 
    DATE(created_at) as time,
    COUNT(*) as enriched_species
FROM taxonomy_enrichment_log
WHERE data_source IN ('worms', 'gbif')
GROUP BY DATE(created_at)
ORDER BY time;
```

**Data Quality Gauge**

```sql
SELECT AVG(quality_score) as quality
FROM taxonomy_quality_metrics;
```

---

## ⚡ Performance Tips

### 1. Use Views for Reporting, Tables for Transactions

Views compute aggregates on-the-fly. For dashboards, cache results:

```sql
CREATE MATERIALIZED VIEW taxonomy_summary_cached AS
SELECT * FROM taxonomy_summary;

-- Refresh periodically
REFRESH MATERIALIZED VIEW taxonomy_summary_cached;
```

---

### 2. Filter Before Aggregating

```sql
-- GOOD: Filter first
SELECT family, COUNT(*)
FROM marine_species
WHERE obs_count > 10
GROUP BY family;

-- SLOW: Aggregate everything
SELECT family, COUNT(*)
FROM taxonomy_full
GROUP BY family
HAVING COUNT(*) > 10;
```

---

### 3. Leverage Indexes

Existing indexes:
- `idx_taxonomy_cache_taxonomy_id` (JOIN performance)
- `idx_taxonomy_cache_worms_id` (WoRMS lookups)
- `idx_taxonomy_cache_gbif_key` (GBIF lookups)
- `idx_taxonomy_cache_marine` (Marine filtering)
- `idx_species_obs_taxonomy` (Observation counts)

---

### 4. Batch API Queries

When re-enriching, process in batches of 30:

```bash
python scripts/enrich_taxonomy_from_worms.py --batch-size 30
```

---

### 5. Use `EXPLAIN ANALYZE` for Slow Queries

```sql
EXPLAIN ANALYZE
SELECT * FROM taxonomy_full WHERE family = 'Lessoniaceae';
```

---

## 📝 Example Outputs

### Habitat Summary

```
 habitat           | species_count | observation_count | worms | gbif
------------------+---------------+-------------------+-------+------
 Marine            | 312           | 4,523             | 42    | 289
 Marine/Estuarine  | 67            | 891               | 6     | 54
 Freshwater        | 12            | 102               | 0     | 12
 Terrestrial       | 8             | 34                | 0     | 8
 Unknown           | 165           | 0                 | 0     | 15
```

---

### Quality Tier Breakdown

```
 quality_tier | species_count | avg_score
--------------+---------------+-----------
 High         | 142           | 87.3
 Medium       | 196           | 62.1
 Low          | 226           | 28.7
```

---

### Top Marine Families

```
 family         | species_count | total_obs
----------------+---------------+-----------
 Rhodophyta     | 89            | 1,234
 Phaeophyceae   | 67            | 2,103
 Chlorophyta    | 34            | 567
 Porifera       | 28            | 289
 Bryozoa        | 21            | 156
```

---

## 📚 Additional Resources

- **WoRMS API Docs:** [https://www.marinespecies.org/rest/](https://www.marinespecies.org/rest/)
- **GBIF API Docs:** [https://www.gbif.org/developer/species](https://www.gbif.org/developer/species)
- **iNaturalist API:** [https://api.inaturalist.org/v1/docs/](https://api.inaturalist.org/v1/docs/)
- **PostgreSQL Views:** [https://www.postgresql.org/docs/current/sql-createview.html](https://www.postgresql.org/docs/current/sql-createview.html)

---

## ❓ Support

For questions or issues:
- **GitHub Issues:** [github.com/botheredbybees/huon-channel-marine-analytics/issues](https://github.com/botheredbybees/huon-channel-marine-analytics/issues)
- **Email:** [pshanks@megalong.com](mailto:pshanks@megalong.com)

---

**Last Updated:** January 6, 2026  
**Schema Version:** 1.0  
**Documentation:** [github.com/botheredbybees/huon-channel-marine-analytics](https://github.com/botheredbybees/huon-channel-marine-analytics)
