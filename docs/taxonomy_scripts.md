# Taxonomy Enrichment Scripts Reference

Complete reference for taxonomy enrichment workflow in the Huon Channel Marine Analytics project.

## Overview

The taxonomy enrichment system enhances species data with authoritative taxonomic information from:
- **WoRMS** (World Register of Marine Species) - Marine species authority
- **GBIF** (Global Biodiversity Information Facility) - Global biodiversity data
- **iNaturalist** - Citizen science observations (previous enrichment)

## Architecture

```
taxonomy (base table, 564 species)
    ↓
taxonomy_cache (enriched data, 426/564 enriched)
    ↓
taxonomy_enrichment_log (audit trail, 564 API calls)
    ↓
VIEWS (8 pre-built views for queries)
    ↓
FUNCTIONS (5 utility functions)
```

## Scripts

### 1. enrich_taxonomy_from_worms.py

**Purpose:** Enrich species with WoRMS and GBIF data

**Input:** `taxonomy` table (species_name)
**Output:** 
- `taxonomy_cache` (enriched fields)
- `taxonomy_enrichment_log` (audit trail)
- `logs/taxonomy_enrichment_worms.log` (detailed log)

**Usage:**
```bash
# Standard run
python scripts/enrich_taxonomy_from_worms.py

# Test with limit
python scripts/enrich_taxonomy_from_worms.py --limit 10

# Retry failed
python scripts/enrich_taxonomy_from_worms.py --retry-failed
```

**Enrichment Logic:**
1. Query WoRMS for marine species (authoritative)
2. If WoRMS fails, try GBIF (broad coverage)
3. Extract taxonomic hierarchy (kingdom → species)
4. Capture habitat flags and conservation status
5. Store photos, Wikipedia links, external IDs
6. Log all API calls for audit
7. Flag low-confidence matches (<80%) for review

**Success Rate:** 75.5% (426/564)
- WoRMS: 48 species (8.5%)
- GBIF: 378 species (67.0%)
- Failed: 138 species (24.5%) - mostly "Unidentified" or substrates

---

### 2. apply_taxonomy_views.sh

**Purpose:** Install database views and functions

**Input:** `db/views/taxonomy_views.sql`
**Output:** 8 views + 5 functions in database

**Usage:**
```bash
chmod +x db/apply_taxonomy_views.sh
./db/apply_taxonomy_views.sh
```

**Installs:**
- Views: taxonomy_full, taxonomy_summary, marine_species, etc.
- Functions: get_species_details(), search_species(), etc.

---

## Workflow

### Initial Setup

```bash
# 1. Enrich species data
python scripts/enrich_taxonomy_from_worms.py

# 2. Install views
./db/apply_taxonomy_views.sh

# 3. Apply hotfix (if needed)
psql -h localhost -p 5433 -U marine_user -d marine_db \
  -f db/views/taxonomy_views_fix.sql
```

### Verify Installation

```sql
-- Check enrichment status
SELECT 
    COUNT(*) as total,
    COUNT(*) FILTER (WHERE worms_aphia_id IS NOT NULL) as worms_count,
    COUNT(*) FILTER (WHERE gbif_taxon_key IS NOT NULL) as gbif_count
FROM taxonomy_cache;

-- Test views
SELECT * FROM taxonomy_summary LIMIT 10;
SELECT * FROM get_habitat_summary();
```

### Query Examples

```sql
-- Species details
SELECT * FROM get_species_details('Ecklonia radiata');

-- Search
SELECT * FROM search_species('kelp', 10);

-- Marine species
SELECT species_name, family, obs_count
FROM marine_species
ORDER BY obs_count DESC
LIMIT 20;

-- Data quality
SELECT quality_score, COUNT(*)
FROM taxonomy_quality_metrics
GROUP BY quality_score
ORDER BY quality_score DESC;
```

### Maintenance

```bash
# Re-enrich failed species
python scripts/enrich_taxonomy_from_worms.py --retry-failed

# Update specific species
psql -h localhost -p 5433 -U marine_user -d marine_db -c "
UPDATE taxonomy_cache 
SET needs_update = TRUE 
WHERE species_name = 'Your Species Name';
"

python scripts/enrich_taxonomy_from_worms.py --force
```

## Documentation

- [TAXONOMY_USAGE.md](./TAXONOMY_USAGE.md) - Query patterns & examples
- [TAXONOMY_IMPLEMENTATION_SUMMARY.md](./TAXONOMY_IMPLEMENTATION_SUMMARY.md) - Complete guide
- [database_schema.md](./database_schema.md) - Schema reference
- [scripts.md](./scripts.md) - All ETL scripts

---

*Last Updated: January 6, 2026*