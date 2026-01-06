# ETL Scripts Reference

This document provides an overview of all Python ETL scripts in the Huon Channel Marine Analytics project. Each script handles a specific aspect of the data ingestion and processing pipeline.

## Quick Reference

For step-by-step instructions on running the ETL pipeline, see the [ETL Quick Reference](../ETL_QUICK_REFERENCE.md) in the project root.

## Development Environment Setup

**Default Development Credentials** (from `docker-compose.yml`):
```bash
export DB_HOST=localhost
export DB_PORT=5433                  # Development port
export DB_NAME=marine_db
export DB_USER=marine_user
export DB_PASSWORD=marine_pass123
export AODN_DATA_PATH=/AODN_data
```

## Core ETL Scripts

### 1. populate_metadata.py ⭐ ENHANCED

**Purpose:** Scans the AODN data directory and extracts comprehensive metadata from ISO 19115-3 XML files.

[... keeping all previous sections unchanged through script #7 ...]

---

## Taxonomy Enrichment Scripts ✨ NEW (Phase 2)

These scripts enhance species taxonomy data by querying external authoritative databases.

### 11. enrich_taxonomy_from_worms.py ✨ NEW

**Purpose:** Enriches species taxonomy with data from WoRMS (World Register of Marine Species) and GBIF (Global Biodiversity Information Facility).

**Problem Solved:**
- Missing taxonomic hierarchy (kingdom → species)
- No common names or conservation status
- Unknown habitat preferences (marine, freshwater, terrestrial)
- No external references (WoRMS, GBIF, Wikipedia, photos)

**Key Functions:**
- Queries WoRMS API for marine species (authoritative)
- Falls back to GBIF for non-marine species
- Extracts full taxonomic classification
- Captures habitat flags (is_marine, is_brackish, is_freshwater, is_terrestrial)
- Retrieves common names and authorities
- Stores photos, Wikipedia links, and external IDs
- Logs all API calls for audit trail
- Flags low-confidence matches for manual review

**Data Sources:**
- **WoRMS (World Register of Marine Species)**: http://www.marinespecies.org/
  - Authoritative for marine organisms
  - 250,000+ marine species
  - Comprehensive habitat data
  - Taxonomic authorities

- **GBIF (Global Biodiversity Information Facility)**: https://www.gbif.org/
  - Broad taxonomic coverage (all kingdoms)
  - 1.4+ million species
  - Global distribution data
  - Fuzzy name matching

**Usage:**
```bash
# Enrich all species without existing enrichment
python scripts/enrich_taxonomy_from_worms.py

# Limit to first N species (testing)
python scripts/enrich_taxonomy_from_worms.py --limit 50

# Re-enrich failed species
python scripts/enrich_taxonomy_from_worms.py --retry-failed

# Force re-enrich all species
python scripts/enrich_taxonomy_from_worms.py --force
```

**Expected Output:**
```
============================================================
Taxonomy Enrichment from WoRMS & GBIF
============================================================
Found 564 species needing enrichment

Processing species 1/564: Ecklonia radiata
✓ WoRMS match found: Ecklonia radiata (AphiaID: 214344)
  - Family: Lessoniaceae
  - Kingdom: Chromista
  - Marine: True
  - Photo: https://inaturalist-open-data.s3.amazonaws.com/...

Processing species 2/564: Acanthastrea lordhowensis
⚠️  WoRMS not found, trying GBIF...
✓ GBIF match found: Acanthastrea lordhowensis (TaxonKey: 2289628)
  - Family: Acroporidae
  - Confidence: 95%

Processing species 3/564: Unidentified algae turf
✗ No match found in WoRMS or GBIF

============================================================
Enrichment Complete
============================================================
Total processed:    564
✓ WoRMS Success:     48 (8.5%)
✓ GBIF Success:     378 (67.0%)
✗ Failed:           138 (24.5%)
⚠️  Needs Review:    140 (24.8%)

Processing time:    497.6 seconds (~8.3 minutes)
Log file:           logs/taxonomy_enrichment_worms.log
```

**Quality Control:**
- Matches flagged for review if confidence < 80%
- "Unidentified" names automatically skipped
- Substrate names (sand, rock, gravel) excluded
- Multiple matches logged for manual disambiguation
- Full API responses stored in taxonomy_cache.worms_response and .gbif_response (JSONB)

**Database Tables Updated:**
1. `taxonomy_cache` - Enriched taxonomic data
2. `taxonomy_enrichment_log` - Audit trail of all API calls
3. `taxonomy_synonyms` - Synonym relationships (if applicable)

**Features:**
- ✅ Intelligent API fallback (WoRMS → GBIF)
- ✅ Rate limiting (respects API quotas)
- ✅ Retry logic with exponential backoff
- ✅ Comprehensive logging
- ✅ Boolean type fix for PostgreSQL
- ✅ Idempotent (safe to re-run)
- ✅ Progress tracking with estimated time remaining

**When to Run:**
- After `populate_biological.py` loads species observations
- Before generating species reports or analyses
- When new species are added to taxonomy table
- Periodically to update conservation status

[Detailed Documentation →](../docs/TAXONOMY_USAGE.md)

---

### 12. apply_taxonomy_views.sh ✨ NEW

**Purpose:** Installs 8 pre-built database views and 5 utility functions for taxonomy queries.

**Problem Solved:**
- Complex JOIN queries between taxonomy and taxonomy_cache
- Repetitive queries for common operations
- No convenient search functionality
- Difficult to assess data quality

**Views Installed:**
1. `taxonomy_full` - Complete denormalized species data
2. `taxonomy_summary` - Lightweight species overview
3. `marine_species` - WoRMS marine species only
4. `species_by_habitat` - Habitat classification
5. `taxonomy_quality_metrics` - Data quality dashboard
6. `species_for_display` - User-friendly format with badges
7. `enrichment_gaps` - Species needing attention
8. `worms_gbif_comparison` - API source comparison

**Functions Installed:**
1. `get_species_details(species_name)` - Complete species profile as JSON
2. `search_species(query, limit)` - Fuzzy species search
3. `get_species_by_family(family)` - All species in family
4. `get_habitat_summary()` - Ecosystem statistics
5. `flag_species_for_review(id, reason, reviewer)` - QC workflow

**Usage:**
```bash
# Make executable
chmod +x db/apply_taxonomy_views.sh

# Dry run (preview changes)
./db/apply_taxonomy_views.sh --dry-run

# Apply views
./db/apply_taxonomy_views.sh
```

**Expected Output:**
```
============================================================
Applying Taxonomy Views & Functions
============================================================
Database: marine_db@localhost:5433
User: marine_user

Installing views and functions...
✓ Views and functions installed successfully

Validating Installation
============================================================
Testing views...
 view_count: 8
 function_count: 5

Testing sample queries...
 species_in_summary: 564
 marine_species_count: 49

Habitat breakdown:
 habitat           | species_count | observation_count
------------------+---------------+-------------------
 Marine            | 49            | 14586
 Unknown           | 515           | 54063

✓ Installation Complete!
```

**Example Queries:**
```sql
-- Get complete species details
SELECT * FROM get_species_details('Ecklonia radiata');

-- Search for kelp species
SELECT * FROM search_species('kelp', 10);

-- Marine species with photos
SELECT species_name, common_name, photo_url, obs_count
FROM marine_species
WHERE photo_url IS NOT NULL
ORDER BY obs_count DESC;

-- Data quality summary
SELECT 
    CASE 
        WHEN quality_score >= 80 THEN 'High'
        WHEN quality_score >= 50 THEN 'Medium'
        ELSE 'Low'
    END AS tier,
    COUNT(*) as species_count
FROM taxonomy_quality_metrics
GROUP BY tier;
```

[Detailed Documentation →](../docs/TAXONOMY_USAGE.md)

---

## Execution Order & Dependencies

**Recommended ETL Pipeline Order:**

```
1. populate_metadata.py [ENHANCED - 30+ fields]
   ↓
2. populate_parameter_mappings.py
   ↓
3. populate_measurements.py
   ↓
4. populate_parameters_from_measurements.py  [NEW]
   ↓
5. analyze_parameter_coverage.py  [NEW - Optional but recommended]
   ↓
6. populate_spatial.py  [ENHANCED v3.0]
   ↓
7. populate_biological.py
   ↓
8. enrich_metadata_from_xml.py  [NEW - Phase 1]
   ↓
9. enrich_measurements_from_netcdf_headers.py  [NEW - Phase 1]
   ↓
10. validate_and_fix_data_issues.py  [NEW - Phase 1]
   ↓
11. enrich_taxonomy_from_worms.py  [NEW - Phase 2] 🆕
   ↓
12. apply_taxonomy_views.sh  [NEW - Phase 2] 🆕
```

**Parallel Execution:**
- Steps 6, 7 can run while 3 completes
- Steps 8, 9 can run in parallel (independent data sources)
- Step 10 should run after 8, 9 complete
- Step 5 can run anytime after step 4
- Steps 11, 12 must run after step 7 completes

---

[... rest of document unchanged ...]

---

*Last Updated: January 6, 2026*
*Schema Version: 3.3 (Taxonomy Enrichment)*