-- Migration 002: Add WoRMS and GBIF columns to taxonomy_cache
-- Created: January 6, 2026
-- Purpose: Enable full WoRMS and GBIF API enrichment

-- Add missing columns for WoRMS enrichment
ALTER TABLE taxonomy_cache 
    ADD COLUMN IF NOT EXISTS scientific_name_authorship TEXT,
    ADD COLUMN IF NOT EXISTS taxonomic_status TEXT,
    ADD COLUMN IF NOT EXISTS accepted_name TEXT,
    ADD COLUMN IF NOT EXISTS accepted_aphia_id INTEGER,
    ADD COLUMN IF NOT EXISTS worms_url TEXT,
    ADD COLUMN IF NOT EXISTS is_extinct BOOLEAN DEFAULT FALSE;

-- Add missing columns for GBIF enrichment
ALTER TABLE taxonomy_cache 
    ADD COLUMN IF NOT EXISTS gbif_scientific_name TEXT,
    ADD COLUMN IF NOT EXISTS match_type TEXT,
    ADD COLUMN IF NOT EXISTS confidence INTEGER;

-- Create indexes on new columns for performance
CREATE INDEX IF NOT EXISTS idx_taxonomy_cache_taxonomic_status 
    ON taxonomy_cache(taxonomic_status);

CREATE INDEX IF NOT EXISTS idx_taxonomy_cache_accepted_aphia_id 
    ON taxonomy_cache(accepted_aphia_id) 
    WHERE accepted_aphia_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_taxonomy_cache_match_type 
    ON taxonomy_cache(match_type);

-- Add comments
COMMENT ON COLUMN taxonomy_cache.scientific_name_authorship IS 'Taxonomic authority (e.g., "(Linnaeus, 1758)")';
COMMENT ON COLUMN taxonomy_cache.taxonomic_status IS 'Status: accepted, synonym, invalid, etc.';
COMMENT ON COLUMN taxonomy_cache.accepted_name IS 'Valid/accepted name if this is a synonym';
COMMENT ON COLUMN taxonomy_cache.accepted_aphia_id IS 'WoRMS AphiaID of accepted name';
COMMENT ON COLUMN taxonomy_cache.worms_url IS 'Direct URL to WoRMS species page';
COMMENT ON COLUMN taxonomy_cache.is_extinct IS 'Whether species is extinct';
COMMENT ON COLUMN taxonomy_cache.gbif_scientific_name IS 'Full scientific name from GBIF';
COMMENT ON COLUMN taxonomy_cache.match_type IS 'GBIF match type: EXACT, FUZZY, HIGHERRANK';
COMMENT ON COLUMN taxonomy_cache.confidence IS 'GBIF match confidence (0-100)';

-- Verify migration
SELECT 
    column_name, 
    data_type, 
    is_nullable
FROM information_schema.columns 
WHERE table_name = 'taxonomy_cache' 
  AND column_name IN (
    'scientific_name_authorship',
    'taxonomic_status',
    'accepted_name',
    'accepted_aphia_id',
    'worms_url',
    'is_extinct',
    'gbif_scientific_name',
    'match_type',
    'confidence'
  )
ORDER BY column_name;
