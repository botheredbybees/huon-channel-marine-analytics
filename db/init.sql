-- =============================================================================
-- Huon Channel Marine Analytics - Database Initialization Script (PostGIS-Free)
-- =============================================================================
-- Purpose: Creates PostgreSQL database schema for AODN marine data
-- Version: 3.3 (Taxonomy Enrichment Complete)
-- Last Updated: January 6, 2026
--
-- IMPORTANT NOTES:
-- 1. Removed ALL PostGIS dependencies (GEOMETRY, ST_* functions, BOX2D, GIST on geometry)
-- 2. Uses pure PostgreSQL lat/lon and bbox columns instead
-- 3. Compatible with timescale/timescaledb:latest-pg18 (Community license)
-- 4. All spatial queries work with DECIMAL bbox columns
-- 5. TimescaleDB hypertable enabled for measurements table
-- 6. Taxonomy enrichment with WoRMS & GBIF integration (v3.3)
-- 7. dataset_path is the primary stable identifier for upserts
-- =============================================================================

-- Enable extensions (NO PostGIS)
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS unaccent;

[... previous schema sections remain the same until TAXONOMY ENRICHMENT TABLES ...]