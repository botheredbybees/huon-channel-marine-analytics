#!/bin/bash
# ============================================================================
# Apply Taxonomy Views & Functions to Database
# ============================================================================
# 
# Purpose: Install all taxonomy views and utility functions
# Usage:
#   ./db/apply_taxonomy_views.sh
#   ./db/apply_taxonomy_views.sh --dry-run
#
# ============================================================================

set -e  # Exit on error

# Database configuration from environment or defaults
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5433}"
DB_NAME="${DB_NAME:-marine_db}"
DB_USER="${DB_USER:-marine_user}"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN}Applying Taxonomy Views & Functions${NC}"
echo -e "${GREEN}============================================================${NC}"
echo -e "Database: ${DB_NAME}@${DB_HOST}:${DB_PORT}"
echo -e "User: ${DB_USER}"
echo ""

# Check if views file exists
if [ ! -f "db/views/taxonomy_views.sql" ]; then
    echo -e "${RED}Error: db/views/taxonomy_views.sql not found${NC}"
    exit 1
fi

# Dry run mode
if [ "$1" = "--dry-run" ]; then
    echo -e "${YELLOW}DRY RUN MODE - No changes will be made${NC}"
    echo ""
    echo "Would execute:"
    cat db/views/taxonomy_views.sql | grep -E "^CREATE|^COMMENT" | head -20
    echo "..."
    exit 0
fi

echo -e "${YELLOW}Installing views and functions...${NC}"

# Apply the views
PGPASSWORD="${DB_PASSWORD}" psql \
    -h "$DB_HOST" \
    -p "$DB_PORT" \
    -U "$DB_USER" \
    -d "$DB_NAME" \
    -f db/views/taxonomy_views.sql

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Views and functions installed successfully${NC}"
else
    echo -e "${RED}✗ Installation failed${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN}Validating Installation${NC}"
echo -e "${GREEN}============================================================${NC}"

# Test queries
echo -e "\n${YELLOW}Testing views...${NC}"

PGPASSWORD="${DB_PASSWORD}" psql \
    -h "$DB_HOST" \
    -p "$DB_PORT" \
    -U "$DB_USER" \
    -d "$DB_NAME" \
    -c "SELECT COUNT(*) as view_count FROM information_schema.views WHERE table_name LIKE 'taxonomy%' OR table_name LIKE '%species%';"

PGPASSWORD="${DB_PASSWORD}" psql \
    -h "$DB_HOST" \
    -p "$DB_PORT" \
    -U "$DB_USER" \
    -d "$DB_NAME" \
    -c "SELECT COUNT(*) as function_count FROM information_schema.routines WHERE routine_name LIKE '%species%';"

echo ""
echo -e "${GREEN}Testing sample queries...${NC}"

# Test taxonomy_summary
PGPASSWORD="${DB_PASSWORD}" psql \
    -h "$DB_HOST" \
    -p "$DB_PORT" \
    -U "$DB_USER" \
    -d "$DB_NAME" \
    -c "SELECT COUNT(*) as species_in_summary FROM taxonomy_summary;"

# Test marine_species
PGPASSWORD="${DB_PASSWORD}" psql \
    -h "$DB_HOST" \
    -p "$DB_PORT" \
    -U "$DB_USER" \
    -d "$DB_NAME" \
    -c "SELECT COUNT(*) as marine_species_count FROM marine_species;"

# Test habitat summary function
echo -e "\n${YELLOW}Habitat breakdown:${NC}"
PGPASSWORD="${DB_PASSWORD}" psql \
    -h "$DB_HOST" \
    -p "$DB_PORT" \
    -U "$DB_USER" \
    -d "$DB_NAME" \
    -c "SELECT * FROM get_habitat_summary();"

echo ""
echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN}✓ Installation Complete!${NC}"
echo -e "${GREEN}============================================================${NC}"
echo ""
echo "Available views:"
echo "  - taxonomy_full              (complete denormalized data)"
echo "  - taxonomy_summary           (lightweight overview)"
echo "  - marine_species             (WoRMS marine species)"
echo "  - species_by_habitat         (habitat classification)"
echo "  - taxonomy_quality_metrics   (QC dashboard)"
echo "  - species_for_display        (user-friendly format)"
echo "  - enrichment_gaps            (prioritized enrichment work)"
echo "  - worms_gbif_comparison      (source comparison)"
echo ""
echo "Available functions:"
echo "  - get_species_details(species_name)"
echo "  - search_species(query, limit)"
echo "  - get_species_by_family(family)"
echo "  - get_habitat_summary()"
echo "  - flag_species_for_review(id, reason, reviewer)"
echo ""
echo "Documentation: docs/TAXONOMY_USAGE.md"
echo ""
