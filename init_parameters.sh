#!/bin/bash
#
# PARAMETER TABLE INITIALIZATION SEQUENCE
# 
# This script ensures proper initialization of parameter-related tables
# in the correct order for the Huon Channel Marine Analytics database.
#
# Usage: ./init_parameters.sh
#
# Prerequisites:
# - PostgreSQL database running on localhost:5433
# - Database credentials configured in scripts
# - Python 3 with required packages installed
#

set -e  # Exit on error

echo "════════════════════════════════════════════════════════════════════"
echo "   Parameter Table Initialization Sequence"
echo "   Huon Channel Marine Analytics Database"
echo "════════════════════════════════════════════════════════════════════"
echo ""

# Database configuration
DB_HOST=${DB_HOST:-localhost}
DB_PORT=${DB_PORT:-5433}
DB_NAME=${DB_NAME:-marine_db}
DB_USER=${DB_USER:-marine_user}

echo "Database Configuration:"
echo "  Host: $DB_HOST"
echo "  Port: $DB_PORT"
echo "  Database: $DB_NAME"
echo "  User: $DB_USER"
echo ""

# Check if database is accessible
echo "Checking database connectivity..."
if ! psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT 1" > /dev/null 2>&1; then
    echo "✗ Error: Cannot connect to database"
    echo "  Please check database is running and credentials are correct"
    exit 1
fi
echo "✓ Database connection successful"
echo ""

# Step 1: Initialize parameter_mappings table
echo "──────────────────────────────────────────────────────────────────"
echo "STEP 1: Initializing parameter_mappings table"
echo "──────────────────────────────────────────────────────────────────"
echo "This table provides standardized mappings from raw parameter names"
echo "to BODC/CF standard codes (e.g., 'temperature' → 'TEMP')"
echo ""

if [ -f "populate_parameter_mappings.py" ]; then
    python3 populate_parameter_mappings.py
    echo ""
    echo "✓ Parameter mappings populated"
else
    echo "⚠  Warning: populate_parameter_mappings.py not found"
    echo "   Skipping parameter mappings initialization"
    echo "   Measurements will use raw parameter names without standardization"
fi

echo ""

# Step 2: Extract and populate metadata (includes parameters from XML)
echo "──────────────────────────────────────────────────────────────────"
echo "STEP 2: Extracting metadata and parameters from AODN datasets"
echo "──────────────────────────────────────────────────────────────────"
echo "This extracts dataset metadata and parameter definitions from"
echo "ISO 19115-3 XML files and populates both metadata and parameters tables"
echo ""

if [ -f "populate_metadata.py" ]; then
    python3 populate_metadata.py --force
    echo ""
    echo "✓ Metadata and parameters extracted from XML"
else
    echo "✗ Error: populate_metadata.py not found"
    echo "   Cannot proceed without metadata extraction"
    exit 1
fi

echo ""

# Step 3: Populate measurements (with parameter standardization)
echo "──────────────────────────────────────────────────────────────────"
echo "STEP 3: Extracting and standardizing measurements"
echo "──────────────────────────────────────────────────────────────────"
echo "This extracts measurements from CSV/NetCDF files and standardizes"
echo "parameter codes using parameter_mappings table"
echo ""

if [ -f "populate_measurements.py" ]; then
    python3 populate_measurements.py
    echo ""
    echo "✓ Measurements extracted and standardized"
else
    echo "✗ Error: populate_measurements.py not found"
    echo "   Metadata and parameters populated but no measurements"
    exit 1
fi

echo ""

# Step 4: Verification
echo "──────────────────────────────────────────────────────────────────"
echo "STEP 4: Verifying parameter table population"
echo "──────────────────────────────────────────────────────────────────"
echo ""

psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME << 'SQL'
\echo 'Table Record Counts:'
\echo '--------------------'
SELECT 
    'parameter_mappings' as table_name,
    COUNT(*) as record_count,
    COUNT(DISTINCT standard_code) as unique_codes
FROM parameter_mappings
UNION ALL
SELECT 
    'parameters' as table_name,
    COUNT(*) as record_count,
    COUNT(DISTINCT parameter_code) as unique_codes
FROM parameters
UNION ALL
SELECT 
    'measurements' as table_name,
    COUNT(*) as record_count,
    COUNT(DISTINCT parameter_code) as unique_codes
FROM measurements;

\echo ''
\echo 'Top 10 Parameters by Measurement Count:'
\echo '----------------------------------------'
SELECT 
    pm.standard_code,
    pm.namespace,
    COUNT(DISTINCT p.metadata_id) as datasets_with_metadata,
    COUNT(DISTINCT m.metadata_id) as datasets_with_measurements,
    SUM(CASE WHEN m.id IS NOT NULL THEN 1 ELSE 0 END) as measurement_count
FROM parameter_mappings pm
LEFT JOIN parameters p ON pm.standard_code = p.parameter_code
LEFT JOIN measurements m ON pm.standard_code = m.parameter_code
GROUP BY pm.standard_code, pm.namespace
ORDER BY measurement_count DESC
LIMIT 10;

\echo ''
\echo 'Data Integrity Check:'
\echo '---------------------'
SELECT 
    'Orphaned measurements (no parameter metadata)' as issue,
    COUNT(*) as count
FROM measurements m
WHERE NOT EXISTS (
    SELECT 1 FROM parameters p 
    WHERE p.parameter_code = m.parameter_code 
    AND p.metadata_id = m.metadata_id
)
UNION ALL
SELECT 
    'Parameters without measurements (metadata only)',
    COUNT(*)
FROM parameters p
WHERE NOT EXISTS (
    SELECT 1 FROM measurements m
    WHERE m.parameter_code = p.parameter_code
    AND m.metadata_id = p.metadata_id
);
SQL

echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "✓ Parameter initialization sequence complete!"
echo "════════════════════════════════════════════════════════════════════"
echo ""
echo "Next steps:"
echo "  1. Review the verification output above"
echo "  2. Check logs/ directory for detailed ETL logs"
echo "  3. Run 'python3 diagnostic_etl.py' for comprehensive validation"
echo "  4. Review PARAMETER_INTEGRATION_GUIDE.md for details"
echo ""
echo "Table population order (completed):"
echo "  ✓ parameter_mappings → parameters → measurements"
echo ""
echo "For issues or questions, see:"
echo "  - PARAMETER_INTEGRATION_GUIDE.md (implementation details)"
echo "  - logs/*.log (detailed execution logs)"
echo "  - DATABASE_SCHEMA.md (table definitions)"
echo ""
