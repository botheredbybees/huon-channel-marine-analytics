-- ============================================================================
-- PARAMETER COVERAGE ANALYSIS
-- ============================================================================
-- Purpose: Analyze the relationship between:
--   1. parameters table (extracted from AODN metadata)
--   2. parameter_mappings table (standardized mappings)
--   3. measurements table (actual data)
--
-- Usage:
--   psql -h localhost -p 5433 -U marine_user -d marine_db -f analyze_parameter_coverage.sql
--
-- Or to save outputs to CSV:
--   psql -h localhost -p 5433 -U marine_user -d marine_db -f analyze_parameter_coverage.sql > logs/parameter_analysis_$(date +%Y%m%d_%H%M%S).log
-- ============================================================================

\echo ''
\echo '================================================================================'
\echo 'PARAMETER COVERAGE ANALYSIS'
\echo '================================================================================'
\echo ''

-- ============================================================================
-- 1. OVERALL STATISTICS
-- ============================================================================
\echo ''
\echo '📊 OVERALL STATISTICS'
\echo '================================================================================'
\echo ''

SELECT 
    'Total Parameters in Metadata' AS metric,
    COUNT(*)::text AS value
FROM parameters

UNION ALL

SELECT 
    'Unique Parameter Codes',
    COUNT(DISTINCT parameter_code)::text
FROM parameters

UNION ALL

SELECT 
    'Parameters with Measurements',
    COUNT(DISTINCT p.id)::text
FROM parameters p
WHERE EXISTS (
    SELECT 1 FROM measurements m 
    WHERE m.parameter_code = p.parameter_code
)

UNION ALL

SELECT 
    'Parameters WITHOUT Measurements',
    COUNT(DISTINCT p.id)::text
FROM parameters p
WHERE NOT EXISTS (
    SELECT 1 FROM measurements m 
    WHERE m.parameter_code = p.parameter_code
)

UNION ALL

SELECT 
    'Total Parameter Mappings',
    COUNT(*)::text
FROM parameter_mappings

UNION ALL

SELECT 
    'Total Measurements',
    COUNT(*)::text
FROM measurements

UNION ALL

SELECT 
    'Unique Parameter Codes in Measurements',
    COUNT(DISTINCT parameter_code)::text
FROM measurements;

-- ============================================================================
-- 2. PARAMETERS WITHOUT MEASUREMENTS
-- ============================================================================
\echo ''
\echo '⚠️  PARAMETERS WITHOUT MEASUREMENTS'
\echo '================================================================================'
\echo ''

\copy (SELECT p.id AS parameter_id, p.parameter_code, p.parameter_label, p.unit_name, p.aodn_parameter_uri, m.dataset_name, m.dataset_path, m.uuid AS dataset_uuid FROM parameters p JOIN metadata m ON p.metadata_id = m.id WHERE NOT EXISTS ( SELECT 1 FROM measurements meas WHERE meas.parameter_code = p.parameter_code ) ORDER BY m.dataset_name, p.parameter_code) TO 'logs/unmeasured_parameters.csv' WITH CSV HEADER;

\echo 'Written to: logs/unmeasured_parameters.csv'

SELECT 
    m.dataset_name,
    COUNT(p.id) AS unmeasured_param_count,
    STRING_AGG(DISTINCT p.parameter_code, ', ' ORDER BY p.parameter_code) AS parameter_codes
FROM parameters p
JOIN metadata m ON p.metadata_id = m.id
WHERE NOT EXISTS (
    SELECT 1 FROM measurements meas 
    WHERE meas.parameter_code = p.parameter_code
)
GROUP BY m.dataset_name
ORDER BY unmeasured_param_count DESC;

-- ============================================================================
-- 3. PARAMETERS WITH MEASUREMENTS - SUMMARY
-- ============================================================================
\echo ''
\echo '✅ PARAMETERS WITH MEASUREMENTS'
\echo '================================================================================'
\echo ''

SELECT 
    p.parameter_code,
    p.parameter_label,
    p.unit_name,
    COUNT(DISTINCT p.metadata_id) AS dataset_count,
    COUNT(DISTINCT m.dataid) AS measurement_count,
    MIN(m.time) AS first_measurement,
    MAX(m.time) AS last_measurement,
    STRING_AGG(DISTINCT md.dataset_name, '; ' ORDER BY md.dataset_name) AS datasets
FROM parameters p
JOIN measurements m ON p.parameter_code = m.parameter_code
JOIN metadata md ON p.metadata_id = md.id
GROUP BY p.parameter_code, p.parameter_label, p.unit_name
ORDER BY measurement_count DESC
LIMIT 20;

-- ============================================================================
-- 4. PARAMETER MAPPINGS COVERAGE
-- ============================================================================
\echo ''
\echo '🔗 PARAMETER MAPPINGS - USAGE IN MEASUREMENTS'
\echo '================================================================================'
\echo ''

SELECT 
    pm.raw_parameter_name,
    pm.standard_code,
    pm.namespace,
    pm.unit,
    COUNT(DISTINCT m.dataid) AS measurement_count,
    CASE 
        WHEN COUNT(DISTINCT m.dataid) > 0 THEN 'IN USE'
        ELSE 'UNUSED'
    END AS status
FROM parameter_mappings pm
LEFT JOIN measurements m ON 
    UPPER(m.parameter_code) = UPPER(pm.raw_parameter_name) OR
    UPPER(m.parameter_code) = UPPER(pm.standard_code)
GROUP BY pm.raw_parameter_name, pm.standard_code, pm.namespace, pm.unit
ORDER BY measurement_count DESC NULLS LAST, pm.standard_code;

-- ============================================================================
-- 5. POTENTIAL MAPPING SUGGESTIONS
-- ============================================================================
\echo ''
\echo '💡 SUGGESTED MAPPINGS (parameters → parameter_mappings)'
\echo '================================================================================'
\echo ''

WITH normalized_params AS (
    SELECT DISTINCT
        p.parameter_code,
        p.parameter_label,
        p.unit_name,
        REGEXP_REPLACE(UPPER(p.parameter_code), '[^A-Z0-9]', '', 'g') AS normalized_code,
        REGEXP_REPLACE(UPPER(p.parameter_label), '[^A-Z0-9]', '', 'g') AS normalized_label
    FROM parameters p
),
normalized_mappings AS (
    SELECT
        pm.id,
        pm.raw_parameter_name,
        pm.standard_code,
        pm.namespace,
        pm.unit,
        REGEXP_REPLACE(UPPER(pm.raw_parameter_name), '[^A-Z0-9]', '', 'g') AS normalized_raw,
        REGEXP_REPLACE(UPPER(pm.standard_code), '[^A-Z0-9]', '', 'g') AS normalized_std
    FROM parameter_mappings pm
)
SELECT 
    np.parameter_code,
    np.parameter_label,
    np.unit_name AS param_unit,
    nm.standard_code AS suggested_mapping,
    nm.namespace AS mapping_namespace,
    nm.unit AS mapping_unit,
    CASE 
        WHEN np.normalized_code = nm.normalized_raw THEN 'EXACT_CODE'
        WHEN np.normalized_code = nm.normalized_std THEN 'EXACT_STANDARD'
        WHEN np.normalized_label LIKE '%' || nm.normalized_raw || '%' THEN 'PARTIAL_LABEL'
        WHEN nm.normalized_raw LIKE '%' || np.normalized_code || '%' THEN 'PARTIAL_CODE'
        ELSE 'FUZZY'
    END AS confidence
FROM normalized_params np
CROSS JOIN normalized_mappings nm
WHERE 
    np.normalized_code = nm.normalized_raw OR
    np.normalized_code = nm.normalized_std OR
    np.normalized_label LIKE '%' || nm.normalized_raw || '%' OR
    nm.normalized_raw LIKE '%' || np.normalized_code || '%'
ORDER BY 
    np.parameter_code,
    CASE 
        WHEN np.normalized_code = nm.normalized_raw THEN 1
        WHEN np.normalized_code = nm.normalized_std THEN 2
        WHEN np.normalized_label LIKE '%' || nm.normalized_raw || '%' THEN 3
        ELSE 4
    END,
    nm.standard_code
LIMIT 50;

-- ============================================================================
-- 6. DATASET-LEVEL COVERAGE
-- ============================================================================
\echo ''
\echo '📦 DATASET-LEVEL PARAMETER COVERAGE'
\echo '================================================================================'
\echo ''

\copy (SELECT m.dataset_name, m.dataset_path, COUNT(DISTINCT p.id) AS total_params, COUNT(DISTINCT CASE WHEN EXISTS ( SELECT 1 FROM measurements meas WHERE meas.parameter_code = p.parameter_code ) THEN p.id END) AS params_with_data, COUNT(DISTINCT CASE WHEN NOT EXISTS ( SELECT 1 FROM measurements meas WHERE meas.parameter_code = p.parameter_code ) THEN p.id END) AS params_without_data, ROUND(100.0 * COUNT(DISTINCT CASE WHEN EXISTS ( SELECT 1 FROM measurements meas WHERE meas.parameter_code = p.parameter_code ) THEN p.id END) / NULLIF(COUNT(DISTINCT p.id), 0), 1) AS coverage_pct FROM metadata m LEFT JOIN parameters p ON m.id = p.metadata_id GROUP BY m.dataset_name, m.dataset_path ORDER BY coverage_pct DESC NULLS LAST, total_params DESC) TO 'logs/dataset_parameter_coverage.csv' WITH CSV HEADER;

\echo 'Written to: logs/dataset_parameter_coverage.csv'

SELECT 
    m.dataset_name,
    COUNT(DISTINCT p.id) AS total_params,
    COUNT(DISTINCT CASE 
        WHEN EXISTS (
            SELECT 1 FROM measurements meas 
            WHERE meas.parameter_code = p.parameter_code
        ) THEN p.id 
    END) AS params_with_data,
    ROUND(100.0 * COUNT(DISTINCT CASE 
        WHEN EXISTS (
            SELECT 1 FROM measurements meas 
            WHERE meas.parameter_code = p.parameter_code
        ) THEN p.id 
    END) / NULLIF(COUNT(DISTINCT p.id), 0), 1) AS coverage_pct
FROM metadata m
LEFT JOIN parameters p ON m.id = p.metadata_id
GROUP BY m.dataset_name
HAVING COUNT(DISTINCT p.id) > 0
ORDER BY coverage_pct DESC NULLS LAST, total_params DESC
LIMIT 20;

-- ============================================================================
-- 7. MOST COMMON PARAMETERS ACROSS DATASETS
-- ============================================================================
\echo ''
\echo '🔝 TOP 20 PARAMETERS BY DATASET OCCURRENCE'
\echo '================================================================================'
\echo ''

SELECT 
    p.parameter_code,
    p.parameter_label,
    p.unit_name,
    COUNT(DISTINCT p.metadata_id) AS dataset_count,
    COALESCE(SUM(meas_count.cnt), 0) AS total_measurements,
    CASE 
        WHEN COALESCE(SUM(meas_count.cnt), 0) > 0 THEN 'YES'
        ELSE 'NO'
    END AS has_measurements
FROM parameters p
LEFT JOIN (
    SELECT parameter_code, COUNT(*) AS cnt
    FROM measurements
    GROUP BY parameter_code
) meas_count ON p.parameter_code = meas_count.parameter_code
GROUP BY p.parameter_code, p.parameter_label, p.unit_name
ORDER BY dataset_count DESC, total_measurements DESC
LIMIT 20;

-- ============================================================================
-- 8. FULL PARAMETER COVERAGE REPORT
-- ============================================================================
\echo ''
\echo '📝 GENERATING FULL PARAMETER COVERAGE REPORT'
\echo '================================================================================'
\echo ''

\copy (SELECT p.id AS parameter_id, p.parameter_code, p.parameter_label, p.unit_name, p.aodn_parameter_uri, m.dataset_name, m.dataset_path, m.uuid AS dataset_uuid, CASE WHEN EXISTS ( SELECT 1 FROM measurements meas WHERE meas.parameter_code = p.parameter_code ) THEN 'YES' ELSE 'NO' END AS has_measurements, COALESCE(meas_stats.measurement_count, 0) AS measurement_count, meas_stats.first_measurement, meas_stats.last_measurement FROM parameters p JOIN metadata m ON p.metadata_id = m.id LEFT JOIN ( SELECT parameter_code, COUNT(*) AS measurement_count, MIN(time) AS first_measurement, MAX(time) AS last_measurement FROM measurements GROUP BY parameter_code ) meas_stats ON p.parameter_code = meas_stats.parameter_code ORDER BY m.dataset_name, p.parameter_code) TO 'logs/full_parameter_coverage.csv' WITH CSV HEADER;

\echo 'Written to: logs/full_parameter_coverage.csv'
\echo ''
\echo '================================================================================'
\echo '✅ ANALYSIS COMPLETE'
\echo '================================================================================'
\echo ''
\echo 'Output files created in logs/ directory:'
\echo '  - unmeasured_parameters.csv'
\echo '  - dataset_parameter_coverage.csv'
\echo '  - full_parameter_coverage.csv'
\echo ''
