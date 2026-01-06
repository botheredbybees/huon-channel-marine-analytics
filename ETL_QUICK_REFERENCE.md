# ETL Quick Reference Card

## One-Minute Start

```bash
# 1. Scan all datasets for issues
python diagnostic_etl.py

# 2. View summary
cat diagnostic_report.json | jq '.summary'

# 3. Ingest all data
python populate_measurements.py

# 4. Check results
psql -d marine_db -U marine_user -c "SELECT COUNT(*) FROM measurements;"
```

---

## Commands

### Diagnostic

```bash
# Full scan
python diagnostic_etl.py

# Review findings
jq '.summary.failure_reasons' diagnostic_report.json
jq '.datasets[] | select(.status=="failed")' diagnostic_report.json
```

### ETL Ingestion

```bash
# Full ingestion
python populate_measurements.py

# Specific dataset (partial name match)
python populate_measurements.py --dataset "Wave buoys"

# Test mode (first 100 rows only)
python populate_measurements.py --limit 100

# With custom parameter mapping
python populate_measurements.py --config config_parameter_mapping.json

# Combine options
python populate_measurements.py --dataset "Chlorophyll" --limit 500
```

### Spatial & Biological

```bash
# Ingest shapefiles
python populate_spatial.py

# Ingest species observations
python populate_biological.py
```

---

## Common Database Queries

```sql
-- Count all measurements
SELECT COUNT(*) FROM measurements;

-- Count by dataset
SELECT uuid, COUNT(*) as count 
FROM measurements 
GROUP BY uuid 
ORDER BY count DESC;

-- View parameters ingested
SELECT DISTINCT parameter_code, namespace, COUNT(*) as count
FROM measurements
GROUP BY parameter_code, namespace
ORDER BY count DESC;

-- Find empty datasets still
SELECT m.title, m.uuid, COUNT(mes.data_id) as measurements_count
FROM metadata m
LEFT JOIN measurements mes ON m.id = mes.metadata_id
GROUP BY m.id
HAVING COUNT(mes.data_id) = 0;

-- Check data quality
SELECT parameter_code, 
       COUNT(*) as total,
       COUNT(*) FILTER (WHERE quality_flag = 1) as good,
       MIN(value), MAX(value), AVG(value)
FROM measurements
GROUP BY parameter_code;

-- Time range per dataset
SELECT uuid, 
       MIN(time) as earliest, 
       MAX(time) as latest,
       COUNT(*) as measurements
FROM measurements
GROUP BY uuid;
```

---

## Troubleshooting

| Problem | Cause | Solution |
|---------|-------|----------|
| "ENCODING_ERROR" | Non-UTF-8 CSV | ETL auto-tries Latin-1/ISO-8859-1 |
| "No time variable found" | Time column not detected | Check column name matches: time, date, datetime |
| "TIME_FORMAT_UNKNOWN" | Unsupported time format | Check numeric value range; see ETL_GUIDE.md table |
| "Batch insert failed: unique constraint" | Duplicate rows | Expected—ETL uses ON CONFLICT DO NOTHING |
| "NETCDF4_NOT_INSTALLED" | Missing library | `pip install netCDF4 xarray cftime` |
| "Empty file" | Zero-byte dataset | Safe to skip; likely metadata artifact |
| Time values wrong (year 3000) | Wrong reference date | Check NetCDF units: `ncdump -v time file.nc` |

---

## Parameter Mapping Tips

### Check Current Mappings

```bash
jq '.parameter_mapping' config_parameter_mapping.json | jq 'keys' | head -20
```

### Add Custom Mapping

```bash
# Edit config_parameter_mapping.json
"MY_PARAM": ["MY_CODE", "custom", "unit"]

# Then run ETL with config
python populate_measurements.py --config config_parameter_mapping.json
```

### Find Unmapped Parameters

```sql
SELECT parameter_code, COUNT(*)
FROM measurements
WHERE namespace = 'custom'
GROUP BY parameter_code
ORDER BY COUNT(*) DESC;
```

---

## Time Format Examples

```
"2023-12-19T04:50:00Z"  → ISO 8601 (standard)
44250                    → Days since 1900-01-01
19500                    → Days since 1970-01-01 (unix epoch)
1487                     → Months since 1900-01-01 (CF convention)
2023.5                   → Decimal year (mid-2023)
2023                     → Year integer
1640000000               → Unix timestamp (seconds)
year=2023, month=12,    → Compound columns
 day=19, hour=04
```

---

## CSV Column Names (Auto-Detected)

```
Time column (pick one):
  time, date, datetime, timestamp

Value column (pick one):
  value, concentration, measurement, result

Parameter column (optional):
  parameter, variable, code

Spatial (optional):
  latitude, lat, northing, y
  longitude, lon, easting, x
  depth, z, altitude, level
```

---

## Performance

```bash
# Monitor insert rate
watch -n 5 'psql -c "SELECT COUNT(*) FROM measurements"'

# After bulk insert, optimize
psql -c "ANALYZE measurements;"
psql -c "REINDEX TABLE measurements;"

# Check table size
psql -c "SELECT pg_size_pretty(pg_total_relation_size('measurements'));"
```

---

## File Locations

```
huon-channel-marine-analytics/
├── diagnostic_etl.py              ← Run first
├── populate_measurements.py    ← Main ETL
├── populate_spatial.py            ← Shapefiles
├── populate_biological.py         ← Species observations
├── config_parameter_mapping.json  ← Edit for custom mappings
├── docs/ETL_GUIDE.md             ← Full documentation
├── diagnostic_report.json         ← Generated output
└── AODN_data/                    ← Your dataset directory
    ├── Dataset1/
    ├── Dataset2/
    └── ...
```

---

## Useful Links

- [ETL Guide (Full)](docs/ETL_GUIDE.md)
- [BODC Parameters](https://www.bodc.ac.uk/data/parameters/)
- [CF Conventions](https://cfconventions.org/)
- [IMOS](https://imos.org.au/)
- [GitHub Repo](https://github.com/botheredbybees/huon-channel-marine-analytics)

---

## Common Patterns

### Ingest + Verify

```bash
python populate_measurements.py --dataset "Name" && \
psql -c "SELECT COUNT(*) FROM measurements WHERE uuid = 'UUID';"
```

### Test Before Full Ingest

```bash
python populate_measurements.py --dataset "Name" --limit 100
# Check results, then remove --limit
```

### Reload Specific Dataset

```bash
# Delete old data
psql -c "DELETE FROM measurements WHERE metadata_id = 123;"

# Re-run ETL
python populate_measurements.py --dataset "Name"
```

### Generate Diagnostic Again

```bash
rm diagnostic_report.json
python diagnostic_etl.py
```

---

## Notes

- ✅ ETL is idempotent (safe to re-run)
- ✅ Duplicates are skipped (ON CONFLICT DO NOTHING)
- ✅ All timestamps converted to UTC
- ✅ Graceful error handling (logs issues, continues)
- ⚠️ Large datasets (>1M rows) may take 5-10 minutes
- ⚠️ First parameter mapping load is comprehensive; update as needed

---

**Last Updated:** 2025-12-19

**Questions?** See `docs/ETL_GUIDE.md` or check code docstrings.
