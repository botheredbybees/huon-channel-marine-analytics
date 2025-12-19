# Huon Channel Marine Analytics Platform

Comprehensive marine data platform for analyzing oceanographic, biological, and environmental datasets from the Huon Estuary and D'Entrecasteaux Channel, Tasmania.

## Features

- **TimescaleDB** for high-performance time-series data
- **PostGIS** for spatial analysis
- **Grafana** dashboards for visualization
- **pgAdmin** for database management
- **ETL pipeline** for AODN/IMOS data ingestion
- **Parameter standardization** via database-backed mappings

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.9+
- Git

### 1. Clone Repository

```bash
git clone https://github.com/botheredbybees/huon-channel-marine-analytics.git
cd huon-channel-marine-analytics
```

### 2. Start Services

```bash
docker-compose up -d
```

This starts:
- **TimescaleDB** on port 5433
- **Grafana** on port 3000
- **pgAdmin** on port 8088

### 3. Initialize Database

The database schema is automatically created via `init.sql`. To populate parameter mappings:

```bash
# Install Python dependencies
pip install -r requirements.txt

# Populate parameter mappings from JSON config
python populate_parameter_mappings.py
```

**Expected output:**
```
2025-12-20 09:00:00 - [INFO] Loaded config from config_parameter_mapping.json
2025-12-20 09:00:00 - [INFO] Connected to database successfully
2025-12-20 09:00:01 - [INFO] Inserted 90 new mappings
2025-12-20 09:00:01 - [INFO] Total parameter mappings in database: 90
```

### 4. Verify Setup

```bash
# Check database
docker exec marine_timescaledb psql -U marine_user -d marine_db -c "\dt"

# Check parameter mappings
docker exec marine_timescaledb psql -U marine_user -d marine_db -c \
  "SELECT COUNT(*) FROM parameter_mappings;"
```

## Data Ingestion

See **[docs/data_ingestion.md](docs/data_ingestion.md)** for complete guide.

### Quick ETL Workflow

```bash
# 1. Download AODN datasets to AODN_data/ directory
#    (See data_ingestion.md for instructions)

# 2. Run diagnostic scan
python diagnostic_etl.py

# 3. Ingest measurements (CSV/NetCDF)
python populate_measurements_v2.py

# 4. Ingest spatial features (Shapefiles)
python populate_spatial.py

# 5. Ingest biological observations
python populate_biological.py
```

## Access Services

### Grafana
- **URL**: http://localhost:3000
- **Username**: `admin`
- **Password**: `grafana123`

### pgAdmin
- **URL**: http://localhost:8088
- **Email**: `peter@huonestuary.local`
- **Password**: `pgadmin123`

**To connect to database in pgAdmin:**
1. Right-click "Servers" → Register → Server
2. **Name**: `marine_db`
3. **Host**: `marine_timescaledb`
4. **Port**: `5432`
5. **Username**: `marine_user`
6. **Password**: `marine_pass123`
7. **Database**: `marine_db`

### Direct Database Access

```bash
# Via docker exec (no password needed)
docker exec -it marine_timescaledb psql -U marine_user -d marine_db

# From host (requires password)
psql -h localhost -p 5433 -U marine_user -d marine_db
```

## Database Schema

See **[docs/database_schema.md](docs/database_schema.md)** for detailed documentation.

### Core Tables

- **`metadata`**: Dataset registry (ISO 19115 metadata)
- **`measurements`**: Time-series hypertable (sensor data)
- **`parameter_mappings`**: Parameter name standardization
- **`spatial_features`**: Polygons, lines (seagrass, kelp)
- **`species_observations`**: Biological surveys
- **`taxonomy`**: Species registry
- **`locations`**: Survey sites

### Example Queries

```sql
-- Find all temperature datasets
SELECT DISTINCT md.title
FROM measurements m
JOIN metadata md ON m.metadata_id = md.id
WHERE m.parameter_code = 'TEMP' AND m.namespace = 'bodc';

-- Get recent chlorophyll measurements
SELECT time, value, uom
FROM measurements
WHERE parameter_code = 'CPHL'
  AND time > NOW() - INTERVAL '30 days'
  AND quality_flag = 1
ORDER BY time DESC
LIMIT 100;

-- Count parameter mapping variants
SELECT standard_code, COUNT(*) as variants
FROM parameter_mappings
GROUP BY standard_code
ORDER BY variants DESC;
```

## Parameter Mappings

The system uses a hybrid approach:

1. **JSON config** (`config_parameter_mapping.json`): Source of truth with hints
2. **Database table** (`parameter_mappings`): Runtime lookup for ETL

### Adding Custom Mappings

**Option 1: Via JSON (recommended for bulk additions)**

1. Edit `config_parameter_mapping.json`:
   ```json
   "MY_CUSTOM_TEMP": ["TEMP", "custom", "Degrees Celsius"]
   ```

2. Re-run migration:
   ```bash
   python populate_parameter_mappings.py
   ```

**Option 2: Via SQL (for one-off additions)**

```sql
INSERT INTO parameter_mappings (raw_parameter_name, standard_code, namespace, unit, source)
VALUES ('CUSTOM_PARAM', 'MY_CODE', 'custom', 'units', 'user');
```

## Project Structure

```
huon-channel-marine-analytics/
├── docker-compose.yml              # Service definitions
├── init.sql                        # Database schema
├── requirements.txt                # Python dependencies
├── config_parameter_mapping.json   # Parameter mappings + hints
├── populate_parameter_mappings.py  # Migration script (JSON → DB)
├── diagnostic_etl.py               # Data diagnostics
├── populate_measurements_v2.py     # Time-series ETL
├── populate_spatial.py             # Spatial ETL
├── populate_biological.py          # Biological ETL
├── docs/
│   ├── data_ingestion.md           # Complete ingestion guide
│   ├── database_schema.md          # Schema documentation
│   ├── ETL_GUIDE.md                # ETL troubleshooting
│   └── ETL_QUICK_REFERENCE.md      # Command cheat sheet
├── grafana/
│   └── provisioning/               # Grafana dashboards
└── AODN_data/                      # Your datasets go here
    ├── Dataset_1/
    ├── Dataset_2/
    └── ...
```

## Maintenance

### Backup Database

```bash
# Backup all data
docker exec marine_timescaledb pg_dump -U marine_user marine_db > backup_$(date +%Y%m%d).sql

# Backup specific table
docker exec marine_timescaledb pg_dump -U marine_user -t measurements marine_db > measurements_backup.sql
```

### Restore Database

```bash
docker exec -i marine_timescaledb psql -U marine_user -d marine_db < backup_20251220.sql
```

### Reset Database

```bash
# WARNING: This deletes all data!
docker-compose down
docker volume rm huon-channel-marine-analytics_timescaledb_data
docker-compose up -d

# Re-populate parameter mappings
python populate_parameter_mappings.py
```

### Update Services

```bash
# Pull latest code
git pull

# Restart services
docker-compose down
docker-compose up -d

# Re-run migrations if schema changed
python populate_parameter_mappings.py
```

## Troubleshooting

### Database won't start

```bash
# Check logs
docker logs marine_timescaledb

# Verify healthcheck
docker ps
```

### ETL fails with "No measurements extracted"

- Check if dataset is spatial (use `populate_spatial.py`)
- Check if dataset is biological (use `populate_biological.py`)
- Run diagnostic: `python diagnostic_etl.py`

### Parameter mapping not found

```bash
# Check if mapping exists
docker exec marine_timescaledb psql -U marine_user -d marine_db -c \
  "SELECT * FROM parameter_mappings WHERE raw_parameter_name = 'YOUR_PARAM';"

# If missing, add to JSON and re-run migration
python populate_parameter_mappings.py
```

## Contributing

See individual scripts for docstrings and inline comments.

## License

MIT License - see LICENSE file

## References

- **AODN Portal**: https://portal.aodn.org.au/
- **IMOS Data**: https://imos.org.au/
- **TimescaleDB**: https://docs.timescale.com/
- **PostGIS**: https://postgis.net/
- **Grafana**: https://grafana.com/docs/

## Contact

For questions about this platform, see the documentation or open an issue on GitHub.
