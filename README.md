# Huon Estuary & D'Entrecasteaux Channel Marine Analytics

A specialized local analytics environment for exploring marine climate, water quality, and benthic ecology in Southeast Tasmania's Huon Estuary and D'Entrecasteaux Channel.

## Project Scope

This platform focuses on the complex waterways separating Bruny Island from the Tasmanian mainland. It integrates disparate datasets to analyze:

*   **Benthic Habitats**: Seagrass meadows (*Posidonia*, *Heterozostera*) and *Macrocystis pyrifera* (giant kelp) forests in the channel.
*   **Water Quality**: Long-term temperature records, chlorophyll-a, and nutrient profiles critical for local aquaculture.
*   **Estuarine Health**: Condition assessments from the Southern NRM region and potential anthropogenic stressors.
*   **Species Distribution**: Reef community surveys (RLS), invasive species (e.g., *Asterias amurensis*), and spotted handfish (*Brachionichthys hirsutus*) population monitoring.

## Data Inventory

The system ingests and standardizes data from:

*   **AODN/IMOS**: Coastal station moorings and satellite altimetry/SST validated for coastal waters.
*   **Derwent & Huon Estuary Programs**: Historical nutrient loads and water quality indicators.
*   **Reef Life Survey (RLS)**: Rocky reef biodiversity (fish, invertebrates, macroalgae).
*   **UTAS/IMAS**: Specific research on salmon cage bathymetry and dissolved oxygen levels.

## Architecture

For a detailed breakdown of tables, relationships, and spatial extensions, see the [Database Schema Documentation](docs/database_schema.md).

### Database Schema (PostGIS + TimescaleDB)

*   **`spatial.regions`**: Polygons defining the D'Entrecasteaux Channel, Huon River, and North/South Bruny management zones.
*   **`measurements.timeseries`**: Hypertables for high-frequency sensor data (CTD profiles, temperature loggers).
*   **`ecology.observations`**: Benthic survey points mapped against substrate types.

### Analysis Stack

*   **Python**: `geopandas` for spatial joins between survey points and habitat maps; `xarray` for satellite anomalies.
*   **Docker**: Containerized PostgreSQL/TimescaleDB instance.
*   **Grafana**: Dashboards tracking marine heatwave intensity in the Channel.

## Getting Started

### Prerequisites

*   Docker & Docker Compose
*   Python 3.9+
*   16GB RAM recommended (for processing satellite NetCDF files)

### Installation

1.  **Clone the repository:**
    ```
    git clone https://github.com/botheredbybees/huon-channel-marine-analytics.git
    cd huon-channel-marine-analytics
    ```

2.  **Start the local database:**
    ```
    docker-compose up -d
    ```

3.  **Run the schema initialization** (tables & spatial extensions):
    ```
    docker exec -i marine-db psql -U postgres -d huon_data < init.sql
    ```

## Project Structure
    ```
    huon-channel-marine-analytics/
    ├── analysis/ # Notebooks for specific channel questions (e.g., "kelp_loss_2016-2023.ipynb")
    ├── docker/ # Container configurations
    ├── docs/ # Schema documentation and data dictionaries
    ├── etl_scripts/ # Python scripts to parse AODN NetCDF and CSVs
    ├── grafana/ # Dashboards for "River Derwent" vs "Channel" comparisons
    └── qgis_styles/ # Layer definition files (.qml) for visualizing habitats
    ```

## Local Data Policy

**Note:** Raw data files (NetCDF, Shapefiles) are stored locally and are **not** committed to this repository. See `.gitignore` for exclusion rules.

## Contact

**Peter Shanks** - [botheredbybees@gmail.com](mailto:botheredbybees@gmail.com)
