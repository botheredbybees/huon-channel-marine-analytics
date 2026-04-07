
how much of the CSIRO coastal water quality forecasting technology is publicly accessible? 

There's a remarkable amount of publicly accessible infrastructure that would directly complement the Huon/D'Entrecasteaux Channel project, and one CSIRO system in particular is almost purpose-built for the region.

## The Storm Bay System: Your Most Direct Asset

CSIRO's **Storm Bay Modelling & Information System** is the single most relevant resource you could tap [1]. It covers southeast Tasmania specifically — Storm Bay feeds directly into the D'Entrecasteaux Channel — and provides a **public dashboard** at `stormbaymodelling.csiro.au` with hindcast and near-real-time simulated circulation and water quality data. Completed in 2023 and funded through FRDC for the Tasmanian DPIPWE, it's publicly accessible to researchers and stakeholders, with login-gated access to direct model results and decision support tools [1]. This is exactly the kind of regional biogeochemical model output that could serve as a validation layer or forcing input for your own analytics pipeline.

## AquaWatch: Scalable but Still Maturing

CSIRO's **AquaWatch Australia** mission is the broader national platform, powered by their Earth Analytics Science and Innovation Platform (EASI) DataCube technology [2]. It integrates multi-sensor satellite EO data with in-situ sensors for near-real-time water quality forecasts. Access is structured in tiers:

- **Base Subscription** — web portal and dashboard access for your region of interest
- **Processing Subscription** — direct access to the ADS for your own analysis and visualisations [2]
- **Collaborative access** — for researchers wanting to integrate their own sensor data into the platform

AquaWatch is ML-driven, testing both full physics/biogeochemical models and pure AI approaches against each other [3], which would make it highly relevant to compare against your own TimescaleDB analytics architecture. The system targets deployment by 2030 for public access, but research-tier access is available now [4].

## CSIRO Data Access Portal: Open API

CSIRO's **Data Access Portal** (DAP) at `data.csiro.au` provides a public **RESTful API** (`https://data.csiro.au/dap/ws/v2/collections`) for programmatic read access to published datasets [5]. This is directly consumable from Python and would fit naturally into your existing stack. Given the EPA Tasmania already relies on CSIRO hydrodynamic and biogeochemical models for the Huon/D'Entrecasteaux BEMP program [6], there's likely relevant published data there already.

## The EPA Tasmania Connection

This is perhaps the most important finding. The Tasmanian EPA's **Broadscale Environmental Monitoring Program (BEMP)** for the Huon Estuary and D'Entrecasteaux Channel explicitly commits to [6]:

- Continuing use of CSIRO hydrodynamic and biogeochemical models for diagnosis and prognosis
- Trialling continuous real-time water quality monitoring
- Commissioning IMAS to undertake advanced statistical analysis of the 20-year BEMP dataset

That 20-year BEMP dataset, combined with IMAS's involvement, represents a significant pre-existing data asset that your project could potentially align with or build upon — and it maps directly onto the "institutional data depth" success predictor we identified earlier [6][7].

## Practical Integration Path

Given your Python/PostgreSQL/TimescaleDB stack, the most logical architecture would be:

- **Storm Bay model outputs** as regional forcing and validation data
- **AquaWatch satellite EO data** (Landsat-8 derived SST, suspended sediment, chlorophyll) via Processing Subscription or DAP API
- **EPA BEMP in-situ monitoring data** as ground truth for model validation
- **Your own sensor network** (IoT/MQTT) feeding TimescaleDB as the real-time layer on top

The CSIRO coastal forecasting project is explicitly testing ML approaches against physics-based models [3], which means your project sits right at the frontier of what they're investigating — making a formal collaboration or data-sharing arrangement with CSIRO Oceans & Atmosphere in Hobart a realistic and worthwhile pursuit.


## 1. What from CSIRO is actually accessible?

**Storm Bay Modelling & Information System**

- Public dashboard: hindcast and near‑real‑time simulated circulation and water quality for southeast Tasmania (including Storm Bay), plus selected observations and publications, is available to the general public via the Storm Bay data portal.[1]
- Advanced access: project stakeholders and “expert users” can log in for **direct access to model results and decision‑support tools**.[1]
- Code vs data: the page makes no mention of open‑sourcing the hydrodynamic/biogeochemical model code; what’s clearly accessible is the **model outputs and associated information system**, not the full modelling system itself.[1]

**CSIRO Data Access Portal (DAP)**

- Public REST API at `https://data.csiro.au/dap/ws/v2/collections` provides **read‑only programmatic access** to collections: search by keyword, location, time, list files, download files under 10 GB, etc.[2]
- This is the main route to obtain **published CSIRO model results and related datasets** (e.g. NetCDF, CSV) that you can ingest into your own DB.[2]

**AquaWatch**

- AquaWatch is being built as a **cloud‑based, integrated monitoring and forecasting system**, combining satellite EO and in‑situ sensors.[3]
- Public pages emphasise collaboration and decision‑support, but don’t yet expose a general open API like DAP; access currently looks to be through **partnerships and project‑level collaboration** rather than fully open feeds.[3]

So: you can **freely consume published CSIRO datasets via DAP and Storm Bay outputs via the portal**, but not simply clone the entire AquaWatch/Storm Bay modelling stack.

***

## 2. High‑level integration strategy

Given the above, a pragmatic integration strategy for your Huon/Channel project:

1. Use **Storm Bay model outputs** (via the portal and/or DAP) as an *outer boundary* / regional context field around your local observations and analytics.[1][2]
2. Use **DAP** to discover and download any CSIRO collections that directly reference “Storm Bay”, “D’Entrecasteaux Channel”, “Huon Estuary”, “coastal Tasmania”, etc., and ingest them into TimescaleDB.[2]
3. Design your schema so **in‑situ time series (BEMP, your sensors, loggers) and CSIRO model/EO data can be joined in space and time**, enabling validation, anomaly detection, and “what if” scenarios.  
4. Treat AquaWatch as a **future integration** (e.g. if you end up in a collaboration or get project access) rather than a dependency.

***

## 3. Phase 1 – Requirements & data inventory

**3.1 Clarify internal goals (your side)**

Even without asking you more questions, the plan should assume you’ll explicitly document:

- Primary use cases:  
  - Characterise long‑term water quality trends in the Channel/Huon.  
  - Compare **local conditions vs regional context** (Storm Bay / shelf signals).  
  - Provide evidence for discussions around salmon expansion, BEMP settings, etc.

- Success criteria (examples):  
  - Ability to overlay your local time‑series against Storm Bay model fields for key variables (temp, salinity, nutrients, chlorophyll, oxygen, turbidity) at daily or better resolution.[1]
  - Reproducible notebooks that compute bias/RMSE between model outputs and in‑situ observations over defined time windows.

**3.2 Discover relevant CSIRO datasets**

Tasks:

- Use DAP API (`/ws/v2/collections`) to search for keywords: “Storm Bay”, “FRDC 2017‑215”, “D’Entrecasteaux Channel”, “Huon Estuary”, “coastal Tasmania”, “salmon aquaculture”, “biogeochemical”.[1][2]
- For each candidate collection, record in a small catalog (YAML/JSON or DB table):  
  - DAP identifier  
  - Variables available  
  - Spatial extent & resolution  
  - Time coverage & resolution  
  - File formats (Likely NetCDF, GeoTIFF, CSV)  

Deliverable: a “CSIRO data inventory” document or table for your repo.

***

## 4. Phase 2 – Data access integration

**4.1 Build a small DAP client in Python**

Using `requests` or `httpx`:

- Implement search endpoints against `/dap/ws/v2/collections` with filters by keyword and bounding box (Huon/Channel/Storm Bay region).[2]
- Implement listing and download of collection files. For larger NetCDF files, consider local storage with metadata in TimescaleDB rather than slurping raw fields into the DB.

You’d likely create a module like `csiro_dap_client.py` with:

- `search_collections(query, bbox=None, date_range=None)`  
- `list_files(collection_id)`  
- `download_file(collection_id, file_path)`  

**4.2 Storm Bay portal integration**

The portal itself (`stormbaymodelling.csiro.au`) is documented as a dashboard with public and authenticated views, but not as a formal API.[1]

- First step: inspect the network calls from the public dashboard (e.g. NetCDF/JSON tiles, WMS/WFS services) to see what’s being pulled.  
- If the data is served via standard spatial services (OGC WMS/WFS, THREDDS/OPeNDAP), write thin wrappers to retrieve gridded data for:  
  - Bounding boxes intersecting the D’Entrecasteaux Channel mouth  
  - Time windows matching your local records  

If programmatic access is restricted to login users, you may need to:

- Request “expert user” access as an independent researcher, referencing the FRDC project and your analytics work.[1]

***

## 5. Phase 3 – Schema and data model in your project

Assuming TimescaleDB/postgis, introduce schema elements like:

- `csiro_model_grid`  
  - `grid_id`, `source` (e.g. "StormBay_2023"), `i`, `j`, `lat`, `lon`, `depth`  

- `csiro_model_timeseries`  
  - `time`, `grid_id`, `temp`, `salinity`, `nitrate`, `phosphate`, `chlorophyll`, `oxygen`, `turbidity`, `source_run_id`  

- `csiro_satellite_products` (for future AquaWatch/EO integration)  
  - `time`, `lat`, `lon`, `sst`, `chl_a`, `turbidity`, `data_source`  

- Mapping tables from **your stations / BEMP sites to model grid points** (nearest neighbour or interpolation weights).

Design considerations:

- Use PostGIS for storing station locations and computing nearest grid points / interpolations.  
- Keep raw files (NetCDF) on disk or object storage, with DB rows referencing file paths and indexes for fast access.

***

## 6. Phase 4 – Spatio‑temporal linking and validation

Once data is flowing in:

**6.1 Spatial linkage**

- For each in‑situ site (BEMP stations, your moorings, etc.), compute:  
  - Nearest Storm Bay model surface cell and perhaps a small neighbourhood for averaging.  
  - Distances to key hydrodynamic features if available (fronts, plume boundaries), using model diagnostics.[1]

**6.2 Temporal alignment**

- Decide on a canonical temporal resolution (e.g. hourly or daily means).  
- Build materialised views or TimescaleDB continuous aggregates that:  
  - Resample in‑situ data to that resolution.  
  - Sample / average model fields to that resolution.  

**6.3 Validation metrics & visualisation**

Create a set of standard analysis notebooks:

- Time‑series overlays: local observation vs Storm Bay model for key variables at each site.  
- Error metrics: bias, RMSE, correlation, seasonally decomposed where relevant.  
- Maps: difference fields for selected dates to show where model under/overestimates conditions near the Channel.

***

## 7. Phase 5 – Higher‑level analytics and storytelling

With the plumbing in place, you can then address more interesting questions:

- How often is the Channel “out of family” with regional Storm Bay conditions for key water quality parameters?  
- Do certain modelled circulation patterns correspond with observed problem states (e.g. algal blooms, low oxygen events) in the Channel?  
- Under the aquaculture expansion scenarios explored in the Storm Bay model, what does your local data suggest about **downstream** impacts into Storm Bay vs **upstream** feedbacks into the Channel?[1]

This is where the integration becomes very powerful for communication with regulators and the community.

***

## 8. Phase 6 – Collaboration and sustainability

Finally, to make this more than a one‑off integration:

- Reach out to the **Storm Bay project lead (Dr Karen Wild‑Allen)** referencing the FRDC 2017‑215 project and the final report.[1]
- Clarify what’s permissible in terms of re‑publishing derived products from the Storm Bay outputs within an open‑source GitHub project (your repo).  
- Explore whether your Huon/Channel analytics can be treated as a “nested” local component of any future **digital twin** style work they’re doing for Storm Bay.[1]

***

Next steps:

- Propose a concrete folder/module structure and naming for the GitHub repo (e.g. `data_sources/csiro_dap`, `pipelines/storm_bay_ingest.py`, `notebooks/validation_csiro.ipynb`) 
- Draft a small Python DAP client skeleton tailored to TimescaleDB that can be dropped straight into the project.

