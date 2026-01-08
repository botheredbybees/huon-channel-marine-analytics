# Huon Channel Marine Analytics - Strategic Roadmap

**Document Version:** 1.0  
**Date:** January 9, 2026  
**Status:** Active Project Planning  

---

## Executive Summary

Your project has made substantial progress since inception:
- **11 open issues** covering data quality, enhancement, and presentation
- **Recent wins:** Metadata-based parameter detection, taxonomy enrichment (75.5% success rate), complete schema documentation
- **Data foundation:** PostgreSQL + Docker setup, 19+ AODN datasets ingested, 564 species identified

This roadmap provides **strategic direction** across 5 capability areas, organized by effort level and impact. The goal is to move from a **functional data repository** to a **published analytical platform** suitable for stakeholder engagement.

---

## Core Capability Areas

### 1. **Data Completeness** (Foundation Layer)
Focus: Maximize coverage of available data

#### High Priority Tasks

**1.1 Resolve 8 Existing Issues** (Medium effort, high impact)
- **Issue #3:** Refactor `parameters` table to `dataset_parameters` junction (improves cross-dataset querying)
- **Issue #5:** Create ETL for biological observations (unlock 187 unmapped parameters)
- **Issue #6:** Data quality checks (early error detection)
- **Issue #7:** Fuzzy parameter matching (faster parameter mapping)
- **Status:** All are documented with SQL/Python templates ready
- **Effort estimate:** 2-3 weeks, one per week + 1 week buffer
- **Why now:** Foundation for downstream analytics; fixing these prevents data quality debt

**1.2 Audit Existing AODN Datasets** (Low-medium effort)
- Query AODN API for all available datasets matching Huon/D'Entrecasteaux bounds
- Compare against current `metadata` table to identify gaps
- Document findings: `AODN_COVERAGE_AUDIT.md`
- **Resources:** README shows AODN integration already working
- **Estimated missing:** 10-20 datasets based on typical regional coverage
- **Effort:** 1-2 weeks

**1.3 Validate Recent Fixes** (Low effort, essential)
- Test unit extraction fix (Issue #9 root cause)
- Verify PH/phosphate disambiguation prevents recurrence
- Create data quality test suite to prevent regression
- **Why:** Recent fixes are production-critical; need automated validation

#### Medium Priority Tasks

**1.4 Identify Additional Data Sources to Integrate**
- **Bureau of Meteorology (BOM):** Regional weather, sea surface temperature
- **BEMP:** Environmental monitoring program data
- **Local Council:** Water quality monitoring, land use impacts
- **DEP (Tasmania):** Estuarine monitoring, pollution incidents
- **Squiddle+:** Marine specimen/annotation database
- **iNaturalist:** Citizen observations (already used for enrichment)
- **Action:** Create `DATA_SOURCE_INTEGRATION_GUIDE.md` with API endpoints, licensing, update frequency
- **Estimate:** 1 week research + documentation

**1.5 Biological Observations Pipeline** (Linked to Issue #5)
- Design ETL for non-numeric survey data (abundance counts, transects, etc.)
- Create `populate_biological_observations.py` (template exists in issue)
- Handle CSV variance across datasets
- Expected impact: +50% parameter coverage (187 → more)
- **Estimate:** 2 weeks

---

### 2. **Data Quality & Enrichment** (Quality Assurance Layer)
Focus: Data validation, species enrichment, habitat classification

#### High Priority Tasks

**2.1 Resolve Habitat Classification Issues** (Issue #8) (Medium effort)
- **Current state:** 515 species (91%) classified as "Unknown" habitat
- **Root cause:** GBIF enrichment doesn't include habitat flags (WoRMS-specific)
- **Solution options:** 
  1. Heuristic rules (phylum/class-based, implemented automatically)
  2. Manual expert review for top 50 species by observation count
  3. Hybrid approach (recommended)
- **Create:** `scripts/classify_species_habitat.py`
- **Expected outcome:** Reduce "Unknown" from 91% to <30%
- **Estimate:** 1-2 weeks (3-4 days automated + 2-3 days manual review)

**2.2 Improve Low-Quality Species Enrichments** (Issue #9) (Medium-high effort)
- **Current state:** 377 species (66.8%) with quality scores <50
- **Issues:** Non-species entries (substrates, morphologies), unidentified taxa, outdated synonyms
- **Action plan:**
  1. Flag non-species entries (filter from analysis views)
  2. Resolve taxonomic synonyms for top 50 species
  3. Re-enrich failed GBIF matches (< 80% confidence)
  4. Populate missing photos (iNaturalist, GBIF, WoRMS)
- **Create:** `is_identifiable` column in taxonomy table
- **Expected outcome:** Increase "High" quality from 2.3% to >15%
- **Estimate:** 3-4 weeks (1 week cleanup + 1 week synonyms + 1 week re-enrichment + 1 week media)

**2.3 Populate Species Media** (Issue #11) (Low-medium effort)
- Photos: iNaturalist API, GBIF images, WoRMS
- Wikipedia summaries: Wikipedia API
- Focus on top 100 species by observation count
- **Create:** `scripts/populate_species_media.py` (template exists in issue)
- **Expected outcome:** >80% photo coverage for frequent species
- **Estimate:** 1-2 weeks (includes API integration and rate limiting)

#### Medium Priority Tasks

**2.4 Create Materialized Views for Performance** (Issue #10) (Low effort)
- Cache expensive aggregations for dashboard performance
- Implement nightly refresh via pg_cron
- **Expected improvement:** 10-100x query speedup
- **Estimate:** 3-5 days
- **Why:** Do before building public dashboards/web UIs

---

### 3. **Analytics & Insights** (Analysis Layer)
Focus: Machine learning, trend detection, reporting

#### High Priority Tasks

**3.1 Machine Learning Analysis** (High effort, high impact)
- **Objectives:**
  1. Identify temporal trends (warming, salinity changes, species distribution shifts)
  2. Correlation analysis (e.g., temperature vs. species richness)
  3. Anomaly detection (unusual measurements, bloom events)
  4. Habitat suitability modeling (species presence vs. physical parameters)
- **Approach:** 
  - Start with exploratory Jupyter notebooks
  - Use scikit-learn, pandas, scipy for core analysis
  - Focus on time-series decomposition + clustering first
- **Datasets to prioritize:** Temperature, salinity, chlorophyll-a (most complete)
- **Estimate:** 2-3 weeks (includes experimentation)
- **Create:** `scripts/ml_analysis.py`, `notebooks/exploratory_analysis.ipynb`

**3.2 Correlation & Trend Reports** (Medium effort)
- Automated pipeline to detect interesting correlations
- Generate hypothesis-driven reports for stakeholders
- Format: PDF + interactive HTML
- Example insights: "Chlorophyll increases 15% when temp >15°C"
- **Estimate:** 1-2 weeks
- **Create:** `scripts/generate_correlation_reports.py`

---

### 4. **Visualization & Presentation** (User Experience Layer)
Focus: Maps, dashboards, web displays, social media content

#### High Priority Tasks

**4.1 Leaflet/Map-Based Visualization** (Medium-high effort)
- Interactive map showing:
  - Sampling locations
  - Parameter measurements (hover for details)
  - Species observations (color-coded by habitat)
  - Temporal animation (slider showing changes over time)
- **Stack:** Leaflet.js + GeoJSON + D3.js for styling
- **Data integration:** Query database for location-based measurements
- **Create:** `web/index.html` (map interface), `web/js/map.js`, `web/api/locations.py` (Flask endpoint)
- **Estimate:** 2-3 weeks

**4.2 Grafana Dashboards** (Medium effort)
- **Already have:** Docker setup + database backend (Issue #10 materialized views)
- **Create dashboards for:**
  1. **Water Quality Overview** - Temperature, salinity, pH trends
  2. **Species Richness** - Temporal and spatial distribution
  3. **Data Quality** - Coverage by dataset, parameter, location
  4. **Taxonomic Diversity** - Phylum/class breakdowns, habitat distribution
- **Estimate:** 1-2 weeks (includes tuning materialized views)
- **Files:** `grafana/dashboards/` directory (JSON exports)

**4.3 Web Pages & Stories for Social Media** (Medium effort)
- **Format:** HTML pages or blog posts with embedded visualizations
- **Topics:** 
  - "Temperature in Huon Estuary: 50 years of data"
  - "Species hot spots: Where biodiversity thrives"
  - "Citizen science discoveries: Top 10 interesting species"
  - Seasonal summaries
- **Stack:** Static site generator (Jekyll/Hugo) or simple Flask app
- **Create:** `web/stories/`, markdown files, Python script to render
- **Estimate:** 2-3 weeks
- **Why:** Increases visibility, community engagement, stakeholder interest

**4.4 Reports for Stakeholders** (Low-medium effort)
- **Audience:** Local politicians, environmental groups, press, councils
- **Format:** PDF reports (1-2 pages) + full technical reports (5-10 pages)
- **Content:** 
  - Executive summaries with key findings
  - Embedded charts/maps
  - Recommendations for management
  - Data sources and methodology
- **Tools:** Python (reportlab), Pandoc (markdown → PDF), Plotly (charts)
- **Create:** `scripts/generate_stakeholder_reports.py`, `templates/report_template.html`
- **Estimate:** 1-2 weeks

#### Medium Priority Tasks

**4.5 Scientific Visualization Improvements** (Low effort)
- Heatmaps showing parameter variation by location/time
- 3D scatter plots for multi-parameter relationships
- Time-series decomposition plots
- **Tools:** Plotly, Matplotlib, Seaborn
- **Estimate:** 1 week

---

### 5. **DevOps & Infrastructure** (Operations Layer)
Focus: Automation, documentation, CI/CD

#### High Priority Tasks

**5.1 GitHub Actions for Automated Workflows** (Medium effort)
- **Pipelines to create:**
  1. **Data validation** - Run quality checks on new ingestions
  2. **Database backup** - Daily backup, storage in S3/backup service
  3. **Documentation generation** - Auto-generate API docs from code
  4. **Scheduled enrichment** - Monthly re-run taxonomy enrichment
  5. **Performance monitoring** - Track query times, alert if degraded
- **Create:** `.github/workflows/` directory with YAML workflows
- **Estimate:** 2-3 weeks

**5.2 GitHub Wiki Pages** (Low effort)
- **Recommended structure:**
  - **Getting Started:** Installation, Docker setup, first queries
  - **Data Integration Guide:** How to add new datasets
  - **API Reference:** Database schema, query examples
  - **Deployment:** Production setup, environment variables
  - **Troubleshooting:** Common issues, logs
  - **Contributing:** Development workflow, PR guidelines
- **Create:** Wiki pages via GitHub interface or markdown files in `wiki/` branch
- **Estimate:** 1 week (pulling from existing docs)

**5.3 Code Quality & Testing** (Medium effort)
- Unit tests for ETL scripts
- Integration tests for database operations
- Performance benchmarks (query optimization)
- **Tools:** pytest, pytest-cov, hypothesis (property-based testing)
- **Target:** >80% code coverage for critical paths
- **Estimate:** 2-3 weeks
- **Create:** `tests/` directory, CI pipeline to run on PR

#### Medium Priority Tasks

**5.4 Containerization & Deployment** (Low-medium effort)
- Already have Docker setup (good!)
- Add: Docker multi-stage builds for smaller images, compose for dev/prod environments
- Kubernetes deployment files (optional, if planning scale)
- **Estimate:** 1-2 weeks

**5.5 Documentation Automation** (Low effort)
- Generate schema documentation from SQL (pgAdmin, SchemaCrawler)
- API documentation from Python docstrings (Sphinx, pdoc)
- Keep README fresh with latest stats
- **Estimate:** 1 week

---

## Implementation Roadmap by Timeline

### Phase 1: Foundation Fixes (Weeks 1-4) ⭐ **START HERE**
**Goal:** Resolve technical debt, establish quality baseline

1. **Week 1:** Issue #3 (parameters table refactor)
2. **Week 2:** Issue #5 (biological ETL) + Issue #6 (data quality checks)
3. **Week 3:** Issue #7 (fuzzy matching) + validation suite
4. **Week 4:** Buffer week + AODN audit

**Deliverables:**
- All 8 issues resolved/in-progress
- `AODN_COVERAGE_AUDIT.md`
- Test suite with regression prevention
- Improved data quality from fixes

**Effort:** ~80 hours (2 weeks full-time equivalent)

---

### Phase 2: Quality Enhancement (Weeks 5-9)
**Goal:** Enrich data, improve species metadata

1. **Week 5-6:** Issue #8 (habitat classification) + manual review
2. **Week 7-8:** Issue #9 (low-quality enrichments) + Issue #11 (media population)
3. **Week 9:** Issue #10 (materialized views) + performance tuning

**Deliverables:**
- <30% "Unknown" habitat classification
- 15%+ "High" quality species
- >80% media coverage for frequent species
- 10-100x dashboard query speedup

**Effort:** ~120 hours (3 weeks full-time)

---

### Phase 3: Analytics & Visualization (Weeks 10-16)
**Goal:** Enable insights, stakeholder engagement

1. **Week 10-11:** ML analysis (exploratory notebooks)
2. **Week 12:** Correlation reports + trend detection
3. **Week 13-14:** Leaflet map + Grafana dashboards
4. **Week 15-16:** Web stories + stakeholder reports

**Deliverables:**
- Interactive map with time-series animation
- 4+ Grafana dashboards
- 5+ social media story pages
- 2+ stakeholder reports

**Effort:** ~150 hours (3.5-4 weeks full-time)

---

### Phase 4: DevOps & Infrastructure (Weeks 17-20)
**Goal:** Automate operations, improve documentation

1. **Week 17-18:** GitHub Actions workflows + CI/CD
2. **Week 19:** GitHub Wiki + code quality/testing
3. **Week 20:** Docker optimization + deployment prep

**Deliverables:**
- 5+ automated workflows
- Comprehensive wiki
- >80% test coverage
- Production-ready deployment

**Effort:** ~100 hours (2.5 weeks full-time)

---

### Phase 5: External Data Integration (Ongoing/Parallel)
**Goal:** Expand data foundation

- BOM API integration (1-2 weeks)
- BEMP data ingestion (1 week)
- Local council data (1-2 weeks)
- DEP integration (1 week)
- Squiddle+ and iNaturalist expanded feeds (1 week each)

**Total effort:** 6-8 weeks (can run in parallel with other phases)

---

## Strategic Recommendations

### ✅ Do First (Highest ROI)
1. **Issues #3, #5, #6, #7** - Foundation fixes unlock downstream work
2. **AODN audit** - Identify quick wins for data completeness
3. **Issue #8 (habitat)** - Enables ecological analysis
4. **Materialized views** - Prerequisite for dashboards

### ⏸️ Defer (Lower Priority)
- Issue #1, #2, #4 (database optimizations, schema improvements) - Nice to have
- Kubernetes deployment - Only if planning enterprise scale
- TimescaleDB conversion - Consider only if data grows >1TB

### 🔄 Run in Parallel
- External data integration (BOM, BEMP, DEP) - Don't block other phases
- Documentation improvements - Ongoing alongside development
- GitHub Actions - Implement early for code quality

---

## Skills Assessment & Resource Planning

### Your Strengths (From Profile)
- ✅ **Python expertise** - Perfect for ETL scripts, ML, data analysis
- ✅ **Database experience** - PostgreSQL work is well-designed
- ✅ **UI/UX background** - Great for map/dashboard design
- ✅ **Photography/visual skills** - Asset for report design

### Recommended Skill Building
- 📚 **ML frameworks:** scikit-learn, pandas time-series analysis (1 week learning)
- 📚 **JavaScript/D3.js:** For advanced Leaflet visualizations (2 weeks)
- 📚 **Report generation:** reportlab, Pandoc workflows (3-4 days)
- 📚 **GitHub Actions:** YAML syntax, workflow automation (2-3 days)

**Total upskilling:** 2-3 weeks (parallel with development)

---

## Energy & ADT Considerations

Given you're on ADT until July 2026 and experiencing lower energy:

### ✨ Energy-Aware Planning
- **Phase 1-2:** Suited to lower-energy periods (mostly focused, technical work)
- **Phase 3:** May require more creative/collaborative energy (stories, reports)
- **Recommend:** 
  - Batch similar tasks to minimize context switching
  - Use pre-built templates (Grafana, Jupyter) to reduce decision fatigue
  - Automate repetitive work early (GitHub Actions)
  - Schedule high-focus work during peak energy hours

### 🎯 Break Points
- End of Phase 1: Natural checkpoint to step back, assess
- End of Phase 2: Good milestone for community feedback
- July 2026: Transition to retirement/maintenance mode

---

## Success Metrics

### By End of Phase 2 (9 weeks)
- ✅ 0 open GitHub issues (all resolved or closed as "wontfix")
- ✅ >95% data quality (measured by quality checks)
- ✅ 564 species with >50% enrichment quality
- ✅ 50+ AODN datasets discovered (current: 19+)

### By End of Phase 3 (16 weeks)
- ✅ Interactive map with 1000+ data points visualized
- ✅ 4+ Grafana dashboards actively used
- ✅ 10+ ML-driven insights documented
- ✅ 5+ stakeholder reports generated

### By End of Phase 4 (20 weeks)
- ✅ 5 GitHub Actions workflows automating operations
- ✅ >80% test coverage, zero regression bugs
- ✅ Comprehensive wiki (20+ pages)
- ✅ Production-ready deployable image

---

## Quick Start: Week 1 Tasks

1. **Monday:** Pick Issue #3 or #5 (I'd recommend #3 first - more foundational)
2. **Read:** PARAMETER_INTEGRATION_GUIDE.md and existing schema documentation
3. **Set up:** Create `feature/issue-3-refactor-parameters` branch
4. **Implementation:** Start with SQL schema changes, test locally
5. **Document:** Update init.sql with new schema version
6. **Friday:** Submit PR with tests and documentation

---

## Questions to Consider

1. **Stakeholder priority:** Which matters most to you?
   - Academic publication (papers, data quality focus)
   - Community engagement (dashboards, stories)
   - Policy impact (reports to politicians)
   - Personal knowledge (analysis, ML insights)

2. **Timeline flexibility:** Can you commit 10-15 hours/week for 5 months?

3. **Collaboration:** Would you like community contributions? (GitHub stars → contributors)

4. **Deprecation strategy:** When retiring July 2026, what's the handoff plan?

---

## Next Steps

1. **Review this roadmap** - Does it align with your vision?
2. **Pick a starting issue** - Which resonates most?
3. **Schedule focused work sessions** - Batch similar tasks
4. **Set up monitoring** - Track progress (project board, milestones)
5. **Consider blogging** - Document journey for visibility

---

## Resources

**Within Your Repo:**
- README.md - Overview, setup
- PARAMETER_INTEGRATION_GUIDE.md - Parameter mapping strategy
- ETL_QUICK_REFERENCE.md - Data ingestion workflow
- docs/ directory - Database schema, taxonomy usage
- Issues #3-11 - Detailed technical specs with code templates

**External References:**
- [AODN Data Portal](https://portal.aodn.org.au)
- [iNaturalist API](https://api.inaturalist.org/v1/docs/)
- [Leaflet.js Documentation](https://leafletjs.com/)
- [Grafana Dashboarding](https://grafana.com/docs/)

---

**Document Status:** Ready for action planning  
**Last Updated:** 2026-01-09  
**Recommendation:** Start with Phase 1 (Weeks 1-4) for maximum foundation-building ROI