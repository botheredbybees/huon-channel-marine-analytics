# Quick Reference: Issue Matrix & Strategic Priorities

## At-a-Glance Issue Status & Selection

### Current Open Issues (8 Total)

| # | Title | Category | Effort | Impact | Status | Start? |
|---|-------|----------|--------|--------|--------|--------|
| **3** | Refactor parameters table | Schema | 🟡 Medium | 🔴 High | Foundation | ⭐⭐⭐ YES |
| **5** | Biological observations ETL | Data | 🟡 Medium | 🔴 High | Ready | ⭐⭐⭐ YES |
| **6** | Data quality checks | QA | 🟠 Low-Med | 🟠 Medium | Ready | ⭐⭐⭐ YES |
| **7** | Fuzzy parameter matching | Automation | 🟡 Medium | 🟠 Medium | Ready | ⭐⭐ NEXT |
| **8** | Habitat classification | Enrichment | 🟡 Medium | 🔴 High | Ready | ⭐⭐⭐ YES |
| **9** | Low-quality species | QA | 🔴 High | 🔴 High | Ready | ⭐⭐⭐ YES |
| **10** | Materialized views | Performance | 🟢 Low | 🟠 Medium | Ready | ⭐⭐ BEFORE DASHBOARDS |
| **11** | Species media (photos/wiki) | Enhancement | 🟡 Medium | 🟠 Medium | Ready | ⭐⭐ AFTER #8/#9 |

---

## Effort Legend
- 🟢 Low (1-2 days)
- 🟡 Medium (3-7 days)
- 🔴 High (1-2 weeks)
- 🟣 Very High (2+ weeks)

## Impact Legend
- 🟢 Low (affects <10% of queries)
- 🟠 Medium (affects 10-50% of queries/features)
- 🔴 High (affects >50% of queries/features)
- 🔵 Critical (blocks other work)

---

## Strategic Dependency Graph

```
START HERE (Foundation Phase 1-4 weeks)
    ↓
Issue #3: Refactor parameters table (foundation for queries)
Issue #5: Biological ETL (unlock 187 parameters)
Issue #6: Data quality checks (prevent future issues)
Issue #7: Fuzzy matching (speed up #5)
    ↓
THEN (Quality Phase 5-9 weeks)
    ↓
Issue #8: Habitat classification (enables ecology)
Issue #9: Low-quality enrichments (improves data)
Issue #11: Species media (enhances UX)
    ↓
Issue #10: Materialized views (10x speed boost)
    ↓
READY FOR (Analytics Phase 10-16 weeks)
    ↓
ML Analysis + Visualizations + Dashboards + Reports
```

---

## Choose Your Starting Point

### 🎯 Path A: "I Want Immediate Data Wins"
**Time commitment:** 2-3 weeks  
**Outcome:** Better data quality, no more ambiguous parameters

**Tasks in order:**
1. Issue #3 (parameters table) - 3-4 days
2. Issue #6 (quality checks) - 2-3 days
3. Issue #5 (biological ETL) - 5-7 days
4. Issue #7 (fuzzy matching) - 3-4 days
5. Validation & testing - 2-3 days

**Why:** Fixes compound - each makes next easier

---

### 🎯 Path B: "I Want Analysis-Ready Data Fast"
**Time commitment:** 3-4 weeks  
**Outcome:** Species enrichment complete, ready for ML

**Tasks in order:**
1. Issue #8 (habitat classification) - 5-7 days
2. Issue #9 (quality improvements) - 7-10 days
3. Issue #11 (media population) - 5-7 days
4. Validation - 2 days

**Why:** Skips schema stuff (Issue #3-7), focuses on data excellence

⚠️ **Caveat:** You might hit Issue #3 dependencies later; not recommended if you plan major refactoring

---

### 🎯 Path C: "I Want Public-Facing Tools Now"
**Time commitment:** 4-6 weeks  
**Outcome:** Dashboards + map + reports

**Tasks in order:**
1. Issue #10 (materialized views) - 3-4 days
2. Build Grafana dashboards - 5-7 days
3. Build Leaflet map - 7-10 days
4. Generate stakeholder reports - 5-7 days
5. Integration & testing - 3-5 days

**Why:** Skips quality work, assumes data is "good enough"

⚠️ **Caveat:** Data quality issues may emerge in reports; recommend Issue #8/#9 first

---

### 🎯 Path D: "Infrastructure First" (My Recommendation)
**Time commitment:** 4-5 weeks  
**Outcome:** Solid foundation + automation

**Tasks in order:**
1. Issues #3, #5, #6 (data foundation) - 8-10 days
2. GitHub Actions + testing - 5-7 days
3. Issue #8 (habitat) - 5-7 days
4. GitHub Wiki documentation - 3-4 days
5. Validation - 2-3 days

**Why:** 
- ✅ Fixes compound, preventing rework
- ✅ Automation reduces manual effort later
- ✅ Documentation attracts community
- ✅ Balanced between quality & progress
- ✅ Energy-sustainable pace

---

## Work Sequencing within Each Issue

### Issue #3: Parameters Table (3-4 days)
```
Day 1: Design review + schema changes
  - Review PARAMETER_INTEGRATION_GUIDE.md
  - Design new dataset_parameters table
  - Plan migration from current parameters table
  - Update init.sql

Day 2: Database changes
  - Create new schema in test database
  - Write migration script (populate dataset_parameters from parameters)
  - Test migration with sample data
  
Day 3: Code updates
  - Update populate_metadata.py to write both tables
  - Update views (measurements_with_metadata, datasets_by_parameter)
  - Test Grafana queries still work

Day 3-4: Testing & validation
  - Unit tests for migration
  - Integration tests with existing data
  - Performance benchmarks
  - Documentation
```

### Issue #5: Biological ETL (5-7 days)
```
Day 1-2: Understanding
  - Review 187 unmapped parameters
  - Find sample biological datasets in AODN
  - Examine CSV structure
  - Map to taxonomy/species_observations tables

Day 2-3: Design & prototype
  - Create populate_biological_observations.py skeleton
  - Handle CSV parsing
  - Map columns to database schema
  - Test with 1 dataset

Day 4-5: Implementation
  - Extend to all biological datasets
  - Add error handling
  - Handle unit conversions if needed
  - Log coverage statistics

Day 6-7: Testing & validation
  - Unit tests
  - Cross-check with metadata
  - Generate coverage report
  - Documentation
```

### Issue #8: Habitat Classification (5-7 days)
```
Day 1: Investigation
  - Query 515 "Unknown" species
  - Analyze phylum/class distribution
  - Identify heuristic rules

Day 2: Automated rules
  - Create classify_species_habitat.py
  - Implement taxonomy-based rules
  - Test coverage % improvement

Day 3: Manual review
  - Export top 50 species by observation count
  - Spreadsheet classification
  - Bulk import back to database

Day 4-5: Validation
  - Re-run habitat views
  - Verify <30% "Unknown" target
  - Document methodology

Day 6-7: Testing & reporting
  - Test all habitat views
  - Update taxonomy_usage.md
  - Create before/after stats
```

---

## Daily Time Budget Recommendations

Given your lower energy from ADT:

### Conservative (5 hours/week)
- **Monday:** 1 hour planning
- **Tuesday-Thursday:** 1 hour each focused work
- **Friday:** 1 hour review
- **Result:** 1 issue every 3-4 weeks

### Moderate (10 hours/week)
- **Monday & Friday:** Planning/review (1 hour each)
- **Tuesday-Thursday:** 2.5 hour focused sessions
- **Result:** 1-1.5 issues per week

### Ambitious (15 hours/week)
- **Dailyish:** 2-3 hours focused work
- **Result:** 2 issues per week

---

## Energy-Aware Issue Sorting

### High Energy Required
- Issue #5: Biological ETL (lots of creative problem-solving)
- Issue #9: Low-quality enrichments (manual review + decisions)
- Issue #11: Media population (API work, many decisions)

### Moderate Energy
- Issue #3: Parameters table (mostly technical, clear spec)
- Issue #8: Habitat classification (hybrid auto + manual)
- Issue #7: Fuzzy matching (algorithm, well-defined)

### Low Energy OK
- Issue #6: Data quality checks (mostly template code)
- Issue #10: Materialized views (well-documented pattern)

**Recommendation:** Start with Issue #6 (low energy), then Issue #3 (moderate) to build momentum

---

## Code Template Reuse

**Good news:** Your issues already include extensive templates!

| Issue | Templates Provided | Copy/Paste Ready? |
|-------|-------------------|-------------------|
| #3 | SQL schema changes | ✅ Yes (update table names) |
| #5 | populate_biological.py skeleton | ✅ Yes (adapt column mappings) |
| #6 | Validation functions | ✅ Yes (adjust ranges for Huon) |
| #7 | Fuzzy matching code | ✅ Yes (install rapidfuzz) |
| #8 | Classification script | ✅ Yes (SQL rules ready) |
| #9 | Quality improvement pipeline | ✅ Yes (apply step-by-step) |
| #10 | Materialized view SQL | ✅ Yes (run in test first) |
| #11 | API integration Python | ✅ Yes (add rate limiting) |

**Effort reduction:** ~40% from templates already in issues!

---

## 2026 Milestone Calendar

```
January 2026:
  Week 1-2: Foundation cleanup (Issues #3,#6)
  Week 3-4: Data ETL (Issues #5, #7)

February 2026:
  Week 5-6: Habitat + quality (Issues #8, #9)
  Week 7-8: Media + views (Issues #11, #10)

March-April 2026:
  Week 9-14: Analytics & visualizations
  - ML analysis, correlations, dashboards, maps, stories, reports

May-June 2026:
  Week 15-20: DevOps & deployment
  - GitHub Actions, testing, wiki, production readiness

July 2026:
  Transition to retirement
  - Handoff documentation?
  - Community transition?
  - Archival vs. ongoing maintenance?
```

---

## Final Recommendation

### For someone with your profile:

**Start with Path D: Infrastructure First**

**Week 1-2:**
1. Issue #3: Refactor parameters (3-4 days)
2. Issue #6: Data quality checks (2-3 days)

**Week 3:**
1. Issue #5: Biological ETL (5-7 days)

**Week 4:**
1. GitHub Actions setup (5-7 days)

**Result by end of Month 1:** Solid foundation, 4 issues closed, automated workflows running

**Why this is best for you:**
- ✅ Python-heavy (your strength)
- ✅ Technical depth (satisfying for a data officer)
- ✅ Establishes momentum
- ✅ Prevents rework later
- ✅ Foundation for everything else
- ✅ Energy-sustainable pace

---

**Ready to start? Pick an issue, create a branch, and let's build! 🚀**