# Enhancement Tracker 031-040

This document tracks planned enhancements and architectural improvements for the BetterJobs Snowflake project.

## Enhancement Format
- **Enhancement ID**: Unique identifier
- **Status**: Planned/In Progress/Completed
- **Priority**: Critical/High/Medium/Low
- **Component**: Which part of the system will be enhanced
- **Description**: Brief description of the enhancement
- **Business Justification**: Why this enhancement is needed
- **Technical Approach**: How the enhancement will be implemented
- **Implementation Plan**: Step-by-step implementation approach
- **Success Criteria**: How to measure success
- **Date Planned**: When the enhancement was identified

--


## ENHANCEMENT STATUS

- **OPEN**

    - ENHANCEMENT-044: Add Category and Subcategory Job Counts to Serve Layer
    - ENHANCEMENT-045: Implement OpenMetadata Integration POC

- **IN PROGRESS**

    - None

- **COMPLETED**
    - ENHANCEMENT-041: Refactor Job Search to Leverage Analytics Layer
    - ENHANCEMENT-042: Add Skills Category and Subcategory Aggregation to Denormalized Job Postings
    - ENHANCEMENT-043: Add Skill Job Counts Table to Serve Layer

- **NO ACTION REQUIRED**


--

## ENHANCEMENT-041: Implement Advanced Job Search Asset to leverage Analytics Layer

**Status:** Completed
**Priority:** High
**Component:** Application – `serve_denorm_job_postings` and `advanced_job_search` asset, denormalised tables (job_postings, skills, keywords)
**Date Planned:** 2025-07-06
**Date Completed:** 2025-07-10
**Estimated Effort:** 2–3 days

### Problem Statement
The existing `search_jobs` asset executes heavyweight text filters directly against the **Stage** layer (`STAGE.JOBS_UNIFIED`). This results in slow query times (>5 s) and fails to leverage curated, normalised data already present in the **Analytics** star-schema. A clean-slate approach, rather than an in-place rewrite, will let us experiment rapidly while keeping the existing search functionality.

### Solution Overview
1. **New Asset:** `dagster_betterjobs.assets.serve_denorm_job_postings`
   • Group: `job_search`
   • Depends on `analytics_fact_job_postings`, bridge tables, and key dimensions.
   • Returns a `pandas.DataFrame` **and** writes results to `SERVE.DENORM_JOB_POSTINGS` for BI tools.
   • **NEW Supporting Asset:** `dagster_betterjobs.assets.serve_denorm_skills` – refreshes a lookup table `SERVE.DENORM_SKILLS` (category, subcategory, skills_csv) to power UI autocomplete and search filters.
2. **Denormalised View Logic:** Inside the asset build a CTE joining:
   • `ANALYTICS.FACT_JOB_POSTINGS   fp`
   • `ANALYTICS.DIM_COMPANY         dc`
   • `ANALYTICS.DIM_LOCATION        dl`
   • `ANALYTICS.DIM_JOB_DESCRIPTION dj`
   • `ANALYTICS.JOB_SKILLS_BRIDGE  jsb → DIM_SKILLS ds`
   • `ANALYTICS.JOB_KEYWORDS_BRIDGE jkb → DIM_KEYWORDS dk`
   • `STAGE.JOBS_LLM_ENRICHED      llm` (salary / experience stop-gap until corresponding dims are fully validated).
   The query flattens skills & keywords into comma-separated strings for quick text search and attaches salary/experience insights.
3. **Config Class:** `AdvancedJobSearchConfig` extends `JobSearchConfig` with analytics-specific filters:
   • `skills_filter`, `keywords_filter`, `industries_filter`, `company_size_filter`
   • `return_table` (bool, default `False`) – write to Snowflake when `True`.
4. **Backward Compatibility:** Keep `search_jobs` unchanged; feature flag (`use_advanced_search`) allows gradual migration.
5. **Testing:** Add `tests/test_advanced_job_search.py` covering SQL generation, filter application and result parity on benchmark queries.

### Implementation Plan
• **Day 0 – Scaffolding**
   – Create `advanced_job_search.py`; register asset in `assets/__init__.py`.
   – Define `AdvancedJobSearchConfig` and basic unit skeleton.
   – Create companion asset `serve_denorm_skills.py` with nightly schedule.
• **Day 1 – SQL Generator & DataFrame Output**
  – Write join CTEs, skill/keyword aggregation (`LISTAGG` in Snowflake).
  – Ensure configurable filters are translated to SQL `WHERE` clauses.
• **Day 1 PM – Optional Snowflake Materialisation**
  – `CREATE OR REPLACE TABLE SERVE.DENORM_JOB_POSTINGS AS <query>` when `return_table=True`.
  – `CREATE OR REPLACE TABLE SERVE.DENORM_SKILLS AS <query>` inside `serve_denorm_skills` asset.
• **Day 2 – HTML/CSV Report Support & Tests**
  – Re-use existing report helpers with new column names.
  – Implement pytest with mocked cursor for SQL validation.
• **Day 3 – Roll-out**
  – Enable feature flag in staging, compare performance & relevance.
  – If metrics met, switch UI default to `advanced_job_search`.

### Success Criteria
• Query runtime ≤ 500 ms on 100 k-row dataset.
• Result relevance within ±3 % of legacy search.
• Config verbosity reduced by ≥ 80 %.
• Adoption: ≥ 80 % of front-end requests use the new asset within 2 weeks.

---

## ENHANCEMENT-042: Add Skills Category and Subcategory Aggregation to Denormalized Job Postings

**Status:** Completed
**Priority:** Medium
**Component:** Serve Layer – `serve_denorm_job_postings` asset and `SERVE.DENORM_JOB_POSTINGS` table
**Date Planned:** 2025-07-10
**Date Completed:** 2025-07-10
**Estimated Effort:** 1 day

### Problem Statement
The current `SERVE.DENORM_JOB_POSTINGS` table contains a `SKILLS_CSV` column with all skills associated with each job posting. However, for enhanced filtering and analytics capabilities, we need to also provide aggregated lists of the skill categories and subcategories that these skills belong to. This would enable users to filter jobs by broader skill categories (e.g., "Programming Languages", "Databases") or subcategories without having to know all the specific skills within those groups.

### Solution Overview
1. **Table Schema Enhancement:** Add two new columns to `SERVE.DENORM_JOB_POSTINGS`:
   • `SKILLS_CATEGORY_CSV` – Comma-separated list of distinct skill categories
   • `SKILLS_SUBCATEGORY_CSV` – Comma-separated list of distinct skill subcategories

2. **Asset Logic Enhancement:** Modify the `serve_denorm_job_postings` asset to:
   • Use `SKILL_CATEGORY` and `SKILL_SUBCATEGORY` fields from `ANALYTICS.DIM_SKILLS` to derive category and subcategory mappings
   • Aggregate distinct subcategories for skills associated with each job posting
   • Aggregate distinct categories from those subcategories
   • Populate the new CSV columns using `LISTAGG`

3. **Data Flow:** For each job posting:
   • Extract skills from existing `SKILLS_CSV` logic
   • Derive category and subcategory directly from `ANALYTICS.DIM_SKILLS` (no additional join required)
   • Aggregate distinct `SUBCATEGORY_NAME` values → `SKILLS_SUBCATEGORY_CSV`
   • Aggregate distinct `CATEGORY_NAME` values → `SKILLS_CATEGORY_CSV`

### Implementation Plan
• **Phase 1 – Schema Update**
  – Update `serve_denorm_job_postings.sql` table definition
  – Add the two new STRING columns with appropriate positioning
• **Phase 2 – Asset Logic Enhancement**
  – Modify the CTE in `serve_denorm_job_postings` asset
  – Leverage `SKILL_CATEGORY` and `SKILL_SUBCATEGORY` columns already present in `ANALYTICS.DIM_SKILLS` for category/subcategory lookup
  – Implement `LISTAGG` aggregation for the new columns
  – Update MERGE statement to handle the new columns
• **Phase 3 – Testing & Validation**
  – Test asset execution with sample data
  – Verify correct category/subcategory aggregation
  – Ensure backward compatibility with existing functionality

### Success Criteria
• New columns are correctly populated with distinct category/subcategory lists
• No performance degradation in asset execution time
• Existing functionality remains unchanged
• Data quality validation passes for the new columns

### Technical Notes
• Use `LISTAGG(DISTINCT category_name, ', ')` for aggregation
• Handle NULL values appropriately when `SKILL_CATEGORY` or `SKILL_SUBCATEGORY` is NULL
• Consider ordering in the CSV lists for consistent output
• Update the MERGE statement's change detection to include the new columns

---

## ENHANCEMENT-043: Add Skill Job Counts Table to Serve Layer

**Status:** Completed
**Priority:** Medium
**Component:** Serve Layer – `serve_skill_job_counts` asset and `SERVE.SKILL_JOB_COUNTS` table
**Date Planned:** 2025-07-11
**Date Completed:** 2025-07-11
**Estimated Effort:** 1 day

### Problem Statement
The frontend job search application needs a simple way to display the number of active jobs associated with each skill. While this information exists in `ANALYTICS.DIM_SKILLS` as `FREQUENCY_COUNT`, we need a dedicated table in the SERVE layer to maintain separation of concerns and provide a clean interface for the frontend.

### Solution Overview
1. **New Table:** Create `SERVE.SKILL_JOB_COUNTS` with schema:
   ```sql
   CREATE TABLE SERVE.SKILL_JOB_COUNTS (
       SKILL_NAME STRING NOT NULL,
       ACTIVE_JOB_COUNT INTEGER NOT NULL,
       LAST_UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
       PRIMARY KEY (SKILL_NAME)
   ) COMMENT = 'Simple lookup table for UI to show number of active jobs per skill';
   ```

2. **Asset Creation:** New asset `serve_skill_job_counts` in `serve_denormalization.py` that will:
   • Read from `ANALYTICS.DIM_SKILLS`
   • Transform and filter the data as needed
   • Maintain the `SERVE.SKILL_JOB_COUNTS` table via MERGE operations

3. **Data Flow:**
   • Source data from `ANALYTICS.DIM_SKILLS.FREQUENCY_COUNT`
   • Filter out inactive or low-frequency skills
   • Update counts daily through the asset

### Implementation Plan
• **Phase 1 – Schema Definition**
  – Create `serve_skill_job_counts.sql` table definition
  – Add to `table_creation_order.yaml` in serve_layer section
• **Phase 2 – Asset Implementation**
  – Add new asset to `serve_denormalization.py`
  – Implement MERGE logic to update counts
  – Add appropriate logging and monitoring
• **Phase 3 – Testing & Validation**
  – Test asset execution
  – Verify count accuracy
  – Validate performance

### Success Criteria
• Table correctly reflects skill job counts from analytics layer
• Asset executes successfully in under 30 seconds
• Data refreshes maintain accuracy and consistency
• Frontend can efficiently query job counts by skill

### Technical Notes
• Use MERGE for efficient updates
• Include appropriate indexes for query performance
• Maintain consistency with `SERVE.DENORM_SKILLS` naming
• Consider adding a minimum job count threshold to filter noise

---

## ENHANCEMENT-044: Add Category and Subcategory Job Counts to Serve Layer

**Status:** Open
**Priority:** Medium
**Component:** Serve Layer – `serve_denorm_skills` table
**Date Planned:** 2025-07-12
**Estimated Effort:** 1 day

### Problem Statement
While we now have job counts for individual skills in `SERVE.SKILL_JOB_COUNTS`, the frontend also needs aggregated job counts at the skill category and subcategory levels. This information would help users understand which skill areas have the most job opportunities and guide their skill development priorities.

### Solution Overview
1. **Table Enhancement:** Add job count columns to `SERVE.DENORM_SKILLS`:
   ```sql
   ALTER TABLE SERVE.DENORM_SKILLS
   ADD COLUMN CATEGORY_JOB_COUNT INTEGER,
   ADD COLUMN SUBCATEGORY_JOB_COUNT INTEGER;
   ```

2. **Asset Enhancement:** Modify `serve_denorm_skills` asset to:
   • Calculate aggregated job counts from `ANALYTICS.DIM_SKILLS`
   • Roll up individual skill counts to subcategory level
   • Roll up subcategory counts to category level
   • Update the counts during regular refresh

3. **Data Flow:**
   • Source individual skill counts from `ANALYTICS.DIM_SKILLS.FREQUENCY_COUNT`
   • Aggregate to subcategory using skill-to-subcategory mapping
   • Aggregate to category using subcategory-to-category mapping
   • Update both counts in single transaction

### Implementation Plan
• **Phase 1 – Schema Update**
  – Add new columns to `serve_denorm_skills.sql`
  – Update table comments to document new columns
• **Phase 2 – Asset Enhancement**
  – Modify `serve_denorm_skills` asset aggregation logic
  – Add job count calculation CTEs
  – Update MERGE statement to handle new columns
• **Phase 3 – Testing & Validation**
  – Verify count accuracy at all levels
  – Ensure performance remains acceptable
  – Test edge cases (zero counts, missing mappings)

### Success Criteria
• Category and subcategory job counts accurately reflect sum of constituent skills
• Counts update correctly with skill count changes
• Query performance remains under 1 second for frontend requests
• Edge cases handled gracefully

### Technical Notes
• Consider materialized views if performance becomes an issue
• Add appropriate indexes for query optimization
• Maintain consistency with `SERVE.SKILL_JOB_COUNTS` naming and patterns
• Consider adding minimum count thresholds to filter noise

## ENHANCEMENT-045: Implement OpenMetadata Integration POC

**Status:** Open
**Priority:** Medium
**Component:** Infrastructure - Data Catalog and Governance
**Date Planned:** 2025-07-13
**Estimated Effort:** 5-7 days

### Problem Statement
As this data project grows, it needs better tools for data discovery, lineage tracking, and governance. While it has decent documentation and SQL-based metadata, it lacks:
- Automated data discovery and documentation
- Visual data lineage 
- Centralized data quality metrics and monitoring
- Standardized data governance practices
- Self-service data discovery for analysts

OpenMetadata could provide these capabilities through its modern data catalog platform.

### Solution Overview
1. **Infrastructure Setup:**
   • Deploy OpenMetadata using Docker Compose for POC
   • Configure Snowflake connector for metadata ingestion
   • Set up Dagster integration for pipeline metadata
   • Configure authentication and basic security

2. **Metadata Integration Points:**
   • Snowflake tables, views, and procedures
   • Dagster assets and their dependencies
   • Data quality metrics from our validation framework
   • Custom metadata for business glossary and governance

3. **Key Features to Test:**
   • Automated schema inference and documentation
   • Data lineage visualization
   • Data profiling and quality metrics
   • Usage analytics and popularity tracking
   • Search and discovery interface
   • API integration capabilities

### Implementation Plan
• **Phase 1 – Infrastructure (2 days)**
  – Set up OpenMetadata development environment
  – Configure Snowflake connector and test basic ingestion
  – Implement authentication and security baseline
  – Document deployment process

• **Phase 2 – Data Integration (2 days)**
  – Configure metadata extractors for each layer:
    • RAW tables and staging
    • ANALYTICS dimensional model
    • SERVE layer denormalized views
  – Set up automated refresh schedule
  – Validate metadata accuracy

• **Phase 3 – Pipeline Integration (2 days)**
  – Integrate Dagster workflow metadata
  – Map asset dependencies to data lineage
  – Connect quality metrics to tables
  – Test bi-directional updates

• **Phase 4 – Evaluation (1 day)**
  – Document key learnings and limitations
  – Assess performance impact
  – Gather user feedback
  – Make recommendations for production deployment

### Success Criteria
• **Technical Success:**
  – Successfully ingest metadata from all three layers (RAW, ANALYTICS, SERVE)
  – Accurate data lineage visualization
  – Working search and discovery interface
  – Reliable metadata refresh process
  – Performance overhead < 5% on pipeline runs

• **Business Success:**
  – Reduced time to find relevant data assets
  – Improved understanding of data relationships
  – Better visibility into data quality
  – Clear path to production implementation

### Technical Notes
• Start with read-only integration to minimize risk
• Use OpenMetadata's REST API for custom integrations
• Consider implementing custom extractors for LLM metadata
• Monitor performance impact of metadata collection
• Plan for eventual migration to production-grade deployment
• Test integration with existing security model




