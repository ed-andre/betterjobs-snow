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

    - ENHANCEMENT-041: Refactor Job Search to Leverage Analytics Layer

- **IN PROGRESS**

    - None

- **COMPLETED**

- **NO ACTION REQUIRED**


--

## ENHANCEMENT-041: Implement Advanced Job Search Asset to leverage Analytics Layer

**Status:** Planned
**Priority:** High
**Component:** Application – `serve_denorm_job_postings` and `advanced_job_search` asset, denormalised tables (job_postings, skills, keywords)
**Date Planned:** 2025-07-06
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




