# ANALYTICS (Gold) Layer Transformation Plan

This document outlines the comprehensive scope and requirements for implementing the ANALYTICS layer transformations in the BetterJobs-Snow pipeline. The ANALYTICS layer will transform cleaned and enriched STAGE layer data into business-ready dimensional models, pre-aggregated metrics, and analytics-optimized structures for job market intelligence and reporting.

## Overview

The ANALYTICS layer serves as the final presentation layer of our medallion architecture, providing business-ready data structures optimized for reporting, analysis, and market intelligence. This layer will implement dimensional modeling principles to enable fast, intuitive analytics for weekly job market insights and executive reporting.

## Important Scope Limitations

**Job Lifecycle Tracking**: This implementation focuses on **job posting analytics** rather than job lifecycle management. We do NOT track when positions are filled, closed, or become inactive, as this would require re-scanning existing job URLs to monitor status changes. All analytics are based on job posting events and trends rather than completion events.

**What We CAN Analyze**:
- Job posting velocity and trends
- Salary and compensation analysis
- Skills demand and technology trends
- Company hiring patterns (based on new postings)
- Geographic and remote work trends
- Platform usage patterns

**What We CANNOT Analyze** (requires job lifecycle tracking):
- Time to fill positions
- Job closure rates or reasons
- Actual hiring completion metrics
- Position reposting patterns
- Days jobs stay active

## Architecture Overview

```
STAGE Layer (Cleaned & Enriched Data)
         ↓
   Dimensional Modeling & Business Keys
         ↓
   Pre-Aggregated Metrics & KPIs
         ↓
   Business Views & Market Intelligence
         ↓
   ANALYTICS Layer (Business-Ready Data)
```

## Business Requirements

### Primary Use Case: Weekly Job Market Intelligence

The ANALYTICS layer must enable comprehensive weekly reports answering:

**📊 Market Dynamics**
- Job posting velocity trends (week-over-week growth/decline) **[HIGH]** ✅
- Market "temperature" indicators (hot vs. cooling hiring markets) **[HIGH]** ✅
- New vs. recurring job patterns by platform and company **[MEDIUM]** ⚠️
- Geographic shifts in job postings and remote work trends **[LOW]** ⏸️

**💰 Compensation Intelligence**
- Salary benchmark trends by role, location, and experience level **[HIGH]** ✅
- Market rate shifts and outlier identification **[HIGH]** ✅
- Total compensation analysis (base + equity + bonus) **[MEDIUM]** ⚠️
- Compensation inflation indicators across industries **[LOW]** ⏸️

**🛠️ Skills & Technology Demand**
- Emerging vs. declining technology trends **[HIGH]** ✅
- Programming language and framework popularity shifts **[HIGH]** ✅
- Technology adoption patterns across company sizes **[MEDIUM]** ⚠️
- Skills gap analysis (high demand, low supply indicators) **[LOW]** ⏸️

**🏢 Company Hiring Patterns**
- Hiring velocity by company size, industry, and funding stage **[HIGH]** ✅
- Company-specific trend analysis and benchmarking **[MEDIUM]** ⚠️
- Platform preference analysis by company characteristics **[MEDIUM]** ⚠️
- Expansion vs. replacement hiring indicators **[LOW]** ⏸️

**🌍 Geographic & Remote Work Analysis**
- Location-based job opportunity distribution **[MEDIUM]** ⚠️
- Remote work policy trends and geographic expansion **[MEDIUM]** ⚠️
- Cost-of-living adjusted salary analysis **[LOW]** ⏸️
- Metro area job market competitiveness **[LOW]** ⏸️

### Priority Analysis & Implementation Strategy

**Priority Legend:**
- **[HIGH]** ✅ - Core MVP features: High business value + readily achievable with current STAGE data
- **[MEDIUM]** ⚠️ - Phase 2 features: Good value + requires some additional data modeling or enrichment
- **[LOW]** ⏸️ - Future enhancements: Nice-to-have + requires significant external data or complex processing

**Feasibility Assessment:**

**HIGH Priority (MVP - Phase 1 & 2)**:
- ✅ **Job posting velocity & market temperature**: We have posting dates and can calculate trends
- ✅ **Salary benchmarks & market shifts**: LLM-extracted salary data enables comprehensive compensation analysis
- ✅ **Skills demand trends**: LLM-extracted technical skills provide rich technology trend analysis
- ✅ **Company hiring velocity**: Company profiles + job postings enable hiring pattern analysis

**MEDIUM Priority (Phase 3 & 4)**:
- ⚠️ **New vs. recurring patterns**: Requires job similarity analysis or URL pattern detection
- ⚠️ **Total compensation analysis**: Need to parse equity/bonus mentions from LLM data
- ⚠️ **Technology adoption by company size**: Requires cross-referencing company size with skills
- ⚠️ **Company benchmarking**: Needs peer group definitions and comparative metrics
- ⚠️ **Location distribution**: Basic analysis possible, but limited by location data quality
- ⚠️ **Remote work trends**: LLM extracts some work arrangement data, but needs standardization

**LOW Priority (Future Enhancements/Tech Debt)**:
- ⏸️ **Geographic shifts**: Requires historical location trend analysis and external geo data
- ⏸️ **Compensation inflation**: Needs external economic indicators for proper inflation context
- ⏸️ **Skills gap analysis**: Requires supply-side data (job seekers, graduates) not available in our dataset
- ⏸️ **Cost-of-living adjustments**: Requires external COL data integration
- ⏸️ **Metro area competitiveness**: Needs market definition and external economic indicators
- ⏸️ **Expansion vs. replacement hiring**: Requires job lifecycle tracking (out of scope)

**Prerequisites:**
✅ **LLM Data Normalization**: The STAGE layer LLM data standardization must be completed before ANALYTICS layer implementation. This includes normalized skills, keywords, and bridge tables as defined in `stage_layer_llm_data_standardization_plan.md`.

**Implementation Focus:**
1. **Phase 1**: Core dimensional model + HIGH priority job posting analytics
2. **Phase 2**: HIGH priority skills and salary analytics (leveraging normalized STAGE data)
3. **Phase 3**: HIGH priority company analysis
4. **Phase 4+**: MEDIUM priority features as time permits



## Dimensional Model Design

### Star Schema Architecture

The ANALYTICS layer will implement a star schema optimized for time-series analysis and cross-dimensional filtering (leveraging the normalized skill data from the STAGE layer):

#### Core Fact Tables

##### 1. `FACT_JOB_POSTINGS` (Primary Fact Table)
**Grain**: One record per unique job posting (simplified for actual business needs)
**Source**: STAGE.JOBS_UNIFIED + STAGE.JOBS_LLM_ENRICHED

```sql
CREATE TABLE ANALYTICS.FACT_JOB_POSTINGS (
    -- Surrogate Key
    job_posting_key STRING PRIMARY KEY,  -- One key per unique job posting

    -- Dimension Keys
    date_posted_key STRING,              -- When job was first posted
    company_key STRING,
    location_key STRING,
    job_family_key STRING,
    platform_key STRING,

    -- Degenerate Dimensions
    job_uid STRING,                      -- Natural key from STAGE.JOBS_UNIFIED.JOB_UID
    job_title STRING,                    -- From STAGE.JOBS_UNIFIED.JOB_TITLE_CLEAN
    posting_url STRING,                  -- From STAGE.JOBS_UNIFIED.JOB_URL

    -- Salary Measures (from STAGE.JOBS_LLM_ENRICHED)
    salary_min NUMBER,                   -- From STAGE.JOBS_LLM_ENRICHED.SALARY_MIN
    salary_max NUMBER,                   -- From STAGE.JOBS_LLM_ENRICHED.SALARY_MAX
    salary_currency STRING,              -- From STAGE.JOBS_LLM_ENRICHED.SALARY_CURRENCY
    salary_period STRING,                -- From STAGE.JOBS_LLM_ENRICHED.SALARY_PERIOD

    -- Experience Measures (from STAGE.JOBS_LLM_ENRICHED)
    experience_min_years NUMBER,         -- From STAGE.JOBS_LLM_ENRICHED.MIN_YEARS_EXPERIENCE
    experience_max_years NUMBER,         -- From STAGE.JOBS_LLM_ENRICHED.MAX_YEARS_EXPERIENCE
    experience_level STRING,             -- From STAGE.JOBS_LLM_ENRICHED.EXPERIENCE_LEVEL

    -- Quality and Confidence Measures
    salary_confidence FLOAT,             -- From STAGE.JOBS_LLM_ENRICHED.SALARY_CONFIDENCE
    llm_overall_confidence FLOAT,        -- From STAGE.JOBS_LLM_ENRICHED.LLM_OVERALL_CONFIDENCE
    data_quality_score FLOAT,            -- From STAGE.JOBS_UNIFIED.DATA_QUALITY_SCORE

    -- Job Classification (from STAGE.JOBS_LLM_ENRICHED)
    job_family STRING,                   -- From STAGE.JOBS_LLM_ENRICHED.JOB_FAMILY
    job_sub_family STRING,               -- From STAGE.JOBS_LLM_ENRICHED.JOB_SUB_FAMILY
    seniority_level STRING,              -- From STAGE.JOBS_LLM_ENRICHED.SENIORITY_LEVEL

    -- Work Arrangement (from STAGE.JOBS_LLM_ENRICHED)
    work_type STRING,                    -- From STAGE.JOBS_LLM_ENRICHED.WORK_TYPE
    remote_flexibility STRING,           -- From STAGE.JOBS_LLM_ENRICHED.REMOTE_FLEXIBILITY

    -- Boolean Flags (from STAGE tables)
    is_active_posting BOOLEAN,           -- From STAGE.JOBS_UNIFIED.IS_ACTIVE
    is_equity_mentioned BOOLEAN,         -- From STAGE.JOBS_LLM_ENRICHED.EQUITY_MENTIONED
    is_bonus_mentioned BOOLEAN,          -- From STAGE.JOBS_LLM_ENRICHED.BONUS_MENTIONED
    llm_needs_manual_review BOOLEAN,     -- From STAGE.JOBS_LLM_ENRICHED.LLM_NEEDS_MANUAL_REVIEW

    -- Important Dates
    first_posted_date DATE,              -- From STAGE.JOBS_UNIFIED.DATE_POSTED
    date_retrieved DATE,                 -- From STAGE.JOBS_UNIFIED.DATE_RETRIEVED

    -- Audit Fields
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,

    -- Partitioning by posting month for query performance
    partition_date DATE                  -- DATE_TRUNC('month', first_posted_date)
) PARTITION BY (partition_date)
CLUSTER BY (date_posted_key, company_key, location_key, job_family_key);
```

##### 2. `FACT_SKILLS_DEMAND_WEEKLY` (Skills Demand Aggregates)
**Grain**: One record per skill per week (enables responsive trend analysis)
**Business Need**: Track skill demand trends with weekly granularity for timely market intelligence

```sql
CREATE TABLE ANALYTICS.FACT_SKILLS_DEMAND_WEEKLY (
    SKILLS_WEEKLY_KEY STRING PRIMARY KEY,
    WEEK_KEY STRING,                        -- YYYY-WW format
    SKILL_KEY STRING,
    JOB_FAMILY_KEY STRING,
    LOCATION_KEY STRING,

    -- Core Demand Metrics
    ACTIVE_JOBS_WITH_SKILL INTEGER,         -- Active jobs requiring this skill
    TOTAL_ACTIVE_JOBS INTEGER,              -- Total active jobs in category
    SKILL_PENETRATION_RATE FLOAT,           -- Percentage requiring this skill

    -- Salary Analysis
    AVG_SALARY_WITH_SKILL NUMBER,           -- Average salary for jobs with this skill
    SALARY_PREMIUM_PERCENTAGE FLOAT,        -- Premium this skill commands

    -- Trend Analysis
    WEEK_OVER_WEEK_CHANGE FLOAT,            -- Change in job count
    TREND_DIRECTION STRING,                 -- 'GROWING', 'STABLE', 'DECLINING'

    -- Market Position
    SKILL_RANK_IN_FAMILY INTEGER,           -- Rank within job family
    SKILL_RANK_OVERALL INTEGER,             -- Overall market rank

    -- Audit Fields
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    WEEK_START_DATE DATE                    -- First day of week for partitioning
) PARTITION BY (WEEK_START_DATE)
CLUSTER BY (WEEK_KEY, SKILL_KEY, JOB_FAMILY_KEY);
```

##### 3. `FACT_COMPANY_HIRING_WEEKLY` (Company Hiring Intelligence)
**Grain**: One record per company per week (aligned with weekly job market intelligence)
**Business Need**: Weekly hiring trends for company intelligence and competitive analysis

```sql
CREATE TABLE ANALYTICS.FACT_COMPANY_HIRING_WEEKLY (
    COMPANY_HIRING_KEY STRING PRIMARY KEY,
    WEEK_KEY STRING,                        -- YYYY-WW format
    COMPANY_KEY STRING,

    -- Core Hiring Metrics
    JOBS_POSTED_COUNT INTEGER,              -- Total jobs posted this week
    ACTIVE_JOBS_COUNT INTEGER,              -- Jobs still active at week end
    NEW_JOBS_THIS_WEEK INTEGER,             -- Net new postings

    -- Hiring Velocity & Trends
    WEEK_OVER_WEEK_CHANGE FLOAT,            -- Change in hiring volume
    HIRING_TREND_DIRECTION STRING,          -- 'ACCELERATING', 'STABLE', 'DECLINING'

    -- Job Portfolio Analysis (from STAGE.JOBS_LLM_ENRICHED)
    AVG_SALARY_OFFERED NUMBER,              -- Average across all roles
    MEDIAN_SALARY_OFFERED NUMBER,           -- Median salary
    SALARY_RANGE_WIDTH FLOAT,               -- Max - Min salary span

    -- Work Arrangement Patterns (from STAGE.JOBS_LLM_ENRICHED)
    REMOTE_JOBS_PERCENTAGE FLOAT,           -- % of remote-eligible jobs
    HYBRID_JOBS_PERCENTAGE FLOAT,           -- % of hybrid jobs
    ONSITE_JOBS_PERCENTAGE FLOAT,           -- % of on-site only jobs

    -- Role Distribution (from STAGE.JOBS_LLM_ENRICHED)
    ENTRY_LEVEL_PERCENTAGE FLOAT,           -- % of entry-level roles
    SENIOR_LEVEL_PERCENTAGE FLOAT,          -- % of senior+ roles
    MANAGEMENT_ROLES_PERCENTAGE FLOAT,      -- % of management positions

    -- Audit Fields
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    WEEK_START_DATE DATE                    -- First day of week for partitioning
) PARTITION BY (WEEK_START_DATE)
CLUSTER BY (WEEK_KEY, COMPANY_KEY);
```

#### Dimension Tables

##### 1. `DIM_DATE` (Date Dimension)
**Essential Date Attributes for Job Market Analytics**

```sql
CREATE TABLE ANALYTICS.DIM_DATE (
    DATE_KEY STRING PRIMARY KEY,
    FULL_DATE DATE,

    -- Essential Date Attributes
    DAY_NAME STRING,
    DAY_OF_WEEK INTEGER,
    WEEK_BEGINNING_DATE DATE,
    WEEK_ENDING_DATE DATE,
    MONTH_NUMBER INTEGER,
    MONTH_NAME STRING,
    QUARTER_NUMBER INTEGER,
    YEAR_NUMBER INTEGER,

    -- Business Context
    IS_BUSINESS_DAY BOOLEAN,
    IS_WEEKEND BOOLEAN,

    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (full_date);
```

##### 2. `DIM_COMPANY` (Company Dimension)
**Type 2 SCD for Company Changes (Built from STAGE.COMPANY_PROFILES and STAGE.JOBS_UNIFIED)**

```sql
CREATE TABLE ANALYTICS.DIM_COMPANY (
    COMPANY_KEY STRING PRIMARY KEY,
    COMPANY_ID STRING,              -- Natural key from STAGE.JOBS_UNIFIED.COMPANY_ID

    -- Company Identity (from STAGE.COMPANY_PROFILES and STAGE.JOBS_UNIFIED)
    COMPANY_NAME STRING,            -- From STAGE.JOBS_UNIFIED.COMPANY_NAME_CLEAN
    COMPANY_NAME_STANDARDIZED STRING, -- From STAGE.COMPANY_PROFILES.COMPANY_NAME_STANDARDIZED

    -- Company Classification (from STAGE.COMPANY_PROFILES)
    INDUSTRY STRING,                -- From STAGE.COMPANY_PROFILES.COMPANY_INDUSTRY_STANDARDIZED

    -- Company Size (from STAGE.COMPANY_PROFILES)
    EMPLOYEE_COUNT_RANGE STRING,    -- From STAGE.COMPANY_PROFILES.EMPLOYEE_COUNT_RANGE
    COMPANY_SIZE_CATEGORY STRING,   -- From STAGE.COMPANY_PROFILES.COMPANY_SIZE_CATEGORY

    -- Company Location (from STAGE.COMPANY_PROFILES)
    HEADQUARTERS_LOCATION STRING,   -- From STAGE.COMPANY_PROFILES.HEADQUARTERS_LOCATION

    -- Company Stage & Funding (from STAGE.COMPANY_PROFILES)
    FUNDING_STAGE STRING,           -- From STAGE.COMPANY_PROFILES.FUNDING_STAGE

    -- SCD Type 2 Fields
    EFFECTIVE_DATE DATE,
    EXPIRATION_DATE DATE,
    IS_CURRENT BOOLEAN,
    VERSION_NUMBER INTEGER,

    -- Audit Fields
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    SOURCE_STAGE_TABLE STRING
) CLUSTER BY (company_id, is_current);
```

##### 3. `DIM_LOCATION` (Location Dimension)
**Geographic Hierarchy (Built from STAGE.LOCATIONS_NORMALIZED)**

```sql
CREATE TABLE ANALYTICS.DIM_LOCATION (
    LOCATION_KEY STRING PRIMARY KEY,
    LOCATION_ID STRING,              -- FK to STAGE.LOCATIONS_NORMALIZED.LOCATION_ID

    -- Location Hierarchy (from STAGE.LOCATIONS_NORMALIZED)
    LOCATION_NAME STRING,
    CITY STRING,
    STATE_PROVINCE STRING,
    COUNTRY STRING,
    REGION STRING,
    METRO_AREA STRING,

    -- Location Intelligence from STAGE
    LOCATION_TYPE STRING,            -- office, remote, hybrid
    IS_REMOTE_FRIENDLY BOOLEAN,      -- From STAGE.LOCATIONS_NORMALIZED
    IS_MAJOR_TECH_HUB BOOLEAN,       -- From STAGE.LOCATIONS_NORMALIZED
    COST_OF_LIVING_INDEX FLOAT,      -- From STAGE.LOCATIONS_NORMALIZED
    AVERAGE_SALARY_ADJUSTMENT FLOAT, -- From STAGE.LOCATIONS_NORMALIZED
    STAGE_CONFIDENCE_SCORE FLOAT,    -- From STAGE.LOCATIONS_NORMALIZED.CONFIDENCE_SCORE
    FREQUENCY_COUNT INTEGER,         -- From STAGE.LOCATIONS_NORMALIZED.FREQUENCY_COUNT

    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (country, state_province, city);
```

##### 4. `DIM_JOB_FAMILY` (Job Classification Dimension)
**Hierarchical Job Taxonomy**

```sql
CREATE TABLE ANALYTICS.DIM_JOB_FAMILY (
    job_family_key STRING PRIMARY KEY,

    -- Job Hierarchy
    JOB_FAMILY STRING,               -- Engineering, Data, Product, Sales, etc.
    JOB_SUB_FAMILY STRING,          -- Backend Engineering, Data Science, etc.
    JOB_SPECIALTY STRING,           -- Python Developer, ML Engineer, etc.

    -- Seniority Classification
    SENIORITY_LEVEL STRING,         -- Entry, Mid, Senior, Staff, Principal, Executive
    SENIORITY_ORDER INTEGER,        -- 1-7 for ordering
    EXPERIENCE_MIN_YEARS INTEGER,
    EXPERIENCE_MAX_YEARS INTEGER,

    -- Role Type
    ROLE_TYPE STRING,               -- Individual Contributor, Manager, Director, VP
    MANAGEMENT_LEVEL INTEGER,       -- 0=IC, 1=Manager, 2=Director, 3=VP, 4=C-Level
    IS_MANAGEMENT_ROLE BOOLEAN,

    -- Department & Function
    DEPARTMENT STRING,              -- Engineering, Sales, Marketing, etc.
    BUSINESS_FUNCTION STRING,       -- Core Product, Growth, Support, etc.

    -- Job Characteristics
    TYPICAL_TEAM_SIZE_MIN INTEGER,
    TYPICAL_TEAM_SIZE_MAX INTEGER,
    REQUIRES_SECURITY_CLEARANCE BOOLEAN,
    TRAVEL_REQUIREMENT_LEVEL STRING, -- None, Low, Medium, High

    -- Market Data
    MARKET_DEMAND_LEVEL STRING,     -- Very High, High, Medium, Low
    SALARY_GROWTH_TREND STRING,     -- Growing, Stable, Declining
    AUTOMATION_RISK_LEVEL STRING,   -- Low, Medium, High

    -- Skills Context
    primary_skill_category STRING,  -- Technical, Creative, Sales, etc.
    REQUIRES_CODING BOOLEAN,
    REQUIRES_CERTIFICATION BOOLEAN,

    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (job_family, seniority_level);
```

##### 5. `DIM_PLATFORM` (ATS Platform Dimension)
**Platform Characteristics from Job Data**

```sql
CREATE TABLE ANALYTICS.DIM_PLATFORM (
    PLATFORM_KEY STRING PRIMARY KEY,
    PLATFORM_NAME STRING,              -- From STAGE.JOBS_UNIFIED.PLATFORM
    PLATFORM_CODE STRING,              -- workday, greenhouse, bamboohr, etc.

    -- Derived Platform Characteristics (from actual job data)
    SUPPORTS_SALARY_DISCLOSURE BOOLEAN, -- Calculated from salary disclosure rates
    DATA_RICHNESS_SCORE FLOAT,         -- Calculated from job description quality metrics
    JOB_VOLUME_CATEGORY STRING,        -- High, Medium, Low (derived from actual job counts)

    IS_ACTIVE BOOLEAN,                  -- Currently processing jobs from this platform
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (platform_name);
```

##### 6. `DIM_SKILLS` (Skills Taxonomy Dimension)
**Skills Classification (Built from STAGE.SKILLS_NORMALIZED)**

```sql
CREATE TABLE ANALYTICS.DIM_SKILLS (
    SKILL_KEY STRING PRIMARY KEY,
    SKILL_ID STRING,               -- FK to STAGE.SKILLS_NORMALIZED.SKILL_ID
    SKILL_NAME STRING,

    -- Skill Hierarchy (from STAGE.SKILLS_NORMALIZED)
    SKILL_CATEGORY STRING,          -- From STAGE.SKILLS_NORMALIZED.SKILL_CATEGORY
    SKILL_SUBCATEGORY STRING,      -- From STAGE.SKILLS_NORMALIZED.SKILL_SUBCATEGORY

    -- STAGE Data Fields
    CANONICAL_FORM STRING,          -- From STAGE.SKILLS_NORMALIZED.CANONICAL_FORM
    COMMON_ALIASES VARIANT,         -- From STAGE.SKILLS_NORMALIZED.COMMON_ALIASES
    ORIGINAL_VARIANTS VARIANT,      -- From STAGE.SKILLS_NORMALIZED.ORIGINAL_VARIANTS
    STAGE_CONFIDENCE_SCORE FLOAT,   -- From STAGE.SKILLS_NORMALIZED.CONFIDENCE_SCORE
    FREQUENCY_COUNT INTEGER,        -- From STAGE.SKILLS_NORMALIZED.FREQUENCY_COUNT
    TREND_DIRECTION STRING,         -- From STAGE.SKILLS_NORMALIZED.TREND_DIRECTION

    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (skill_category, skill_name);
```

## Pre-Aggregated Metrics Tables

### Market Intelligence Tables

##### 1. `MARKET_WEEKLY_SUMMARY` (Executive Dashboard)
**Weekly market snapshots for instant reporting**

```sql
CREATE TABLE ANALYTICS.MARKET_WEEKLY_SUMMARY (
    SUMMARY_KEY STRING PRIMARY KEY,
    WEEK_ENDING_DATE DATE,
    WEEK_KEY STRING,                        -- YYYY-WW format

    -- Core Market Metrics
    TOTAL_JOBS_POSTED INTEGER,
    TOTAL_ACTIVE_JOBS INTEGER,
    NEW_JOBS_POSTED INTEGER,

    -- Velocity Metrics
    WEEK_OVER_WEEK_GROWTH_RATE FLOAT,
    POSTING_VELOCITY_DAILY FLOAT,

    -- Salary Intelligence
    MEDIAN_SALARY_ALL_ROLES INTEGER,
    AVG_SALARY_ALL_ROLES INTEGER,

    -- Work Arrangement Trends
    REMOTE_WORK_PERCENTAGE FLOAT,
    HYBRID_WORK_PERCENTAGE FLOAT,
    ONSITE_WORK_PERCENTAGE FLOAT,

    -- Quality Metrics
    SAMPLE_SIZE INTEGER,
    DATA_QUALITY_SCORE FLOAT,

    -- Audit Fields
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) PARTITION BY (WEEK_ENDING_DATE)
CLUSTER BY (WEEK_KEY);
```

##### 2. `SKILLS_TREND_ANALYSIS` (Skills Intelligence)
**Skills market analysis with essential metrics**

```sql
CREATE TABLE ANALYTICS.SKILLS_TREND_ANALYSIS (
    ANALYSIS_KEY STRING PRIMARY KEY,
    ANALYSIS_DATE DATE,
    SKILL_KEY STRING,
    WEEK_KEY STRING,                        -- YYYY-WW format

    -- Current Demand Metrics
    JOBS_REQUIRING_SKILL INTEGER,
    TOTAL_JOBS_ANALYZED INTEGER,
    MARKET_PENETRATION_RATE FLOAT,          -- Percentage of jobs requiring this skill
    DEMAND_RANK_OVERALL INTEGER,

    -- Growth Metrics
    DEMAND_GROWTH_WEEKLY FLOAT,
    DEMAND_GROWTH_MONTHLY FLOAT,

    -- Ranking Changes
    RANK_CHANGE_WEEKLY INTEGER,
    RANK_CHANGE_MONTHLY INTEGER,

    -- Salary Impact
    AVERAGE_SALARY_WITH_SKILL NUMBER,
    SALARY_PREMIUM_PERCENTAGE FLOAT,

    -- Work Arrangement
    REMOTE_AVAILABILITY_RATE FLOAT,         -- Percentage of remote jobs with this skill

    -- Seniority Distribution
    ENTRY_LEVEL_DEMAND INTEGER,
    MID_LEVEL_DEMAND INTEGER,
    SENIOR_LEVEL_DEMAND INTEGER,

    -- Quality Metrics
    SAMPLE_SIZE INTEGER,
    DATA_QUALITY_SCORE FLOAT,

    -- Audit Fields
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) PARTITION BY (ANALYSIS_DATE)
CLUSTER BY (ANALYSIS_DATE, SKILL_KEY);
```

##### 3. `COMPANY_HIRING_INTELLIGENCE` (Company Analysis)
**Company-specific hiring patterns and intelligence**

```sql
CREATE TABLE ANALYTICS.COMPANY_HIRING_INTELLIGENCE (
    INTELLIGENCE_KEY STRING PRIMARY KEY,
    ANALYSIS_DATE DATE,
    COMPANY_KEY STRING,
    WEEK_KEY STRING,                        -- YYYY-WW format

    -- Hiring Velocity
    JOBS_POSTED_LAST_7_DAYS INTEGER,
    JOBS_POSTED_LAST_30_DAYS INTEGER,
    CURRENT_JOB_POSTINGS INTEGER,

    -- Growth Indicators
    HIRING_VELOCITY_TREND STRING,           -- 'ACCELERATING', 'STABLE', 'DECELERATING'
    WEEK_OVER_WEEK_GROWTH_RATE FLOAT,

    -- Compensation Strategy
    SALARY_TRANSPARENCY_RATE FLOAT,         -- Percentage of jobs with salary disclosed
    AVG_SALARY_OFFERED NUMBER,

    -- Role Distribution
    ENTRY_VS_SENIOR_RATIO FLOAT,
    TECHNICAL_VS_BUSINESS_RATIO FLOAT,
    REMOTE_JOB_PERCENTAGE FLOAT,

    -- Work Arrangement Policy
    REMOTE_WORK_POLICY STRING,              -- 'FULL_REMOTE', 'HYBRID', 'ONSITE'

    -- Market Position
    HIRING_RANK_IN_INDUSTRY INTEGER,
    HIRING_COMPETITIVENESS_SCORE FLOAT,

    -- Quality Metrics
    SAMPLE_SIZE INTEGER,
    DATA_COMPLETENESS_SCORE FLOAT,

    -- Audit Fields
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) PARTITION BY (ANALYSIS_DATE)
CLUSTER BY (ANALYSIS_DATE, COMPANY_KEY);
```

## Business Views & Analytics Layer

### Executive Dashboard Views

##### 1. `WEEKLY_MARKET_OVERVIEW` (Weekly Market Overview)
```sql
CREATE VIEW ANALYTICS.WEEKLY_MARKET_OVERVIEW AS
SELECT
    DATE_TRUNC('week', F.FIRST_POSTED_DATE) AS REPORT_WEEK,

    -- Key Metrics
    COUNT(*) AS TOTAL_JOB_POSTINGS,
    COUNT(DISTINCT F.COMPANY_KEY) AS ACTIVE_COMPANIES,
    COUNT(CASE WHEN F.IS_ACTIVE_POSTING THEN 1 END) AS ACTIVE_POSTINGS,

    -- Salary Intelligence
    AVG(F.SALARY_MIN + F.SALARY_MAX) / 2 AS AVG_SALARY_MIDPOINT,
    MEDIAN((F.SALARY_MIN + F.SALARY_MAX) / 2) AS MEDIAN_SALARY_MIDPOINT,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY (F.SALARY_MIN + F.SALARY_MAX) / 2) AS P75_SALARY,

    -- Remote Work Trends
    COUNT(CASE WHEN F.WORK_TYPE = 'Remote' THEN 1 END)::FLOAT / COUNT(*) * 100 AS REMOTE_PERCENTAGE,
    COUNT(CASE WHEN F.WORK_TYPE = 'Hybrid' THEN 1 END)::FLOAT / COUNT(*) * 100 AS HYBRID_PERCENTAGE,
    COUNT(CASE WHEN F.WORK_TYPE = 'On-site' THEN 1 END)::FLOAT / COUNT(*) * 100 AS ONSITE_PERCENTAGE,

    -- Geographic Distribution
    COUNT(DISTINCT L.LOCATION_KEY) AS UNIQUE_LOCATIONS,

    -- Experience Level Distribution
    COUNT(CASE WHEN F.SENIORITY_LEVEL = 'Entry' THEN 1 END) AS ENTRY_LEVEL_JOBS,
    COUNT(CASE WHEN F.SENIORITY_LEVEL = 'Mid' THEN 1 END) AS MID_LEVEL_JOBS,
    COUNT(CASE WHEN F.SENIORITY_LEVEL = 'Senior' THEN 1 END) AS SENIOR_LEVEL_JOBS,

    -- Growth Metrics
    LAG(COUNT(*)) OVER (ORDER BY DATE_TRUNC('week', F.FIRST_POSTED_DATE)) AS PREV_WEEK_POSTINGS,
    (COUNT(*)::FLOAT / LAG(COUNT(*)) OVER (ORDER BY DATE_TRUNC('week', F.FIRST_POSTED_DATE)) - 1) * 100 AS WOW_GROWTH_RATE,

    -- Quality Metrics
    AVG(F.DATA_QUALITY_SCORE) AS AVG_DATA_QUALITY_SCORE,
    COUNT(CASE WHEN F.SALARY_MIN IS NOT NULL AND F.SALARY_MAX IS NOT NULL THEN 1 END)::FLOAT / COUNT(*) * 100 AS SALARY_DISCLOSURE_RATE

FROM ANALYTICS.FACT_JOB_POSTINGS F
JOIN ANALYTICS.DIM_DATE D ON F.DATE_POSTED_KEY = D.DATE_KEY
JOIN ANALYTICS.DIM_COMPANY C ON F.COMPANY_KEY = C.COMPANY_KEY
JOIN ANALYTICS.DIM_LOCATION L ON F.LOCATION_KEY = L.LOCATION_KEY

WHERE D.FULL_DATE >= CURRENT_DATE - 90
  AND F.IS_ACTIVE_POSTING = TRUE

GROUP BY DATE_TRUNC('week', F.FIRST_POSTED_DATE)
ORDER BY REPORT_WEEK DESC;
```

##### 2. `SALARY_INTELLIGENCE` (Compensation Analysis)
```sql
CREATE VIEW ANALYTICS.SALARY_INTELLIGENCE AS
SELECT
    JF.JOB_FAMILY,
    JF.SENIORITY_LEVEL,
    L.METRO_AREA,
    L.COUNTRY,
    DATE_TRUNC('month', F.FIRST_POSTED_DATE) AS SALARY_MONTH,

    -- Statistical Measures
    COUNT(*) AS SAMPLE_SIZE,
    AVG((F.SALARY_MIN + F.SALARY_MAX) / 2) AS MEAN_SALARY,
    MEDIAN((F.SALARY_MIN + F.SALARY_MAX) / 2) AS MEDIAN_SALARY,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY (F.SALARY_MIN + F.SALARY_MAX) / 2) AS P25_SALARY,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY (F.SALARY_MIN + F.SALARY_MAX) / 2) AS P75_SALARY,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY (F.SALARY_MIN + F.SALARY_MAX) / 2) AS P90_SALARY,

    -- Growth Metrics
    LAG(AVG((F.SALARY_MIN + F.SALARY_MAX) / 2)) OVER (
        PARTITION BY JF.JOB_FAMILY, JF.SENIORITY_LEVEL, L.METRO_AREA
        ORDER BY DATE_TRUNC('month', F.FIRST_POSTED_DATE)
    ) AS PREV_MONTH_AVG_SALARY,

    ((AVG((F.SALARY_MIN + F.SALARY_MAX) / 2) / LAG(AVG((F.SALARY_MIN + F.SALARY_MAX) / 2)) OVER (
        PARTITION BY JF.JOB_FAMILY, JF.SENIORITY_LEVEL, L.METRO_AREA
        ORDER BY DATE_TRUNC('month', F.FIRST_POSTED_DATE)
    )) - 1) * 100 AS MOM_SALARY_GROWTH,

    -- Work Arrangement Impact
    AVG(CASE WHEN F.WORK_TYPE = 'Remote' THEN (F.SALARY_MIN + F.SALARY_MAX) / 2 END) AS AVG_REMOTE_SALARY,
    AVG(CASE WHEN F.WORK_TYPE = 'On-site' THEN (F.SALARY_MIN + F.SALARY_MAX) / 2 END) AS AVG_ONSITE_SALARY,

    -- Quality Indicators
    AVG(F.SALARY_CONFIDENCE) AS AVG_CONFIDENCE_SCORE,
    COUNT(CASE WHEN F.SALARY_MIN IS NOT NULL AND F.SALARY_MAX IS NOT NULL THEN 1 END)::FLOAT / COUNT(*) * 100 AS DISCLOSURE_RATE

FROM ANALYTICS.FACT_JOB_POSTINGS F
JOIN ANALYTICS.DIM_JOB_FAMILY JF ON F.JOB_FAMILY_KEY = JF.JOB_FAMILY_KEY
JOIN ANALYTICS.DIM_LOCATION L ON F.LOCATION_KEY = L.LOCATION_KEY
JOIN ANALYTICS.DIM_DATE D ON F.DATE_POSTED_KEY = D.DATE_KEY

WHERE F.SALARY_MIN IS NOT NULL
  AND F.SALARY_MAX IS NOT NULL
  AND (F.SALARY_MIN + F.SALARY_MAX) / 2 BETWEEN 30000 AND 500000  -- Reasonable bounds
  AND F.IS_ACTIVE_POSTING = TRUE
  AND D.FULL_DATE >= CURRENT_DATE - 365

GROUP BY JF.JOB_FAMILY, JF.SENIORITY_LEVEL, L.METRO_AREA, L.COUNTRY, DATE_TRUNC('month', F.FIRST_POSTED_DATE)
HAVING COUNT(*) >= 5  -- Minimum sample size for statistical validity
ORDER BY JF.JOB_FAMILY, JF.SENIORITY_LEVEL, L.METRO_AREA, SALARY_MONTH DESC;
```

##### 3. `SKILLS_MARKET_INTELLIGENCE` (Technology Trends)
```sql
CREATE VIEW ANALYTICS.SKILLS_MARKET_INTELLIGENCE AS
WITH SKILL_DEMAND_TRENDS AS (
    SELECT
        S.SKILL_NAME,
        S.SKILL_CATEGORY,
        S.SKILL_SUBCATEGORY,
        DATE_TRUNC('week', F.FIRST_POSTED_DATE) AS DEMAND_WEEK,
        COUNT(DISTINCT F.JOB_POSTING_KEY) AS JOBS_REQUIRING_SKILL,

        -- Market Penetration Calculation
        COUNT(DISTINCT F.JOB_POSTING_KEY)::FLOAT /
        SUM(COUNT(DISTINCT F.JOB_POSTING_KEY)) OVER (PARTITION BY DATE_TRUNC('week', F.FIRST_POSTED_DATE)) * 100 AS MARKET_PENETRATION_RATE,

        -- Salary Analysis
        AVG((F.SALARY_MIN + F.SALARY_MAX) / 2) AS AVG_SALARY_WITH_SKILL,

        -- Remote Work Analysis
        COUNT(CASE WHEN F.WORK_TYPE = 'Remote' THEN 1 END)::FLOAT / COUNT(*) * 100 AS REMOTE_AVAILABILITY_RATE

    FROM ANALYTICS.FACT_JOB_POSTINGS F
    JOIN STAGE.JOB_SKILLS_BRIDGE JSB ON F.JOB_UID = JSB.JOB_UID
    JOIN ANALYTICS.DIM_SKILLS S ON JSB.SKILL_ID = S.SKILL_ID
    JOIN ANALYTICS.DIM_DATE D ON F.DATE_POSTED_KEY = D.DATE_KEY

    WHERE D.FULL_DATE >= CURRENT_DATE - 90
      AND F.IS_ACTIVE_POSTING = TRUE
      AND F.SALARY_MIN IS NOT NULL
      AND F.SALARY_MAX IS NOT NULL

    GROUP BY S.SKILL_NAME, S.SKILL_CATEGORY, S.SKILL_SUBCATEGORY, DATE_TRUNC('week', F.FIRST_POSTED_DATE)
    HAVING COUNT(DISTINCT F.JOB_POSTING_KEY) >= 5  -- Minimum threshold for reliability
)

SELECT
    SKILL_NAME,
    SKILL_CATEGORY,
    SKILL_SUBCATEGORY,
    DEMAND_WEEK,
    JOBS_REQUIRING_SKILL,
    MARKET_PENETRATION_RATE,

    -- Growth Analysis
    LAG(JOBS_REQUIRING_SKILL, 1) OVER (PARTITION BY SKILL_NAME ORDER BY DEMAND_WEEK) AS PREV_WEEK_DEMAND,
    ((JOBS_REQUIRING_SKILL::FLOAT / LAG(JOBS_REQUIRING_SKILL, 1) OVER (PARTITION BY SKILL_NAME ORDER BY DEMAND_WEEK)) - 1) * 100 AS WOW_GROWTH_RATE,

    -- Salary Intelligence
    AVG_SALARY_WITH_SKILL,
    REMOTE_AVAILABILITY_RATE,

    -- Market Classification
    CASE
        WHEN MARKET_PENETRATION_RATE > 20 THEN 'POPULAR'
        WHEN MARKET_PENETRATION_RATE > 5 THEN 'NICHE'
        ELSE 'SPECIALIZED'
    END AS MARKET_ADOPTION_LEVEL,

    -- Trend Classification
    CASE
        WHEN ((JOBS_REQUIRING_SKILL::FLOAT / LAG(JOBS_REQUIRING_SKILL, 1) OVER (PARTITION BY SKILL_NAME ORDER BY DEMAND_WEEK)) - 1) * 100 > 25 THEN 'HOT'
        WHEN ((JOBS_REQUIRING_SKILL::FLOAT / LAG(JOBS_REQUIRING_SKILL, 1) OVER (PARTITION BY SKILL_NAME ORDER BY DEMAND_WEEK)) - 1) * 100 > 10 THEN 'GROWING'
        WHEN ((JOBS_REQUIRING_SKILL::FLOAT / LAG(JOBS_REQUIRING_SKILL, 1) OVER (PARTITION BY SKILL_NAME ORDER BY DEMAND_WEEK)) - 1) * 100 > -10 THEN 'STABLE'
        ELSE 'DECLINING'
    END AS TREND_CATEGORY

FROM SKILL_DEMAND_TRENDS
ORDER BY DEMAND_WEEK DESC, JOBS_REQUIRING_SKILL DESC;
```

## Phase 5 Analytics Layer - Summary of Key Updates

### Major Changes Made:

#### Latest Update: Grain Simplification for Business Reality
**Root Cause**: Original plan used unnecessarily granular time dimensions that don't align with actual business needs and decision-making cycles.

1. **`FACT_JOB_POSTINGS`**: Changed from "one record per job per day" to "one record per unique job posting"
   - **Problem**: Daily grain added 90% more records without business value since job attributes rarely change daily
   - **Solution**: Simplified to natural grain with `FIRST_POSTED_DATE` for trends, `IS_ACTIVE_POSTING` for status tracking
   - **Benefit**: Simpler analytics, better performance, easier to understand

2. **`FACT_SKILLS_DEMAND_WEEKLY`**: Replaced `fact_skills_demand_monthly` with weekly aggregation
   - **Problem**: Monthly skill demand tracking was too slow for responsive market intelligence
   - **Solution**: Weekly trends provide timely insights for skill analysis and strategic decision-making
   - **Benefit**: Enables responsive trend analysis while maintaining analytical value

3. **`FACT_COMPANY_HIRING_WEEKLY`**: Changed to weekly tracking for business alignment
   - **Problem**: Monthly tracking doesn't align with the primary business goal of weekly job market intelligence
   - **Solution**: Weekly hiring patterns provide timely insights for competitive analysis and trend detection
   - **Benefit**: Aligns with weekly reporting requirements and enables early trend detection

#### Previous Updates:
1. **Table Reference Standardization**: Updated all table references to match actual STAGE schema implementation
2. **Enhanced Dimension Tables**: Added fields from actual STAGE implementation (e.g., `CANONICAL_FORM`, `COMMON_ALIASES`, `FREQUENCY_COUNT` in `DIM_SKILLS`)
3. **Improved Location Intelligence**: Added international support, tech hub classification, and geographic hierarchy from STAGE
4. **Enhanced Job Classification**: Updated `FACT_JOB_POSTINGS` with actual LLM enrichment fields and confidence scores
5. **Corrected SQL Queries**: Fixed column names and skill category values to match STAGE implementation
6. **Aligned Business Views**: Updated all business views to use correct table and column names from STAGE

**Result**: Analytics layer now focuses on business-driven grain and timing that aligns with real-world decision-making processes, eliminating unnecessary complexity while maintaining full analytical capability.

## Implementation Phases

**Prerequisites**: STAGE layer LLM data normalization must be completed before beginning ANALYTICS implementation. This includes `STAGE.SKILLS_NORMALIZED`, `STAGE.JOB_SKILLS_BRIDGE`, `STAGE.KEYWORDS_NORMALIZED`, `STAGE.LOCATIONS_NORMALIZED`, and `STAGE.JOB_LOCATIONS_BRIDGE` tables.

**✅ UPDATED PLAN ALIGNED WITH STAGE IMPLEMENTATION** (January 2025):
- **Table References**: Updated all SQL references to use actual STAGE table names (e.g., `STAGE.SKILLS_NORMALIZED` instead of `stage.skills_normalized`)
- **Schema Alignment**: Updated column references to match actual STAGE schema (e.g., `SKILL_ID`, `JOB_UID`, `SKILL_CATEGORY`)
- **Enhanced Data Sources**: Incorporated additional STAGE capabilities including international location support, skill family classification, and enhanced confidence scoring
- **Improved Dimensions**: Enhanced `DIM_SKILLS` and `DIM_LOCATION` with actual STAGE data fields like `CANONICAL_FORM`, `COMMON_ALIASES`, `IS_MAJOR_TECH_HUB`, and geographic hierarchy
- **Fact Table Enhancement**: Added LLM confidence fields, job classification, and work arrangement data from `STAGE.JOBS_LLM_ENRICHED`
- **Ready for Implementation**: All dimensional model components now accurately reference implemented STAGE layer structure

### Phase 1: Foundation & Core Dimensional Model
**Objective**: Establish the core star schema structure and essential dimension tables

**Components**:
- Create ANALYTICS schema and core dimension tables
- Implement `DIM_DATE` with business calendar
- Build `DIM_COMPANY` with Type 2 SCD logic
- Create `DIM_LOCATION` with geographic hierarchy
- Develop `DIM_JOB_FAMILY` with role taxonomy
- Build `DIM_PLATFORM` for ATS classification
- Create `DIM_SKILLS` with skills taxonomy

**Key Deliverables**:
- Fully populated dimension tables with proper hierarchies
- Surrogate key generation and management
- Data quality validation for all dimensions
- Foreign key relationships and referential integrity

### Phase 2: Primary Fact Table Implementation
**Objective**: Create the main `FACT_JOB_POSTINGS` table with complete measure library

**Components**:
- Design and implement `FACT_JOB_POSTINGS` grain and measures
- Build ETL pipeline from STAGE to ANALYTICS layer
- Implement incremental loading and SCD processing
- Create data quality monitoring and validation
- Establish partitioning and clustering strategy

**Key Deliverables**:
- Production-ready `FACT_JOB_POSTINGS` table
- Automated daily refresh processes
- Data lineage tracking and audit capabilities
- Performance-optimized table structure

### Phase 3: Specialized Aggregate Tables
**Objective**: Build domain-specific aggregate tables for advanced analytics (revised for business-appropriate grain)

**Components**:
- Implement `FACT_SKILLS_DEMAND_WEEKLY` for technology trend analysis (weekly grain)
- Build `FACT_COMPANY_HIRING_WEEKLY` for company analysis (weekly grain)
- Develop efficient aggregation strategies aligned with business decision cycles

**Key Deliverables**:
- Business-aligned aggregate tables with appropriate grain and measures
- Cross-table consistency and performance optimization
- Advanced analytics capabilities that match real business needs

### Phase 4: Pre-Aggregated Metrics & KPIs
**Objective**: Create business-ready metric tables for fast reporting

**Components**:
- Build `MARKET_WEEKLY_SUMMARY` for executive dashboards
- Create `SKILLS_TREND_ANALYSIS` for technology intelligence
- Implement `COMPANY_HIRING_INTELLIGENCE` for company analysis
- Develop automated refresh and calculation processes

**Key Deliverables**:
- Pre-calculated metric tables with sub-second query response
- Weekly automated metric generation
- Business rule validation and anomaly detection

### Phase 5: Business Views & Analytics Interface
**Objective**: Create user-friendly views and analytics interfaces

**Components**:
- Develop executive dashboard views
- Create specialized analytics views for each business domain
- Build data access layer for BI tools
- Implement row-level security and access controls

**Key Deliverables**:
- Production-ready business views
- BI tool connectivity and optimization
- User access management and security

### Phase 6: Advanced Analytics & Intelligence
**Objective**: Implement predictive analytics and market intelligence

**Components**:
- Build trend analysis and forecasting capabilities
- Create market anomaly detection
- Implement comparative analytics and benchmarking
- Develop alerting and notification systems

**Key Deliverables**:
- Advanced analytics capabilities
- Automated market intelligence reporting
- Predictive insights and trend forecasting

## Success Metrics & KPIs

### Technical Performance
- **Query Performance**: <1 second response for executive dashboard queries
- **Data Freshness**: Weekly metrics available within 2 hours of STAGE refresh
- **Data Quality**: >99% data completeness for critical business measures
- **System Uptime**: 99.9% availability for analytics layer

### Business Value
- **Market Intelligence**: Enable accurate weekly job market trend identification
- **Competitive Analysis**: Provide comprehensive company hiring intelligence
- **Salary Benchmarking**: Deliver accurate compensation market data
- **Skills Intelligence**: Track technology demand and career trend insights

### User Adoption
- **Dashboard Usage**: Weekly executive dashboard utilization
- **Query Performance**: Average query response time <5 seconds
- **Data Accuracy**: Business user confidence in reported metrics
- **Self-Service Analytics**: Reduced ad-hoc query requests to data team

## Data Governance & Quality

### Data Quality Framework
- **Completeness**: Minimum 95% data completeness for critical measures
- **Accuracy**: Automated data validation and business rule checking
- **Consistency**: Cross-table consistency validation and monitoring
- **Timeliness**: Defined SLA for data freshness and update frequency

### Security & Access Control
- **Role-Based Access**: Hierarchical access control by business function
- **Data Masking**: Sensitive data protection for non-production environments
- **Audit Logging**: Complete audit trail for all data access and modifications
- **Compliance**: Data retention and privacy compliance framework

### Monitoring & Alerting
- **Data Pipeline Monitoring**: Automated monitoring of all ETL processes
- **Quality Alerting**: Automated alerts for data quality issues and anomalies
- **Performance Monitoring**: Query performance and system health monitoring
- **Business Alerting**: Market trend alerts and threshold-based notifications

## Future Enhancements

### Advanced Analytics Capabilities
- **Machine Learning Integration**: Predictive hiring models and trend forecasting
- **Natural Language Querying**: Business user self-service analytics interface
- **Real-Time Analytics**: Near real-time job market monitoring and alerting
- **External Data Integration**: Economic indicators and market context data

### Extended Business Intelligence
- **Geographic Analytics**: Detailed location-based market intelligence
- **Industry Benchmarking**: Cross-industry hiring pattern analysis
- **Competitive Intelligence**: Company-specific competitive positioning
- **Career Path Analytics**: Skills progression and career development insights

This comprehensive ANALYTICS layer will transform the BetterJobs-Snow pipeline into a robust job market intelligence platform, enabling data-driven decision making and comprehensive market insights for weekly business reporting and strategic planning.

## DEVELOPMENT IMPLEMENTATION PLAN

*Updated: 2024-12-28*

Now that all database objects are defined for the analytics layer, this section provides the detailed implementation roadmap with specific development tasks and deliverables.

### Development Prerequisites

**✅ STAGE Layer Dependencies (Already Completed)**:
- `STAGE.JOBS_UNIFIED` - Core job data with standardized fields
- `STAGE.JOBS_LLM_ENRICHED` - LLM-extracted job attributes and classifications
- `STAGE.COMPANY_PROFILES` - Company metadata and classifications
- `STAGE.SKILLS_NORMALIZED` - Standardized skills taxonomy
- `STAGE.JOB_SKILLS_BRIDGE` - Job-to-skills relationships
- `STAGE.LOCATIONS_NORMALIZED` - Standardized location hierarchy
- `STAGE.JOB_LOCATIONS_BRIDGE` - Job-to-location relationships
- `STAGE.KEYWORDS_NORMALIZED` - Standardized keyword taxonomy

### Dagster Assets Architecture

The analytics layer implementation will follow the existing Dagster asset patterns used in the STAGE layer. Each asset has clear dependencies and specific business purposes.

#### Asset Dependency Flow

```
STAGE Layer Assets (prerequisite)
    ↓
analytics_dimension_tables (Phase 1)
    ↓
analytics_fact_job_postings (Phase 2)
    ↓
┌─ analytics_fact_skills_demand_weekly (Phase 3)
├─ analytics_fact_company_hiring_weekly (Phase 3)
└─ analytics_market_intelligence_tables (Phase 4)
    ↓
analytics_business_views (Phase 5)
```

### Phase 1: Dimension Tables Implementation

#### 1.1 `analytics_dim_date`
**Purpose**: Create date dimension for time-based analysis
**Dependencies**: None (reference data)
**Output**: Complete date dimension with business calendar

```python
@asset(
    description="Create date dimension table for analytics time-based analysis",
    group_name="analytics_dimensions",
    kinds={"snowflake", "SQL"}
)
def analytics_dim_date(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Create comprehensive date dimension spanning 10 years.

    Features:
    - Business day indicators
    - Week/month/quarter/year hierarchy
    - Holiday flags for business intelligence
    """
```

#### 1.2 `analytics_dim_company`
**Purpose**: Create company dimension with SCD Type 2 logic
**Dependencies**: `stage_company_profiles`, `stage_jobs_unified`
**Output**: Company dimension with historical tracking

```python
@asset(
    deps=["stage_company_profiles", "stage_jobs_unified"],
    description="Create company dimension with Type 2 SCD for company changes",
    group_name="analytics_dimensions",
    kinds={"snowflake", "SQL"}
)
def analytics_dim_company(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build company dimension from STAGE sources.

    Processing:
    - Apply Type 2 SCD logic for company changes
    - Standardize company names and classifications
    - Add company size and industry attributes
    """
```

#### 1.3 `analytics_dim_location`
**Purpose**: Create location dimension with geographic hierarchy
**Dependencies**: `stage_locations_normalized`
**Output**: Location dimension with geographic intelligence

#### 1.4 `analytics_dim_job_family`
**Purpose**: Create job classification dimension
**Dependencies**: `stage_jobs_llm_enriched`
**Output**: Job family hierarchy for role analysis

#### 1.5 `analytics_dim_platform`
**Purpose**: Create platform dimension for ATS classification
**Dependencies**: `stage_jobs_unified`
**Output**: Platform characteristics dimension

#### 1.6 `analytics_dim_skills`
**Purpose**: Create skills dimension from normalized skills
**Dependencies**: `stage_skills_normalized`
**Output**: Skills taxonomy dimension

### Phase 2: Primary Fact Table Implementation

#### 2.1 `analytics_fact_job_postings`
**Purpose**: Create primary fact table for job posting analytics
**Dependencies**: All dimension tables, `stage_jobs_unified`, `stage_jobs_llm_enriched`
**Output**: Core fact table with measures and dimension keys

```python
@asset(
    deps=["analytics_dim_date", "analytics_dim_company", "analytics_dim_location",
          "analytics_dim_job_family", "analytics_dim_platform", "analytics_dim_skills",
          "stage_jobs_unified", "stage_jobs_llm_enriched"],
    description="Create primary fact table for job posting analytics",
    group_name="analytics_facts",
    kinds={"snowflake", "SQL"}
)
def analytics_fact_job_postings(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build the primary fact table for job posting analytics.

    Processing:
    - Join STAGE tables with dimension lookups
    - Apply business rules and data quality filters
    - Generate surrogate keys and measures
    - Implement incremental loading logic
    """
```

**Key Implementation Tasks**:
1. **Dimension Key Lookups**: Map natural keys to surrogate keys
2. **Measure Calculations**: Salary midpoints, experience calculations
3. **Data Quality Rules**: Filter invalid or incomplete records
4. **Incremental Processing**: Daily refresh with change detection

### Phase 3: Aggregate Fact Tables Implementation

#### 3.1 `analytics_fact_skills_demand_weekly`
**Purpose**: Create weekly skills demand aggregates
**Dependencies**: `analytics_fact_job_postings`, `stage_job_skills_bridge`
**Output**: Skills demand trends with weekly granularity

```python
@asset(
    deps=["analytics_fact_job_postings", "stage_job_skills_bridge"],
    description="Create weekly skills demand aggregate fact table",
    group_name="analytics_aggregates",
    kinds={"snowflake", "SQL"}
)
def analytics_fact_skills_demand_weekly(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Aggregate job postings by skill and week for trend analysis.

    Processing:
    - Calculate skill penetration rates
    - Compute salary premiums by skill
    - Track week-over-week growth
    - Generate skill ranking metrics
    """
```

#### 3.2 `analytics_fact_company_hiring_weekly`
**Purpose**: Create weekly company hiring intelligence
**Dependencies**: `analytics_fact_job_postings`
**Output**: Company hiring patterns and velocity metrics

### Phase 4: Market Intelligence Tables Implementation

#### 4.1 `analytics_market_weekly_summary`
**Purpose**: Pre-aggregated executive metrics
**Dependencies**: `analytics_fact_job_postings`
**Output**: Weekly market overview for dashboards

#### 4.2 `analytics_skills_trend_analysis`
**Purpose**: Skills intelligence with trend analysis
**Dependencies**: `analytics_fact_skills_demand_weekly`
**Output**: Skills market intelligence for reporting

#### 4.3 `analytics_company_hiring_intelligence`
**Purpose**: Company-specific hiring analytics
**Dependencies**: `analytics_fact_company_hiring_weekly`
**Output**: Company intelligence for competitive analysis

### Phase 5: Business Views Implementation

#### 5.1 `analytics_view_weekly_market_overview`
**Purpose**: Executive dashboard view
**Dependencies**: Market intelligence tables
**Output**: Business-ready view for reporting

#### 5.2 `analytics_view_salary_intelligence`
**Purpose**: Compensation analysis view
**Dependencies**: `analytics_fact_job_postings`
**Output**: Salary benchmarking analytics

#### 5.3 `analytics_view_skills_market_intelligence`
**Purpose**: Technology trends view
**Dependencies**: Skills trend tables
**Output**: Skills demand analytics

### Implementation Phases Timeline

#### Phase 1: Foundation
**Deliverables**:
- All 6 dimension tables created and populated
- Surrogate key generation logic
- Data quality validation framework
- Performance optimization (clustering/partitioning)

#### Phase 2: Core Facts
**Deliverables**:
- `FACT_JOB_POSTINGS` table operational
- Incremental loading process
- Data lineage and audit capabilities
- Query performance benchmarks

#### Phase 3: Aggregates
**Deliverables**:
- Weekly skills demand aggregates
- Company hiring intelligence aggregates
- Automated weekly refresh processes
- Cross-table consistency validation

#### Phase 4: Market Intelligence
**Deliverables**:
- Pre-calculated market metrics
- Executive dashboard data
- Automated metric generation
- Business rule validation

#### Phase 5: Business Views
**Deliverables**:
- Production-ready analytics views
- Optimized query performance
- Business user access layer
- Documentation and training materials

### Processing Functions and Utilities

#### Core Transformation Classes

```python
# File: pipeline/dagster_betterjobs/dagster_betterjobs/transformations/analytics_layer.py

class AnalyticsDimensionBuilder:
    """Handle dimension table creation and SCD processing"""

    def build_company_dimension(self, snowflake: SnowflakeResource) -> Dict[str, Any]:
        """Build company dimension with Type 2 SCD logic"""
        # Implementation for company dimension processing

    def build_location_dimension(self, snowflake: SnowflakeResource) -> Dict[str, Any]:
        """Build location dimension with geographic hierarchy"""
        # Implementation for location dimension processing

class AnalyticsFactBuilder:
    """Handle fact table creation and measures calculation"""

    def build_job_postings_fact(self, snowflake: SnowflakeResource) -> Dict[str, Any]:
        """Build primary job postings fact table"""
        # Implementation for fact table processing

    def calculate_weekly_aggregates(self, snowflake: SnowflakeResource) -> Dict[str, Any]:
        """Calculate weekly aggregate measures"""
        # Implementation for aggregate calculations

class AnalyticsQualityValidator:
    """Validate analytics layer data quality"""

    def validate_dimension_integrity(self) -> Dict[str, Any]:
        """Validate dimension table integrity"""
        # Implementation for dimension validation

    def validate_fact_completeness(self) -> Dict[str, Any]:
        """Validate fact table completeness"""
        # Implementation for fact validation
```

### Success Criteria

#### Technical KPIs
- **Query Performance**: <1 second for executive dashboards, <5 seconds for complex analytics
- **Data Freshness**: Weekly metrics available within 2 hours of STAGE refresh
- **Data Quality**: >99% completeness for critical business measures
- **Processing Performance**: Complete daily refresh within 30 minutes

#### Business KPIs
- **Market Intelligence**: Enable accurate weekly job market trend identification
- **Salary Intelligence**: Provide comprehensive compensation analysis
- **Skills Intelligence**: Track technology demand trends
- **Company Intelligence**: Deliver hiring pattern insights

#### Coverage KPIs
- **Job Coverage**: >95% of STAGE jobs represented in analytics
- **Skills Coverage**: >90% of jobs with skills analysis
- **Geographic Coverage**: >85% of jobs with location intelligence
- **Temporal Coverage**: Complete historical analysis capability

This implementation plan provides a focused roadmap for building the analytics layer assets using the established Dagster patterns, with clear phases, deliverables, and success metrics.