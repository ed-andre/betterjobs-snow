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

**✅ STAGE Layer Dependencies (Already Completed)**:
- `STAGE.JOBS_UNIFIED` - Core job data with standardized fields
- `STAGE.JOBS_LLM_ENRICHED` - LLM-extracted job attributes and classifications
- `STAGE.COMPANY_PROFILES` - Company metadata and classifications
- `STAGE.SKILLS_NORMALIZED` - Standardized skills taxonomy
- `STAGE.JOB_SKILLS_BRIDGE` - Job-to-skills relationships
- `STAGE.LOCATIONS_NORMALIZED` - Standardized location hierarchy
- `STAGE.JOB_LOCATIONS_BRIDGE` - Job-to-location relationships
- `STAGE.KEYWORDS_NORMALIZED` - Standardized keyword taxonomy
- `STAGE.JOB_KEYWORDS_BRIDGE` - Job-to-keywords relationships
- `STAGE.EXPERIENCE_NORMALIZED` - Standardized experience levels and requirements
- `STAGE.JOB_EXPERIENCE_BRIDGE` - Job-to-experience relationships


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
    experience_key STRING,               -- FK to DIM_EXPERIENCE
    keyword_key STRING,                  -- FK to DIM_KEYWORDS (primary keyword for job)
    salary_key STRING,                   -- FK to DIM_SALARY (normalized salary data)

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

    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (skill_category, skill_subcategory);
```

##### 7. `DIM_SALARY` (Salary Dimension)
**Normalized Salary Ranges (Built from STAGE.SALARY_NORMALIZED)**

```sql
CREATE TABLE ANALYTICS.DIM_SALARY (
    SALARY_KEY STRING PRIMARY KEY,
    SALARY_ID STRING,                    -- FK to STAGE.SALARY_NORMALIZED.SALARY_ID
    SALARY_RANGE_NAME STRING,            -- Human-readable salary range description

    -- Original Values (Audit Trail)
    SALARY_MIN_ORIGINAL NUMBER,          -- From STAGE.SALARY_NORMALIZED.SALARY_MIN_ORIGINAL
    SALARY_MAX_ORIGINAL NUMBER,          -- From STAGE.SALARY_NORMALIZED.SALARY_MAX_ORIGINAL
    SALARY_PERIOD_ORIGINAL STRING,       -- From STAGE.SALARY_NORMALIZED.SALARY_PERIOD_ORIGINAL
    SALARY_CURRENCY_ORIGINAL STRING,     -- From STAGE.SALARY_NORMALIZED.SALARY_CURRENCY_ORIGINAL

    -- Normalized Values (All Annual USD)
    SALARY_MIN_ANNUAL_USD NUMBER,        -- From STAGE.SALARY_NORMALIZED.SALARY_MIN_ANNUAL_USD
    SALARY_MAX_ANNUAL_USD NUMBER,        -- From STAGE.SALARY_NORMALIZED.SALARY_MAX_ANNUAL_USD
    SALARY_MIDPOINT_ANNUAL_USD NUMBER,   -- From STAGE.SALARY_NORMALIZED.SALARY_MIDPOINT_ANNUAL_USD
    NORMALIZATION_FACTOR FLOAT,          -- From STAGE.SALARY_NORMALIZED.NORMALIZATION_FACTOR

    -- Quality & Validation
    CONFIDENCE_SCORE FLOAT,              -- From STAGE.SALARY_NORMALIZED.CONFIDENCE_SCORE
    OUTLIER_FLAG BOOLEAN,                -- From STAGE.SALARY_NORMALIZED.OUTLIER_FLAG
    MANUAL_REVIEW_FLAG BOOLEAN,          -- From STAGE.SALARY_NORMALIZED.MANUAL_REVIEW_FLAG
    APPROVED_BY_ADMIN BOOLEAN,           -- From STAGE.SALARY_NORMALIZED.APPROVED_BY_ADMIN

    -- Market Intelligence
    FREQUENCY_COUNT INTEGER,             -- From STAGE.SALARY_NORMALIZED.FREQUENCY_COUNT
    MARKET_PERCENTILE INTEGER,           -- From STAGE.SALARY_NORMALIZED.MARKET_PERCENTILE

    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (salary_midpoint_annual_usd);
```

##### 8. `DIM_EXPERIENCE` (Experience Requirements Dimension)
**Experience Levels and Requirements Classification (Built from STAGE.EXPERIENCE_NORMALIZED)**

```sql
CREATE TABLE ANALYTICS.DIM_EXPERIENCE (
    EXPERIENCE_KEY STRING PRIMARY KEY,
    EXPERIENCE_ID STRING,              -- FK to STAGE.EXPERIENCE_NORMALIZED.EXPERIENCE_ID
    EXPERIENCE_NAME STRING,

    -- Experience Classification (from STAGE.EXPERIENCE_NORMALIZED)
    EXPERIENCE_CATEGORY STRING,        -- From STAGE.EXPERIENCE_NORMALIZED.EXPERIENCE_CATEGORY
    MIN_YEARS_REQUIRED INTEGER,        -- From STAGE.EXPERIENCE_NORMALIZED.MIN_YEARS_REQUIRED
    MAX_YEARS_REQUIRED INTEGER,        -- From STAGE.EXPERIENCE_NORMALIZED.MAX_YEARS_REQUIRED
    SENIORITY_ORDER INTEGER,           -- From STAGE.EXPERIENCE_NORMALIZED.SENIORITY_ORDER
    EXPERIENCE_DESCRIPTION STRING,     -- From STAGE.EXPERIENCE_NORMALIZED.EXPERIENCE_DESCRIPTION

    -- Market Intelligence (from STAGE.EXPERIENCE_NORMALIZED)
    MARKET_FREQUENCY INTEGER,          -- From STAGE.EXPERIENCE_NORMALIZED.MARKET_FREQUENCY
    CONFIDENCE_SCORE FLOAT,            -- From STAGE.EXPERIENCE_NORMALIZED.CONFIDENCE_SCORE

    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (experience_category, seniority_order);
```

##### 9. `DIM_KEYWORDS` (Keywords Taxonomy Dimension)
**Keywords Classification (Built from STAGE.KEYWORDS_NORMALIZED)**

```sql
CREATE TABLE ANALYTICS.DIM_KEYWORDS (
    KEYWORD_KEY STRING PRIMARY KEY,
    KEYWORD_ID STRING,                 -- FK to STAGE.KEYWORDS_NORMALIZED.KEYWORD_ID
    KEYWORD_TEXT STRING,
    KEYWORD_TEXT_CLEAN STRING,

    -- Keyword Classification (from STAGE.KEYWORDS_NORMALIZED)
    KEYWORD_TYPE STRING,               -- From STAGE.KEYWORDS_NORMALIZED.KEYWORD_TYPE
    KEYWORD_CATEGORY STRING,           -- From STAGE.KEYWORDS_NORMALIZED.KEYWORD_CATEGORY
    CANONICAL_FORM STRING,             -- From STAGE.KEYWORDS_NORMALIZED.CANONICAL_FORM
    ORIGINAL_VARIANTS VARIANT,         -- From STAGE.KEYWORDS_NORMALIZED.ORIGINAL_VARIANTS

    -- Market Intelligence (from STAGE.KEYWORDS_NORMALIZED)
    FREQUENCY_COUNT INTEGER,           -- From STAGE.KEYWORDS_NORMALIZED.FREQUENCY_COUNT
    TREND_SCORE FLOAT,                 -- From STAGE.KEYWORDS_NORMALIZED.TREND_SCORE
    CONFIDENCE_SCORE FLOAT,            -- From STAGE.KEYWORDS_NORMALIZED.CONFIDENCE_SCORE
    APPROVED_BY_ADMIN BOOLEAN,         -- From STAGE.KEYWORDS_NORMALIZED.APPROVED_BY_ADMIN

    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (keyword_type, keyword_category);
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

##### 3. `COMPANY_HIRING_INTELLIGENCE` (Company Analysis View)
**Company-specific hiring patterns and market intelligence computed from weekly fact table**

**Implementation Note**: This is implemented as a **VIEW** rather than a table to eliminate redundancy with `FACT_COMPANY_HIRING_WEEKLY`. The view provides market intelligence calculations while sourcing from the foundational weekly fact data.

**Key Calculations**:
- **Rolling Windows**: 7-day and 30-day hiring metrics calculated using window functions
- **Industry Rankings**: Company position within industry peer groups using RANK() functions
- **Competitiveness Scoring**: Market position scoring based on hiring velocity vs industry averages
- **Work Policy Classification**: Remote work policies derived from percentage breakdowns
- **Role Distribution Analysis**: Entry vs senior ratios calculated from weekly percentages

**Business Value**: Provides executive-ready company intelligence metrics without storing duplicate data, enabling competitive analysis and market positioning insights while maintaining architectural simplicity.

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
- Create `DIM_SALARY` with normalized salary ranges
- Create `DIM_EXPERIENCE` with experience requirements
- Create `DIM_KEYWORDS` with keywords taxonomy

**Key Deliverables**:
- All 9 dimension tables created and populated (including DIM_SALARY, DIM_EXPERIENCE, and DIM_KEYWORDS)
- Surrogate key generation logic
- Data quality validation framework
- Performance optimization (clustering/partitioning)

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

### Phase 4: Market Intelligence Tables
**Objective**: Create business-ready metric tables for fast reporting

**Components**:
- Build `MARKET_WEEKLY_SUMMARY` for executive dashboards
- Create `SKILLS_TREND_ANALYSIS` for technology intelligence
- Implement `COMPANY_HIRING_INTELLIGENCE` view for company analysis (computed from weekly fact table)
- Develop automated refresh and calculation processes

**Key Deliverables**:
- Pre-calculated metric tables with sub-second query response
- Weekly automated metric generation
- Business rule validation and anomaly detection

#### 4.1 `analytics_market_weekly_summary`
**Purpose**: Create weekly market summary table for executive dashboard performance
**Dependencies**: `analytics_fact_job_postings`
**Output**: Pre-aggregated weekly market metrics with sub-second query response

```python
@asset(
    deps=["analytics_fact_job_postings"],
    description="Create weekly market summary table for executive dashboard performance",
    group_name="3d_analytics_market_intelligence",
    kinds={"snowflake", "SQL"}
)
def analytics_market_weekly_summary(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build weekly market summary table from FACT_JOB_POSTINGS for executive dashboard performance.

    This asset creates pre-aggregated weekly market metrics to ensure sub-second executive dashboard
    performance while minimizing storage overhead (~52 records per year).

    Processing Logic:
    1. Aggregate job postings by week (Sunday-Saturday) from FACT_JOB_POSTINGS
    2. Calculate core market metrics (job counts, velocity, growth rates)
    3. Compute salary intelligence using denormalized annual USD fields
    4. Analyze work arrangement trends from WORK_TYPE field
    5. Generate data quality metrics and sample size indicators
    6. Implement incremental processing for new weeks only

    Data Quality Rules:
    - Include only active job postings (IS_ACTIVE_POSTING = TRUE)
    - Use salary data only where SALARY_CONFIDENCE >= 0.6
    - Apply LLM confidence filtering (LLM_OVERALL_CONFIDENCE >= 0.5)
    - Require minimum 100 jobs per week for statistical validity
    - Validate week-over-week calculations for outlier detection

    Performance Features:
    - Weekly grain provides optimal balance of detail vs. performance
    - Pre-calculated metrics eliminate dashboard query complexity
    - Partitioned by WEEK_ENDING_DATE for efficient time-based queries
    - Clustered by WEEK_KEY for analytical access patterns

    Business Intelligence:
    - Executive KPI tracking: hiring velocity, market temperature, growth trends
    - Salary market intelligence: median/average compensation trends
    - Work arrangement insights: remote/hybrid/onsite adoption patterns
    - Data quality monitoring: completeness and confidence metrics

    Returns:
        Dict containing processing statistics and market summary metrics
    """
```

**Implementation Steps**:

**Step 1: Field Validation & Upstream Alignment**
✅ **All Required Fields Available in FACT_JOB_POSTINGS**:
- `FIRST_POSTED_DATE` → Week calculations ✅
- `IS_ACTIVE_POSTING` → TOTAL_ACTIVE_JOBS ✅
- `SALARY_MIN_ANNUAL_USD`, `SALARY_MAX_ANNUAL_USD` → Salary intelligence ✅
- `WORK_TYPE` → Work arrangement percentages ✅
- `DATA_QUALITY_SCORE` → Quality metrics ✅
- `SALARY_CONFIDENCE` → Salary quality filtering ✅
- `LLM_OVERALL_CONFIDENCE` → LLM quality filtering ✅

**Step 2: Weekly Aggregation Strategy**
```sql
-- Core aggregation approach:
WITH weekly_job_data AS (
    SELECT
        DATE_TRUNC('week', FIRST_POSTED_DATE) as week_start_date,
        DATE_TRUNC('week', FIRST_POSTED_DATE) + 6 as week_ending_date,
        TO_CHAR(FIRST_POSTED_DATE, 'IYYY-IW') as week_key,

        -- Job counting logic
        COUNT(*) as total_jobs_posted,
        COUNT(CASE WHEN IS_ACTIVE_POSTING THEN 1 END) as total_active_jobs,
        COUNT(CASE WHEN DATE_TRUNC('week', FIRST_POSTED_DATE) = week_start_date THEN 1 END) as new_jobs_posted,

        -- Salary calculations (using denormalized fields for performance)
        MEDIAN(CASE WHEN SALARY_CONFIDENCE >= 0.6 AND SALARY_MIDPOINT_ANNUAL_USD > 0
                    THEN SALARY_MIDPOINT_ANNUAL_USD END) as median_salary_all_roles,
        AVG(CASE WHEN SALARY_CONFIDENCE >= 0.6 AND SALARY_MIDPOINT_ANNUAL_USD > 0
                 THEN SALARY_MIDPOINT_ANNUAL_USD END) as avg_salary_all_roles,

        -- Work arrangement percentages
        COUNT(CASE WHEN WORK_TYPE = 'Remote' THEN 1 END)::FLOAT / COUNT(*) * 100 as remote_work_percentage,
        COUNT(CASE WHEN WORK_TYPE = 'Hybrid' THEN 1 END)::FLOAT / COUNT(*) * 100 as hybrid_work_percentage,
        COUNT(CASE WHEN WORK_TYPE = 'On-site' THEN 1 END)::FLOAT / COUNT(*) * 100 as onsite_work_percentage,

        -- Quality metrics
        COUNT(*) as sample_size,
        AVG(DATA_QUALITY_SCORE) as data_quality_score

    FROM ANALYTICS.FACT_JOB_POSTINGS
    WHERE IS_ACTIVE_POSTING = TRUE
      AND LLM_OVERALL_CONFIDENCE >= 0.5
      AND FIRST_POSTED_DATE >= CURRENT_DATE - 365  -- 1 year retention
    GROUP BY week_start_date, week_ending_date, week_key
    HAVING COUNT(*) >= 100  -- Minimum statistical validity
)
```

**Step 3: Growth Calculations**
```sql
-- Add week-over-week growth metrics:
weekly_with_trends AS (
    SELECT wjd.*,
           LAG(wjd.total_jobs_posted, 1) OVER (ORDER BY wjd.week_start_date) as prev_week_jobs,
           ((wjd.total_jobs_posted::FLOAT / LAG(wjd.total_jobs_posted, 1) OVER (ORDER BY wjd.week_start_date)) - 1) * 100 as week_over_week_growth_rate,
           wjd.total_jobs_posted::FLOAT / 7 as posting_velocity_daily
    FROM weekly_job_data wjd
)
```

**Step 4: Business Rules & Data Quality**
- **Minimum Sample Size**: 100 jobs per week for executive reporting validity
- **Salary Quality**: Include salary data only with confidence >= 0.6
- **LLM Quality**: Apply overall confidence filter >= 0.5
- **Date Range**: 1 year retention for trending analysis
- **Active Jobs**: Include only active postings for current market state

**Step 5: Incremental Processing Strategy**
```sql
-- Process only new weeks to optimize performance:
WHERE NOT EXISTS (
    SELECT 1 FROM ANALYTICS.MARKET_WEEKLY_SUMMARY existing
    WHERE existing.WEEK_KEY = calculated.week_key
)
-- OR update existing weeks if data has changed (rare)
```

**Expected Data Volume & Performance**:
- **Weekly Records**: ~52 per year (minimal storage overhead)
- **Processing Time**: <2 minutes for weekly refresh
- **Query Performance**: <500ms for executive dashboard queries
- **Data Retention**: 2 years for trend analysis

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
- `STAGE.JOB_KEYWORDS_BRIDGE` - Job-to-keywords relationships
- `STAGE.EXPERIENCE_NORMALIZED` - Standardized experience levels and requirements
- `STAGE.JOB_EXPERIENCE_BRIDGE` - Job-to-experience relationships


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

#### 1.1 `analytics_dim_date` ✅ COMPLETE
**Purpose**: Create date dimension for time-based analysis
**Dependencies**: None (reference data)
**Output**: Complete date dimension with business calendar

```python
@asset(
    description="Create date dimension table for analytics time-based analysis",
    group_name="3a_analytics_dimensions",
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

#### 1.2 `analytics_dim_company` ✅ COMPLETE
**Purpose**: Create company dimension with SCD Type 2 logic
**Dependencies**: `stage_company_profiles`, `stage_jobs_unified`
**Output**: Company dimension with historical tracking

**Implementation Approach**:
1. **Source Data Integration**: Combine company data from JOBS_UNIFIED (operational) and COMPANY_PROFILES (enriched)
2. **SCD Type 2 Logic**: Track historical changes to company attributes over time
3. **Surrogate Key Generation**: Create unique company_key for each version
4. **Data Quality Rules**: Handle missing/null company profiles gracefully

**Key Processing Steps**:
```sql
-- Step 1: Create unified company view from both sources
WITH company_current_state AS (
    SELECT
        ju.COMPANY_ID,
        ju.COMPANY_NAME_CLEAN,
        cp.COMPANY_NAME_STANDARDIZED,
        cp.COMPANY_INDUSTRY_STANDARDIZED as industry,
        cp.EMPLOYEE_COUNT_RANGE,
        cp.COMPANY_SIZE_CATEGORY,
        cp.HEADQUARTERS_LOCATION,
        cp.FUNDING_STAGE,
        CURRENT_DATE as snapshot_date
    FROM STAGE.JOBS_UNIFIED ju
    LEFT JOIN STAGE.COMPANY_PROFILES cp ON ju.COMPANY_ID = cp.COMPANY_ID
    WHERE ju.COMPANY_ID IS NOT NULL
    GROUP BY ALL -- Deduplicate companies
),

-- Step 2: Detect changes by comparing with existing dimension
changes_detected AS (
    SELECT c.*,
           d.company_key as existing_key,
           d.version_number as current_version,
           CASE WHEN d.company_key IS NULL THEN 'NEW'
                WHEN (d.company_name != c.COMPANY_NAME_CLEAN OR
                      d.industry != c.industry OR
                      d.company_size_category != c.COMPANY_SIZE_CATEGORY) THEN 'CHANGED'
                ELSE 'UNCHANGED'
           END as change_type
    FROM company_current_state c
    LEFT JOIN ANALYTICS.DIM_COMPANY d ON c.COMPANY_ID = d.company_id AND d.is_current = TRUE
)

-- Step 3: Apply SCD Type 2 logic
-- Expire changed records, insert new/changed records
```

**SCD Type 2 Change Detection**:
- **Company Name Changes**: COMPANY_NAME_CLEAN vs existing company_name
- **Industry Changes**: COMPANY_INDUSTRY_STANDARDIZED vs existing industry
- **Size Changes**: COMPANY_SIZE_CATEGORY vs existing company_size_category
- **Location Changes**: HEADQUARTERS_LOCATION vs existing headquarters_location

```python
@asset(
    deps=["stage_company_profiles", "stage_jobs_unified"],
    description="Create company dimension with Type 2 SCD for company changes",
    group_name="3a_analytics_dimensions",
    kinds={"snowflake", "SQL"}
)
def analytics_dim_company(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build company dimension from STAGE sources with SCD Type 2 processing.

    Processing:
    - Integrate data from JOBS_UNIFIED and COMPANY_PROFILES
    - Apply Type 2 SCD logic for company attribute changes
    - Generate surrogate keys and manage version history
    - Handle missing company profiles gracefully

    SCD Logic:
    - NEW companies get new records with is_current=TRUE
    - CHANGED companies: expire old record, create new record
    - UNCHANGED companies: no action needed
    """
```

#### 1.3 `analytics_dim_location` ✅ COMPLETE
**Purpose**: Create location dimension with geographic hierarchy
**Dependencies**: `stage_locations_normalized`
**Output**: Location dimension with geographic intelligence

```python
@asset(
    deps=["stage_locations_normalized"],
    description="Create location dimension with geographic hierarchy and intelligence",
    group_name="3a_analytics_dimensions",
    kinds={"snowflake", "SQL"}
)
def analytics_dim_location(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build location dimension from STAGE.LOCATIONS_NORMALIZED with geographic hierarchy.

    Processing:
    - Generate surrogate keys for each location
    - Map STAGE fields to ANALYTICS dimension structure
    - Apply data quality filters for location completeness
    - Preserve geographic hierarchy and intelligence fields
    - Handle international locations and remote work designations
    """
```

**Implementation Steps**:

**Step 1: Surrogate Key Generation**
- Generate `location_key` as surrogate key using `LOCATION_ID` as natural key
- Format: `LOC_` + `LOCATION_ID` for clear identification
- Ensure uniqueness and consistency across refreshes

**Step 2: Field Mapping & Transformation**
```sql
-- Core field mappings from STAGE.LOCATIONS_NORMALIZED:
location_id → location_id (natural key preservation)
LOCATION_NAME_CLEAN → location_name (use cleaned version)
CITY → city
STATE_PROVINCE → state_province
COUNTRY → country
REGION → region
METRO_AREA → metro_area
LOCATION_TYPE → location_type
IS_REMOTE_FRIENDLY → is_remote_friendly
IS_MAJOR_TECH_HUB → is_major_tech_hub
COST_OF_LIVING_INDEX → cost_of_living_index
AVERAGE_SALARY_ADJUSTMENT → average_salary_adjustment
CONFIDENCE_SCORE → stage_confidence_score
FREQUENCY_COUNT → frequency_count
```

**Step 3: Data Quality Rules**
- Filter locations with `CONFIDENCE_SCORE >= 0.5` for quality assurance
- Exclude locations marked for `MANUAL_REVIEW_FLAG = TRUE` if not admin approved
- Ensure required fields (city, country) are not null
- Handle special location types: 'Remote', 'Hybrid', 'Global'

**Step 4: Geographic Hierarchy Validation**
- Validate city-state-country relationships
- Handle international locations without state/province
- Ensure metro area assignments are logical
- Preserve original location variants for reference

**Step 5: Performance Optimization**
- Cluster by (country, state_province, city) for geographic queries
- Index on location_key for dimension lookups
- Optimize for common filtering patterns

**Key Processing Logic**:
```sql
-- Primary transformation query structure:
WITH location_prep AS (
    SELECT
        'LOC_' || LOCATION_ID as location_key,
        LOCATION_ID as location_id,
        LOCATION_NAME_CLEAN as location_name,
        CITY,
        STATE_PROVINCE as state_province,
        COUNTRY,
        REGION,
        METRO_AREA as metro_area,
        LOCATION_TYPE as location_type,
        IS_REMOTE_FRIENDLY as is_remote_friendly,
        IS_MAJOR_TECH_HUB as is_major_tech_hub,
        COST_OF_LIVING_INDEX as cost_of_living_index,
        AVERAGE_SALARY_ADJUSTMENT as average_salary_adjustment,
        CONFIDENCE_SCORE as stage_confidence_score,
        FREQUENCY_COUNT as frequency_count,
        CURRENT_TIMESTAMP as created_timestamp
    FROM STAGE.LOCATIONS_NORMALIZED
    WHERE CONFIDENCE_SCORE >= 0.5
      AND (MANUAL_REVIEW_FLAG = FALSE OR APPROVED_BY_ADMIN = TRUE)
      AND CITY IS NOT NULL
      AND COUNTRY IS NOT NULL
)
SELECT * FROM location_prep
ORDER BY country, state_province, city;
```

**Data Quality Validation**:
- Count source vs target records
- Verify geographic hierarchy integrity
- Validate special location types (Remote, Hybrid)
- Check clustering effectiveness for query performance
- Confirm stage confidence score distribution

#### 1.4 `analytics_dim_job_family` ✅ COMPLETE
**Purpose**: Create job classification dimension with hierarchical role taxonomy
**Dependencies**: `stage_jobs_llm_enriched_unified`
**Output**: Job family hierarchy for role analysis and market intelligence

```python
@asset(
    deps=["stage_jobs_llm_enriched_unified"],
    description="Create job classification dimension with hierarchical role taxonomy",
    group_name="3a_analytics_dimensions",
    kinds={"snowflake", "SQL"}
)
def analytics_dim_job_family(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build job family dimension from STAGE.JOBS_LLM_ENRICHED_UNIFIED with role hierarchy.

    Processing:
    - Generate surrogate keys for unique job family combinations
    - Extract and normalize job family hierarchies from LLM data
    - Calculate market metrics and role characteristics
    - Apply business rules for role classification
    - Handle seniority levels and management indicators
    """
```

**Implementation Steps**:

**Step 1: Surrogate Key Generation**
- Generate `job_family_key` as composite surrogate key from job family hierarchy
- Format: `JF_` + hash of (`JOB_FAMILY`, `JOB_SUB_FAMILY`, `SENIORITY_LEVEL`) for uniqueness
- Ensure consistent key generation across refreshes for referential integrity

**Step 2: Field Mapping & Transformation**
```sql
-- Simplified transformation query using only available STAGE data:
WITH job_family_prep AS (
    SELECT DISTINCT
        MD5(CONCAT(
            COALESCE(JOB_FAMILY, 'Unknown'),
            '|',
            COALESCE(JOB_SUB_FAMILY, 'General'),
            '|',
            COALESCE(SENIORITY_LEVEL, 'Not Specified'),
            '|',
            COALESCE(ROLE_TYPE, 'Not Specified')
        )) as job_family_key,

        -- Direct LLM Fields (no transformation)
        JOB_FAMILY as job_family,
        JOB_SUB_FAMILY as job_sub_family,
        SENIORITY_LEVEL as seniority_level,
        ROLE_TYPE as role_type,

        -- Simple Derived Fields Only
        CASE
            WHEN LOWER(SENIORITY_LEVEL) LIKE '%entry%' OR LOWER(SENIORITY_LEVEL) LIKE '%junior%' THEN 1
            WHEN LOWER(SENIORITY_LEVEL) LIKE '%mid%' OR LOWER(SENIORITY_LEVEL) LIKE '%intermediate%' THEN 2
            WHEN LOWER(SENIORITY_LEVEL) LIKE '%senior%' THEN 3
            WHEN LOWER(SENIORITY_LEVEL) LIKE '%staff%' THEN 4
            WHEN LOWER(SENIORITY_LEVEL) LIKE '%principal%' THEN 5
            WHEN LOWER(SENIORITY_LEVEL) LIKE '%director%' THEN 6
            WHEN LOWER(SENIORITY_LEVEL) LIKE '%vp%' OR LOWER(SENIORITY_LEVEL) LIKE '%vice%' THEN 7
            ELSE 0
        END as seniority_order,

        -- Simple management role identification
        (LOWER(ROLE_TYPE) LIKE '%manager%'
         OR LOWER(ROLE_TYPE) LIKE '%director%'
         OR LOWER(ROLE_TYPE) LIKE '%lead%'
         OR LOWER(SENIORITY_LEVEL) LIKE '%manager%'
         OR LOWER(SENIORITY_LEVEL) LIKE '%director%'
         OR LOWER(SENIORITY_LEVEL) LIKE '%vp%') as is_management_role,

        CURRENT_TIMESTAMP as created_timestamp

    FROM BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED
    WHERE JOB_FAMILY IS NOT NULL
      AND JOB_FAMILY != 'Unknown'
      AND LLM_OVERALL_CONFIDENCE >= 0.6
      AND (LLM_NEEDS_MANUAL_REVIEW = FALSE OR LLM_OVERALL_CONFIDENCE >= 0.8)
)

SELECT
    'JF_' || job_family_key as job_family_key,
    job_family,
    job_sub_family,
    seniority_level,
    role_type,
    seniority_order,
    is_management_role,
    created_timestamp
FROM job_family_prep
ORDER BY job_family, seniority_order, job_sub_family;
```

**Step 3: Data Quality Rules**
- Filter jobs with valid `JOB_FAMILY` (not null, not 'Unknown')
- Exclude jobs with `LLM_NEEDS_MANUAL_REVIEW = TRUE` unless high confidence
- Require minimum `LLM_OVERALL_CONFIDENCE >= 0.6` for job classification
- Handle standardization of common job family variations
- Apply consistent seniority level normalization

**Step 4: Business Logic Implementation**
- **Seniority Mapping**: Map text seniority levels to numeric ordering (1-7)
- **Management Classification**: Identify management roles from titles and levels
- **Department Mapping**: Standardize job families to business departments
- **Skills Context**: Infer primary skill category from job family patterns
- **Market Metrics**: Calculate demand levels and trends from job volume

**Step 5: Performance Optimization**
- Cluster by (job_family, seniority_level) for analytical queries
- Index on job_family_key for dimension lookups
- Optimize for role-based filtering and hierarchy navigation

**Key Processing Logic**:
```sql
-- Simplified transformation query using only available STAGE data:
WITH job_family_prep AS (
    SELECT DISTINCT
        MD5(CONCAT(
            COALESCE(JOB_FAMILY, 'Unknown'),
            '|',
            COALESCE(JOB_SUB_FAMILY, 'General'),
            '|',
            COALESCE(SENIORITY_LEVEL, 'Not Specified'),
            '|',
            COALESCE(ROLE_TYPE, 'Not Specified')
        )) as job_family_key,

        -- Direct LLM Fields (no transformation)
        JOB_FAMILY as job_family,
        JOB_SUB_FAMILY as job_sub_family,
        SENIORITY_LEVEL as seniority_level,
        ROLE_TYPE as role_type,

        -- Simple Derived Fields Only
        CASE
            WHEN LOWER(SENIORITY_LEVEL) LIKE '%entry%' OR LOWER(SENIORITY_LEVEL) LIKE '%junior%' THEN 1
            WHEN LOWER(SENIORITY_LEVEL) LIKE '%mid%' OR LOWER(SENIORITY_LEVEL) LIKE '%intermediate%' THEN 2
            WHEN LOWER(SENIORITY_LEVEL) LIKE '%senior%' THEN 3
            WHEN LOWER(SENIORITY_LEVEL) LIKE '%staff%' THEN 4
            WHEN LOWER(SENIORITY_LEVEL) LIKE '%principal%' THEN 5
            WHEN LOWER(SENIORITY_LEVEL) LIKE '%director%' THEN 6
            WHEN LOWER(SENIORITY_LEVEL) LIKE '%vp%' OR LOWER(SENIORITY_LEVEL) LIKE '%vice%' THEN 7
            ELSE 0
        END as seniority_order,

        -- Simple management role identification
        (LOWER(ROLE_TYPE) LIKE '%manager%'
         OR LOWER(ROLE_TYPE) LIKE '%director%'
         OR LOWER(ROLE_TYPE) LIKE '%lead%'
         OR LOWER(SENIORITY_LEVEL) LIKE '%manager%'
         OR LOWER(SENIORITY_LEVEL) LIKE '%director%'
         OR LOWER(SENIORITY_LEVEL) LIKE '%vp%') as is_management_role,

        CURRENT_TIMESTAMP as created_timestamp

    FROM BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED
    WHERE JOB_FAMILY IS NOT NULL
      AND JOB_FAMILY != 'Unknown'
      AND LLM_OVERALL_CONFIDENCE >= 0.6
      AND (LLM_NEEDS_MANUAL_REVIEW = FALSE OR LLM_OVERALL_CONFIDENCE >= 0.8)
)

SELECT
    'JF_' || job_family_key as job_family_key,
    job_family,
    job_sub_family,
    seniority_level,
    role_type,
    seniority_order,
    is_management_role,
    created_timestamp
FROM job_family_prep
ORDER BY job_family, seniority_order, job_sub_family;
```

**Data Quality Validation**:
- Count unique job family combinations vs source records
- Verify seniority level ordering consistency
- Validate department mapping completeness
- Check management level hierarchy logic
- Confirm clustering effectiveness for analytical queries

**Business Intelligence Features**:
- **Role Hierarchy Navigation**: Enable drill-down from department → family → specialty
- **Seniority Analysis**: Support career progression and compensation analysis
- **Market Intelligence**: Track demand levels and hiring trends by role type
- **Management Analytics**: Analyze leadership hiring patterns and team structures

#### 1.5 `analytics_dim_platform` ✅ COMPLETE
**Purpose**: Create platform dimension for ATS classification
**Dependencies**: `stage_jobs_unified`, `stage_jobs_llm_enriched_unified`
**Output**: Platform characteristics dimension

```python
@asset(
    deps=["stage_jobs_unified"],
    description="Create platform dimension with ATS characteristics",
    group_name="3a_analytics_dimensions",
    kinds={"snowflake", "SQL"}
)
def analytics_dim_platform(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build platform dimension from STAGE.JOBS_UNIFIED platform data.

    Processing:
    - Generate surrogate keys for each ATS platform
    - Calculate platform characteristics from actual job data
    - Map platform names to standardized codes
    - Derive platform metrics as per table definition
    """
```

**Implementation Steps**:

**Step 1: Surrogate Key Generation**
- Generate `platform_key` as surrogate key using `PLATFORM` as natural key
- Format: `PLT_` + platform name for clear identification
- Ensure uniqueness and consistency across refreshes

**Step 2: Platform Code Standardization**
- **Workday**: `workday` → `workday` (platform_code)
- **Greenhouse**: `greenhouse` → `greenhouse` (platform_code)
- **BambooHR**: `bamboohr` → `bamboohr` (platform_code)
- **SmartRecruiters**: `smartrecruiters` → `smartrecruiters` (platform_code)
- **iCIMS**: `icims` → `icims` (platform_code)

**Step 3: Platform Characteristics Calculation**
```sql
-- Calculate platform metrics from actual job data:
WITH platform_metrics AS (
    SELECT
        PLATFORM,
        COUNT(*) as total_jobs,
        COUNT(CASE WHEN SALARY_MIN IS NOT NULL AND SALARY_MAX IS NOT NULL THEN 1 END) as jobs_with_salary,
        AVG(DATA_QUALITY_SCORE) as avg_quality_score,
        MAX(DATE_RETRIEVED) as last_activity_date
    FROM STAGE.JOBS_UNIFIED
    WHERE PLATFORM IS NOT NULL
      AND PLATFORM != ''
    GROUP BY PLATFORM
)
SELECT
    'PLT_' || PLATFORM as platform_key,
    PLATFORM as platform_name,
    LOWER(PLATFORM) as platform_code,

    -- Derived characteristics per table definition
    (jobs_with_salary::FLOAT / total_jobs) >= 0.30 as supports_salary_disclosure,
    LEAST(1.0, GREATEST(0.0, avg_quality_score)) as data_richness_score,
    CASE
        WHEN total_jobs >= 1000 THEN 'High'
        WHEN total_jobs >= 100 THEN 'Medium'
        ELSE 'Low'
    END as job_volume_category,

    -- Currently processing jobs (jobs retrieved in last 30 days)
    (DATEDIFF('day', last_activity_date, CURRENT_DATE) <= 30) as is_active,

    CURRENT_TIMESTAMP as created_timestamp

FROM platform_metrics
ORDER BY platform_name;
```

**Step 4: Data Quality Rules**
- Include only platforms with actual job data
- Handle null/empty platform values
- Ensure metric calculations are within expected ranges

**Step 5: Performance Optimization**
- Cluster by (platform_name) as specified in table definition
- Optimize for platform-based analytics and filtering

#### 1.6 `analytics_dim_salary`
**Purpose**: Create salary dimension from normalized salary ranges
**Dependencies**: `stage_salary_normalized`
**Output**: Salary dimension with normalized ranges and market intelligence

```python
@asset(
    deps=["stage_salary_normalized"],
    description="Create salary dimension with normalized ranges and market intelligence",
    group_name="3a_analytics_dimensions",
    kinds={"snowflake", "SQL"}
)
def analytics_dim_salary(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build salary dimension from STAGE.SALARY_NORMALIZED with market intelligence.

    Processing:
    - Generate surrogate keys for each salary range
    - Map normalized salary fields to dimension structure
    - Include quality indicators and market percentile data
    - Handle outlier flags and manual review requirements
    - Optimize for salary-based analytical queries
    """
```

**Implementation Steps**:

**Step 1: Surrogate Key Generation**
- Generate `salary_key` using `'SAL_' + SALARY_ID` format for unique identification
- Ensure consistency across refreshes for referential integrity

**Step 2: Field Mapping & Transformation**
```sql
-- Direct field mappings from STAGE.SALARY_NORMALIZED:
SALARY_ID → salary_id (natural key preservation)
SALARY_RANGE_NAME → salary_range_name (human-readable description)
SALARY_MIN_ORIGINAL → salary_min_original (audit trail)
SALARY_MAX_ORIGINAL → salary_max_original (audit trail)
SALARY_PERIOD_ORIGINAL → salary_period_original (audit trail)
SALARY_CURRENCY_ORIGINAL → salary_currency_original (audit trail)
SALARY_MIN_ANNUAL_USD → salary_min_annual_usd (normalized value)
SALARY_MAX_ANNUAL_USD → salary_max_annual_usd (normalized value)
SALARY_MIDPOINT_ANNUAL_USD → salary_midpoint_annual_usd (normalized value)
NORMALIZATION_FACTOR → normalization_factor (conversion multiplier)
CONFIDENCE_SCORE → confidence_score (quality indicator)
OUTLIER_FLAG → outlier_flag (quality flag)
MANUAL_REVIEW_FLAG → manual_review_flag (quality flag)
APPROVED_BY_ADMIN → approved_by_admin (approval status)
FREQUENCY_COUNT → frequency_count (market frequency)
MARKET_PERCENTILE → market_percentile (market position)
```

**Step 3: Data Quality Rules**
- Filter salaries with `CONFIDENCE_SCORE >= 0.5` for quality assurance
- Include flagged outliers but mark appropriately for analysis
- Ensure normalized values are positive and logical
- Handle manual review flagged records appropriately

**Step 4: Performance Optimization**
- Cluster by `salary_midpoint_annual_usd` for salary-based analytics
- Optimize for salary range queries and market analysis
- Index on salary_key for dimension lookups

**Key Processing Logic**:
```sql
WITH salary_prep AS (
    SELECT
        'SAL_' || SALARY_ID as salary_key,
        SALARY_ID as salary_id,
        SALARY_RANGE_NAME as salary_range_name,
        SALARY_MIN_ORIGINAL as salary_min_original,
        SALARY_MAX_ORIGINAL as salary_max_original,
        SALARY_PERIOD_ORIGINAL as salary_period_original,
        SALARY_CURRENCY_ORIGINAL as salary_currency_original,
        SALARY_MIN_ANNUAL_USD as salary_min_annual_usd,
        SALARY_MAX_ANNUAL_USD as salary_max_annual_usd,
        SALARY_MIDPOINT_ANNUAL_USD as salary_midpoint_annual_usd,
        NORMALIZATION_FACTOR as normalization_factor,
        CONFIDENCE_SCORE as confidence_score,
        OUTLIER_FLAG as outlier_flag,
        MANUAL_REVIEW_FLAG as manual_review_flag,
        APPROVED_BY_ADMIN as approved_by_admin,
        FREQUENCY_COUNT as frequency_count,
        MARKET_PERCENTILE as market_percentile,
        CURRENT_TIMESTAMP as created_timestamp
    FROM BETTERJOBS_DB.STAGE.SALARY_NORMALIZED
    WHERE CONFIDENCE_SCORE >= 0.5
      AND SALARY_MIN_ANNUAL_USD > 0
      AND SALARY_MAX_ANNUAL_USD > 0
      AND SALARY_MIN_ANNUAL_USD <= SALARY_MAX_ANNUAL_USD
)
SELECT * FROM salary_prep
ORDER BY salary_midpoint_annual_usd;
```

**Business Intelligence Features**:
- **Salary Range Analytics**: Support normalized salary analysis across all periods and currencies
- **Market Intelligence**: Track salary percentiles and market position
- **Quality Indicators**: Enable confidence-based filtering for analysis
- **Outlier Analysis**: Support identification and analysis of salary outliers
- **Audit Trail**: Maintain original values for data lineage and validation

#### 1.7 `analytics_dim_skills` ✅ COMPLETE
**Purpose**: Create skills dimension from normalized skills taxonomy
**Dependencies**: `stage_skills_normalized`
**Output**: Skills classification dimension with hierarchy and market intelligence

```python
@asset(
    deps=["stage_skills_normalized"],
    description="Create skills dimension with taxonomy hierarchy and market intelligence",
    group_name="3a_analytics_dimensions",
    kinds={"snowflake", "SQL"}
)
def analytics_dim_skills(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build skills dimension from STAGE.SKILLS_NORMALIZED taxonomy.

    Processing:
    - Generate surrogate keys for each skill
    - Map STAGE fields to dimension structure
    - Preserve skill hierarchy and classification
    - Include market intelligence and trend data
    - Validate data quality and completeness
    """
```

**Implementation Steps**:

**Step 1: Surrogate Key Generation**
- Generate `skill_key` as surrogate key using `SKILL_ID` as natural key
- Format: `SKL_` + `SKILL_ID` for clear identification
- Ensure uniqueness and consistency across refreshes

**Step 2: Field Mapping & Transformation**
```sql
-- Direct field mappings from STAGE.SKILLS_NORMALIZED:
SKILL_ID → skill_id (natural key preservation)
SKILL_NAME → skill_name (primary skill identifier)
SKILL_CATEGORY → skill_category (high-level classification)
SKILL_SUBCATEGORY → skill_subcategory (detailed classification)
CANONICAL_FORM → canonical_form (standardized skill name)
COMMON_ALIASES → common_aliases (variant names)
ORIGINAL_VARIANTS → original_variants (raw extraction variants)
CONFIDENCE_SCORE → stage_confidence_score (normalization quality)
FREQUENCY_COUNT → frequency_count (market demand indicator)
TREND_DIRECTION → trend_direction (growth/decline indicator)
```

**Step 3: Data Quality Rules**
- Filter skills with `CONFIDENCE_SCORE >= 0.5` for quality assurance
- Exclude skills marked for manual review unless approved by admin
- Ensure required fields (skill_name, skill_category) are not null
- Handle special skill types: programming languages, frameworks, tools

**Step 4: Skill Hierarchy Validation**
- Validate category-subcategory relationships
- Ensure skill categorization consistency
- Handle uncategorized skills (assign to 'Other' subcategory)
- Preserve canonical forms for standardization

**Step 5: Market Intelligence Integration**
- Include frequency count for demand analysis
- Preserve trend direction for growth tracking
- Calculate skill popularity rankings within categories
- Maintain variant mappings for search optimization

**Step 6: Performance Optimization**
- Cluster by (skill_category, skill_subcategory) for analytical queries
- Index on skill_key for dimension lookups
- Optimize for skill-based filtering and hierarchy navigation

**Key Processing Logic**:
```sql
WITH skills_prep AS (
    SELECT
        'SKL_' || SKILL_ID as skill_key,
        SKILL_ID as skill_id,
        SKILL_NAME as skill_name,

        -- Skill hierarchy
        SKILL_CATEGORY as skill_category,
        SKILL_SUBCATEGORY as skill_subcategory,

        -- Standardization fields
        CANONICAL_FORM as canonical_form,
        COMMON_ALIASES as common_aliases,
        ORIGINAL_VARIANTS as original_variants,

        -- Market intelligence
        CONFIDENCE_SCORE as stage_confidence_score,
        FREQUENCY_COUNT as frequency_count,
        TREND_DIRECTION as trend_direction,

        CURRENT_TIMESTAMP as created_timestamp

    FROM BETTERJOBS_DB.STAGE.SKILLS_NORMALIZED
    WHERE CONFIDENCE_SCORE >= 0.5
      AND (MANUAL_REVIEW_FLAG = FALSE OR APPROVED_BY_ADMIN = TRUE)
      AND SKILL_NAME IS NOT NULL
      AND SKILL_CATEGORY IS NOT NULL
)
SELECT * FROM skills_prep
ORDER BY skill_category, skill_subcategory, frequency_count DESC;
```

**Data Quality Validation**:
- Count source vs target skills for completeness
- Verify skill hierarchy integrity (category-subcategory relationships)
- Validate confidence score distribution
- Check clustering effectiveness for analytical queries
- Confirm variant and alias preservation

**Business Intelligence Features**:
- **Skills Taxonomy Navigation**: Enable drill-down from category → subcategory → individual skills
- **Market Demand Analysis**: Track skill frequency and popularity trends
- **Skills Standardization**: Support canonical forms and variant mapping
- **Growth Intelligence**: Monitor emerging vs declining technology skills
- **Search Optimization**: Enable variant-based skill matching and discovery

#### 1.7 `analytics_dim_experience` ✅ COMPLETE
**Purpose**: Create experience dimension from normalized experience requirements
**Dependencies**: `stage_experience_normalized`
**Output**: Experience levels and requirements dimension with seniority ordering and market intelligence

```python
@asset(
    deps=["stage_experience_normalized"],
    description="Create experience requirements dimension table",
    group_name="3a_analytics_dimensions",
    kinds={"snowflake", "SQL"}
)
def analytics_dim_experience(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build experience dimension from normalized experience requirements.

    Processing:
    - Map experience levels to dimension structure
    - Include seniority ordering for analytics
    - Add market frequency and confidence metrics
    - Handle both general and technology-specific experience
    """
```

**Implementation Steps**:

**Step 1: Surrogate Key Generation**
- Generate `experience_key` using `'EXP_' + EXPERIENCE_ID` format for unique identification
- Ensure consistency across refreshes for referential integrity

**Step 2: Field Mapping & Transformation**
```sql
-- Direct field mappings from STAGE.EXPERIENCE_NORMALIZED:
EXPERIENCE_ID → experience_id (natural key preservation)
EXPERIENCE_NAME → experience_name (e.g., 'Entry Level', 'Senior Level')
EXPERIENCE_CATEGORY → experience_category (general, technology_specific, role_seniority)
MIN_YEARS_REQUIRED → min_years_required (minimum years for level)
MAX_YEARS_REQUIRED → max_years_required (maximum years for level)
SENIORITY_ORDER → seniority_order (1=Entry to 8=Executive ordering)
EXPERIENCE_DESCRIPTION → experience_description (human-readable description)
MARKET_FREQUENCY → market_frequency (demand indicator)
CONFIDENCE_SCORE → confidence_score (normalization quality)
```

**Step 3: Data Quality Rules**
- Filter experience levels with `CONFIDENCE_SCORE >= 0.5` for quality assurance
- Ensure required fields are not null: `experience_name`, `experience_category`
- Validate experience year ranges: `MIN_YEARS_REQUIRED <= MAX_YEARS_REQUIRED` when both present
- Handle seniority ordering consistency (1-8 scale validation)

**Step 4: Business Logic Implementation**
- **Experience Categories**: Handle 'general', 'technology_specific', 'role_seniority' classifications
- **Seniority Ordering**: Preserve 1-8 scale for analytics queries (1=Entry, 8=Executive)
- **Market Intelligence**: Include frequency data for experience demand analysis
- **Year Range Logic**: Ensure logical min/max year relationships for analytics

**Step 5: Performance Optimization**
- Cluster by (experience_category, seniority_order) for analytical queries
- Index on experience_key for dimension lookups
- Optimize for experience-based filtering and seniority hierarchy navigation

**Key Processing Logic**:
```sql
WITH experience_prep AS (
    SELECT
        'EXP_' || EXPERIENCE_ID as experience_key,
        EXPERIENCE_ID as experience_id,
        EXPERIENCE_NAME as experience_name,
        EXPERIENCE_CATEGORY as experience_category,
        MIN_YEARS_REQUIRED as min_years_required,
        MAX_YEARS_REQUIRED as max_years_required,
        SENIORITY_ORDER as seniority_order,
        EXPERIENCE_DESCRIPTION as experience_description,
        MARKET_FREQUENCY as market_frequency,
        CONFIDENCE_SCORE as confidence_score,
        CURRENT_TIMESTAMP as created_timestamp
    FROM BETTERJOBS_DB.STAGE.EXPERIENCE_NORMALIZED
    WHERE CONFIDENCE_SCORE >= 0.5
      AND EXPERIENCE_NAME IS NOT NULL
      AND TRIM(EXPERIENCE_NAME) != ''
      AND EXPERIENCE_CATEGORY IS NOT NULL
      AND TRIM(EXPERIENCE_CATEGORY) != ''
      AND (MIN_YEARS_REQUIRED IS NULL OR MAX_YEARS_REQUIRED IS NULL
           OR MIN_YEARS_REQUIRED <= MAX_YEARS_REQUIRED)
)
SELECT * FROM experience_prep
ORDER BY experience_category, seniority_order, experience_name;
```

**Data Quality Validation**:
- Count source vs target experience levels for completeness
- Verify seniority order consistency and gaps
- Validate experience category distribution
- Check year range logic and outliers
- Confirm clustering effectiveness for analytical queries

**Business Intelligence Features**:
- **Experience Level Analytics**: Support experience requirement distribution analysis
- **Seniority Progression**: Enable career path and progression analytics
- **Market Demand Analysis**: Track frequency of experience levels in job market
- **Category Intelligence**: Analyze general vs technology-specific experience patterns
- **Experience Inflation**: Support trend analysis of experience requirements over time

#### 1.8 `analytics_dim_keywords`
**Purpose**: Create keywords dimension from normalized keywords taxonomy
**Dependencies**: `stage_keywords_normalized`
**Output**: Keywords classification dimension with hierarchy and market intelligence

```python
@asset(
    deps=["stage_keywords_normalized"],
    description="Create keywords taxonomy dimension table",
    group_name="3a_analytics_dimensions",
    kinds={"snowflake", "SQL"}
)
def analytics_dim_keywords(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build keywords dimension from normalized keywords taxonomy.

    Processing:
    - Map keywords to dimension structure
    - Include keyword type and category hierarchy
    - Add market frequency and trend metrics
    - Handle canonical forms and variants
    """
```

**Implementation Steps**:

**Step 1: Surrogate Key Generation**
- Generate `keyword_key` as surrogate key using `KEYWORD_ID` as natural key
- Format: `KWD_` + `KEYWORD_ID` for clear identification
- Ensure uniqueness and consistency across refreshes

**Step 2: Field Mapping & Transformation**
```sql
-- Direct field mappings from STAGE.KEYWORDS_NORMALIZED:
KEYWORD_ID → keyword_id (natural key preservation)
KEYWORD_TEXT → keyword_text (primary keyword identifier)
KEYWORD_TEXT_CLEAN → keyword_text_clean (cleaned version)
KEYWORD_TYPE → keyword_type (high-level classification)
KEYWORD_CATEGORY → keyword_category (detailed classification)
CANONICAL_FORM → canonical_form (standardized keyword form)
ORIGINAL_VARIANTS → original_variants (raw extraction variants)
FREQUENCY_COUNT → frequency_count (market demand indicator)
TREND_SCORE → trend_score (growth/decline indicator)
CONFIDENCE_SCORE → confidence_score (normalization quality)
APPROVED_BY_ADMIN → approved_by_admin (manual approval flag)
```

**Step 3: Data Quality Rules**
- Filter keywords with `CONFIDENCE_SCORE >= 0.5` for quality assurance
- Exclude keywords marked for manual review unless approved by admin
- Ensure required fields (keyword_text, keyword_type) are not null
- Handle keyword categorization and variant mappings

**Step 4: Keyword Hierarchy Validation**
- Validate type-category relationships
- Ensure keyword categorization consistency
- Handle uncategorized keywords (assign to 'Other' category)
- Preserve canonical forms for standardization

**Step 5: Market Intelligence Integration**
- Include frequency count for demand analysis
- Preserve trend scores for growth tracking
- Calculate keyword popularity rankings within categories
- Maintain variant mappings for search optimization

**Step 6: Performance Optimization**
- Cluster by (keyword_type, keyword_category) for analytical queries
- Index on keyword_key for dimension lookups
- Optimize for keyword-based filtering and hierarchy navigation

**Key Processing Logic**:
```sql
WITH keywords_prep AS (
    SELECT
        'KWD_' || KEYWORD_ID as keyword_key,
        KEYWORD_ID as keyword_id,
        KEYWORD_TEXT as keyword_text,
        KEYWORD_TEXT_CLEAN as keyword_text_clean,

        -- Keyword hierarchy
        KEYWORD_TYPE as keyword_type,
        KEYWORD_CATEGORY as keyword_category,

        -- Standardization fields
        CANONICAL_FORM as canonical_form,
        ORIGINAL_VARIANTS as original_variants,

        -- Market intelligence
        FREQUENCY_COUNT as frequency_count,
        TREND_SCORE as trend_score,
        CONFIDENCE_SCORE as confidence_score,
        APPROVED_BY_ADMIN as approved_by_admin,

        CURRENT_TIMESTAMP as created_timestamp

    FROM BETTERJOBS_DB.STAGE.KEYWORDS_NORMALIZED
    WHERE CONFIDENCE_SCORE >= 0.5
      AND (MANUAL_REVIEW_FLAG = FALSE OR APPROVED_BY_ADMIN = TRUE)
      AND KEYWORD_TEXT IS NOT NULL
      AND KEYWORD_TYPE IS NOT NULL
)
SELECT * FROM keywords_prep
ORDER BY keyword_type, keyword_category, frequency_count DESC;
```

**Data Quality Validation**:
- Count source vs target keywords for completeness
- Verify keyword hierarchy integrity (type-category relationships)
- Validate confidence score distribution
- Check clustering effectiveness for analytical queries
- Confirm variant and canonical form preservation

**Business Intelligence Features**:
- **Keywords Taxonomy Navigation**: Enable drill-down from type → category → individual keywords
- **Market Demand Analysis**: Track keyword frequency and popularity trends
- **Keywords Standardization**: Support canonical forms and variant mapping
- **Trend Intelligence**: Monitor emerging vs declining keyword usage
- **Search Optimization**: Enable variant-based keyword matching and discovery

### Phase 2: Primary Fact Table Implementation

#### 2.1 `analytics_fact_job_postings`
**Purpose**: Create primary fact table for job posting analytics with complete business measures
**Dependencies**: All dimension tables, `stage_jobs_unified`, `stage_jobs_llm_enriched_unified`
**Output**: Core fact table with measures and dimension keys

```python
@asset(
    deps=["analytics_dim_date", "analytics_dim_company", "analytics_dim_location",
          "analytics_dim_job_family", "analytics_dim_platform", "analytics_dim_skills",
          "analytics_dim_salary", "analytics_dim_experience", "analytics_dim_keywords",
          "stage_jobs_unified", "stage_jobs_llm_enriched_unified", "stage_job_salary_bridge"],
    description="Create primary fact table for job posting analytics",
    group_name="3b_analytics_facts",
    kinds={"snowflake", "SQL"}
)
def analytics_fact_job_postings(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build the primary fact table for job posting analytics.

    This asset creates the core fact table that serves as the foundation for all job market analytics.
    It combines job data from STAGE sources with dimension lookups to create a business-ready
    analytical structure.

    Processing Logic:
    1. Join STAGE.JOBS_UNIFIED with STAGE.JOBS_LLM_ENRICHED for complete job data
    2. Lookup dimension keys for all foreign key relationships
    3. Apply data quality filters and business rules
    4. Generate surrogate keys and calculate derived measures
    5. Implement incremental loading with change detection
    6. Validate fact table completeness and integrity

    Data Quality Rules:
    - Filter jobs with IS_ACTIVE = TRUE for current analysis
    - Require valid COMPANY_ID for company dimension lookups
    - Apply LLM confidence thresholds for enriched data inclusion
    - Validate date ranges and posting date logic
    - Handle missing dimension keys gracefully with default values

    Returns:
        Dict containing execution results and job posting statistics
    """
```

**Implementation Steps**:

**Step 1: Source Data Integration**
- **Primary Source**: `STAGE.JOBS_UNIFIED` (core job data)
- **Secondary Source**: `STAGE.JOBS_LLM_ENRICHED` (enriched attributes)
- **Join Strategy**: LEFT JOIN on JOB_UID to preserve all jobs even without LLM enrichment
- **Data Quality Filter**: Include jobs with basic data completeness requirements

**Step 2: Dimension Key Lookups**
```sql
-- Dimension key mapping strategy:
DATE_POSTED_KEY     ← TO_CHAR(ju.DATE_POSTED, 'YYYYMMDD')          -- Direct date key generation
COMPANY_KEY         ← dc.COMPANY_KEY WHERE dc.COMPANY_ID = ju.COMPANY_ID AND dc.IS_CURRENT = TRUE
LOCATION_KEY        ← dl.LOCATION_KEY WHERE dl.LOCATION_NAME = ju.LOCATION_STANDARDIZED
JOB_FAMILY_KEY      ← djf.JOB_FAMILY_KEY WHERE djf.JOB_FAMILY = lle.JOB_FAMILY AND djf.SENIORITY_LEVEL = lle.SENIORITY_LEVEL
PLATFORM_KEY        ← dp.PLATFORM_KEY WHERE dp.PLATFORM_NAME = ju.PLATFORM
EXPERIENCE_KEY      ← de.EXPERIENCE_KEY WHERE de.EXPERIENCE_LEVEL = lle.EXPERIENCE_LEVEL (primary lookup)
KEYWORD_KEY         ← dk.KEYWORD_KEY WHERE dk.KEYWORD_TEXT = primary keyword from lle.PRIMARY_KEYWORDS[0]
SALARY_KEY          ← ds.SALARY_KEY FROM stage_job_salary_bridge jsb JOIN ds WHERE jsb.JOB_UID = ju.JOB_UID
```

**Step 3: Field Mapping & Transformation**
```sql
-- Direct field mappings from STAGE sources:
JOB_UID             ← ju.JOB_UID (natural key preservation)
JOB_TITLE           ← ju.JOB_TITLE_CLEAN (cleaned job title)
POSTING_URL         ← ju.JOB_URL (original job posting URL)

-- Salary measures from STAGE.JOBS_LLM_ENRICHED:
SALARY_MIN          ← lle.SALARY_MIN
SALARY_MAX          ← lle.SALARY_MAX
SALARY_CURRENCY     ← COALESCE(lle.SALARY_CURRENCY, 'USD')
SALARY_PERIOD       ← lle.SALARY_PERIOD

-- Experience measures from STAGE.JOBS_LLM_ENRICHED:
EXPERIENCE_MIN_YEARS ← lle.MIN_YEARS_EXPERIENCE
EXPERIENCE_MAX_YEARS ← lle.MAX_YEARS_EXPERIENCE
EXPERIENCE_LEVEL     ← lle.EXPERIENCE_LEVEL

-- Quality and confidence measures:
SALARY_CONFIDENCE     ← lle.SALARY_CONFIDENCE
LLM_OVERALL_CONFIDENCE ← lle.LLM_OVERALL_CONFIDENCE
DATA_QUALITY_SCORE    ← ju.DATA_QUALITY_SCORE

-- Job classification from STAGE.JOBS_LLM_ENRICHED:
JOB_FAMILY           ← lle.JOB_FAMILY
JOB_SUB_FAMILY       ← lle.JOB_SUB_FAMILY
SENIORITY_LEVEL      ← lle.SENIORITY_LEVEL

-- Work arrangement from STAGE.JOBS_LLM_ENRICHED:
WORK_TYPE            ← lle.WORK_TYPE
REMOTE_FLEXIBILITY   ← lle.REMOTE_FLEXIBILITY

-- Boolean flags:
IS_ACTIVE_POSTING    ← ju.IS_ACTIVE
IS_EQUITY_MENTIONED  ← COALESCE(lle.EQUITY_MENTIONED, FALSE)
IS_BONUS_MENTIONED   ← COALESCE(lle.BONUS_MENTIONED, FALSE)
LLM_NEEDS_MANUAL_REVIEW ← COALESCE(lle.LLM_NEEDS_MANUAL_REVIEW, FALSE)

-- Important dates:
FIRST_POSTED_DATE    ← ju.DATE_POSTED
DATE_RETRIEVED       ← ju.DATE_RETRIEVED

-- Partitioning field:
PARTITION_DATE       ← DATE_TRUNC('month', ju.DATE_POSTED)
```

**Step 4: Surrogate Key Generation**
- **Primary Key**: `JOB_POSTING_KEY` = `'JP_' + JOB_UID` for unique identification
- **Consistent Generation**: Ensure same key for same job across refreshes
- **Performance Optimization**: Use clustering on dimension keys for query performance

**Step 5: Data Quality Rules**
- **Active Jobs**: `ju.IS_ACTIVE = TRUE` for current market analysis
- **Valid Companies**: `ju.COMPANY_ID IS NOT NULL AND ju.COMPANY_ID != ''`
- **Date Validation**: `ju.DATE_POSTED >= '2020-01-01'` for reasonable date ranges
- **Language Filter**: `ju.IS_ENGLISH = TRUE` for consistent analysis (optional)
- **LLM Quality**: Include LLM data only when `lle.LLM_OVERALL_CONFIDENCE >= 0.5`
- **Salary Quality**: Include salary data only when `lle.SALARY_CONFIDENCE >= 0.6`

**Step 6: Default Value Handling**
```sql
-- Handle missing dimension keys with defaults:
COMPANY_KEY         ← COALESCE(lookup_result, 'COMP_UNKNOWN')
LOCATION_KEY        ← COALESCE(lookup_result, 'LOC_UNKNOWN')
JOB_FAMILY_KEY      ← COALESCE(lookup_result, 'JF_UNKNOWN')
PLATFORM_KEY        ← COALESCE(lookup_result, 'PLT_' + ju.PLATFORM)
EXPERIENCE_KEY      ← COALESCE(lookup_result, 'EXP_UNKNOWN')
KEYWORD_KEY         ← COALESCE(lookup_result, 'KWD_UNKNOWN')
```

**Step 7: Incremental Loading Strategy**
- **Processing Mode**: Complete refresh for simplicity (jobs don't change frequently)
- **Change Detection**: Compare against existing records for delta identification
- **Partition Management**: Partition by PARTITION_DATE (monthly) for query performance
- **Data Freshness**: Process all jobs from STAGE layer daily

**Step 8: Business Rules & Validations**
```sql
-- Core business rules:
1. One record per unique job posting (JOB_UID is unique)
2. All jobs must have valid date posted
3. Company dimension lookup required for business analysis
4. Platform must be from known ATS systems
5. Salary data included only with sufficient confidence
6. Experience data validated for logical ranges (0-50 years)
7. Work arrangement standardized to known categories
```

**Step 9: Performance Optimization**
- **Clustering**: `(DATE_POSTED_KEY, COMPANY_KEY, LOCATION_KEY, JOB_FAMILY_KEY)`
- **Partitioning**: `PARTITION BY (PARTITION_DATE)` for monthly partitions
- **Indexing Strategy**: Optimize for common analytical query patterns
- **Query Performance Target**: <2 seconds for standard fact table queries

**Step 10: Data Validation & Quality Checks**
```sql
-- Post-load validation queries:
1. Record count validation: Compare with source STAGE tables
2. Dimension key integrity: Verify all foreign keys exist in dimensions
3. Date range validation: Ensure posting dates are reasonable
4. Salary range validation: Check for outliers and invalid values
5. Completeness metrics: Calculate percentage of jobs with each measure
6. Business rule compliance: Verify all business rules are enforced
```

**Key Implementation Challenges**:

**Challenge 1: Dimension Key Lookups**
- **Issue**: Multiple dimension lookups per job can be slow
- **Solution**: Use efficient JOIN strategy with dimension tables
- **Mitigation**: Implement lookup caching and optimized JOIN ordering

**Challenge 2: Missing LLM Enrichment**
- **Issue**: Not all jobs have LLM enrichment data
- **Solution**: LEFT JOIN with STAGE.JOBS_LLM_ENRICHED and handle NULLs gracefully
- **Business Rule**: Include jobs without enrichment but flag for limited analytics

**Challenge 3: Primary Keyword Selection**
- **Issue**: Jobs may have multiple keywords, need to select primary
- **Solution**: Use first element of lle.PRIMARY_KEYWORDS array or most frequent keyword
- **Fallback**: Default to 'KWD_UNKNOWN' if no keywords available

**Challenge 4: Experience Mapping**
- **Issue**: LLM experience levels may not match dimension experience categories
- **Solution**: Implement fuzzy matching logic for experience level lookups
- **Business Rule**: Map similar experience levels to closest dimension match

**Challenge 5: Location Standardization**
- **Issue**: STAGE location names may not exactly match dimension location names
- **Solution**: Implement location matching logic with fuzzy string matching
- **Data Quality**: Track location lookup success rates for improvement

**Step 11: Success Metrics & KPIs**
```sql
-- Key success metrics to track:
- Total job postings loaded
- Percentage with successful dimension lookups
- Percentage with LLM enrichment data
- Percentage with salary information
- Data quality score distribution
- Processing time per batch
- Query performance benchmarks
```

**Expected Data Volume**: ~50,000-100,000 job postings per refresh
**Processing Time Target**: <10 minutes for complete refresh
**Data Quality Target**: >95% successful dimension lookups
**Business Coverage**: >90% of jobs with essential business measures

### Phase 3: Aggregate Fact Tables Implementation

#### 3.1 `analytics_fact_skills_demand_weekly`
**Purpose**: Create weekly skills demand aggregates for responsive technology trend analysis
**Dependencies**: `analytics_fact_job_postings`, `stage_job_skills_bridge`, `analytics_dim_skills`
**Output**: Skills demand trends with weekly granularity for market intelligence

**Business Need**: Track technology skills demand with weekly granularity to enable timely identification of:
- Emerging vs declining technology trends
- Skills salary premiums and market value
- Geographic and industry skill distribution patterns
- Weekly hiring velocity by technology stack

```python
@asset(
    deps=["analytics_fact_job_postings", "stage_job_skills_bridge", "analytics_dim_skills"],
    description="Create weekly skills demand aggregate fact table for technology trend analysis",
    group_name="3c_analytics_aggregates",
    kinds={"snowflake", "SQL"}
)
def analytics_fact_skills_demand_weekly(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build weekly skills demand aggregates from job postings and skills relationships.

    This asset creates comprehensive weekly skill demand intelligence by aggregating job postings
    by skill, week, job family, and location to enable responsive technology trend analysis.

    Processing Logic:
    1. Join FACT_JOB_POSTINGS with JOB_SKILLS_BRIDGE to get job-skill relationships
    2. Group by week, skill, job family, and location for comprehensive market view
    3. Calculate core demand metrics (penetration rates, job counts, growth rates)
    4. Compute salary analysis and skill premiums using denormalized salary fields
    5. Generate skill rankings within job families and overall market
    6. Apply week-over-week trend analysis with directional classification
    7. Implement data quality filtering using confidence scores from bridge table

    Data Quality Rules:
    - Include only skills with OVERALL_CONFIDENCE >= 0.7 from bridge table
    - Filter active job postings (IS_ACTIVE_POSTING = TRUE)
    - Require minimum 3 jobs per skill-week combination for statistical validity
    - Use salary data only where SALARY_CONFIDENCE >= 0.6
    - Apply LLM confidence filtering (LLM_OVERALL_CONFIDENCE >= 0.5)

    Grain: One record per skill per week per job family per location
    Aggregation Level: Weekly (Sunday-Saturday weeks)
    Retention: 104 weeks (2 years) for trend analysis

    Performance Optimization:
    - Partition by WEEK_START_DATE for time-based queries
    - Cluster by (WEEK_KEY, SKILL_KEY, JOB_FAMILY_KEY) for analytical patterns
    - Pre-calculate rankings and growth rates for dashboard performance

    Returns:
        Dict containing processing statistics and data quality metrics
    """
```

**Table Structure Review & Updates**:

Based on the current `analytics_fact_job_postings` implementation with denormalized salary fields, the table structure needs these updates:

```sql
-- UPDATED TABLE STRUCTURE (aligned with fact table implementation):
CREATE TABLE ANALYTICS.FACT_SKILLS_DEMAND_WEEKLY (
    -- Primary Key & Week Hierarchy
    SKILLS_WEEKLY_KEY STRING PRIMARY KEY,       -- Format: 'SW_' + WEEK_KEY + '_' + SKILL_KEY + '_' + JOB_FAMILY_KEY + '_' + LOCATION_KEY
    WEEK_KEY STRING,                            -- YYYY-WW format (e.g., '2025-25')
    SKILL_KEY STRING,                           -- FK to DIM_SKILLS
    JOB_FAMILY_KEY STRING,                      -- FK to DIM_JOB_FAMILY
    LOCATION_KEY STRING,                        -- FK to DIM_LOCATION

    -- Core Demand Metrics
    ACTIVE_JOBS_WITH_SKILL INTEGER,             -- Active jobs requiring this skill in the week
    TOTAL_ACTIVE_JOBS INTEGER,                  -- Total active jobs in same category (job family + location)
    SKILL_PENETRATION_RATE FLOAT,               -- Percentage of jobs requiring this skill (jobs_with_skill/total_jobs)

    -- Enhanced Salary Analysis (using denormalized annual USD fields)
    AVG_SALARY_MIDPOINT_ANNUAL_USD NUMBER,      -- Average salary midpoint for jobs with this skill
    BASELINE_SALARY_MIDPOINT_ANNUAL_USD NUMBER, -- Average salary midpoint for jobs WITHOUT this skill (for premium calc)
    SALARY_PREMIUM_ANNUAL_USD NUMBER,           -- Absolute premium in USD (avg_with_skill - avg_without_skill)
    SALARY_PREMIUM_PERCENTAGE FLOAT,            -- Percentage premium this skill commands
    SALARY_SAMPLE_SIZE INTEGER,                 -- Number of jobs with salary data for confidence

    -- Trend Analysis & Growth Metrics
    WEEK_OVER_WEEK_CHANGE INTEGER,              -- Change in job count from previous week
    WEEK_OVER_WEEK_GROWTH_RATE FLOAT,          -- Percentage change week-over-week
    TREND_DIRECTION STRING,                     -- 'GROWING', 'STABLE', 'DECLINING', 'NEW' (based on growth rate thresholds)
    FOUR_WEEK_MOVING_AVERAGE FLOAT,            -- 4-week moving average for trend smoothing

    -- Market Position & Competitive Analysis
    SKILL_RANK_IN_FAMILY INTEGER,              -- Rank within job family (1 = most in-demand)
    SKILL_RANK_OVERALL INTEGER,                -- Overall market rank across all skills
    MARKET_SHARE_IN_FAMILY FLOAT,              -- Share of total job family demand

    -- Data Quality & Confidence Metrics
    AVG_SKILL_CONFIDENCE FLOAT,                -- Average extraction confidence from bridge table
    DATA_COMPLETENESS_SCORE FLOAT,             -- Percentage of complete skill records
    SAMPLE_SIZE INTEGER,                       -- Total job postings analyzed for this skill

    -- Work Arrangement Analysis (new insight)
    REMOTE_JOBS_WITH_SKILL INTEGER,            -- Remote jobs requiring this skill
    REMOTE_SKILL_PERCENTAGE FLOAT,             -- Percentage of skill demand that's remote-friendly

    -- Audit & Processing Fields
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    PROCESSING_DATE DATE DEFAULT CURRENT_DATE,  -- When this week's data was calculated
    WEEK_START_DATE DATE,                       -- First day of week (Sunday) for partitioning
    WEEK_END_DATE DATE                          -- Last day of week (Saturday) for reference
) PARTITION BY (WEEK_START_DATE)
CLUSTER BY (WEEK_KEY, SKILL_KEY, JOB_FAMILY_KEY);
```

**Implementation Steps**:

**Step 1: Source Data Integration & Quality Filtering**
```sql
-- Core source query with quality filters:
WITH quality_job_skills AS (
    SELECT
        fjp.JOB_POSTING_KEY,
        fjp.JOB_UID,
        fjp.DATE_POSTED_KEY,
        fjp.FIRST_POSTED_DATE,
        fjp.JOB_FAMILY_KEY,
        fjp.LOCATION_KEY,
        fjp.SALARY_MIDPOINT_ANNUAL_USD,
        fjp.WORK_TYPE,
        fjp.IS_ACTIVE_POSTING,
        fjp.SALARY_CONFIDENCE,
        fjp.LLM_OVERALL_CONFIDENCE,

        -- Skills from bridge table with confidence filtering
        jsb.SKILL_ID,
        jsb.SKILL_CATEGORY,
        jsb.OVERALL_CONFIDENCE as skill_extraction_confidence,

        -- Generate week keys for aggregation
        TO_CHAR(fjp.FIRST_POSTED_DATE, 'IYYY-IW') as week_key,
        DATE_TRUNC('week', fjp.FIRST_POSTED_DATE) as week_start_date,
        DATE_TRUNC('week', fjp.FIRST_POSTED_DATE) + 6 as week_end_date

    FROM ANALYTICS.FACT_JOB_POSTINGS fjp
    INNER JOIN BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE jsb
        ON fjp.JOB_UID = jsb.JOB_UID

    WHERE fjp.IS_ACTIVE_POSTING = TRUE
      AND fjp.LLM_OVERALL_CONFIDENCE >= 0.5
      AND jsb.OVERALL_CONFIDENCE >= 0.7        -- High-confidence skill extractions only
      AND jsb.NEEDS_REVIEW = FALSE
      AND fjp.FIRST_POSTED_DATE >= CURRENT_DATE - 730  -- 2 years of data
)
```

**Step 2: Weekly Skill Demand Aggregation**
```sql
-- Primary aggregation by skill, week, job family, location:
skills_weekly_base AS (
    SELECT
        qjs.week_key,
        qjs.week_start_date,
        qjs.week_end_date,
        ds.SKILL_KEY,
        ds.SKILL_NAME,
        ds.SKILL_CATEGORY,
        qjs.JOB_FAMILY_KEY,
        qjs.LOCATION_KEY,

        -- Core demand metrics
        COUNT(DISTINCT qjs.JOB_UID) as active_jobs_with_skill,
        AVG(qjs.skill_extraction_confidence) as avg_skill_confidence,
        COUNT(DISTINCT qjs.JOB_UID) as sample_size,

        -- Salary analysis (using denormalized fields)
        AVG(CASE WHEN qjs.SALARY_CONFIDENCE >= 0.6 AND qjs.SALARY_MIDPOINT_ANNUAL_USD > 0
                 THEN qjs.SALARY_MIDPOINT_ANNUAL_USD END) as avg_salary_midpoint_annual_usd,
        COUNT(CASE WHEN qjs.SALARY_CONFIDENCE >= 0.6 AND qjs.SALARY_MIDPOINT_ANNUAL_USD > 0
                   THEN 1 END) as salary_sample_size,

        -- Work arrangement analysis
        COUNT(CASE WHEN qjs.WORK_TYPE = 'Remote' THEN 1 END) as remote_jobs_with_skill,

        -- Data completeness tracking
        COUNT(CASE WHEN qjs.SALARY_CONFIDENCE >= 0.6 THEN 1 END)::FLOAT /
        COUNT(DISTINCT qjs.JOB_UID) as data_completeness_score

    FROM quality_job_skills qjs
    INNER JOIN ANALYTICS.DIM_SKILLS ds ON qjs.SKILL_ID = ds.SKILL_ID

    WHERE ds.SKILL_KEY IS NOT NULL

    GROUP BY qjs.week_key, qjs.week_start_date, qjs.week_end_date,
             ds.SKILL_KEY, ds.SKILL_NAME, ds.SKILL_CATEGORY,
             qjs.JOB_FAMILY_KEY, qjs.LOCATION_KEY

    HAVING COUNT(DISTINCT qjs.JOB_UID) >= 3  -- Minimum statistical validity
)
```

**Step 3: Market Context & Baseline Calculations**
```sql
-- Calculate total market context and salary baselines:
market_context AS (
    SELECT
        qjs.week_key,
        qjs.JOB_FAMILY_KEY,
        qjs.LOCATION_KEY,

        -- Total market size for penetration rate calculation
        COUNT(DISTINCT qjs.JOB_UID) as total_active_jobs,

        -- Baseline salary (jobs WITHOUT specific skills) for premium calculation
        AVG(CASE WHEN qjs.SALARY_CONFIDENCE >= 0.6 AND qjs.SALARY_MIDPOINT_ANNUAL_USD > 0
                 THEN qjs.SALARY_MIDPOINT_ANNUAL_USD END) as baseline_salary_midpoint_annual_usd

    FROM quality_job_skills qjs
    GROUP BY qjs.week_key, qjs.JOB_FAMILY_KEY, qjs.LOCATION_KEY
)
```

**Step 4: Trend Analysis & Week-over-Week Calculations**
```sql
-- Add trend analysis with previous week comparison:
skills_with_trends AS (
    SELECT swb.*,
           mc.total_active_jobs,
           mc.baseline_salary_midpoint_annual_usd,

           -- Penetration rate calculation
           swb.active_jobs_with_skill::FLOAT / mc.total_active_jobs * 100 as skill_penetration_rate,

           -- Salary premium calculations
           (swb.avg_salary_midpoint_annual_usd - mc.baseline_salary_midpoint_annual_usd) as salary_premium_annual_usd,
           CASE WHEN mc.baseline_salary_midpoint_annual_usd > 0
                THEN ((swb.avg_salary_midpoint_annual_usd / mc.baseline_salary_midpoint_annual_usd) - 1) * 100
                ELSE NULL END as salary_premium_percentage,

           -- Remote work percentage
           swb.remote_jobs_with_skill::FLOAT / swb.active_jobs_with_skill * 100 as remote_skill_percentage,

           -- Week-over-week trend analysis
           LAG(swb.active_jobs_with_skill, 1) OVER (
               PARTITION BY swb.skill_key, swb.job_family_key, swb.location_key
               ORDER BY swb.week_start_date
           ) as prev_week_jobs,

           -- 4-week moving average for trend smoothing
           AVG(swb.active_jobs_with_skill) OVER (
               PARTITION BY swb.skill_key, swb.job_family_key, swb.location_key
               ORDER BY swb.week_start_date
               ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
           ) as four_week_moving_average

    FROM skills_weekly_base swb
    INNER JOIN market_context mc
        ON swb.week_key = mc.week_key
        AND swb.job_family_key = mc.job_family_key
        AND swb.location_key = mc.location_key
)
```

**Step 5: Ranking & Market Position Analysis**
```sql
-- Calculate skill rankings within job families and overall market:
skills_with_rankings AS (
    SELECT swt.*,
           -- Week-over-week change calculations
           (swt.active_jobs_with_skill - swt.prev_week_jobs) as week_over_week_change,
           CASE WHEN swt.prev_week_jobs > 0
                THEN ((swt.active_jobs_with_skill::FLOAT / swt.prev_week_jobs) - 1) * 100
                ELSE NULL END as week_over_week_growth_rate,

           -- Trend direction classification
           CASE
               WHEN swt.prev_week_jobs IS NULL THEN 'NEW'
               WHEN ((swt.active_jobs_with_skill::FLOAT / swt.prev_week_jobs) - 1) * 100 > 25 THEN 'GROWING'
               WHEN ((swt.active_jobs_with_skill::FLOAT / swt.prev_week_jobs) - 1) * 100 > 5 THEN 'STABLE'
               WHEN ((swt.active_jobs_with_skill::FLOAT / swt.prev_week_jobs) - 1) * 100 > -10 THEN 'STABLE'
               ELSE 'DECLINING'
           END as trend_direction,

           -- Skill rankings within job family
           RANK() OVER (
               PARTITION BY swt.week_key, swt.job_family_key
               ORDER BY swt.active_jobs_with_skill DESC
           ) as skill_rank_in_family,

           -- Overall market ranking
           RANK() OVER (
               PARTITION BY swt.week_key
               ORDER BY swt.active_jobs_with_skill DESC
           ) as skill_rank_overall,

           -- Market share within job family
           swt.active_jobs_with_skill::FLOAT / SUM(swt.active_jobs_with_skill) OVER (
               PARTITION BY swt.week_key, swt.job_family_key
           ) * 100 as market_share_in_family

    FROM skills_with_trends swt
)
```

**Step 6: Final Aggregation & Key Generation**
```sql
-- Generate final table with surrogate keys:
SELECT
    -- Primary key generation
    'SW_' || swr.week_key || '_' || swr.skill_key || '_' || swr.job_family_key || '_' || swr.location_key as skills_weekly_key,

    -- Dimension keys
    swr.week_key,
    swr.skill_key,
    swr.job_family_key,
    swr.location_key,

    -- Core demand metrics
    swr.active_jobs_with_skill,
    swr.total_active_jobs,
    swr.skill_penetration_rate,

    -- Enhanced salary analysis
    swr.avg_salary_midpoint_annual_usd,
    swr.baseline_salary_midpoint_annual_usd,
    swr.salary_premium_annual_usd,
    swr.salary_premium_percentage,
    swr.salary_sample_size,

    -- Trend analysis
    swr.week_over_week_change,
    swr.week_over_week_growth_rate,
    swr.trend_direction,
    swr.four_week_moving_average,

    -- Market position
    swr.skill_rank_in_family,
    swr.skill_rank_overall,
    swr.market_share_in_family,

    -- Data quality metrics
    swr.avg_skill_confidence,
    swr.data_completeness_score,
    swr.sample_size,

    -- Work arrangement analysis
    swr.remote_jobs_with_skill,
    swr.remote_skill_percentage,

    -- Audit fields
    CURRENT_TIMESTAMP as created_timestamp,
    CURRENT_DATE as processing_date,
    swr.week_start_date,
    swr.week_end_date

FROM skills_with_rankings swr
ORDER BY swr.week_start_date DESC, swr.skill_rank_overall ASC;
```

**Data Quality Validation & Business Rules**:

```sql
-- Post-processing validation checks:
1. UNIQUENESS: Verify SKILLS_WEEKLY_KEY uniqueness
2. COMPLETENESS: Ensure all active skills have weekly records
3. PENETRATION RATES: Validate 0 <= skill_penetration_rate <= 100
4. TREND LOGIC: Verify trend_direction matches growth_rate thresholds
5. RANKING INTEGRITY: Ensure ranking consistency within partitions
6. SALARY LOGIC: Validate salary premium calculations and outliers
7. TEMPORAL CONSISTENCY: Ensure week keys align with actual week dates
```

**Performance Optimization Strategy**:

- **Partitioning**: Monthly partitions by `WEEK_START_DATE` for 2-year retention
- **Clustering**: `(WEEK_KEY, SKILL_KEY, JOB_FAMILY_KEY)` for analytical query patterns
- **Incremental Processing**: Process only new/changed weeks to minimize processing time
- **Pre-aggregation**: Calculate complex metrics once for dashboard performance
- **Index Strategy**: Optimize for skill trend queries and ranking analysis

**Expected Data Volume & Performance**:
- **Weekly Records**: ~50,000-100,000 skill-week-family-location combinations
- **Processing Time**: <15 minutes for full weekly refresh
- **Query Performance**: <2 seconds for skill trend analysis
- **Data Retention**: 104 weeks (2 years) for comprehensive trend analysis

**Business Intelligence Capabilities**:
- **Technology Trend Detection**: Identify emerging and declining skills weekly
- **Salary Premium Analysis**: Quantify skill value in the job market
- **Geographic Skill Distribution**: Track skills demand by location
- **Remote Work Trends**: Analyze remote-friendly skill categories
- **Market Share Intelligence**: Track skill adoption within job families
- **Competitive Skills Analysis**: Compare skill demand across industries

#### 3.2 `analytics_fact_company_hiring_weekly`
**Purpose**: Create weekly company hiring intelligence for competitive analysis and market positioning
**Dependencies**: `analytics_fact_job_postings`, `analytics_dim_company`
**Output**: Company hiring patterns, velocity metrics, and competitive intelligence

**Business Need**: Track company-specific hiring trends with weekly granularity to enable:
- Company hiring velocity analysis and competitive benchmarking
- Compensation strategy intelligence by company
- Work arrangement policy analysis across companies
- Role distribution patterns and hiring focus identification
- Market position tracking and hiring competitiveness scoring

```python
@asset(
    deps=["analytics_fact_job_postings", "analytics_dim_company"],
    description="Create weekly company hiring aggregate fact table for competitive analysis",
    group_name="3c_analytics_aggregates",
    kinds={"snowflake", "SQL"}
)
def analytics_fact_company_hiring_weekly(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build weekly company hiring aggregates from job postings for competitive intelligence.

    This asset creates comprehensive weekly company hiring intelligence by aggregating job postings
    by company and week to enable responsive competitive analysis and market positioning.

    Business Intelligence Capabilities:
    - Company hiring velocity tracking and trend analysis
    - Competitive benchmarking across companies in same industry/size
    - Compensation strategy analysis (salary ranges, transparency rates)
    - Work arrangement policy intelligence (remote, hybrid, onsite percentages)
    - Role distribution analysis (entry vs senior, management ratios)
    - Market position scoring and hiring competitiveness metrics

    Data Quality Rules:
    - Include only companies with valid COMPANY_KEY from dimension lookup
    - Filter active job postings (IS_ACTIVE_POSTING = TRUE)
    - Require minimum 2 jobs per company-week for trend reliability
    - Use salary data only where SALARY_CONFIDENCE >= 0.6
    - Apply LLM confidence filtering (LLM_OVERALL_CONFIDENCE >= 0.5)

    Grain: One record per company per week
    Aggregation Level: Weekly (Sunday-Saturday weeks)
    Retention: 104 weeks (2 years) for competitive trend analysis
    """
```

**Table Structure Analysis**:

All fields in the table definition align with available upstream data from `FACT_JOB_POSTINGS`:

**✅ Core Hiring Metrics (Available)**:
- `JOBS_POSTED_COUNT` ← COUNT(*) from FACT_JOB_POSTINGS by company/week
- `ACTIVE_JOBS_COUNT` ← COUNT WHERE IS_ACTIVE_POSTING = TRUE
- `NEW_JOBS_THIS_WEEK` ← COUNT of jobs with FIRST_POSTED_DATE in week

**✅ Hiring Velocity & Trends (Calculable)**:
- `WEEK_OVER_WEEK_CHANGE` ← Current week jobs - Previous week jobs
- `HIRING_TREND_DIRECTION` ← Calculated from WoW growth rate thresholds

**✅ Job Portfolio Analysis (From denormalized salary fields)**:
- `AVG_SALARY_OFFERED` ← AVG(SALARY_MIDPOINT_ANNUAL_USD)
- `MEDIAN_SALARY_OFFERED` ← MEDIAN(SALARY_MIDPOINT_ANNUAL_USD)
- `SALARY_RANGE_WIDTH` ← MAX(SALARY_MAX) - MIN(SALARY_MIN) per company

**✅ Work Arrangement Patterns (From WORK_TYPE field)**:
- `REMOTE_JOBS_PERCENTAGE` ← % WHERE WORK_TYPE = 'Remote'
- `HYBRID_JOBS_PERCENTAGE` ← % WHERE WORK_TYPE = 'Hybrid'
- `ON_SITE_JOBS_PERCENTAGE` ← % WHERE WORK_TYPE = 'On-site'

**✅ Role Distribution (From SENIORITY_LEVEL field)**:
- `ENTRY_LEVEL_PERCENTAGE` ← % WHERE SENIORITY_LEVEL LIKE '%Entry%' OR '%Junior%'
- `SENIOR_LEVEL_PERCENTAGE` ← % WHERE SENIORITY_LEVEL LIKE '%Senior%' OR '%Staff%' OR '%Principal%'
- `MANAGEMENT_ROLES_PERCENTAGE` ← % WHERE SENIORITY_LEVEL LIKE '%Manager%' OR '%Director%' OR '%VP%'

**Implementation Steps**:

**Step 1: Source Data Quality Filtering**
```sql
-- Filter high-quality job postings by company and week
WITH quality_company_jobs AS (
    SELECT
        COMPANY_KEY,
        FIRST_POSTED_DATE,
        TO_CHAR(FIRST_POSTED_DATE, 'IYYY-IW') as week_key,
        DATE_TRUNC('week', FIRST_POSTED_DATE) as week_start_date,
        IS_ACTIVE_POSTING,
        SALARY_MIDPOINT_ANNUAL_USD,
        SALARY_MIN,
        SALARY_MAX,
        SALARY_CONFIDENCE,
        WORK_TYPE,
        SENIORITY_LEVEL,
        LLM_OVERALL_CONFIDENCE
    FROM ANALYTICS.FACT_JOB_POSTINGS
    WHERE COMPANY_KEY IS NOT NULL
      AND COMPANY_KEY != 'COMP_UNKNOWN'
      AND LLM_OVERALL_CONFIDENCE >= 0.5
      AND FIRST_POSTED_DATE >= CURRENT_DATE - 730  -- 2 years of data
)
```

**Step 2: Weekly Company Aggregation**
```sql
-- Aggregate hiring metrics by company and week
company_weekly_base AS (
    SELECT
        week_key,
        week_start_date,
        COMPANY_KEY,

        -- Core hiring metrics
        COUNT(*) as jobs_posted_count,
        COUNT(CASE WHEN IS_ACTIVE_POSTING THEN 1 END) as active_jobs_count,
        COUNT(CASE WHEN DATE_TRUNC('week', FIRST_POSTED_DATE) = week_start_date THEN 1 END) as new_jobs_this_week,

        -- Salary analysis (with confidence filtering)
        AVG(CASE WHEN SALARY_CONFIDENCE >= 0.6 AND SALARY_MIDPOINT_ANNUAL_USD > 0
                 THEN SALARY_MIDPOINT_ANNUAL_USD END) as avg_salary_offered,
        MEDIAN(CASE WHEN SALARY_CONFIDENCE >= 0.6 AND SALARY_MIDPOINT_ANNUAL_USD > 0
                   THEN SALARY_MIDPOINT_ANNUAL_USD END) as median_salary_offered,
        (MAX(CASE WHEN SALARY_CONFIDENCE >= 0.6 THEN SALARY_MAX END) -
         MIN(CASE WHEN SALARY_CONFIDENCE >= 0.6 THEN SALARY_MIN END)) as salary_range_width,

        -- Work arrangement patterns
        COUNT(CASE WHEN WORK_TYPE = 'Remote' THEN 1 END)::FLOAT / COUNT(*) * 100 as remote_jobs_percentage,
        COUNT(CASE WHEN WORK_TYPE = 'Hybrid' THEN 1 END)::FLOAT / COUNT(*) * 100 as hybrid_jobs_percentage,
        COUNT(CASE WHEN WORK_TYPE = 'On-site' THEN 1 END)::FLOAT / COUNT(*) * 100 as on_site_jobs_percentage,

        -- Role distribution patterns
        COUNT(CASE WHEN LOWER(SENIORITY_LEVEL) LIKE '%entry%' OR LOWER(SENIORITY_LEVEL) LIKE '%junior%'
                   THEN 1 END)::FLOAT / COUNT(*) * 100 as entry_level_percentage,
        COUNT(CASE WHEN LOWER(SENIORITY_LEVEL) LIKE '%senior%' OR LOWER(SENIORITY_LEVEL) LIKE '%staff%'
                    OR LOWER(SENIORITY_LEVEL) LIKE '%principal%'
                   THEN 1 END)::FLOAT / COUNT(*) * 100 as senior_level_percentage,
        COUNT(CASE WHEN LOWER(SENIORITY_LEVEL) LIKE '%manager%' OR LOWER(SENIORITY_LEVEL) LIKE '%director%'
                    OR LOWER(SENIORITY_LEVEL) LIKE '%vp%'
                   THEN 1 END)::FLOAT / COUNT(*) * 100 as management_roles_percentage

    FROM quality_company_jobs
    GROUP BY week_key, week_start_date, COMPANY_KEY
    HAVING COUNT(*) >= 2  -- Minimum statistical validity for company trends
)
```

**Step 3: Trend Analysis & Growth Calculations**
```sql
-- Add week-over-week trend analysis
company_with_trends AS (
    SELECT cwb.*,
           -- Previous week comparison for trend analysis
           LAG(cwb.jobs_posted_count, 1) OVER (
               PARTITION BY cwb.COMPANY_KEY
               ORDER BY cwb.week_start_date
           ) as prev_week_jobs,

           -- Calculate week-over-week change
           (cwb.jobs_posted_count - LAG(cwb.jobs_posted_count, 1) OVER (
               PARTITION BY cwb.COMPANY_KEY
               ORDER BY cwb.week_start_date
           )) as week_over_week_change,

           -- Trend direction classification
           CASE
               WHEN LAG(cwb.jobs_posted_count, 1) OVER (PARTITION BY cwb.COMPANY_KEY ORDER BY cwb.week_start_date) IS NULL THEN 'New'
               WHEN ((cwb.jobs_posted_count::FLOAT / LAG(cwb.jobs_posted_count, 1) OVER (PARTITION BY cwb.COMPANY_KEY ORDER BY cwb.week_start_date)) - 1) * 100 > 20 THEN 'Accelerating'
               WHEN ((cwb.jobs_posted_count::FLOAT / LAG(cwb.jobs_posted_count, 1) OVER (PARTITION BY cwb.COMPANY_KEY ORDER BY cwb.week_start_date)) - 1) * 100 > -10 THEN 'Stable'
               ELSE 'Declining'
           END as hiring_trend_direction
    FROM company_weekly_base cwb
)
```

**Step 4: Final Aggregation with Primary Key Generation**
```sql
-- Generate final company hiring weekly records
SELECT
    'CH_' || cwt.week_key || '_' || cwt.COMPANY_KEY as company_hiring_key,
    cwt.week_key,
    cwt.COMPANY_KEY,
    cwt.jobs_posted_count,
    cwt.active_jobs_count,
    cwt.new_jobs_this_week,
    cwt.week_over_week_change,
    cwt.hiring_trend_direction,
    cwt.avg_salary_offered,
    cwt.median_salary_offered,
    cwt.salary_range_width,
    cwt.remote_jobs_percentage,
    cwt.hybrid_jobs_percentage,
    cwt.on_site_jobs_percentage,
    cwt.entry_level_percentage,
    cwt.senior_level_percentage,
    cwt.management_roles_percentage,
    CURRENT_TIMESTAMP as created_timestamp,
    cwt.week_start_date
FROM company_with_trends cwt
ORDER BY cwt.week_start_date DESC, cwt.jobs_posted_count DESC;
```

**Performance Optimization**:
- **Clustering**: `(WEEK_KEY, COMPANY_KEY)` for company-specific trend analysis
- **Incremental Processing**: Process only new/changed weeks to minimize processing time
- **Data Retention**: 104 weeks (2 years) for comprehensive competitive analysis
- **Query Performance Target**: <3 seconds for company trend queries

**Business Intelligence Capabilities**:
- **Competitive Benchmarking**: Compare hiring velocity across similar companies
- **Compensation Intelligence**: Track salary strategy changes and market positioning
- **Work Policy Analysis**: Monitor remote work adoption and policy shifts
- **Hiring Focus Analysis**: Identify whether companies are hiring junior vs senior talent
- **Market Position Scoring**: Rank companies by hiring aggressiveness and competitiveness

**Expected Data Volume**:
- **Weekly Records**: ~500-2,000 company-week combinations (depending on active companies)
- **Processing Time**: <5 minutes for full weekly refresh
- **Data Quality**: Minimum 2 jobs per company-week for statistical reliability
- **Business Coverage**: All companies with sufficient job posting volume

### Phase 4: Market Intelligence Tables
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

### Implementation Phases Timeline

#### Phase 1: Foundation ✅ **COMPLETE**
**Status**: All dimension tables implemented and operational
**Deliverables**:
- ✅ All 9 dimension tables created and populated:
  - `DIM_DATE` - Date dimension with business calendar
  - `DIM_COMPANY` - Company dimension with SCD Type 2
  - `DIM_LOCATION` - Location dimension with geographic hierarchy
  - `DIM_JOB_FAMILY` - Job classification with role taxonomy
  - `DIM_PLATFORM` - ATS platform characteristics
  - `DIM_SKILLS` - Skills taxonomy from normalized data
  - `DIM_SALARY` - Normalized salary ranges and market intelligence
  - `DIM_EXPERIENCE` - Experience levels and requirements
  - `DIM_KEYWORDS` - Keywords classification with hierarchy
- ✅ Surrogate key generation logic implemented
- ✅ Data quality validation framework established
- ✅ Performance optimization (clustering) implemented

#### Phase 2: Core Facts ✅ **COMPLETE**
**Status**: Primary fact table implemented and operational
**Deliverables**:
- ✅ `FACT_JOB_POSTINGS` table operational with comprehensive measures
- ✅ Complete dimensional model integration with all 9 dimensions
- ✅ Data quality filtering and business rules implementation
- ✅ LLM enrichment data integration with confidence scoring
- ✅ Salary, skills, experience, and location bridge table integration
- ✅ Performance optimization with clustering strategy

#### Phase 3: Aggregates ✅ **COMPLETE**
**Status**: Both weekly aggregate fact tables implemented and operational
**Deliverables**:
- ✅ `FACT_SKILLS_DEMAND_WEEKLY` - Weekly skills demand with trend analysis
  - Skills penetration rates and market demand metrics
  - Salary premium analysis and skill value quantification
  - Week-over-week growth tracking and trend classification
  - Market position rankings and competitive analysis
  - Remote work arrangement patterns by skill
  - Comprehensive data quality and confidence metrics
- ✅ `FACT_COMPANY_HIRING_WEEKLY` - Company hiring intelligence and competitive analysis
  - Weekly company hiring velocity tracking and trend analysis
  - Competitive benchmarking across companies in same industry/size
  - Compensation strategy analysis (salary ranges, transparency rates)
  - Work arrangement policy intelligence (remote, hybrid, onsite percentages)
  - Role distribution analysis (entry vs senior, management ratios)
  - Market position scoring and hiring competitiveness metrics
- ⏳ Automated weekly refresh processes (pending)
- ⏳ Cross-table consistency validation (pending)

#### Phase 4: Market Intelligence ✅ **IN PROGRESS**
**Status**: Market weekly summary table implemented and operational
**Deliverables**:
- ✅ `MARKET_WEEKLY_SUMMARY` - Pre-calculated market metrics for executive dashboards
  - Weekly job posting velocity and market temperature indicators
  - Salary intelligence with median/average compensation trends
  - Work arrangement patterns (remote/hybrid/onsite percentages)
  - Executive KPI tracking with week-over-week growth rates
  - Data quality monitoring and completeness metrics
- ⏳ `SKILLS_TREND_ANALYSIS` - Technology intelligence (pending)
- ⏳ `COMPANY_HIRING_INTELLIGENCE` - Company analysis (pending)
- ⏳ Automated metric generation (pending)
- ⏳ Business rule validation (pending)

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