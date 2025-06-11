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

##### 1. `fact_job_postings` (Primary Fact Table)
**Grain**: One record per job posting per day (for historical tracking)

```sql
CREATE TABLE ANALYTICS.fact_job_postings (
    -- Surrogate Keys
    job_posting_key STRING PRIMARY KEY,
    date_key STRING,
    company_key STRING,
    location_key STRING,
    job_family_key STRING,
    platform_key STRING,

    -- Degenerate Dimensions
    job_uid STRING,  -- Natural key from STAGE
    job_title STRING,
    posting_url STRING,

    -- Measures (Additive)
    salary_min NUMBER,
    salary_max NUMBER,
    salary_midpoint NUMBER,  -- Calculated: (min + max) / 2
    experience_min_years NUMBER,
    experience_max_years NUMBER,
    experience_midpoint_years NUMBER,

    -- Measures (Semi-Additive)
    posting_age_days NUMBER,  -- Days since first posted

    -- Measures (Non-Additive - Ratios/Percentages)
    salary_confidence_score FLOAT,
    extraction_confidence_score FLOAT,
    data_quality_score FLOAT,
    remote_work_score FLOAT,  -- 0=On-site, 0.5=Hybrid, 1=Remote

    -- Flags (Additive for Counts)
    is_active_posting BOOLEAN,
    is_new_posting BOOLEAN,  -- New this week
    is_salary_disclosed BOOLEAN,
    is_remote_eligible BOOLEAN,
    is_equity_mentioned BOOLEAN,
    is_bonus_mentioned BOOLEAN,
    has_education_requirement BOOLEAN,
    has_certification_requirement BOOLEAN,

    -- Dates
    first_posted_date DATE,
    last_seen_date DATE,

    -- Audit Fields
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    source_stage_table STRING,

    -- Partitioning
    partition_date DATE
) PARTITION BY (partition_date)
CLUSTER BY (date_key, company_key, location_key, job_family_key);
```

##### 2. `fact_skills_demand` (Skills Analysis Fact)
**Grain**: One record per skill per time period per job family

```sql
CREATE TABLE ANALYTICS.fact_skills_demand (
    -- Surrogate Keys
    skills_demand_key STRING PRIMARY KEY,
    date_key STRING,
    skill_key STRING,
    job_family_key STRING,
    location_key STRING,

    -- Measures
    jobs_requiring_skill INTEGER,
    total_jobs_in_category INTEGER,
    skill_demand_percentage FLOAT,  -- jobs_requiring / total_jobs
    average_salary_premium FLOAT,  -- Salary boost for this skill
    median_salary_with_skill NUMBER,
    skill_growth_rate_weekly FLOAT,
    skill_growth_rate_monthly FLOAT,

    -- Rankings
    skill_rank_overall INTEGER,
    skill_rank_in_category INTEGER,
    skill_rank_change_weekly INTEGER,

    -- Flags
    is_emerging_skill BOOLEAN,  -- New to top rankings
    is_declining_skill BOOLEAN, -- Dropping in rankings
    is_hot_skill BOOLEAN,       -- High growth + high demand

    -- Audit Fields
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    partition_date DATE
) PARTITION BY (partition_date)
CLUSTER BY (date_key, skill_key, job_family_key);
```

##### 3. `fact_salary_benchmarks` (Compensation Analysis Fact)
**Grain**: One record per job family per location per experience level per time period

```sql
CREATE TABLE ANALYTICS.fact_salary_benchmarks (
    -- Surrogate Keys
    salary_benchmark_key STRING PRIMARY KEY,
    date_key STRING,
    job_family_key STRING,
    location_key STRING,
    experience_level_key STRING,

    -- Statistical Measures
    salary_count INTEGER,            -- Sample size
    salary_min NUMBER,              -- Minimum observed
    salary_max NUMBER,              -- Maximum observed
    salary_mean NUMBER,             -- Average
    salary_median NUMBER,           -- Median (50th percentile)
    salary_p25 NUMBER,              -- 25th percentile
    salary_p75 NUMBER,              -- 75th percentile
    salary_p90 NUMBER,              -- 90th percentile
    salary_stddev NUMBER,           -- Standard deviation

    -- Growth Metrics
    salary_growth_mom FLOAT,        -- Month-over-month growth
    salary_growth_yoy FLOAT,        -- Year-over-year growth

    -- Market Indicators
    market_competitiveness_score FLOAT,  -- 0-100 scale
    salary_inflation_indicator FLOAT,    -- Compared to general inflation

    -- Quality Metrics
    confidence_interval_95_lower NUMBER,
    confidence_interval_95_upper NUMBER,
    data_quality_score FLOAT,

    -- Audit Fields
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    partition_date DATE
) PARTITION BY (partition_date)
CLUSTER BY (date_key, job_family_key, location_key);
```

##### 4. `fact_company_hiring_velocity` (Company Intelligence Fact)
**Grain**: One record per company per time period

```sql
CREATE TABLE ANALYTICS.fact_company_hiring_velocity (
    -- Surrogate Keys
    hiring_velocity_key STRING PRIMARY KEY,
    date_key STRING,
    company_key STRING,

    -- Hiring Metrics
    jobs_posted_count INTEGER,
    net_new_postings INTEGER,        -- New postings this period

    -- Velocity Indicators
    posting_velocity_daily FLOAT,    -- Jobs per day
    posting_velocity_weekly FLOAT,   -- Jobs per week
    velocity_change_percentage FLOAT, -- Week-over-week change

    -- Hiring Patterns
    avg_salary_offered NUMBER,
    remote_jobs_percentage FLOAT,
    senior_roles_percentage FLOAT,

    -- Market Position
    hiring_rank_in_industry INTEGER,
    hiring_rank_overall INTEGER,

    -- Company Health Indicators
    hiring_intensity_score FLOAT,    -- Based on posting frequency and volume

    -- Audit Fields
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    partition_date DATE
) PARTITION BY (partition_date)
CLUSTER BY (date_key, company_key);
```

#### Dimension Tables

##### 1. `dim_date` (Date Dimension)
**Business Calendar with Job Market Context**

```sql
CREATE TABLE ANALYTICS.dim_date (
    date_key STRING PRIMARY KEY,
    full_date DATE,

    -- Standard Date Attributes
    day_name STRING,
    day_of_week INTEGER,
    day_of_month INTEGER,
    day_of_year INTEGER,
    week_number INTEGER,
    week_beginning_date DATE,
    week_ending_date DATE,
    month_number INTEGER,
    month_name STRING,
    month_beginning_date DATE,
    month_ending_date DATE,
    quarter_number INTEGER,
    quarter_name STRING,
    year_number INTEGER,

    -- Business Context
    is_business_day BOOLEAN,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    holiday_name STRING,
    is_quarter_end BOOLEAN,
    is_month_end BOOLEAN,
    is_year_end BOOLEAN,

    -- Job Market Context
    is_peak_hiring_season BOOLEAN,    -- Typically Jan-Mar, Sep-Oct
    is_slow_hiring_period BOOLEAN,    -- Typically Nov-Dec, July-Aug
    hiring_season STRING,             -- 'Peak', 'Normal', 'Slow'

    -- Relative Date Attributes
    days_ago INTEGER,                 -- Days from current date
    weeks_ago INTEGER,                -- Weeks from current date
    months_ago INTEGER,               -- Months from current date
    years_ago INTEGER,                -- Years from current date

    -- Fiscal Calendar (if needed)
    fiscal_year INTEGER,
    fiscal_quarter INTEGER,
    fiscal_month INTEGER,

    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (full_date);
```

##### 2. `dim_company` (Company Dimension)
**Type 2 SCD for Company Changes**

```sql
CREATE TABLE ANALYTICS.dim_company (
    company_key STRING PRIMARY KEY,
    company_id STRING,              -- Natural key from STAGE

    -- Company Identity
    company_name STRING,
    company_name_clean STRING,
    company_legal_name STRING,
    company_ticker_symbol STRING,
    company_website STRING,

    -- Company Classification
    industry STRING,
    industry_category STRING,       -- Standardized categories
    sub_industry STRING,
    business_model STRING,          -- B2B, B2C, B2B2C, Marketplace

    -- Company Size
    employee_count_range STRING,
    employee_count_min INTEGER,
    employee_count_max INTEGER,
    company_size_category STRING,   -- Startup, Small, Medium, Large, Enterprise

    -- Company Location
    headquarters_city STRING,
    headquarters_state STRING,
    headquarters_country STRING,
    headquarters_region STRING,
    is_multinational BOOLEAN,

    -- Company Stage & Funding
    funding_stage STRING,           -- Seed, Series A, B, C, IPO, etc.
    total_funding_usd NUMBER,
    last_funding_date DATE,
    is_public_company BOOLEAN,
    is_unicorn BOOLEAN,             -- Valuation > $1B

    -- Company Metrics
    estimated_revenue_range STRING,
    revenue_growth_stage STRING,    -- Growth, Mature, Declining
    technology_stack_profile STRING, -- Modern, Legacy, Mixed

    -- Hiring Characteristics
    typical_hiring_velocity STRING,  -- High, Medium, Low
    remote_work_policy STRING,       -- Full Remote, Hybrid, On-site
    geographic_hiring_scope STRING,  -- Local, National, Global

    -- SCD Type 2 Fields
    effective_date DATE,
    expiration_date DATE,
    is_current BOOLEAN,
    version_number INTEGER,

    -- Audit Fields
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    source_stage_table STRING
) CLUSTER BY (company_id, is_current);
```

##### 3. `dim_location` (Location Dimension)
**Geographic Hierarchy with Market Context**

```sql
CREATE TABLE ANALYTICS.dim_location (
    location_key STRING PRIMARY KEY,

    -- Location Hierarchy
    city STRING,
    state STRING,
    state_abbreviation STRING,
    country STRING,
    country_code STRING,
    region STRING,                   -- West Coast, East Coast, Midwest, etc.

    -- Metropolitan Area
    metro_area STRING,
    metro_area_code STRING,
    metro_population INTEGER,

    -- Geographic Coordinates (for mapping)
    latitude FLOAT,
    longitude FLOAT,
    timezone STRING,

    -- Market Characteristics
    cost_of_living_index FLOAT,      -- Relative to national average
    median_home_price INTEGER,
    tech_job_market_rank INTEGER,    -- Based on job volume & competition
    university_count INTEGER,        -- Talent pipeline indicator

    -- Market Classification
    market_tier STRING,              -- Tier 1, Tier 2, Tier 3
    is_tech_hub BOOLEAN,
    is_financial_center BOOLEAN,
    is_government_center BOOLEAN,

    -- Remote Work Context
    is_remote_location BOOLEAN,      -- For fully remote jobs
    remote_work_adoption_rate FLOAT, -- Local companies offering remote

    -- Economic Indicators
    unemployment_rate FLOAT,
    job_growth_rate FLOAT,
    startup_density_score FLOAT,

    -- Quality of Life
    walkability_score INTEGER,
    public_transit_score INTEGER,
    weather_rating STRING,

    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (state, city);
```

##### 4. `dim_job_family` (Job Classification Dimension)
**Hierarchical Job Taxonomy**

```sql
CREATE TABLE ANALYTICS.dim_job_family (
    job_family_key STRING PRIMARY KEY,

    -- Job Hierarchy
    job_family STRING,               -- Engineering, Data, Product, Sales, etc.
    job_sub_family STRING,          -- Backend Engineering, Data Science, etc.
    job_specialty STRING,           -- Python Developer, ML Engineer, etc.

    -- Seniority Classification
    seniority_level STRING,         -- Entry, Mid, Senior, Staff, Principal, Executive
    seniority_order INTEGER,        -- 1-7 for ordering
    experience_min_years INTEGER,
    experience_max_years INTEGER,

    -- Role Type
    role_type STRING,               -- Individual Contributor, Manager, Director, VP
    management_level INTEGER,       -- 0=IC, 1=Manager, 2=Director, 3=VP, 4=C-Level
    is_management_role BOOLEAN,

    -- Department & Function
    department STRING,              -- Engineering, Sales, Marketing, etc.
    business_function STRING,       -- Core Product, Growth, Support, etc.

    -- Job Characteristics
    typical_team_size_min INTEGER,
    typical_team_size_max INTEGER,
    requires_security_clearance BOOLEAN,
    travel_requirement_level STRING, -- None, Low, Medium, High

    -- Market Data
    market_demand_level STRING,     -- Very High, High, Medium, Low
    salary_growth_trend STRING,     -- Growing, Stable, Declining
    automation_risk_level STRING,   -- Low, Medium, High

    -- Skills Context
    primary_skill_category STRING,  -- Technical, Creative, Sales, etc.
    requires_coding BOOLEAN,
    requires_certification BOOLEAN,

    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (job_family, seniority_level);
```

##### 5. `dim_platform` (ATS Platform Dimension)
**Job Platform Characteristics**

```sql
CREATE TABLE ANALYTICS.dim_platform (
    platform_key STRING PRIMARY KEY,
    platform_name STRING,
    platform_code STRING,           -- workday, greenhouse, bamboohr, etc.

    -- Platform Classification
    ats_type STRING,                -- Enterprise, Mid-market, Small Business
    platform_category STRING,      -- ATS, Job Board, Company Site

    -- Platform Characteristics
    typical_company_size STRING,    -- Enterprise, Mid-market, Small
    industry_focus STRING,          -- Technology, Healthcare, General, etc.
    geographic_focus STRING,        -- Global, North America, US Only

    -- Platform Features
    supports_salary_disclosure BOOLEAN,
    supports_remote_filtering BOOLEAN,
    supports_skills_tagging BOOLEAN,
    data_richness_score FLOAT,      -- Quality of job descriptions

    -- Market Share
    estimated_job_volume STRING,    -- High, Medium, Low
    market_share_percentage FLOAT,
    growth_trend STRING,            -- Growing, Stable, Declining

    -- Technical Characteristics
    data_extraction_difficulty STRING, -- Easy, Medium, Hard
    update_frequency_hours INTEGER,     -- How often jobs are updated
    historical_data_retention_days INTEGER,

    is_active BOOLEAN,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (platform_name);
```

##### 6. `dim_skills` (Skills Taxonomy Dimension)
**Hierarchical Skills Classification (Built from STAGE.skills_normalized)**

```sql
CREATE TABLE ANALYTICS.dim_skills (
    skill_key STRING PRIMARY KEY,
    skill_id STRING,               -- FK to STAGE.skills_normalized
    skill_name STRING,
    skill_name_clean STRING,

    -- Skill Hierarchy (from STAGE.skills_normalized)
    skill_category STRING,          -- Programming Language, Database, Cloud, etc.
    skill_subcategory STRING,      -- Backend Language, NoSQL Database, etc.
    skill_family STRING,           -- Development, Data, DevOps, etc.

    -- Enhanced Skill Characteristics (ANALYTICS layer enrichment)
    skill_type STRING,             -- Technical, Soft, Business, Certification
    complexity_level STRING,       -- Beginner, Intermediate, Advanced
    learning_curve STRING,         -- Easy, Medium, Hard

    -- Market Context (calculated from fact tables)
    demand_trend STRING,           -- Rising, Stable, Declining
    supply_level STRING,           -- Abundant, Moderate, Scarce
    salary_impact STRING,          -- High Premium, Medium Premium, Low Premium

    -- Skill Relationships (derived from co-occurrence analysis)
    complementary_skills VARIANT,   -- JSON array of related skills
    prerequisite_skills VARIANT,   -- JSON array of prerequisite skills
    alternative_skills VARIANT,    -- JSON array of alternative skills

    -- Industry Context (calculated from job patterns)
    primary_industries VARIANT,    -- JSON array of main industries using this skill
    adoption_maturity STRING,      -- Emerging, Growing, Mature, Legacy

    -- Certification Context
    is_certifiable BOOLEAN,
    certification_providers VARIANT, -- JSON array of cert providers
    typical_cert_cost_range STRING,

    -- Source tracking
    source_stage_skill_id STRING,   -- Reference to STAGE.skills_normalized.skill_id

    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (skill_category, skill_name);
```

## Pre-Aggregated Metrics Tables

### Market Intelligence Tables

##### 1. `market_weekly_summary` (Executive Dashboard)
**Weekly market snapshots for instant reporting**

```sql
CREATE TABLE ANALYTICS.market_weekly_summary (
    summary_key STRING PRIMARY KEY,
    week_ending_date DATE,
    report_date DATE,

    -- Overall Market Metrics
    total_jobs_posted INTEGER,
    total_active_jobs INTEGER,
    new_jobs_posted INTEGER,
    net_new_postings INTEGER,

    -- Velocity Metrics
    posting_velocity_daily FLOAT,
    week_over_week_growth_rate FLOAT,
    month_over_month_growth_rate FLOAT,
    year_over_year_growth_rate FLOAT,

    -- Market Temperature (0-100 scale)
    market_temperature_score FLOAT,
    hiring_intensity_score FLOAT,
    candidate_competition_score FLOAT,

    -- Salary Intelligence
    median_salary_all_roles INTEGER,
    salary_inflation_rate FLOAT,
    salary_growth_mom FLOAT,
    salary_growth_yoy FLOAT,

    -- Skills & Technology
    top_demanded_skills VARIANT,         -- JSON array of top 20 skills
    fastest_growing_skills VARIANT,      -- JSON array with growth rates
    emerging_technologies VARIANT,       -- JSON array of new trending skills
    declining_technologies VARIANT,      -- JSON array of declining skills

    -- Company Intelligence
    most_active_hiring_companies VARIANT,  -- JSON array of top hiring companies
    fastest_growing_companies VARIANT,     -- JSON array with growth metrics
    average_company_hiring_velocity FLOAT,

    -- Geographic Intelligence
    top_hiring_locations VARIANT,        -- JSON array of top locations
    remote_work_percentage FLOAT,
    hybrid_work_percentage FLOAT,
    location_demand_shifts VARIANT,      -- JSON array of location changes

    -- Platform Intelligence
    platform_job_distribution VARIANT,   -- JSON object with platform breakdown
    platform_growth_rates VARIANT,       -- JSON object with platform growth

    -- Quality Metrics
    data_freshness_hours INTEGER,
    sample_size INTEGER,
    confidence_score FLOAT,

    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (week_ending_date);
```

##### 2. `skills_trend_analysis` (Skills Intelligence)
**Comprehensive skills market analysis**

```sql
CREATE TABLE ANALYTICS.skills_trend_analysis (
    analysis_key STRING PRIMARY KEY,
    analysis_date DATE,
    skill_key STRING,

    -- Current Demand Metrics
    jobs_requiring_skill INTEGER,
    total_jobs_analyzed INTEGER,
    market_penetration_rate FLOAT,     -- Percentage of jobs requiring this skill
    demand_rank_overall INTEGER,
    demand_rank_in_category INTEGER,

    -- Growth Metrics
    demand_growth_weekly FLOAT,
    demand_growth_monthly FLOAT,
    demand_growth_quarterly FLOAT,
    demand_growth_yearly FLOAT,

    -- Ranking Changes
    rank_change_weekly INTEGER,
    rank_change_monthly INTEGER,
    rank_change_quarterly INTEGER,

    -- Salary Impact
    average_salary_with_skill NUMBER,
    average_salary_without_skill NUMBER,
    salary_premium_amount NUMBER,
    salary_premium_percentage FLOAT,

    -- Market Characteristics
    supply_demand_ratio FLOAT,         -- Estimated supply vs demand
    competition_level STRING,          -- Low, Medium, High, Very High
    market_saturation_level STRING,    -- Undersupplied, Balanced, Oversupplied

    -- Geographic Distribution
    top_locations_for_skill VARIANT,   -- JSON array of top cities/states
    remote_availability_rate FLOAT,    -- Percentage of remote jobs with this skill

    -- Industry Distribution
    top_industries_for_skill VARIANT,  -- JSON array of industries
    industry_concentration_score FLOAT, -- How concentrated in specific industries

    -- Career Progression
    entry_level_demand INTEGER,
    mid_level_demand INTEGER,
    senior_level_demand INTEGER,
    career_progression_score FLOAT,    -- Demand across all levels

    -- Skill Ecosystem
    commonly_paired_skills VARIANT,    -- JSON array of frequently co-occurring skills
    prerequisite_skills VARIANT,       -- JSON array of typical prerequisites
    career_path_skills VARIANT,        -- JSON array of typical next skills

    -- Market Predictions (if applicable)
    demand_forecast_next_quarter FLOAT,
    growth_sustainability_score FLOAT,
    automation_risk_score FLOAT,

    -- Quality Metrics
    sample_size INTEGER,
    confidence_interval_95 VARIANT,    -- JSON object with upper/lower bounds
    data_quality_score FLOAT,

    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (analysis_date, skill_key);
```

##### 3. `company_hiring_intelligence` (Company Analysis)
**Company-specific hiring patterns and intelligence**

```sql
CREATE TABLE ANALYTICS.company_hiring_intelligence (
    intelligence_key STRING PRIMARY KEY,
    analysis_date DATE,
    company_key STRING,

    -- Hiring Velocity
    jobs_posted_last_7_days INTEGER,
    jobs_posted_last_30_days INTEGER,
    jobs_posted_last_90_days INTEGER,
    current_job_postings INTEGER,

    -- Growth Indicators
    hiring_velocity_trend STRING,      -- Accelerating, Stable, Decelerating
    headcount_growth_rate FLOAT,
    department_expansion_areas VARIANT, -- JSON array of growing departments

    -- Hiring Patterns
    seasonal_hiring_pattern STRING,    -- High Q1, Steady, etc.
    preferred_platforms VARIANT,       -- JSON array of most-used platforms

    -- Compensation Strategy
    salary_competitiveness_score FLOAT, -- Compared to market
    salary_transparency_rate FLOAT,     -- Percentage of jobs with salary
    equity_offering_rate FLOAT,         -- Percentage mentioning equity
    benefits_competitiveness_score FLOAT,

    -- Role Distribution
    ic_vs_management_ratio FLOAT,
    entry_vs_senior_ratio FLOAT,
    technical_vs_business_ratio FLOAT,
    remote_job_percentage FLOAT,

    -- Geographic Strategy
    hiring_locations VARIANT,          -- JSON array of locations
    remote_work_policy STRING,         -- Full Remote, Hybrid, On-site
    geographic_expansion_trend STRING, -- Expanding, Stable, Consolidating

    -- Skills & Technology Focus
    top_required_skills VARIANT,       -- JSON array of most-required skills
    technology_stack_profile VARIANT,  -- JSON object with tech categories
    skills_evolution_trend VARIANT,    -- JSON array of changing skill requirements

    -- Market Position
    hiring_rank_in_industry INTEGER,
    hiring_rank_by_size INTEGER,
    hiring_competitiveness_score FLOAT,

    -- Company Health Indicators
    hiring_consistency_score FLOAT,       -- Based on posting patterns and frequency
    growth_sustainability_score FLOAT,    -- Based on hiring patterns

    -- Benchmarking
    vs_industry_hiring_velocity FLOAT,    -- Multiple of industry average
    vs_size_peer_hiring_velocity FLOAT,   -- Multiple of size peer average
    vs_location_hiring_velocity FLOAT,    -- Multiple of location average

    -- Quality Metrics
    sample_size INTEGER,
    confidence_score FLOAT,
    data_completeness_score FLOAT,

    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (analysis_date, company_key);
```

## Business Views & Analytics Layer

### Executive Dashboard Views

##### 1. `view_executive_dashboard` (Weekly Market Overview)
```sql
CREATE VIEW ANALYTICS.view_executive_dashboard AS
SELECT
    DATE_TRUNC('week', f.first_posted_date) as report_week,

    -- Key Metrics
    COUNT(*) as total_job_postings,
    COUNT(DISTINCT f.company_key) as active_companies,
    COUNT(CASE WHEN f.is_new_posting THEN 1 END) as new_postings,

    -- Salary Intelligence
    AVG(f.salary_midpoint) as avg_salary_midpoint,
    MEDIAN(f.salary_midpoint) as median_salary_midpoint,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY f.salary_midpoint) as p75_salary,

    -- Market Temperature
    AVG(CASE
        WHEN f.posting_age_days <= 7 THEN 100
        WHEN f.posting_age_days <= 14 THEN 75
        WHEN f.posting_age_days <= 30 THEN 50
        ELSE 25
    END) as market_temperature_score,

    -- Remote Work Trends
    AVG(f.remote_work_score) as avg_remote_score,
    COUNT(CASE WHEN f.is_remote_eligible THEN 1 END)::FLOAT / COUNT(*) * 100 as remote_percentage,

    -- Geographic Distribution
    MODE() WITHIN GROUP (ORDER BY l.metro_area) as top_metro_area,
    COUNT(DISTINCT l.metro_area) as unique_metro_areas,

    -- Skills Intelligence
    COUNT(CASE WHEN s.skill_category = 'Programming Language' THEN 1 END) as programming_roles,
    COUNT(CASE WHEN s.skill_category = 'Cloud Platform' THEN 1 END) as cloud_roles,

    -- Growth Metrics
    LAG(COUNT(*)) OVER (ORDER BY DATE_TRUNC('week', f.first_posted_date)) as prev_week_postings,
    (COUNT(*)::FLOAT / LAG(COUNT(*)) OVER (ORDER BY DATE_TRUNC('week', f.first_posted_date)) - 1) * 100 as wow_growth_rate

FROM ANALYTICS.fact_job_postings f
JOIN ANALYTICS.dim_date d ON f.date_key = d.date_key
JOIN ANALYTICS.dim_company c ON f.company_key = c.company_key
JOIN ANALYTICS.dim_location l ON f.location_key = l.location_key
LEFT JOIN STAGE.job_skills_bridge jsb ON f.job_uid = jsb.job_uid
LEFT JOIN ANALYTICS.dim_skills s ON jsb.skill_id = s.source_stage_skill_id

WHERE d.full_date >= CURRENT_DATE - 90
  AND f.is_active_posting = TRUE

GROUP BY DATE_TRUNC('week', f.first_posted_date)
ORDER BY report_week DESC;
```

##### 2. `view_salary_intelligence` (Compensation Analysis)
```sql
CREATE VIEW ANALYTICS.view_salary_intelligence AS
SELECT
    jf.job_family,
    jf.seniority_level,
    l.metro_area,
    DATE_TRUNC('month', f.first_posted_date) as salary_month,

    -- Statistical Measures
    COUNT(*) as sample_size,
    AVG(f.salary_midpoint) as mean_salary,
    MEDIAN(f.salary_midpoint) as median_salary,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY f.salary_midpoint) as p25_salary,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY f.salary_midpoint) as p75_salary,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY f.salary_midpoint) as p90_salary,

    -- Growth Rates
    LAG(AVG(f.salary_midpoint)) OVER (
        PARTITION BY jf.job_family, jf.seniority_level, l.metro_area
        ORDER BY DATE_TRUNC('month', f.first_posted_date)
    ) as prev_month_avg_salary,

    ((AVG(f.salary_midpoint) / LAG(AVG(f.salary_midpoint)) OVER (
        PARTITION BY jf.job_family, jf.seniority_level, l.metro_area
        ORDER BY DATE_TRUNC('month', f.first_posted_date)
    )) - 1) * 100 as mom_salary_growth,

    -- Market Context
    AVG(f.salary_midpoint) - LAG(AVG(f.salary_midpoint), 12) OVER (
        PARTITION BY jf.job_family, jf.seniority_level, l.metro_area
        ORDER BY DATE_TRUNC('month', f.first_posted_date)
    ) as yoy_salary_change,

    -- Cost of Living Adjustment
    AVG(f.salary_midpoint) / l.cost_of_living_index as cola_adjusted_salary,

    -- Quality Indicators
    AVG(f.salary_confidence_score) as avg_confidence_score,
    COUNT(CASE WHEN f.is_salary_disclosed THEN 1 END)::FLOAT / COUNT(*) as disclosure_rate

FROM ANALYTICS.fact_job_postings f
JOIN ANALYTICS.dim_job_family jf ON f.job_family_key = jf.job_family_key
JOIN ANALYTICS.dim_location l ON f.location_key = l.location_key
JOIN ANALYTICS.dim_date d ON f.date_key = d.date_key

WHERE f.salary_midpoint IS NOT NULL
  AND f.salary_midpoint BETWEEN 30000 AND 500000  -- Reasonable bounds
  AND f.is_active_posting = TRUE
  AND d.full_date >= CURRENT_DATE - 365

GROUP BY jf.job_family, jf.seniority_level, l.metro_area, DATE_TRUNC('month', f.first_posted_date)
HAVING COUNT(*) >= 5  -- Minimum sample size for statistical validity
ORDER BY jf.job_family, jf.seniority_level, l.metro_area, salary_month DESC;
```

##### 3. `view_skills_market_intelligence` (Technology Trends)
```sql
CREATE VIEW ANALYTICS.view_skills_market_intelligence AS
WITH skill_demand_trends AS (
    SELECT
        s.skill_name,
        s.skill_category,
        DATE_TRUNC('week', f.first_posted_date) as demand_week,
        COUNT(DISTINCT f.job_posting_key) as jobs_requiring_skill,
        COUNT(DISTINCT f.job_posting_key) / (
            SELECT COUNT(DISTINCT job_posting_key)
            FROM ANALYTICS.fact_job_postings
            WHERE DATE_TRUNC('week', first_posted_date) = DATE_TRUNC('week', f.first_posted_date)
        )::FLOAT * 100 as market_penetration_rate,

        AVG(f.salary_midpoint) as avg_salary_with_skill,

        -- Calculate salary premium vs. market average
        AVG(f.salary_midpoint) - (
            SELECT AVG(salary_midpoint)
            FROM ANALYTICS.fact_job_postings
            WHERE DATE_TRUNC('week', first_posted_date) = DATE_TRUNC('week', f.first_posted_date)
              AND salary_midpoint IS NOT NULL
        ) as salary_premium

    FROM ANALYTICS.fact_job_postings f
JOIN STAGE.job_skills_bridge jsb ON f.job_uid = jsb.job_uid
JOIN ANALYTICS.dim_skills s ON jsb.skill_id = s.source_stage_skill_id
JOIN ANALYTICS.dim_date d ON f.date_key = d.date_key

    WHERE d.full_date >= CURRENT_DATE - 90
      AND f.is_active_posting = TRUE
      AND s.skill_type = 'Technical'
      AND f.salary_midpoint IS NOT NULL

    GROUP BY s.skill_name, s.skill_category, DATE_TRUNC('week', f.first_posted_date)
    HAVING COUNT(DISTINCT f.job_posting_key) >= 10  -- Minimum threshold
)

SELECT
    skill_name,
    skill_category,
    demand_week,
    jobs_requiring_skill,
    market_penetration_rate,

    -- Trend Analysis
    LAG(jobs_requiring_skill, 1) OVER (PARTITION BY skill_name ORDER BY demand_week) as prev_week_demand,
    LAG(jobs_requiring_skill, 4) OVER (PARTITION BY skill_name ORDER BY demand_week) as four_weeks_ago_demand,

    -- Growth Calculations
    ((jobs_requiring_skill::FLOAT / LAG(jobs_requiring_skill, 1) OVER (PARTITION BY skill_name ORDER BY demand_week)) - 1) * 100 as wow_growth_rate,
    ((jobs_requiring_skill::FLOAT / LAG(jobs_requiring_skill, 4) OVER (PARTITION BY skill_name ORDER BY demand_week)) - 1) * 100 as four_week_growth_rate,

    -- Salary Intelligence
    avg_salary_with_skill,
    salary_premium,
    CASE
        WHEN salary_premium > 10000 THEN 'High Premium'
        WHEN salary_premium > 5000 THEN 'Medium Premium'
        WHEN salary_premium > 0 THEN 'Low Premium'
        ELSE 'No Premium'
    END as premium_category,

    -- Market Classification
    CASE
        WHEN market_penetration_rate > 50 THEN 'Mainstream'
        WHEN market_penetration_rate > 20 THEN 'Popular'
        WHEN market_penetration_rate > 5 THEN 'Niche'
        ELSE 'Specialized'
    END as market_adoption_level,

    -- Trend Classification
    CASE
        WHEN ((jobs_requiring_skill::FLOAT / LAG(jobs_requiring_skill, 4) OVER (PARTITION BY skill_name ORDER BY demand_week)) - 1) * 100 > 25 THEN 'Hot'
        WHEN ((jobs_requiring_skill::FLOAT / LAG(jobs_requiring_skill, 4) OVER (PARTITION BY skill_name ORDER BY demand_week)) - 1) * 100 > 10 THEN 'Growing'
        WHEN ((jobs_requiring_skill::FLOAT / LAG(jobs_requiring_skill, 4) OVER (PARTITION BY skill_name ORDER BY demand_week)) - 1) * 100 > -10 THEN 'Stable'
        ELSE 'Declining'
    END as trend_category

FROM skill_demand_trends
ORDER BY demand_week DESC, jobs_requiring_skill DESC;
```

## Implementation Phases

**Prerequisites**: STAGE layer LLM data normalization must be completed before beginning ANALYTICS implementation. This includes `STAGE.skills_normalized`, `STAGE.job_skills_bridge`, and `STAGE.keywords_normalized` tables.

### Phase 1: Foundation & Core Dimensional Model
**Objective**: Establish the core star schema structure and essential dimension tables

**Components**:
- Create ANALYTICS schema and core dimension tables
- Implement `dim_date` with business calendar
- Build `dim_company` with Type 2 SCD logic
- Create `dim_location` with geographic hierarchy
- Develop `dim_job_family` with role taxonomy
- Build `dim_platform` for ATS classification
- Create `dim_skills` with skills taxonomy

**Key Deliverables**:
- Fully populated dimension tables with proper hierarchies
- Surrogate key generation and management
- Data quality validation for all dimensions
- Foreign key relationships and referential integrity

### Phase 2: Primary Fact Table Implementation
**Objective**: Create the main `fact_job_postings` table with complete measure library

**Components**:
- Design and implement `fact_job_postings` grain and measures
- Build ETL pipeline from STAGE to ANALYTICS layer
- Implement incremental loading and SCD processing
- Create data quality monitoring and validation
- Establish partitioning and clustering strategy

**Key Deliverables**:
- Production-ready `fact_job_postings` table
- Automated daily refresh processes
- Data lineage tracking and audit capabilities
- Performance-optimized table structure

### Phase 3: Specialized Fact Tables
**Objective**: Build domain-specific fact tables for advanced analytics

**Components**:
- Implement `fact_skills_demand` for technology trend analysis
- Create `fact_salary_benchmarks` for compensation intelligence
- Build `fact_company_hiring_velocity` for company analysis
- Develop aggregation and rollup strategies

**Key Deliverables**:
- Specialized fact tables with appropriate grain and measures
- Cross-fact table consistency and alignment
- Advanced analytics capabilities for each domain

### Phase 4: Pre-Aggregated Metrics & KPIs
**Objective**: Create business-ready metric tables for fast reporting

**Components**:
- Build `market_weekly_summary` for executive dashboards
- Create `skills_trend_analysis` for technology intelligence
- Implement `company_hiring_intelligence` for company analysis
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