# STAGE Schema Setup for Silver Layer Transformations

This document provides instructions for setting up the STAGE schema and `jobs_unified` table in Snowflake for the Silver layer transformations.

## Overview

The STAGE schema serves as the Silver layer in our medallion architecture, providing:
- Unified job data from all ATS platforms (Workday, Greenhouse, BambooHR, SmartRecruiters)
- Cleaned and standardized text fields
- Language detection and filtering
- AI-powered data extraction (salary, skills, keywords)
- Data quality validation and scoring
- Platform-specific data preservation via VARIANT columns

## Prerequisites

- Snowflake environment with RAW schema already configured
- `BETTERJOBS_DB` database created
- Appropriate permissions for schema and table creation

## Schema Creation

### 1. Create STAGE Schema

```sql
-- Connect to the database
USE DATABASE BETTERJOBS_DB;

-- Create STAGE schema
CREATE SCHEMA IF NOT EXISTS STAGE;

-- Grant permissions to role (adjust role name as needed)
GRANT USAGE ON SCHEMA STAGE TO ROLE BETTERJOBS_ROLE;
GRANT CREATE TABLE ON SCHEMA STAGE TO ROLE BETTERJOBS_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA STAGE TO ROLE BETTERJOBS_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA STAGE TO ROLE BETTERJOBS_ROLE;
```

### 2. Create Main Unified Jobs Table

```sql
-- Switch to STAGE schema
USE SCHEMA STAGE;

-- Create the unified jobs table
CREATE TABLE IF NOT EXISTS jobs_unified (
    -- Core identifiers
    job_id STRING PRIMARY KEY,
    company_id STRING,
    platform STRING,  -- 'workday', 'greenhouse', 'bamboohr', 'smartrecruiters'

    -- Standardized core fields
    job_title_clean STRING,
    job_description_clean STRING,
    company_name_clean STRING,
    location_standardized STRING,
    job_url STRING,

    -- Date fields
    date_posted DATE,
    date_retrieved TIMESTAMP_NTZ,

    -- Status and classification
    is_active BOOLEAN DEFAULT TRUE,
    employment_status STRING,  -- Full-time, Part-time, Contract, Internship
    department STRING,

    -- Language detection
    detected_language STRING,
    language_confidence FLOAT,
    is_english BOOLEAN,
    language_detection_method STRING,

    -- Extracted salary info (from LLM)
    salary_min NUMBER,
    salary_max NUMBER,
    salary_currency STRING DEFAULT 'USD',
    salary_period STRING,  -- hourly, annually, monthly
    salary_confidence FLOAT,

    -- Experience and requirements (from LLM)
    min_years_experience NUMBER,
    max_years_experience NUMBER,
    experience_level STRING,  -- Entry, Mid, Senior, Executive
    education_requirements VARIANT,  -- Array of requirements

    -- Technical skills (from LLM, stored as JSON arrays)
    programming_languages VARIANT,
    databases VARIANT,
    cloud_platforms VARIANT,
    frameworks VARIANT,
    tools VARIANT,
    soft_skills VARIANT,

    -- Work arrangement (from LLM)
    work_type STRING,  -- Remote, Hybrid, On-site
    remote_flexibility STRING,
    travel_requirements STRING,
    office_locations VARIANT,

    -- Keywords and classification (from LLM)
    primary_keywords VARIANT,
    industry_keywords VARIANT,
    role_type_keywords VARIANT,
    company_stage_keywords VARIANT,
    job_family STRING,
    job_sub_family STRING,

    -- Platform-specific data (flexible VARIANT column)
    platform_specific_data VARIANT,

    -- Quality and metadata
    transformation_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    data_quality_score FLOAT,
    extraction_confidence_avg FLOAT,
    keyword_quality_score FLOAT,

    -- Source tracking
    source_raw_table STRING,  -- Original table name for lineage
    raw_data VARIANT,  -- Compressed original raw data for reference

    -- Partitioning and clustering
    partition_date DATE
);
```

### 3. Add Clustering and Indexes

```sql
-- Add clustering for optimal query performance
ALTER TABLE jobs_unified CLUSTER BY (platform, date_posted);

-- Add search optimization for text fields (optional, for production)
-- ALTER TABLE jobs_unified ADD SEARCH OPTIMIZATION;
```

### 4. Create Supporting Tables

```sql
-- Company profiles enrichment table
CREATE TABLE IF NOT EXISTS company_profiles (
    company_id STRING PRIMARY KEY,
    company_name_standardized STRING,
    company_industry_standardized STRING,
    company_size_category STRING,
    employee_count_range STRING,
    funding_stage STRING,
    headquarters_location STRING,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    updated_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- Location mapping table
CREATE TABLE IF NOT EXISTS location_mapping (
    location_raw STRING,
    location_standardized STRING,
    city STRING,
    state STRING,
    country STRING,
    metro_area STRING,
    cost_of_living_index FLOAT,
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- Skills taxonomy table
CREATE TABLE IF NOT EXISTS skills_taxonomy (
    skill_name STRING PRIMARY KEY,
    skill_category STRING,  -- programming_language, database, cloud_platform, framework, tool
    skill_aliases VARIANT,  -- Array of alternative names
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- Transformation logs for audit trail
CREATE TABLE IF NOT EXISTS transformation_logs (
    log_id STRING PRIMARY KEY,
    job_id STRING,
    transformation_step STRING,
    status STRING,  -- success, failed, skipped
    error_message STRING,
    processing_time_seconds FLOAT,
    timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);
```

### 5. Create Views for Easy Querying

```sql
-- View for English jobs only
CREATE OR REPLACE VIEW jobs_english AS
SELECT * FROM jobs_unified
WHERE is_english = TRUE;

-- View for recent jobs (last 30 days)
CREATE OR REPLACE VIEW jobs_recent AS
SELECT * FROM jobs_unified
WHERE date_posted >= CURRENT_DATE - 30
AND is_active = TRUE;

-- View for jobs with salary information
CREATE OR REPLACE VIEW jobs_with_salary AS
SELECT * FROM jobs_unified
WHERE salary_min IS NOT NULL OR salary_max IS NOT NULL;

-- Platform summary view
CREATE OR REPLACE VIEW platform_summary AS
SELECT
    platform,
    COUNT(*) as total_jobs,
    COUNT(CASE WHEN is_english = TRUE THEN 1 END) as english_jobs,
    COUNT(CASE WHEN salary_min IS NOT NULL THEN 1 END) as jobs_with_salary,
    AVG(data_quality_score) as avg_quality_score,
    MIN(date_posted) as earliest_job,
    MAX(date_posted) as latest_job
FROM jobs_unified
GROUP BY platform;
```

## Environment Variables

Add the STAGE schema environment variable to your configuration:

```bash
# Add to your .env file
export SNOWFLAKE_STAGE_SCHEMA="STAGE"
```

## Verification

After setup, verify the schema and tables were created correctly:

```sql
-- Check schema exists
SHOW SCHEMAS IN DATABASE BETTERJOBS_DB;

-- Check tables in STAGE schema
USE SCHEMA STAGE;
SHOW TABLES;

-- Verify table structure
DESCRIBE TABLE jobs_unified;

-- Check views
SHOW VIEWS;
```

## Next Steps

After completing the schema setup:

1. **Create Dagster Assets**: Set up the STAGE layer transformation assets
2. **Implement Data Utils**: Create utility functions for data cleaning and standardization
3. **Language Detection**: Implement language filtering functionality
4. **LLM Integration**: Add Gemini-powered data extraction
5. **Quality Validation**: Implement validation and monitoring

## Troubleshooting

### Common Issues

**Permission Errors:**
```sql
-- Grant additional permissions if needed
GRANT ALL PRIVILEGES ON SCHEMA STAGE TO ROLE BETTERJOBS_ROLE;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA STAGE TO ROLE BETTERJOBS_ROLE;
```

**Schema Already Exists:**
- The `CREATE SCHEMA IF NOT EXISTS` statement is safe to run multiple times
- If you need to recreate tables, use `DROP TABLE IF EXISTS` first

**VARIANT Column Issues:**
- VARIANT columns store JSON data natively in Snowflake
- Access nested data using dot notation: `column_name:key`
- For arrays, use `ARRAY_SIZE()` and `FLATTEN()` functions

## Performance Considerations

- **Clustering**: The table is clustered by `platform` and `date_posted` for optimal query performance
- **Partitioning**: Use `partition_date` for time-based queries
- **VARIANT Indexing**: Snowflake automatically indexes VARIANT columns
- **Search Optimization**: Consider enabling for text search in production