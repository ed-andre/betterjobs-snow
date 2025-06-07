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

-- Create the unified jobs table (Phase 1 - Basic Transformation with UID)
CREATE TABLE IF NOT EXISTS jobs_unified (
    -- Generated unique identifier (replaces composite primary key)
    -- Increased from 16 to 32 characters to prevent hash collisions
    job_uid STRING(32) PRIMARY KEY,

    -- Core identifiers
    job_id STRING,
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

    -- Platform-specific data (preserved as JSON)
    platform_specific_data VARIANT,

    -- Quality and metadata
    transformation_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    data_quality_score FLOAT,

    -- Source tracking
    source_raw_table STRING,  -- Original table name for lineage
    raw_data VARIANT,  -- Compressed original raw data for reference

    -- Partitioning
    partition_date DATE
);
```

### 2.1 Future Schema Extensions (Phase 2 - LLM Enrichments)

When Phase 2 LLM processing is implemented, the following columns will be added:

```sql
-- Add LLM extraction columns in Phase 2
ALTER TABLE jobs_unified ADD COLUMN salary_min NUMBER;
ALTER TABLE jobs_unified ADD COLUMN salary_max NUMBER;
ALTER TABLE jobs_unified ADD COLUMN salary_currency STRING DEFAULT 'USD';
ALTER TABLE jobs_unified ADD COLUMN salary_period STRING;  -- hourly, annually, monthly
ALTER TABLE jobs_unified ADD COLUMN salary_confidence FLOAT;

-- Experience and requirements (from LLM)
ALTER TABLE jobs_unified ADD COLUMN min_years_experience NUMBER;
ALTER TABLE jobs_unified ADD COLUMN max_years_experience NUMBER;
ALTER TABLE jobs_unified ADD COLUMN experience_level STRING;  -- Entry, Mid, Senior, Executive
ALTER TABLE jobs_unified ADD COLUMN education_requirements VARIANT;  -- Array of requirements

-- Technical skills (from LLM, stored as JSON arrays)
ALTER TABLE jobs_unified ADD COLUMN programming_languages VARIANT;
ALTER TABLE jobs_unified ADD COLUMN databases VARIANT;
ALTER TABLE jobs_unified ADD COLUMN cloud_platforms VARIANT;
ALTER TABLE jobs_unified ADD COLUMN frameworks VARIANT;
ALTER TABLE jobs_unified ADD COLUMN tools VARIANT;
ALTER TABLE jobs_unified ADD COLUMN soft_skills VARIANT;

-- Work arrangement (from LLM)
ALTER TABLE jobs_unified ADD COLUMN work_type STRING;  -- Remote, Hybrid, On-site
ALTER TABLE jobs_unified ADD COLUMN remote_flexibility STRING;
ALTER TABLE jobs_unified ADD COLUMN travel_requirements STRING;
ALTER TABLE jobs_unified ADD COLUMN office_locations VARIANT;

-- Keywords and classification (from LLM)
ALTER TABLE jobs_unified ADD COLUMN primary_keywords VARIANT;
ALTER TABLE jobs_unified ADD COLUMN industry_keywords VARIANT;
ALTER TABLE jobs_unified ADD COLUMN role_type_keywords VARIANT;
ALTER TABLE jobs_unified ADD COLUMN company_stage_keywords VARIANT;
ALTER TABLE jobs_unified ADD COLUMN job_family STRING;
ALTER TABLE jobs_unified ADD COLUMN job_sub_family STRING;

-- Additional quality metrics
ALTER TABLE jobs_unified ADD COLUMN extraction_confidence_avg FLOAT;
ALTER TABLE jobs_unified ADD COLUMN keyword_quality_score FLOAT;
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

### Key Changes from Previous Schema:

1. **Primary Key Change**: `job_uid STRING PRIMARY KEY` replaces the composite key approach
2. **UID Generation**: Deterministic UIDs generated from `job_id + platform + company_id + date_posted`
3. **Simpler Uniqueness**: Single field uniqueness instead of complex composite key logic
4. **Performance**: Single indexed primary key for faster lookups and joins

### UID Benefits:

- **True Uniqueness**: Single field for unique identification
- **Deterministic**: Same job always gets same UID across processing runs
- **Performance**: Single indexed field vs composite key lookups
- **Future-proof**: Schema changes don't affect uniqueness logic
- **Simplified Analytics**: Easier downstream joins and references

### UID Implementation Details:

**Generation Algorithm**:
```python
# Deterministic UID generation (Updated to prevent collisions)
composite_key = f"JOB_ID:{job_id}|PLATFORM:{platform}|COMPANY:{company_id}|DATE:{date_posted}"
job_uid = hashlib.sha256(composite_key.encode()).hexdigest()[:32]  # 32 chars instead of 16
```

**Example UIDs**:
- Input: `("12345", "workday", "company_1", "2024-01-15")`
- Output: `"a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6"` (32-character hex string)

**Collision Prevention**:
- **Hash Length**: Increased from 16 to 32 characters (64-bit to 128-bit hash space)
- **Collision Probability**: Reduced from ~2^64 to ~2^128 possible values
- **Explicit Labels**: Added field labels in composite key to prevent ambiguity
- **Null Handling**: Explicit "NULL_*" values instead of empty strings

### UID Migration Considerations:

- Existing data will be automatically migrated when the asset runs
- UIDs are generated during data processing, not as database defaults
- All downstream references should use `job_uid` instead of composite keys