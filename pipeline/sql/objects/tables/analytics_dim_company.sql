CREATE TABLE ANALYTICS.dim_company (
    company_key STRING PRIMARY KEY,
    company_id STRING,              -- Natural key from STAGE.JOBS_UNIFIED.COMPANY_ID

    -- Company Identity (from STAGE.COMPANY_PROFILES and STAGE.JOBS_UNIFIED)
    company_name STRING,            -- From STAGE.JOBS_UNIFIED.COMPANY_NAME_CLEAN
    company_name_standardized STRING, -- From STAGE.COMPANY_PROFILES.COMPANY_NAME_STANDARDIZED

    -- Company Classification (from STAGE.COMPANY_PROFILES)
    industry STRING,                -- From STAGE.COMPANY_PROFILES.COMPANY_INDUSTRY_STANDARDIZED

    -- Company Size (from STAGE.COMPANY_PROFILES)
    employee_count_range STRING,    -- From STAGE.COMPANY_PROFILES.EMPLOYEE_COUNT_RANGE
    company_size_category STRING,   -- From STAGE.COMPANY_PROFILES.COMPANY_SIZE_CATEGORY

    -- Company Location (from STAGE.COMPANY_PROFILES)
    headquarters_location STRING,   -- From STAGE.COMPANY_PROFILES.HEADQUARTERS_LOCATION

    -- Company Stage & Funding (from STAGE.COMPANY_PROFILES)
    funding_stage STRING,           -- From STAGE.COMPANY_PROFILES.FUNDING_STAGE

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