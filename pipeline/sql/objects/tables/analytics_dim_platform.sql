CREATE TABLE ANALYTICS.dim_platform (
    platform_key STRING PRIMARY KEY,
    platform_name STRING,              -- From STAGE.JOBS_UNIFIED.PLATFORM
    platform_code STRING,              -- workday, greenhouse, bamboohr, etc.

    -- Derived Platform Characteristics (from actual job data)
    supports_salary_disclosure BOOLEAN, -- Calculated from salary disclosure rates
    data_richness_score FLOAT,         -- Calculated from job description quality metrics
    job_volume_category STRING,        -- High, Medium, Low (derived from actual job counts)

    is_active BOOLEAN,                  -- Currently processing jobs from this platform
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (platform_name);