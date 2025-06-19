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
) CLUSTER BY (PLATFORM_NAME);