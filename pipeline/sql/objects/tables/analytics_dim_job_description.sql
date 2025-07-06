CREATE TABLE ANALYTICS.DIM_JOB_DESCRIPTION (
    JOB_DESCRIPTION_KEY STRING PRIMARY KEY,  -- Surrogate key
    JOB_UID STRING UNIQUE,                   -- Natural key from STAGE.JOBS_UNIFIED.JOB_UID

    -- Description Fields
    DESCRIPTION_CLEAN VARCHAR(16777216),     -- Cleaned job description text
    LANGUAGE STRING,                         -- Detected language of the description
    TOKENS_COUNT INTEGER,                    -- Approximate word/token count

    -- Audit Fields
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (JOB_UID);