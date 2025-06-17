CREATE TABLE ANALYTICS.fact_job_postings (
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