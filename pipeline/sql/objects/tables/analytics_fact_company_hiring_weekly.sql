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
    HIRING_TREND_DIRECTION STRING,          -- 'Accelerating', 'Stable', 'Declining'

    -- Job Portfolio Analysis (from STAGE.JOBS_LLM_ENRICHED)
    AVG_SALARY_OFFERED NUMBER,              -- Average across all roles
    MEDIAN_SALARY_OFFERED NUMBER,           -- Median salary
    SALARY_RANGE_WIDTH FLOAT,               -- Max - Min salary span

    -- Work Arrangement Patterns (from STAGE.JOBS_LLM_ENRICHED)
    REMOTE_JOBS_PERCENTAGE FLOAT,           -- % of remote-eligible jobs
    HYBRID_JOBS_PERCENTAGE FLOAT,           -- % of hybrid jobs
    ON_SITE_JOBS_PERCENTAGE FLOAT,           -- % of on-site only jobs

    -- Role Distribution (from STAGE.JOBS_LLM_ENRICHED)
    ENTRY_LEVEL_PERCENTAGE FLOAT,           -- % of entry-level roles
    SENIOR_LEVEL_PERCENTAGE FLOAT,          -- % of senior+ roles
    MANAGEMENT_ROLES_PERCENTAGE FLOAT,      -- % of management positions

    -- Audit Fields
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    WEEK_START_DATE DATE                    -- First day of week for partitioning
) PARTITION BY (WEEK_START_DATE)
CLUSTER BY (WEEK_KEY, COMPANY_KEY);