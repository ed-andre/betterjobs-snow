CREATE TABLE ANALYTICS.fact_company_hiring_weekly (
    company_hiring_key STRING PRIMARY KEY,
    week_key STRING,                        -- YYYY-WW format
    company_key STRING,

    -- Core Hiring Metrics
    jobs_posted_count INTEGER,              -- Total jobs posted this week
    active_jobs_count INTEGER,              -- Jobs still active at week end
    new_jobs_this_week INTEGER,             -- Net new postings

    -- Hiring Velocity & Trends
    week_over_week_change FLOAT,            -- Change in hiring volume
    hiring_trend_direction STRING,          -- 'Accelerating', 'Stable', 'Declining'

    -- Job Portfolio Analysis (from STAGE.JOBS_LLM_ENRICHED)
    avg_salary_offered NUMBER,              -- Average across all roles
    median_salary_offered NUMBER,           -- Median salary
    salary_range_width FLOAT,               -- Max - Min salary span

    -- Work Arrangement Patterns (from STAGE.JOBS_LLM_ENRICHED)
    remote_jobs_percentage FLOAT,           -- % of remote-eligible jobs
    hybrid_jobs_percentage FLOAT,           -- % of hybrid jobs
    onsite_jobs_percentage FLOAT,           -- % of on-site only jobs

    -- Role Distribution (from STAGE.JOBS_LLM_ENRICHED)
    entry_level_percentage FLOAT,           -- % of entry-level roles
    senior_level_percentage FLOAT,          -- % of senior+ roles
    management_roles_percentage FLOAT,      -- % of management positions

    -- Audit Fields
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    week_start_date DATE                    -- First day of week for partitioning
) PARTITION BY (week_start_date)
CLUSTER BY (week_key, company_key);