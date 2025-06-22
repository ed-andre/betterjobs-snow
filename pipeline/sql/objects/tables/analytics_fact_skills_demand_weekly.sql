-- UPDATED TABLE STRUCTURE: aligned with analytics_fact_job_postings implementation
-- Enhancement-023: Updated skills demand weekly with denormalized salary fields and enhanced metrics
CREATE TABLE ANALYTICS.FACT_SKILLS_DEMAND_WEEKLY (
    -- Primary Key & Week Hierarchy
    SKILLS_WEEKLY_KEY STRING PRIMARY KEY,       -- Format: 'SW_' + WEEK_KEY + '_' + SKILL_KEY + '_' + JOB_FAMILY_KEY + '_' + LOCATION_KEY
    WEEK_KEY STRING,                            -- YYYY-WW format (e.g., '2025-25')
    SKILL_KEY STRING,                           -- FK to DIM_SKILLS
    JOB_FAMILY_KEY STRING,                      -- FK to DIM_JOB_FAMILY
    LOCATION_KEY STRING,                        -- FK to DIM_LOCATION

    -- Core Demand Metrics
    ACTIVE_JOBS_WITH_SKILL INTEGER,             -- Active jobs requiring this skill in the week
    TOTAL_ACTIVE_JOBS INTEGER,                  -- Total active jobs in same category (job family + location)
    SKILL_PENETRATION_RATE FLOAT,               -- Percentage of jobs requiring this skill (jobs_with_skill/total_jobs)

    -- Enhanced Salary Analysis (using denormalized annual USD fields from fact table)
    AVG_SALARY_MIDPOINT_ANNUAL_USD NUMBER,      -- Average salary midpoint for jobs with this skill
    BASELINE_SALARY_MIDPOINT_ANNUAL_USD NUMBER, -- Average salary midpoint for jobs WITHOUT this skill (for premium calc)
    SALARY_PREMIUM_ANNUAL_USD NUMBER,           -- Absolute premium in USD (avg_with_skill - avg_without_skill)
    SALARY_PREMIUM_PERCENTAGE FLOAT,            -- Percentage premium this skill commands
    SALARY_SAMPLE_SIZE INTEGER,                 -- Number of jobs with salary data for confidence

    -- Trend Analysis & Growth Metrics
    WEEK_OVER_WEEK_CHANGE INTEGER,              -- Change in job count from previous week
    WEEK_OVER_WEEK_GROWTH_RATE FLOAT,          -- Percentage change week-over-week
    TREND_DIRECTION STRING,                     -- 'GROWING', 'STABLE', 'DECLINING', 'NEW' (based on growth rate thresholds)
    FOUR_WEEK_MOVING_AVERAGE FLOAT,            -- 4-week moving average for trend smoothing

    -- Market Position & Competitive Analysis
    SKILL_RANK_IN_FAMILY INTEGER,              -- Rank within job family (1 = most in-demand)
    SKILL_RANK_OVERALL INTEGER,                -- Overall market rank across all skills
    MARKET_SHARE_IN_FAMILY FLOAT,              -- Share of total job family demand

    -- Data Quality & Confidence Metrics
    AVG_SKILL_CONFIDENCE FLOAT,                -- Average extraction confidence from bridge table
    DATA_COMPLETENESS_SCORE FLOAT,             -- Percentage of complete skill records
    SAMPLE_SIZE INTEGER,                       -- Total job postings analyzed for this skill

    -- Work Arrangement Analysis (new insight)
    REMOTE_JOBS_WITH_SKILL INTEGER,            -- Remote jobs requiring this skill
    REMOTE_SKILL_PERCENTAGE FLOAT,             -- Percentage of skill demand that's remote-friendly

    -- Audit & Processing Fields
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    PROCESSING_DATE DATE DEFAULT CURRENT_DATE,  -- When this week's data was calculated
    WEEK_START_DATE DATE,                       -- First day of week (Sunday) for partitioning
    WEEK_END_DATE DATE                          -- Last day of week (Saturday) for reference
)
CLUSTER BY (WEEK_KEY, SKILL_KEY, JOB_FAMILY_KEY);