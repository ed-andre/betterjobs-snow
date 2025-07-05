-- UPDATED TABLE STRUCTURE: aligned with analytics_fact_job_postings implementation
-- Enhancement-023: Updated skills demand weekly with denormalized salary fields and enhanced metrics
-- Enhancement-039: Removed JOB_FAMILY and LOCATION dimensions, added SKILL_CATEGORY and SKILL_SUBCATEGORY
CREATE TABLE ANALYTICS.FACT_SKILLS_DEMAND_WEEKLY (
    -- Primary Key & Week Hierarchy
    SKILLS_WEEKLY_KEY STRING PRIMARY KEY,       -- Format: 'SW_' + WEEK_KEY + '_' + SKILL_KEY + '_' + SKILL_CATEGORY
    WEEK_KEY STRING,                            -- YYYY-WW format (e.g., '2025-25')
    SKILL_KEY STRING,                           -- FK to DIM_SKILLS
    SKILL_NAME STRING,                          -- Denormalized from DIM_SKILLS for convenience
    SKILL_CATEGORY STRING,                      -- Denormalized from DIM_SKILLS for analysis
    SKILL_SUBCATEGORY STRING,                   -- Denormalized from DIM_SKILLS for analysis
    SKILL_TYPE STRING,                          -- Skill type (technical, soft, etc.)

    -- Core Demand Metrics
    ACTIVE_JOBS_WITH_SKILL INTEGER,             -- Active jobs requiring this skill in the week
    TOTAL_ACTIVE_JOBS_FOR_WEEK INTEGER,         -- Total active jobs across all categories for the week
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
    SKILL_RANK_OVERALL INTEGER,                -- Overall market rank across all skills
    SKILL_RANK_IN_CATEGORY INTEGER,            -- Rank within skill category (1 = most in-demand)
    MARKET_SHARE_IN_CATEGORY FLOAT,            -- Share of total category demand

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
CLUSTER BY (WEEK_KEY, SKILL_KEY, SKILL_CATEGORY);