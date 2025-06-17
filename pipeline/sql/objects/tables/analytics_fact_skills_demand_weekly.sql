CREATE TABLE ANALYTICS.FACT_SKILLS_DEMAND_WEEKLY (
    SKILLS_WEEKLY_KEY STRING PRIMARY KEY,
    WEEK_KEY STRING,                        -- YYYY-WW format
    SKILL_KEY STRING,
    JOB_FAMILY_KEY STRING,
    LOCATION_KEY STRING,

    -- Core Demand Metrics
    ACTIVE_JOBS_WITH_SKILL INTEGER,         -- Active jobs requiring this skill
    TOTAL_ACTIVE_JOBS INTEGER,              -- Total active jobs in category
    SKILL_PENETRATION_RATE FLOAT,           -- Percentage requiring this skill

    -- Salary Analysis
    AVG_SALARY_WITH_SKILL NUMBER,           -- Average salary for jobs with this skill
    SALARY_PREMIUM_PERCENTAGE FLOAT,        -- Premium this skill commands

    -- Trend Analysis
    WEEK_OVER_WEEK_CHANGE FLOAT,            -- Change in job count
    TREND_DIRECTION STRING,                 -- 'GROWING', 'STABLE', 'DECLINING'

    -- Market Position
    SKILL_RANK_IN_FAMILY INTEGER,           -- Rank within job family
    SKILL_RANK_OVERALL INTEGER,             -- Overall market rank

    -- Audit Fields
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    WEEK_START_DATE DATE                    -- First day of week for partitioning
) PARTITION BY (WEEK_START_DATE)
CLUSTER BY (WEEK_KEY, SKILL_KEY, JOB_FAMILY_KEY);