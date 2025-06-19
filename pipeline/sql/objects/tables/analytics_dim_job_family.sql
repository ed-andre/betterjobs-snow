CREATE TABLE ANALYTICS.DIM_JOB_FAMILY (
    JOB_FAMILY_KEY STRING PRIMARY KEY,

    -- Job Hierarchy
    job_family STRING,               -- Engineering, Data, Product, Sales, etc.
    JOB_SUB_FAMILY STRING,          -- Backend Engineering, Data Science, etc.
    JOB_SPECIALTY STRING,           -- Python Developer, ML Engineer, etc.

    -- Seniority Classification
    SENIORITY_LEVEL STRING,         -- Entry, Mid, Senior, Staff, Principal, Executive
    SENIORITY_ORDER INTEGER,        -- 1-7 for ordering
    EXPERIENCE_MIN_YEARS INTEGER,
    EXPERIENCE_MAX_YEARS INTEGER,

    -- Role Type
    ROLE_TYPE STRING,               -- Individual Contributor, Manager, Director, VP
    MANAGEMENT_LEVEL INTEGER,       -- 0=IC, 1=Manager, 2=Director, 3=VP, 4=C-Level
    IS_MANAGEMENT_ROLE BOOLEAN,

    -- Department & Function
    DEPARTMENT STRING,              -- Engineering, Sales, Marketing, etc.
    BUSINESS_FUNCTION STRING,       -- Core Product, Growth, Support, etc.

    -- Job Characteristics
    TYPICAL_TEAM_SIZE_MIN INTEGER,
    TYPICAL_TEAM_SIZE_MAX INTEGER,
    REQUIRES_SECURITY_CLEARANCE BOOLEAN,
    TRAVEL_REQUIREMENT_LEVEL STRING, -- None, Low, Medium, High

    -- Market Data
    MARKET_DEMAND_LEVEL STRING,     -- Very High, High, Medium, Low
    SALARY_GROWTH_TREND STRING,     -- Growing, Stable, Declining
    AUTOMATION_RISK_LEVEL STRING,   -- Low, Medium, High

    -- Skills Context
    PRIMARY_SKILL_CATEGORY STRING,  -- Technical, Creative, Sales, etc.
    REQUIRES_CODING BOOLEAN,
    REQUIRES_CERTIFICATION BOOLEAN,

    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (JOB_FAMILY, SENIORITY_LEVEL);