CREATE TABLE ANALYTICS.dim_job_family (
    job_family_key STRING PRIMARY KEY,

    -- Job Hierarchy
    job_family STRING,               -- Engineering, Data, Product, Sales, etc.
    job_sub_family STRING,          -- Backend Engineering, Data Science, etc.
    job_specialty STRING,           -- Python Developer, ML Engineer, etc.

    -- Seniority Classification
    seniority_level STRING,         -- Entry, Mid, Senior, Staff, Principal, Executive
    seniority_order INTEGER,        -- 1-7 for ordering
    experience_min_years INTEGER,
    experience_max_years INTEGER,

    -- Role Type
    role_type STRING,               -- Individual Contributor, Manager, Director, VP
    management_level INTEGER,       -- 0=IC, 1=Manager, 2=Director, 3=VP, 4=C-Level
    is_management_role BOOLEAN,

    -- Department & Function
    department STRING,              -- Engineering, Sales, Marketing, etc.
    business_function STRING,       -- Core Product, Growth, Support, etc.

    -- Job Characteristics
    typical_team_size_min INTEGER,
    typical_team_size_max INTEGER,
    requires_security_clearance BOOLEAN,
    travel_requirement_level STRING, -- None, Low, Medium, High

    -- Market Data
    market_demand_level STRING,     -- Very High, High, Medium, Low
    salary_growth_trend STRING,     -- Growing, Stable, Declining
    automation_risk_level STRING,   -- Low, Medium, High

    -- Skills Context
    primary_skill_category STRING,  -- Technical, Creative, Sales, etc.
    requires_coding BOOLEAN,
    requires_certification BOOLEAN,

    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (job_family, seniority_level);