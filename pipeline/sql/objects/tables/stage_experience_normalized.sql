CREATE TABLE IF NOT EXISTS BETTERJOBS_DB.STAGE.EXPERIENCE_NORMALIZED (
    EXPERIENCE_ID STRING PRIMARY KEY,
    EXPERIENCE_NAME STRING NOT NULL,          -- 'Entry Level', 'Mid Level', 'Senior Level', 'Executive Level'
    EXPERIENCE_CATEGORY STRING NOT NULL,      -- 'general', 'technology_specific', 'role_seniority'
    MIN_YEARS_REQUIRED INTEGER,              -- Minimum years for this level
    MAX_YEARS_REQUIRED INTEGER,              -- Maximum years for this level
    SENIORITY_ORDER INTEGER,                 -- 1=Entry, 2=Mid, 3=Senior, 4=Staff, 5=Principal, 6=Director, 7=VP, 8=Executive
    EXPERIENCE_DESCRIPTION STRING,           -- Human-readable description
    MARKET_FREQUENCY INTEGER DEFAULT 0,      -- How often this requirement appears
    CONFIDENCE_SCORE FLOAT DEFAULT 1.0,     -- Confidence in standardization
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (EXPERIENCE_CATEGORY, SENIORITY_ORDER);