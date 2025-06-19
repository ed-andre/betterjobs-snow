CREATE TABLE IF NOT EXISTS BETTERJOBS_DB.STAGE.EXPERIENCE_STANDARDIZATION_RULES (
    RULE_ID STRING PRIMARY KEY,
    EXPERIENCE_CATEGORY STRING NOT NULL,           -- 'general', 'role_seniority', 'experience_level_mapping', 'seniority_level_mapping', 'technology_specific', 'special'
    EXPERIENCE_NAME STRING NOT NULL,               -- 'Entry Level', 'Mid Level', 'Senior Level', etc.
    MIN_YEARS_REQUIRED INTEGER,                    -- Minimum years for this level (NULL for non-year-based)
    MAX_YEARS_REQUIRED INTEGER,                    -- Maximum years for this level (NULL for non-year-based)
    SENIORITY_ORDER INTEGER NOT NULL,              -- Ordering for analytics (1=Entry, 2=Junior, etc.)
    EXPERIENCE_DESCRIPTION STRING,                 -- Human-readable description
    SOURCE_PATTERNS VARIANT,                       -- JSON array of patterns that map to this level
    MAPPING_TYPE STRING NOT NULL,                  -- 'years_range', 'experience_level', 'seniority_level', 'technology_years', 'special'
    IS_ACTIVE BOOLEAN DEFAULT TRUE,                -- Whether this rule is currently active
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (EXPERIENCE_CATEGORY, SENIORITY_ORDER);