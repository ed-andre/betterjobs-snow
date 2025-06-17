CREATE TABLE ANALYTICS.dim_skills (
    skill_key STRING PRIMARY KEY,
    skill_id STRING,               -- FK to STAGE.SKILLS_NORMALIZED.SKILL_ID
    skill_name STRING,

    -- Skill Hierarchy (from STAGE.SKILLS_NORMALIZED)
    skill_category STRING,          -- From STAGE.SKILLS_NORMALIZED.SKILL_CATEGORY
    skill_subcategory STRING,      -- From STAGE.SKILLS_NORMALIZED.SKILL_SUBCATEGORY

    -- STAGE Data Fields
    canonical_form STRING,          -- From STAGE.SKILLS_NORMALIZED.CANONICAL_FORM
    common_aliases VARIANT,         -- From STAGE.SKILLS_NORMALIZED.COMMON_ALIASES
    original_variants VARIANT,      -- From STAGE.SKILLS_NORMALIZED.ORIGINAL_VARIANTS
    stage_confidence_score FLOAT,   -- From STAGE.SKILLS_NORMALIZED.CONFIDENCE_SCORE
    frequency_count INTEGER,        -- From STAGE.SKILLS_NORMALIZED.FREQUENCY_COUNT
    trend_direction STRING,         -- From STAGE.SKILLS_NORMALIZED.TREND_DIRECTION

    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (skill_category, skill_name);