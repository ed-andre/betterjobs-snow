CREATE TABLE IF NOT EXISTS SERVE.DENORM_SKILLS (
    SKILL_CATEGORY STRING NOT NULL,
    SKILL_SUBCATEGORY STRING NOT NULL,
    SKILLS_CSV STRING NOT NULL,      -- Comma-separated list of skills under the subcategory
    SKILL_COUNT INTEGER,             -- Number of individual skills in list (optional)
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (SKILL_CATEGORY, SKILL_SUBCATEGORY)
) COMMENT = 'Denormalised skills lookup table for UI autocomplete/search, generated  from DIM_SKILLS.';