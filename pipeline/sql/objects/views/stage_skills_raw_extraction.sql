create or replace view BETTERJOBS_DB.STAGE.SKILLS_RAW_EXTRACTION(
	JOB_UID,
	SKILL_SOURCE,
	SKILL_CATEGORY,
	SKILL_NAME_RAW,
	SKILL_NAME_ORIGINAL
) as
        WITH         TECHNICAL_SKILLS_EXPLODED AS (
            -- Extract technical skills by category
            SELECT
                jle.JOB_UID,
                'technical_skills' as SKILL_SOURCE,
                SKILL_CATEGORY.KEY::STRING as SKILL_CATEGORY,
                TRIM(LOWER(SKILL_NAME.VALUE::STRING)) as SKILL_NAME_RAW,
                SKILL_NAME.VALUE::STRING as SKILL_NAME_ORIGINAL
            FROM BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED jle,
            LATERAL FLATTEN(input => jle.TECHNICAL_SKILLS) SKILL_CATEGORY,
            LATERAL FLATTEN(input => SKILL_CATEGORY.VALUE) SKILL_NAME
            WHERE jle.TECHNICAL_SKILLS IS NOT NULL
              AND SKILL_CATEGORY.VALUE IS NOT NULL
              AND IS_ARRAY(SKILL_CATEGORY.VALUE)
              AND ARRAY_SIZE(SKILL_CATEGORY.VALUE) > 0
              AND LENGTH(TRIM(SKILL_NAME.VALUE::STRING)) > 1
              AND LOWER(TRIM(SKILL_NAME.VALUE::STRING)) NOT IN ('null', 'none', 'n/a', '')
        ),

        SOFT_SKILLS_EXPLODED AS (
            -- Extract soft skills
            SELECT
                jle.JOB_UID,
                'soft_skills' as SKILL_SOURCE,
                'soft' as SKILL_CATEGORY,
                TRIM(LOWER(SKILL.VALUE::STRING)) as SKILL_NAME_RAW,
                SKILL.VALUE::STRING as SKILL_NAME_ORIGINAL
            FROM BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED jle,
            LATERAL FLATTEN(input => jle.SOFT_SKILLS) SKILL
            WHERE jle.SOFT_SKILLS IS NOT NULL
              AND SKILL.VALUE IS NOT NULL
              AND LENGTH(TRIM(SKILL.VALUE::STRING)) > 1
              AND LOWER(TRIM(SKILL.VALUE::STRING)) NOT IN ('null', 'none', 'n/a', '')
        ),

        PRIMARY_KEYWORDS_EXPLODED AS (
            -- Extract primary keywords as skills
            SELECT
                jle.JOB_UID,
                'primary_keywords' as SKILL_SOURCE,
                'keyword' as SKILL_CATEGORY,
                TRIM(LOWER(KEYWORD.VALUE::STRING)) as SKILL_NAME_RAW,
                KEYWORD.VALUE::STRING as SKILL_NAME_ORIGINAL
            FROM BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED jle,
            LATERAL FLATTEN(input => jle.PRIMARY_KEYWORDS) KEYWORD
            WHERE jle.PRIMARY_KEYWORDS IS NOT NULL
              AND KEYWORD.VALUE IS NOT NULL
              AND LENGTH(TRIM(KEYWORD.VALUE::STRING)) > 1
              AND LOWER(TRIM(KEYWORD.VALUE::STRING)) NOT IN ('null', 'none', 'n/a', '')
        )

        SELECT * FROM TECHNICAL_SKILLS_EXPLODED
        UNION ALL
        SELECT * FROM SOFT_SKILLS_EXPLODED
        UNION ALL
        SELECT * FROM PRIMARY_KEYWORDS_EXPLODED;