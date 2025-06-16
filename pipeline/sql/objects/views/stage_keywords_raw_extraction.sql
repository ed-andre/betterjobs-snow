create or replace view BETTERJOBS_DB.STAGE.KEYWORDS_RAW_EXTRACTION(
	JOB_UID,
	KEYWORD_SOURCE,
	KEYWORD_TYPE,
	KEYWORD_TEXT_RAW,
	KEYWORD_TEXT_ORIGINAL
) as
        WITH INDUSTRY_KEYWORDS_EXPLODED AS (
            -- Extract industry classification keywords from array
            SELECT
                jle.JOB_UID,
                'industry_keywords' as KEYWORD_SOURCE,
                'industry' as KEYWORD_TYPE,
                TRIM(LOWER(KEYWORD.VALUE::STRING)) as KEYWORD_TEXT_RAW,
                KEYWORD.VALUE::STRING as KEYWORD_TEXT_ORIGINAL
            FROM BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED jle,
            LATERAL FLATTEN(input => jle.INDUSTRY_KEYWORDS) KEYWORD
            WHERE jle.INDUSTRY_KEYWORDS IS NOT NULL
              AND IS_ARRAY(jle.INDUSTRY_KEYWORDS)
              AND ARRAY_SIZE(jle.INDUSTRY_KEYWORDS) > 0
              AND KEYWORD.VALUE IS NOT NULL
              AND LENGTH(TRIM(KEYWORD.VALUE::STRING)) > 1
              AND LOWER(TRIM(KEYWORD.VALUE::STRING)) NOT IN ('null', 'none', 'n/a', '')
        ),

        ROLE_TYPE_EXPLODED AS (
            -- Extract role type from single string value (not an array)
            SELECT
                jle.JOB_UID,
                'role_type' as KEYWORD_SOURCE,
                'role_type' as KEYWORD_TYPE,
                TRIM(LOWER(jle.ROLE_TYPE)) as KEYWORD_TEXT_RAW,
                jle.ROLE_TYPE as KEYWORD_TEXT_ORIGINAL
            FROM BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED jle
            WHERE jle.ROLE_TYPE IS NOT NULL
              AND LENGTH(TRIM(jle.ROLE_TYPE)) > 1
              AND LOWER(TRIM(jle.ROLE_TYPE)) NOT IN ('null', 'none', 'n/a', '')
        )

        SELECT * FROM INDUSTRY_KEYWORDS_EXPLODED
        UNION ALL
        SELECT * FROM ROLE_TYPE_EXPLODED;