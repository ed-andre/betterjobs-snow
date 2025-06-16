create or replace view BETTERJOBS_DB.STAGE.LOCATIONS_RAW_EXTRACTION(
	JOB_UID,
	LOCATION_SOURCE,
	LOCATION_TYPE,
	LOCATION_NAME_RAW,
	LOCATION_NAME_ORIGINAL
) as
    WITH OFFICE_LOCATIONS_EXPLODED AS (
        -- Extract office locations from VARIANT array
        SELECT
            jle.JOB_UID,
            'office_locations' as LOCATION_SOURCE,
            'office' as LOCATION_TYPE,
            TRIM(LOWER(LOCATION.VALUE::STRING)) as LOCATION_NAME_RAW,
            LOCATION.VALUE::STRING as LOCATION_NAME_ORIGINAL
        FROM BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED jle,
        LATERAL FLATTEN(input => jle.OFFICE_LOCATIONS) LOCATION
        WHERE jle.OFFICE_LOCATIONS IS NOT NULL
          AND IS_ARRAY(jle.OFFICE_LOCATIONS)
          AND ARRAY_SIZE(jle.OFFICE_LOCATIONS) > 0
          AND LOCATION.VALUE IS NOT NULL
          AND LENGTH(TRIM(LOCATION.VALUE::STRING)) > 1
          AND LOWER(TRIM(LOCATION.VALUE::STRING)) NOT IN ('null', 'none', 'n/a', '', 'unknown')
    ),

    BASE_LOCATIONS AS (
        -- Include basic location from jobs_unified
        SELECT
            ju.JOB_UID,
            'location_standardized' as LOCATION_SOURCE,
            'standard' as LOCATION_TYPE,
            TRIM(LOWER(ju.LOCATION_STANDARDIZED)) as LOCATION_NAME_RAW,
            ju.LOCATION_STANDARDIZED as LOCATION_NAME_ORIGINAL
                FROM BETTERJOBS_DB.STAGE.JOBS_UNIFIED ju
        WHERE ju.LOCATION_STANDARDIZED IS NOT NULL
          AND LENGTH(TRIM(ju.LOCATION_STANDARDIZED)) > 1
          AND LOWER(TRIM(ju.LOCATION_STANDARDIZED)) NOT IN ('null', 'none', 'n/a', '', 'no location found', 'unknown')
    )

    SELECT * FROM OFFICE_LOCATIONS_EXPLODED
    UNION ALL
    SELECT * FROM BASE_LOCATIONS;