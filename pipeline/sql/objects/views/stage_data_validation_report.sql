create if not exists view BETTERJOBS_DB.STAGE.DATA_VALIDATION_REPORT(
	CHECK_NAME,
	ISSUE_COUNT
) as
WITH VALIDATION_CHECKS AS (
    -- Check 1: Skills without jobs
    SELECT 'orphaned_skills' as CHECK_NAME, COUNT(*) as ISSUE_COUNT
    FROM SKILLS_NORMALIZED sn
    LEFT JOIN JOB_SKILLS_BRIDGE jsb ON sn.SKILL_ID = jsb.SKILL_ID
    WHERE jsb.SKILL_ID IS NULL

    UNION ALL

    -- Check 2: Jobs without skills
    SELECT 'jobs_without_skills' as CHECK_NAME, COUNT(*) as ISSUE_COUNT
    FROM JOBS_UNIFIED ju
    LEFT JOIN JOB_SKILLS_BRIDGE jsb ON ju.JOB_UID = jsb.JOB_UID
    WHERE jsb.JOB_UID IS NULL AND ju.IS_ENGLISH = TRUE

    UNION ALL

    -- Check 3: Low confidence skills
    SELECT 'low_confidence_skills' as CHECK_NAME, COUNT(*) as ISSUE_COUNT
    FROM SKILLS_NORMALIZED
    WHERE CONFIDENCE_SCORE < 0.5

    UNION ALL

    -- Check 4: Duplicate skill names
    SELECT 'duplicate_skill_names' as CHECK_NAME, COUNT(*) - COUNT(DISTINCT SKILL_NAME) as ISSUE_COUNT
    FROM SKILLS_NORMALIZED

    UNION ALL

    -- Check 5: Locations without jobs
    SELECT 'orphaned_locations' as CHECK_NAME, COUNT(*) as ISSUE_COUNT
    FROM LOCATIONS_NORMALIZED ln
    LEFT JOIN JOB_LOCATIONS_BRIDGE jlb ON ln.LOCATION_ID = jlb.LOCATION_ID
    WHERE jlb.LOCATION_ID IS NULL

    UNION ALL

    -- Check 6: Jobs without locations
    SELECT 'jobs_without_locations' as CHECK_NAME, COUNT(*) as ISSUE_COUNT
    FROM JOBS_UNIFIED ju
    LEFT JOIN JOB_LOCATIONS_BRIDGE jlb ON ju.JOB_UID = jlb.JOB_UID
    WHERE jlb.JOB_UID IS NULL AND ju.IS_ENGLISH = TRUE

    UNION ALL

    -- Check 7: Low confidence locations
    SELECT 'low_confidence_locations' as CHECK_NAME, COUNT(*) as ISSUE_COUNT
    FROM LOCATIONS_NORMALIZED
    WHERE CONFIDENCE_SCORE < 0.5
)
SELECT * FROM VALIDATION_CHECKS
WHERE ISSUE_COUNT > 0;