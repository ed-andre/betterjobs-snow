create if not exists view BETTERJOBS_DB.STAGE.PLATFORM_SUMMARY(
	PLATFORM,
	TOTAL_JOBS,
	ENGLISH_JOBS,
	JOBS_WITH_SALARY,
	AVG_QUALITY_SCORE,
	EARLIEST_JOB,
	LATEST_JOB
) as
SELECT
    platform,
    COUNT(*) as total_jobs,
    COUNT(CASE WHEN is_english = TRUE THEN 1 END) as english_jobs,
    COUNT(CASE WHEN salary_min IS NOT NULL THEN 1 END) as jobs_with_salary,
    AVG(data_quality_score) as avg_quality_score,
    MIN(date_posted) as earliest_job,
    MAX(date_posted) as latest_job
FROM jobs_unified
GROUP BY platform;