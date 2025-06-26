CREATE VIEW IF NOT EXISTS ANALYTICS.COMPANY_HIRING_INTELLIGENCE AS
WITH company_weekly_with_context AS (
    SELECT
        ch.*,
        dc.INDUSTRY,
        dc.COMPANY_SIZE_CATEGORY,

        -- Rolling window calculations
        SUM(ch.JOBS_POSTED_COUNT) OVER (
            PARTITION BY ch.COMPANY_KEY
            ORDER BY ch.WEEK_START_DATE
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as jobs_posted_last_7_days,

        SUM(ch.JOBS_POSTED_COUNT) OVER (
            PARTITION BY ch.COMPANY_KEY
            ORDER BY ch.WEEK_START_DATE
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as jobs_posted_last_30_days,

        -- Industry context for rankings
        AVG(ch.JOBS_POSTED_COUNT) OVER (
            PARTITION BY dc.INDUSTRY, ch.WEEK_KEY
        ) as industry_avg_jobs,

        AVG(ch.AVG_SALARY_OFFERED) OVER (
            PARTITION BY dc.INDUSTRY, ch.WEEK_KEY
        ) as industry_avg_salary

    FROM ANALYTICS.FACT_COMPANY_HIRING_WEEKLY ch
    LEFT JOIN ANALYTICS.DIM_COMPANY dc ON ch.COMPANY_KEY = dc.COMPANY_KEY AND dc.IS_CURRENT = TRUE
    WHERE ch.WEEK_START_DATE >= CURRENT_DATE - 730  -- 2 years of data
),

company_with_rankings AS (
    SELECT cwc.*,
           -- Industry rankings
           RANK() OVER (
               PARTITION BY cwc.INDUSTRY, cwc.WEEK_KEY
               ORDER BY cwc.JOBS_POSTED_COUNT DESC
           ) as hiring_rank_in_industry,

           -- Competitiveness score calculation
           CASE
               WHEN cwc.JOBS_POSTED_COUNT >= cwc.industry_avg_jobs * 2 THEN 1.0
               WHEN cwc.JOBS_POSTED_COUNT >= cwc.industry_avg_jobs * 1.5 THEN 0.8
               WHEN cwc.JOBS_POSTED_COUNT >= cwc.industry_avg_jobs THEN 0.6
               WHEN cwc.JOBS_POSTED_COUNT >= cwc.industry_avg_jobs * 0.5 THEN 0.4
               ELSE 0.2
           END as hiring_competitiveness_score

    FROM company_weekly_with_context cwc
    WHERE cwc.JOBS_POSTED_COUNT > 0
)

SELECT
    -- Primary identifiers
    'CI_' || cwr.WEEK_KEY || '_' || cwr.COMPANY_KEY as INTELLIGENCE_KEY,
    cwr.WEEK_START_DATE as ANALYSIS_DATE,
    cwr.COMPANY_KEY,
    cwr.WEEK_KEY,

    -- Hiring Velocity (calculated from rolling windows)
    cwr.JOBS_POSTED_COUNT as JOBS_POSTED_LAST_7_DAYS,  -- Current week as 7-day proxy
    cwr.jobs_posted_last_30_days as JOBS_POSTED_LAST_30_DAYS,
    cwr.ACTIVE_JOBS_COUNT as CURRENT_JOB_POSTINGS,

    -- Growth Indicators (from weekly fact)
    cwr.HIRING_TREND_DIRECTION as HIRING_VELOCITY_TREND,
    ((cwr.JOBS_POSTED_COUNT::FLOAT / NULLIF(LAG(cwr.JOBS_POSTED_COUNT) OVER (
        PARTITION BY cwr.COMPANY_KEY ORDER BY cwr.WEEK_START_DATE
    ), 0)) - 1) * 100 as WEEK_OVER_WEEK_GROWTH_RATE,

    -- Compensation Strategy
    CASE WHEN cwr.AVG_SALARY_OFFERED IS NOT NULL THEN 1.0 ELSE 0.0 END as SALARY_TRANSPARENCY_RATE,
    cwr.AVG_SALARY_OFFERED,

    -- Role Distribution (calculated from percentages)
    CASE WHEN cwr.SENIOR_LEVEL_PERCENTAGE > 0
         THEN cwr.ENTRY_LEVEL_PERCENTAGE / NULLIF(cwr.SENIOR_LEVEL_PERCENTAGE, 0)
         ELSE NULL END as ENTRY_VS_SENIOR_RATIO,

    -- Technical vs Business approximation (placeholder - would need job family data)
    0.7 as TECHNICAL_VS_BUSINESS_RATIO,  -- Default assumption, can be enhanced with job family joins

    cwr.REMOTE_JOBS_PERCENTAGE as REMOTE_JOB_PERCENTAGE,

    -- Work Arrangement Policy (derived from percentages)
    CASE
        WHEN cwr.REMOTE_JOBS_PERCENTAGE >= 80 THEN 'FULL_REMOTE'
        WHEN (cwr.REMOTE_JOBS_PERCENTAGE + cwr.HYBRID_JOBS_PERCENTAGE) >= 50 THEN 'HYBRID'
        ELSE 'ONSITE'
    END as REMOTE_WORK_POLICY,

    -- Market Position
    cwr.hiring_rank_in_industry as HIRING_RANK_IN_INDUSTRY,
    cwr.hiring_competitiveness_score as HIRING_COMPETITIVENESS_SCORE,

    -- Quality Metrics
    cwr.JOBS_POSTED_COUNT as SAMPLE_SIZE,
    CASE WHEN cwr.AVG_SALARY_OFFERED IS NOT NULL THEN 1.0 ELSE 0.5 END as DATA_COMPLETENESS_SCORE,

    -- Audit Fields
    CURRENT_TIMESTAMP as CREATED_TIMESTAMP

FROM company_with_rankings cwr
WHERE cwr.WEEK_START_DATE = (
    SELECT MAX(WEEK_START_DATE)
    FROM ANALYTICS.FACT_COMPANY_HIRING_WEEKLY
    WHERE COMPANY_KEY = cwr.COMPANY_KEY
)  -- Latest week only for current intelligence
ORDER BY cwr.hiring_rank_in_industry ASC, cwr.JOBS_POSTED_COUNT DESC;