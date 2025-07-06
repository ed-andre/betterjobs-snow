"""
Analytics Layer Fact Table Assets

This module contains Dagster assets for creating and managing fact tables
in the ANALYTICS layer following the Schema-as-Code approach.
"""

from typing import Dict, Any
from dagster import AssetExecutionContext, asset, MetadataValue
from dagster_snowflake import SnowflakeResource

from dagster_betterjobs.utils.schema_utils import ensure_object_exists


@asset(
    deps=["analytics_dim_date", "analytics_dim_company", "analytics_dim_location",
          "analytics_dim_job_family", "analytics_dim_platform", "analytics_dim_skills",
          "analytics_dim_salary",
          "stage_jobs_unified", "stage_jobs_llm_enriched_unified", "stage_job_salary_bridge"],
    description="Create primary fact table for job posting analytics",
    group_name="3b_analytics_facts_aggregates_analysis",
    kinds={"snowflake", "SQL"}
)
def analytics_fact_job_postings(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build the primary fact table for job posting analytics.

    This asset creates the core fact table that serves as the foundation for all job market analytics.
    It combines job data from STAGE sources with dimension lookups to create a business-ready
    analytical structure.

    Processing Logic:
    1. Join STAGE.JOBS_UNIFIED with STAGE.JOBS_LLM_ENRICHED for complete job data
    2. Lookup dimension keys for all foreign key relationships
    3. Apply data quality filters and business rules
    4. Generate surrogate keys and calculate derived measures
    5. Validate fact table completeness and integrity

    Data Quality Rules:
    - Filter jobs with IS_ACTIVE = TRUE for current analysis
    - Require valid COMPANY_ID for company dimension lookups
    - Apply LLM confidence thresholds for enriched data inclusion
    - Validate date ranges and posting date logic
    - Handle missing dimension keys gracefully with default values

    Returns:
        Dict containing execution results and job posting statistics
    """

    # Ensure the table exists using Schema-as-Code pattern
    table_name = ensure_object_exists("tables/analytics_fact_job_postings.sql", snowflake, context)

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting fact job postings build from STAGE sources")

            # Step 1: Clear existing data for complete refresh
            context.log.info("Clearing existing fact job postings data")
            cursor.execute(f"TRUNCATE TABLE {table_name}")

            # Step 2: Build fact table with dimension lookups and business rules
            context.log.info("Building fact table with dimension lookups and data quality filters")
            context.log.info("BUGFIX: Enhanced job family dimension join to prevent duplicate records by matching all key fields")

            build_sql = f"""
            INSERT INTO {table_name} (
                job_posting_key,
                date_posted_key,
                company_key,
                location_key,
                job_family_key,
                platform_key,
                salary_key,
                job_uid,
                job_title,
                posting_url,
                salary_min_annual_usd,
                salary_max_annual_usd,
                salary_midpoint_annual_usd,
                salary_confidence,
                llm_overall_confidence,
                data_quality_score,
                job_family,
                job_sub_family,
                seniority_level,
                work_type,
                remote_flexibility,
                is_active_posting,
                is_equity_mentioned,
                is_bonus_mentioned,
                llm_needs_manual_review,
                first_posted_date,
                date_retrieved,
                created_timestamp,
                updated_timestamp,
                partition_date
            )
            WITH job_data_prep AS (
                SELECT
                    ju.JOB_UID,
                    ju.COMPANY_ID,
                    ju.PLATFORM,
                    ju.JOB_TITLE_CLEAN,
                    ju.JOB_URL,
                    ju.LOCATION_STANDARDIZED,
                    ju.DATE_POSTED,
                    ju.DATE_RETRIEVED,
                    ju.IS_ACTIVE,
                    ju.DATA_QUALITY_SCORE,

                    -- LLM enriched data
                    lle.SALARY_MIN,
                    lle.SALARY_MAX,
                    lle.SALARY_CURRENCY,
                    lle.SALARY_PERIOD,
                    lle.MIN_YEARS_EXPERIENCE,
                    lle.MAX_YEARS_EXPERIENCE,
                    lle.EXPERIENCE_LEVEL,
                    lle.SALARY_CONFIDENCE,
                    lle.LLM_OVERALL_CONFIDENCE,
                    lle.JOB_FAMILY,
                    lle.JOB_SUB_FAMILY,
                    lle.SENIORITY_LEVEL,
                    lle.ROLE_TYPE,
                    lle.WORK_TYPE,
                    lle.REMOTE_FLEXIBILITY,
                    lle.EQUITY_MENTIONED,
                    lle.BONUS_MENTIONED,
                    lle.LLM_NEEDS_MANUAL_REVIEW,
                    lle.PRIMARY_KEYWORDS

                FROM BETTERJOBS_DB.STAGE.JOBS_UNIFIED ju
                LEFT JOIN BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED lle ON ju.JOB_UID = lle.JOB_UID
                WHERE ju.IS_ACTIVE = TRUE
                  AND ju.COMPANY_ID IS NOT NULL
                  AND ju.COMPANY_ID != ''
                  AND ju.DATE_POSTED >= '2020-01-01'
                  AND ju.DATE_POSTED <= CURRENT_DATE
            ),

            dimension_lookups AS (
                SELECT
                    jd.*,

                    -- Date dimension key (direct generation)
                    TO_CHAR(jd.DATE_POSTED, 'YYYYMMDD') as date_posted_key,

                    -- Company dimension lookup
                    COALESCE(dc.COMPANY_KEY, 'COMP_UNKNOWN') as company_key,

                    -- Location dimension lookup
                    COALESCE(dl.LOCATION_KEY, 'LOC_UNKNOWN') as location_key,

                    -- Job family dimension lookup (compound lookup on family + seniority)
                    COALESCE(djf.JOB_FAMILY_KEY, 'JF_UNKNOWN') as job_family_key,

                    -- Platform dimension lookup
                    COALESCE(dp.PLATFORM_KEY, 'PLT_' || UPPER(jd.PLATFORM)) as platform_key,

                    -- Salary dimension lookup (via bridge table)
                    COALESCE(ds.SALARY_KEY, 'SAL_UNKNOWN') as salary_key,

                    -- Salary measures (denormalized from DIM_SALARY for performance)
                    ds.SALARY_MIN_ANNUAL_USD,
                    ds.SALARY_MAX_ANNUAL_USD,
                    ds.SALARY_MIDPOINT_ANNUAL_USD,

                    -- Salary bridge confidence (for salary_confidence field)
                    jsb.OVERALL_CONFIDENCE as salary_bridge_confidence

                FROM job_data_prep jd

                -- Company dimension lookup
                LEFT JOIN BETTERJOBS_DB.ANALYTICS.DIM_COMPANY dc
                    ON jd.COMPANY_ID = dc.COMPANY_ID AND dc.IS_CURRENT = TRUE

                -- Location dimension lookup (BUGFIX: Select best record per location name to prevent duplicates)
                LEFT JOIN (
                    SELECT
                        LOCATION_KEY,
                        LOCATION_NAME,
                        ROW_NUMBER() OVER (
                            PARTITION BY UPPER(LOCATION_NAME)
                            ORDER BY STAGE_CONFIDENCE_SCORE DESC NULLS LAST,
                                     FREQUENCY_COUNT DESC NULLS LAST,
                                     LOCATION_KEY ASC
                        ) as rn
                    FROM BETTERJOBS_DB.ANALYTICS.DIM_LOCATION
                ) dl ON LOWER(jd.LOCATION_STANDARDIZED) = LOWER(dl.LOCATION_NAME) AND dl.rn = 1  -- BUG-020: All FACT_JOB_POSTINGS Records Show LOCATION_KEY

                -- Job family dimension lookup (complete match to prevent duplicates)
                LEFT JOIN BETTERJOBS_DB.ANALYTICS.DIM_JOB_FAMILY djf
                    ON jd.JOB_FAMILY = djf.JOB_FAMILY
                    AND COALESCE(jd.JOB_SUB_FAMILY, 'General') = djf.JOB_SUB_FAMILY
                    AND COALESCE(jd.SENIORITY_LEVEL, 'Not Specified') = djf.SENIORITY_LEVEL
                    AND COALESCE(jd.ROLE_TYPE, 'Not Specified') = djf.ROLE_TYPE

                -- Platform dimension lookup
                LEFT JOIN BETTERJOBS_DB.ANALYTICS.DIM_PLATFORM dp
                    ON jd.PLATFORM = dp.PLATFORM_NAME

                -- Salary dimension lookup (via bridge table)
                LEFT JOIN BETTERJOBS_DB.STAGE.JOB_SALARY_BRIDGE jsb
                    ON jd.JOB_UID = jsb.JOB_UID
                    AND jsb.VALIDATION_STATUS != 'rejected'
                LEFT JOIN BETTERJOBS_DB.ANALYTICS.DIM_SALARY ds
                    ON jsb.SALARY_ID = ds.SALARY_ID
            )

            SELECT
                'JP_' || JOB_UID as job_posting_key,
                date_posted_key,
                company_key,
                location_key,
                job_family_key,
                platform_key,
                salary_key,

                -- Degenerate dimensions
                JOB_UID as job_uid,
                JOB_TITLE_CLEAN as job_title,
                JOB_URL as posting_url,

                -- Salary measures (normalized annual USD)
                SALARY_MIN_ANNUAL_USD as salary_min_annual_usd,
                SALARY_MAX_ANNUAL_USD as salary_max_annual_usd,
                SALARY_MIDPOINT_ANNUAL_USD as salary_midpoint_annual_usd,

                -- Quality and confidence measures
                COALESCE(salary_bridge_confidence, 0.0) as salary_confidence,
                LLM_OVERALL_CONFIDENCE as llm_overall_confidence,
                DATA_QUALITY_SCORE as data_quality_score,

                -- Job classification
                JOB_FAMILY as job_family,
                JOB_SUB_FAMILY as job_sub_family,
                SENIORITY_LEVEL as seniority_level,

                -- Work arrangement
                WORK_TYPE as work_type,
                REMOTE_FLEXIBILITY as remote_flexibility,

                -- Boolean flags
                IS_ACTIVE as is_active_posting,
                COALESCE(EQUITY_MENTIONED, FALSE) as is_equity_mentioned,
                COALESCE(BONUS_MENTIONED, FALSE) as is_bonus_mentioned,
                COALESCE(LLM_NEEDS_MANUAL_REVIEW, FALSE) as llm_needs_manual_review,

                -- Important dates
                DATE_POSTED as first_posted_date,
                DATE_RETRIEVED as date_retrieved,

                -- Audit fields
                CURRENT_TIMESTAMP as created_timestamp,
                CURRENT_TIMESTAMP as updated_timestamp,

                -- Partitioning field
                DATE_TRUNC('month', DATE_POSTED) as partition_date

            FROM dimension_lookups
            ORDER BY DATE_POSTED DESC, JOB_UID
            """

            cursor.execute(build_sql)
            rows_inserted = cursor.rowcount

            context.log.info(f"Successfully inserted {rows_inserted} job posting records")

            # Step 3: Validate data quality and gather statistics
            context.log.info("Validating fact table data quality and gathering statistics")

            # BUGFIX: Check for duplicate job records to detect cardinality issues early
            cursor.execute(f"""
                SELECT
                    COUNT(*) as total_records,
                    COUNT(DISTINCT job_uid) as unique_jobs,
                    COUNT(*) - COUNT(DISTINCT job_uid) as duplicate_jobs
                FROM {table_name}
            """)
            duplicate_check = cursor.fetchone()
            if duplicate_check[2] > 0:
                context.log.error(f"CRITICAL: Found {duplicate_check[2]} duplicate job records! "
                                f"Total: {duplicate_check[0]}, Unique: {duplicate_check[1]}")
            else:
                context.log.info(f"Duplicate check passed: {duplicate_check[1]} unique jobs from {duplicate_check[0]} records")

            validation_sql = f"""
            SELECT
                COUNT(*) as total_job_postings,
                COUNT(DISTINCT job_uid) as unique_jobs,
                COUNT(DISTINCT company_key) as unique_companies,
                COUNT(DISTINCT location_key) as unique_locations,
                COUNT(DISTINCT job_family_key) as unique_job_families,
                COUNT(DISTINCT platform_key) as unique_platforms,

                -- Dimension key success rates
                COUNT(CASE WHEN company_key != 'COMP_UNKNOWN' THEN 1 END) as successful_company_lookups,
                COUNT(CASE WHEN location_key != 'LOC_UNKNOWN' THEN 1 END) as successful_location_lookups,
                COUNT(CASE WHEN job_family_key != 'JF_UNKNOWN' THEN 1 END) as successful_job_family_lookups,
                COUNT(CASE WHEN salary_key != 'SAL_UNKNOWN' THEN 1 END) as successful_salary_lookups,

                -- Data completeness metrics
                COUNT(CASE WHEN salary_min_annual_usd IS NOT NULL AND salary_max_annual_usd IS NOT NULL THEN 1 END) as jobs_with_salary,
                COUNT(CASE WHEN work_type IS NOT NULL THEN 1 END) as jobs_with_work_type,
                COUNT(CASE WHEN llm_overall_confidence >= 0.5 THEN 1 END) as jobs_with_good_llm_confidence,

                -- Quality metrics
                AVG(data_quality_score) as avg_data_quality_score,
                AVG(llm_overall_confidence) as avg_llm_confidence,
                AVG(salary_confidence) as avg_salary_confidence,

                -- Date range
                MIN(first_posted_date) as earliest_job_date,
                MAX(first_posted_date) as latest_job_date,

                -- Boolean flag statistics
                COUNT(CASE WHEN is_equity_mentioned = TRUE THEN 1 END) as jobs_with_equity,
                COUNT(CASE WHEN is_bonus_mentioned = TRUE THEN 1 END) as jobs_with_bonus,
                COUNT(CASE WHEN llm_needs_manual_review = TRUE THEN 1 END) as jobs_needing_review

            FROM {table_name}
            """

            cursor.execute(validation_sql)
            validation_result = cursor.fetchone()

            # Step 4: Calculate lookup success rates
            total_jobs = validation_result[0]
            company_success_rate = (validation_result[6] / total_jobs * 100) if total_jobs > 0 else 0
            location_success_rate = (validation_result[7] / total_jobs * 100) if total_jobs > 0 else 0
            job_family_success_rate = (validation_result[8] / total_jobs * 100) if total_jobs > 0 else 0
            salary_success_rate = (validation_result[9] / total_jobs * 100) if total_jobs > 0 else 0

            # Step 5: Platform distribution analysis
            cursor.execute(f"""
            SELECT
                platform_key,
                COUNT(*) as job_count,
                COUNT(CASE WHEN salary_min_annual_usd IS NOT NULL AND salary_max_annual_usd IS NOT NULL THEN 1 END) as jobs_with_salary,
                AVG(data_quality_score) as avg_quality_score,
                COUNT(CASE WHEN llm_overall_confidence >= 0.5 THEN 1 END) as good_llm_confidence_count
            FROM {table_name}
            GROUP BY platform_key
            ORDER BY job_count DESC
            """)

            platform_stats = [
                {
                    "platform_key": row[0],
                    "job_count": row[1],
                    "jobs_with_salary": row[2],
                    "avg_quality_score": float(row[3]) if row[3] is not None else 0.0,
                    "good_llm_confidence_count": row[4]
                }
                for row in cursor.fetchall()
            ]

            # Step 6: Time-based distribution analysis
            cursor.execute(f"""
            SELECT
                DATE_TRUNC('month', first_posted_date) as posting_month,
                COUNT(*) as job_count,
                COUNT(CASE WHEN salary_min_annual_usd IS NOT NULL AND salary_max_annual_usd IS NOT NULL THEN 1 END) as jobs_with_salary,
                AVG(llm_overall_confidence) as avg_llm_confidence
            FROM {table_name}
            GROUP BY DATE_TRUNC('month', first_posted_date)
            ORDER BY posting_month DESC
            LIMIT 12
            """)

            monthly_stats = [
                {
                    "posting_month": str(row[0]),
                    "job_count": row[1],
                    "jobs_with_salary": row[2],
                    "avg_llm_confidence": float(row[3]) if row[3] is not None else 0.0
                }
                for row in cursor.fetchall()
            ]

            # Step 7: Data quality validation checks
            quality_issues = []

            # Check for jobs with invalid salary ranges
            cursor.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE salary_min_annual_usd IS NOT NULL AND salary_max_annual_usd IS NOT NULL
                AND salary_min_annual_usd > salary_max_annual_usd
            """)
            invalid_salary_ranges = cursor.fetchone()[0]
            if invalid_salary_ranges > 0:
                quality_issues.append(f"{invalid_salary_ranges} jobs with invalid salary ranges (min > max)")

            # Check for jobs with unrealistic salaries
            cursor.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE salary_min_annual_usd IS NOT NULL AND (salary_min_annual_usd < 15000 OR salary_min_annual_usd > 1000000)
            """)
            unrealistic_salaries = cursor.fetchone()[0]
            if unrealistic_salaries > 0:
                quality_issues.append(f"{unrealistic_salaries} jobs with potentially unrealistic annual salaries")

            # Check dimension key distribution
            unknown_keys_threshold = total_jobs * 0.1  # 10% threshold
            if validation_result[6] < (total_jobs - unknown_keys_threshold):  # company lookups
                quality_issues.append(f"Low company lookup success rate: {company_success_rate:.1f}%")

            if validation_result[7] < (total_jobs - unknown_keys_threshold):  # location lookups
                quality_issues.append(f"Low location lookup success rate: {location_success_rate:.1f}%")

            if quality_issues:
                context.log.warning(f"Data quality issues detected: {', '.join(quality_issues)}")

            # Step 8: Calculate completeness percentages
            salary_completeness = (validation_result[10] / total_jobs * 100) if total_jobs > 0 else 0
            work_type_completeness = (validation_result[11] / total_jobs * 100) if total_jobs > 0 else 0
            good_llm_completeness = (validation_result[12] / total_jobs * 100) if total_jobs > 0 else 0

            context.log.info(f"Fact table validation: {total_jobs} total job postings, "
                           f"{validation_result[1]} unique jobs, {validation_result[2]} unique companies")

            context.log.info(f"Dimension lookup success rates: Company {company_success_rate:.1f}%, "
                           f"Location {location_success_rate:.1f}%, Job Family {job_family_success_rate:.1f}%, "
                           f"Salary {salary_success_rate:.1f}%")

            context.log.info(f"Data completeness: Salary {salary_completeness:.1f}%, "
                           f"Work Type {work_type_completeness:.1f}%")

            context.log.info(f"Quality metrics: Avg data quality {validation_result[13]:.3f}, "
                           f"Avg LLM confidence {validation_result[14]:.3f}")

            # Add metadata for Dagster UI
            context.add_output_metadata({
                "total_job_postings": MetadataValue.int(total_jobs),
                "unique_jobs": MetadataValue.int(validation_result[1]),
                "unique_companies": MetadataValue.int(validation_result[2]),
                "unique_locations": MetadataValue.int(validation_result[3]),
                "unique_job_families": MetadataValue.int(validation_result[4]),
                "unique_platforms": MetadataValue.int(validation_result[5]),
                "company_lookup_success_rate": MetadataValue.float(company_success_rate),
                "location_lookup_success_rate": MetadataValue.float(location_success_rate),
                "job_family_lookup_success_rate": MetadataValue.float(job_family_success_rate),
                "salary_lookup_success_rate": MetadataValue.float(salary_success_rate),
                "salary_completeness": MetadataValue.float(salary_completeness),
                "work_type_completeness": MetadataValue.float(work_type_completeness),
                "good_llm_confidence_percentage": MetadataValue.float(good_llm_completeness),
                "avg_data_quality_score": MetadataValue.float(float(validation_result[13]) if validation_result[13] is not None else 0.0),
                "avg_llm_confidence": MetadataValue.float(float(validation_result[14]) if validation_result[14] is not None else 0.0),
                "platform_distribution": MetadataValue.json(platform_stats[:10]),  # Top 10 platforms
                "monthly_distribution": MetadataValue.json(monthly_stats),  # Last 12 months
                "quality_issues_count": MetadataValue.int(len(quality_issues)),
                "earliest_job_date": MetadataValue.text(str(validation_result[16])),
                "latest_job_date": MetadataValue.text(str(validation_result[17])),
                "jobs_with_equity": MetadataValue.int(validation_result[18]),
                "jobs_with_bonus": MetadataValue.int(validation_result[19]),
                "jobs_needing_review": MetadataValue.int(validation_result[20])
            })

            return {
                "status": "success",
                "table_name": table_name,
                "rows_inserted": rows_inserted,
                "total_job_postings": total_jobs,
                "data_coverage": {
                    "unique_jobs": validation_result[1],
                    "unique_companies": validation_result[2],
                    "unique_locations": validation_result[3],
                    "unique_job_families": validation_result[4],
                    "unique_platforms": validation_result[5]
                },
                "dimension_lookup_success": {
                    "company_success_rate": company_success_rate,
                    "location_success_rate": location_success_rate,
                    "job_family_success_rate": job_family_success_rate,
                    "salary_success_rate": salary_success_rate
                },
                "data_completeness": {
                    "salary_completeness": salary_completeness,
                    "work_type_completeness": work_type_completeness,
                    "good_llm_confidence_percentage": good_llm_completeness
                },
                "quality_metrics": {
                    "avg_data_quality_score": float(validation_result[13]) if validation_result[13] is not None else 0.0,
                    "avg_llm_confidence": float(validation_result[14]) if validation_result[14] is not None else 0.0,
                    "avg_salary_confidence": float(validation_result[15]) if validation_result[15] is not None else 0.0,
                    "quality_issues": quality_issues
                },
                "temporal_coverage": {
                    "earliest_job_date": str(validation_result[16]),
                    "latest_job_date": str(validation_result[17]),
                    "monthly_distribution": monthly_stats
                },
                "business_features": {
                    "jobs_with_equity": validation_result[18],
                    "jobs_with_bonus": validation_result[19],
                    "jobs_needing_review": validation_result[20]
                },
                "platform_analysis": platform_stats
            }

        finally:
            cursor.close()


@asset(
    deps=["analytics_fact_job_postings", "analytics_job_skills_bridge", "analytics_dim_skills", "analytics_dim_date"],
    description="Create weekly skills demand aggregate fact table for technology trend analysis",
    group_name="3b_analytics_facts_aggregates_analysis",
    kinds={"snowflake", "SQL"}
)
def analytics_fact_skills_demand_weekly(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build weekly skills demand aggregates from job postings and skills relationships.

    This asset creates comprehensive weekly skill demand intelligence by aggregating job postings
    by skill, week, and skill category to enable responsive technology trend analysis.

    Processing Logic:
    1. Join FACT_JOB_POSTINGS with JOB_SKILLS_BRIDGE to get job-skill relationships
    2. Group by week, skill, skill category, and skill subcategory for comprehensive market view
    3. Calculate core demand metrics (penetration rates, job counts, growth rates)
    4. Compute salary analysis and skill premiums using denormalized salary fields
    5. Generate skill rankings within categories and overall market
    6. Apply week-over-week trend analysis with directional classification
    7. Implement data quality filtering using confidence scores from bridge table

    Data Quality Rules:
    - Include only skills with OVERALL_CONFIDENCE >= 0.5 from bridge table
    - Filter active job postings (IS_ACTIVE_POSTING = TRUE)
    - Require minimum 3 jobs per skill-week combination for statistical validity
    - Use salary data only where SALARY_CONFIDENCE >= 0.6
    - Apply LLM confidence filtering (LLM_OVERALL_CONFIDENCE >= 0.5)

    Grain: One record per skill per week per skill category
    Aggregation Level: Weekly (Sunday-Saturday weeks)
    Retention: 104 weeks (2 years) for trend analysis

    Performance Optimization:
    - Partition by WEEK_START_DATE for time-based queries
    - Cluster by (WEEK_KEY, SKILL_KEY, SKILL_CATEGORY) for analytical patterns
    - Pre-calculate rankings and growth rates for dashboard performance

    Returns:
        Dict containing processing statistics and data quality metrics
    """

    # Ensure the table exists using Schema-as-Code pattern
    table_name = ensure_object_exists("tables/analytics_fact_skills_demand_weekly.sql", snowflake, context)

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting weekly skills demand aggregation from fact job postings and skills bridge")

            # Step 1: Clear existing data for complete refresh
            context.log.info("Clearing existing skills demand weekly data")
            cursor.execute(f"TRUNCATE TABLE {table_name}")

            # Step 2: Build skills demand aggregates with comprehensive analytics
            context.log.info("Building weekly skills demand aggregates with trend analysis and market intelligence")

            build_sql = f"""
            INSERT INTO {table_name} (
                skills_weekly_key,
                week_key,
                skill_key,
                skill_name,
                skill_category,
                skill_subcategory,
                skill_type,
                active_jobs_with_skill,
                total_active_jobs_for_week,
                skill_penetration_rate,
                avg_salary_midpoint_annual_usd,
                baseline_salary_midpoint_annual_usd,
                salary_premium_annual_usd,
                salary_premium_percentage,
                salary_sample_size,
                week_over_week_change,
                week_over_week_growth_rate,
                trend_direction,
                four_week_moving_average,
                skill_rank_overall,
                skill_rank_in_category,
                market_share_in_category,
                avg_skill_confidence,
                data_completeness_score,
                sample_size,
                remote_jobs_with_skill,
                remote_skill_percentage,
                created_timestamp,
                processing_date,
                week_start_date,
                week_end_date
            )
            WITH quality_job_skills AS (
                SELECT
                    fjp.JOB_POSTING_KEY,
                    fjp.JOB_UID,
                    fjp.DATE_POSTED_KEY,
                    fjp.FIRST_POSTED_DATE,
                    fjp.SALARY_MIDPOINT_ANNUAL_USD,
                    fjp.WORK_TYPE,
                    fjp.IS_ACTIVE_POSTING,
                    fjp.SALARY_CONFIDENCE,
                    fjp.LLM_OVERALL_CONFIDENCE,

                    -- Skills from analytics bridge and dim_skill table with confidence filtering
                    jsb.SKILL_KEY,
                    ds.SKILL_NAME,
                    ds.SKILL_CATEGORY,
                    ds.SKILL_SUBCATEGORY,
                    ds.SKILL_TYPE,
                    jsb.EXTRACTION_CONFIDENCE as skill_extraction_confidence,

                    -- Generate week keys from date dimension for consistency
                    dd.WEEK_KEY,
                    dd.WEEK_BEGINNING_DATE as week_start_date,
                    dd.WEEK_ENDING_DATE as week_end_date

                FROM BETTERJOBS_DB.ANALYTICS.FACT_JOB_POSTINGS fjp
                INNER JOIN BETTERJOBS_DB.ANALYTICS.JOB_SKILLS_BRIDGE jsb
                    ON fjp.JOB_POSTING_KEY = jsb.JOB_POSTING_KEY
                INNER JOIN BETTERJOBS_DB.ANALYTICS.DIM_SKILLS ds
                    ON jsb.SKILL_KEY = ds.SKILL_KEY
                INNER JOIN BETTERJOBS_DB.ANALYTICS.DIM_DATE dd
                    ON fjp.FIRST_POSTED_DATE = dd.FULL_DATE

                WHERE fjp.IS_ACTIVE_POSTING = TRUE
                  AND fjp.LLM_OVERALL_CONFIDENCE >= 0.5
                  AND jsb.EXTRACTION_CONFIDENCE >= 0.5        -- High-confidence skill extractions only
                  AND fjp.FIRST_POSTED_DATE >= CURRENT_DATE - 730  -- 2 years of data
            ),

            skills_weekly_base AS (
                SELECT
                    qjs.week_key,
                    qjs.week_start_date,
                    qjs.week_end_date,
                    qjs.SKILL_KEY,
                    qjs.SKILL_NAME,
                    qjs.SKILL_CATEGORY,
                    qjs.SKILL_SUBCATEGORY,
                    qjs.SKILL_TYPE,
                    -- Core demand metrics
                    COUNT(DISTINCT qjs.JOB_UID) as active_jobs_with_skill,
                    AVG(qjs.skill_extraction_confidence) as avg_skill_confidence,
                    COUNT(DISTINCT qjs.JOB_UID) as sample_size,

                    -- Salary analysis (using denormalized fields)
                    AVG(CASE WHEN qjs.SALARY_CONFIDENCE >= 0.6 AND qjs.SALARY_MIDPOINT_ANNUAL_USD > 0
                             THEN qjs.SALARY_MIDPOINT_ANNUAL_USD END) as avg_salary_midpoint_annual_usd,
                    COUNT(CASE WHEN qjs.SALARY_CONFIDENCE >= 0.6 AND qjs.SALARY_MIDPOINT_ANNUAL_USD > 0
                               THEN 1 END) as salary_sample_size,

                    -- Work arrangement analysis
                    COUNT(CASE WHEN qjs.WORK_TYPE = 'Remote' THEN 1 END) as remote_jobs_with_skill,

                    -- Data completeness tracking
                    COUNT(CASE WHEN qjs.SALARY_CONFIDENCE >= 0.6 THEN 1 END)::FLOAT /
                    COUNT(DISTINCT qjs.JOB_UID) as data_completeness_score

                FROM quality_job_skills qjs
                WHERE qjs.SKILL_KEY IS NOT NULL

                GROUP BY qjs.week_key, qjs.week_start_date, qjs.week_end_date,
                         qjs.SKILL_KEY, qjs.SKILL_NAME, qjs.SKILL_CATEGORY, qjs.SKILL_SUBCATEGORY, qjs.SKILL_TYPE

                HAVING COUNT(DISTINCT qjs.JOB_UID) >= 2  -- Minimum statistical validity
            ),

            market_context AS (
                SELECT
                    qjs.week_key,

                    -- Total market size for penetration rate calculation (all jobs for the week)
                    COUNT(DISTINCT qjs.JOB_UID) as total_active_jobs_for_week,

                    -- Baseline salary (jobs WITHOUT specific skills) for premium calculation
                    AVG(CASE WHEN qjs.SALARY_CONFIDENCE >= 0.6 AND qjs.SALARY_MIDPOINT_ANNUAL_USD > 0
                             THEN qjs.SALARY_MIDPOINT_ANNUAL_USD END) as baseline_salary_midpoint_annual_usd

                FROM quality_job_skills qjs
                GROUP BY qjs.week_key
            ),

            skills_with_trends AS (
                SELECT swb.*,
                       mc.total_active_jobs_for_week,
                       mc.baseline_salary_midpoint_annual_usd,

                       -- Penetration rate calculation
                       swb.active_jobs_with_skill::FLOAT / mc.total_active_jobs_for_week * 100 as skill_penetration_rate,

                       -- Salary premium calculations
                       (swb.avg_salary_midpoint_annual_usd - mc.baseline_salary_midpoint_annual_usd) as salary_premium_annual_usd,
                       CASE WHEN mc.baseline_salary_midpoint_annual_usd > 0
                            THEN ((swb.avg_salary_midpoint_annual_usd / mc.baseline_salary_midpoint_annual_usd) - 1) * 100
                            ELSE NULL END as salary_premium_percentage,

                       -- Remote work percentage
                       swb.remote_jobs_with_skill::FLOAT / swb.active_jobs_with_skill * 100 as remote_skill_percentage,

                       -- Week-over-week trend analysis
                       LAG(swb.active_jobs_with_skill, 1) OVER (
                           PARTITION BY swb.skill_key, swb.skill_category
                           ORDER BY swb.week_start_date
                       ) as prev_week_jobs,

                       -- 4-week moving average for trend smoothing
                       AVG(swb.active_jobs_with_skill) OVER (
                           PARTITION BY swb.skill_key, swb.skill_category
                           ORDER BY swb.week_start_date
                           ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                       ) as four_week_moving_average

                FROM skills_weekly_base swb
                INNER JOIN market_context mc
                    ON swb.week_key = mc.week_key
            ),

            skills_with_rankings AS (
                SELECT swt.*,
                       -- Week-over-week change calculations
                       (swt.active_jobs_with_skill - swt.prev_week_jobs) as week_over_week_change,
                       CASE WHEN swt.prev_week_jobs > 0
                            THEN ((swt.active_jobs_with_skill::FLOAT / swt.prev_week_jobs) - 1) * 100
                            ELSE NULL END as week_over_week_growth_rate,

                       -- Trend direction classification
                       CASE
                           WHEN swt.prev_week_jobs IS NULL THEN 'NEW'
                           WHEN ((swt.active_jobs_with_skill::FLOAT / swt.prev_week_jobs) - 1) * 100 > 25 THEN 'GROWING'
                           WHEN ((swt.active_jobs_with_skill::FLOAT / swt.prev_week_jobs) - 1) * 100 > 5 THEN 'STABLE'
                           WHEN ((swt.active_jobs_with_skill::FLOAT / swt.prev_week_jobs) - 1) * 100 > -10 THEN 'STABLE'
                           ELSE 'DECLINING'
                       END as trend_direction,

                       -- Skill rankings within category
                       RANK() OVER (
                           PARTITION BY swt.week_key, swt.skill_category
                           ORDER BY swt.active_jobs_with_skill DESC
                       ) as skill_rank_in_category,

                       -- Overall market ranking
                       RANK() OVER (
                           PARTITION BY swt.week_key
                           ORDER BY swt.active_jobs_with_skill DESC
                       ) as skill_rank_overall,

                       -- Market share within category
                       swt.active_jobs_with_skill::FLOAT / SUM(swt.active_jobs_with_skill) OVER (
                           PARTITION BY swt.week_key, swt.skill_category
                       ) * 100 as market_share_in_category

                FROM skills_with_trends swt
            )

            SELECT
                -- Primary key generation
                LOWER('SW_' || swr.week_key || '_' || swr.skill_key || '_' || REPLACE(swr.skill_category, ' ', '_') || '_' || REPLACE(swr.skill_type, ' ', '_')) as skills_weekly_key,

                -- Dimension keys
                swr.week_key,
                swr.skill_key,
                swr.skill_name,
                swr.skill_category,
                swr.skill_subcategory,
                swr.skill_type,
                -- Core demand metrics
                swr.active_jobs_with_skill,
                swr.total_active_jobs_for_week,
                swr.skill_penetration_rate,

                -- Enhanced salary analysis
                swr.avg_salary_midpoint_annual_usd,
                swr.baseline_salary_midpoint_annual_usd,
                swr.salary_premium_annual_usd,
                swr.salary_premium_percentage,
                swr.salary_sample_size,

                -- Trend analysis
                swr.week_over_week_change,
                swr.week_over_week_growth_rate,
                swr.trend_direction,
                swr.four_week_moving_average,

                -- Market position
                swr.skill_rank_overall,
                swr.skill_rank_in_category,
                swr.market_share_in_category,

                -- Data quality metrics
                swr.avg_skill_confidence,
                swr.data_completeness_score,
                swr.sample_size,

                -- Work arrangement analysis
                swr.remote_jobs_with_skill,
                swr.remote_skill_percentage,

                -- Audit fields
                CURRENT_TIMESTAMP as created_timestamp,
                CURRENT_DATE as processing_date,
                swr.week_start_date,
                swr.week_end_date

            FROM skills_with_rankings swr
            ORDER BY swr.week_start_date DESC, swr.skill_rank_overall ASC
            """

            cursor.execute(build_sql)
            rows_inserted = cursor.rowcount

            context.log.info(f"Successfully inserted {rows_inserted} weekly skills demand records")

            # Step 3: Validate data quality and gather statistics
            context.log.info("Validating skills demand data quality and gathering market intelligence statistics")

            validation_sql = f"""
            SELECT
                COUNT(*) as total_skill_weeks,
                COUNT(DISTINCT skill_key) as unique_skills,
                COUNT(DISTINCT week_key) as unique_weeks,
                COUNT(DISTINCT skill_category) as unique_skill_categories,
                COUNT(DISTINCT skill_subcategory) as unique_skill_subcategories,

                -- Demand metrics
                SUM(active_jobs_with_skill) as total_skill_job_instances,
                AVG(skill_penetration_rate) as avg_penetration_rate,
                AVG(active_jobs_with_skill) as avg_jobs_per_skill_week,

                -- Salary analysis
                COUNT(CASE WHEN avg_salary_midpoint_annual_usd IS NOT NULL THEN 1 END) as skill_weeks_with_salary,
                AVG(avg_salary_midpoint_annual_usd) as overall_avg_salary,
                AVG(salary_premium_percentage) as avg_salary_premium,
                AVG(salary_sample_size) as avg_salary_sample_size,

                -- Trend analysis
                COUNT(CASE WHEN trend_direction = 'GROWING' THEN 1 END) as growing_skills,
                COUNT(CASE WHEN trend_direction = 'DECLINING' THEN 1 END) as declining_skills,
                COUNT(CASE WHEN trend_direction = 'STABLE' THEN 1 END) as stable_skills,
                COUNT(CASE WHEN trend_direction = 'NEW' THEN 1 END) as new_skills,

                -- Work arrangement analysis
                AVG(remote_skill_percentage) as avg_remote_percentage,
                COUNT(CASE WHEN remote_skill_percentage > 50 THEN 1 END) as remote_friendly_skills,

                -- Quality metrics
                AVG(avg_skill_confidence) as overall_avg_confidence,
                AVG(data_completeness_score) as overall_completeness,
                AVG(sample_size) as avg_sample_size,

                -- Temporal coverage
                MIN(week_start_date) as earliest_week,
                MAX(week_start_date) as latest_week

            FROM {table_name}
            """

            cursor.execute(validation_sql)
            validation_result = cursor.fetchone()

            # Step 4: Analyze skills trend distribution
            context.log.info("Analyzing skills trend distribution and market intelligence")

            cursor.execute(f"""
            SELECT
                trend_direction,
                COUNT(*) as skill_week_count,
                AVG(skill_penetration_rate) as avg_penetration,
                AVG(week_over_week_growth_rate) as avg_growth_rate,
                AVG(salary_premium_percentage) as avg_premium
            FROM {table_name}
            WHERE week_over_week_growth_rate IS NOT NULL
            GROUP BY trend_direction
            ORDER BY skill_week_count DESC
            """)

            trend_stats = [
                {
                    "trend_direction": row[0],
                    "skill_week_count": row[1],
                    "avg_penetration": float(row[2]) if row[2] is not None else 0.0,
                    "avg_growth_rate": float(row[3]) if row[3] is not None else 0.0,
                    "avg_premium": float(row[4]) if row[4] is not None else 0.0
                }
                for row in cursor.fetchall()
            ]

            # Step 5: Top skills analysis
            cursor.execute(f"""
            SELECT
                SKILL_CATEGORY,
                SKILL_SUBCATEGORY,
                AVG(active_jobs_with_skill) as avg_weekly_demand,
                AVG(skill_penetration_rate) as avg_penetration_rate,
                AVG(salary_premium_percentage) as avg_salary_premium,
                AVG(remote_skill_percentage) as avg_remote_percentage,
                COUNT(*) as weeks_tracked
            FROM {table_name}
            WHERE week_start_date >= CURRENT_DATE - 30  -- Last 4 weeks
            GROUP BY SKILL_CATEGORY, SKILL_SUBCATEGORY
            ORDER BY avg_weekly_demand DESC
            LIMIT 20
            """)

            top_categories_stats = [
                {
                    "skill_category": row[0],
                    "skill_subcategory": row[1],
                    "avg_weekly_demand": float(row[2]) if row[2] is not None else 0.0,
                    "avg_penetration_rate": float(row[3]) if row[3] is not None else 0.0,
                    "avg_salary_premium": float(row[4]) if row[4] is not None else 0.0,
                    "avg_remote_percentage": float(row[5]) if row[5] is not None else 0.0,
                    "weeks_tracked": row[6]
                }
                for row in cursor.fetchall()
            ]

            # Step 6: Data quality validation checks
            quality_issues = []

            # Check for skills with impossible penetration rates
            cursor.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE skill_penetration_rate < 0 OR skill_penetration_rate > 100
            """)
            invalid_penetration = cursor.fetchone()[0]
            if invalid_penetration > 0:
                quality_issues.append(f"{invalid_penetration} skill weeks with invalid penetration rates")

            # Check for unrealistic salary premiums
            cursor.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE salary_premium_percentage IS NOT NULL AND ABS(salary_premium_percentage) > 500
            """)
            unrealistic_premiums = cursor.fetchone()[0]
            if unrealistic_premiums > 0:
                quality_issues.append(f"{unrealistic_premiums} skill weeks with unrealistic salary premiums (>500%)")

            # Check for inconsistent trend directions
            cursor.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE trend_direction = 'GROWING' AND week_over_week_growth_rate <= 5
            """)
            inconsistent_trends = cursor.fetchone()[0]
            if inconsistent_trends > 0:
                quality_issues.append(f"{inconsistent_trends} skill weeks with inconsistent trend classification")

            if quality_issues:
                context.log.warning(f"Data quality issues detected: {', '.join(quality_issues)}")

            # Step 7: Calculate derived statistics
            total_skill_weeks = validation_result[0]
            salary_coverage = (validation_result[8] / total_skill_weeks * 100) if total_skill_weeks > 0 else 0
            growing_percentage = (validation_result[12] / total_skill_weeks * 100) if total_skill_weeks > 0 else 0
            declining_percentage = (validation_result[13] / total_skill_weeks * 100) if total_skill_weeks > 0 else 0

            context.log.info(f"Skills demand validation: {total_skill_weeks} total skill-week records, "
                           f"{validation_result[1]} unique skills, {validation_result[2]} unique weeks")

            context.log.info(f"Market intelligence: {validation_result[5]} total skill-job instances, "
                           f"avg penetration {validation_result[6]:.2f}%, avg premium {validation_result[10]:.1f}%")

            context.log.info(f"Trend distribution: {growing_percentage:.1f}% growing, "
                           f"{declining_percentage:.1f}% declining, avg confidence {validation_result[18]:.3f}")

            # Add metadata for Dagster UI (convert all numeric types properly for Dagster compatibility)
            context.add_output_metadata({
                "total_skill_weeks": MetadataValue.int(int(total_skill_weeks)),
                "unique_skills": MetadataValue.int(int(validation_result[1])),
                "unique_weeks": MetadataValue.int(int(validation_result[2])),
                "unique_skill_categories": MetadataValue.int(int(validation_result[3])),
                "unique_skill_subcategories": MetadataValue.int(int(validation_result[4])),
                "total_skill_job_instances": MetadataValue.int(int(validation_result[5])),
                "avg_penetration_rate": MetadataValue.float(float(validation_result[6]) if validation_result[6] is not None else 0.0),
                "avg_jobs_per_skill_week": MetadataValue.float(float(validation_result[7]) if validation_result[7] is not None else 0.0),
                "salary_coverage_percentage": MetadataValue.float(float(salary_coverage)),
                "overall_avg_salary": MetadataValue.float(float(validation_result[9]) if validation_result[9] is not None else 0.0),
                "avg_salary_premium": MetadataValue.float(float(validation_result[10]) if validation_result[10] is not None else 0.0),
                "growing_skills_percentage": MetadataValue.float(float(growing_percentage)),
                "declining_skills_percentage": MetadataValue.float(float(declining_percentage)),
                "avg_remote_percentage": MetadataValue.float(float(validation_result[16]) if validation_result[16] is not None else 0.0),
                "remote_friendly_skills": MetadataValue.int(int(validation_result[17])),
                "overall_avg_confidence": MetadataValue.float(float(validation_result[18]) if validation_result[18] is not None else 0.0),
                "overall_completeness": MetadataValue.float(float(validation_result[19]) if validation_result[19] is not None else 0.0),
                "avg_sample_size": MetadataValue.float(float(validation_result[20]) if validation_result[20] is not None else 0.0),
                "earliest_week": MetadataValue.text(str(validation_result[21])),
                "latest_week": MetadataValue.text(str(validation_result[22])),
                "trend_distribution": MetadataValue.json(trend_stats),
                "top_categories_last_4_weeks": MetadataValue.json(top_categories_stats),
                "quality_issues_count": MetadataValue.int(len(quality_issues))
            })

            return {
                "status": "success",
                "table_name": table_name,
                "rows_inserted": rows_inserted,
                "total_skill_weeks": total_skill_weeks,
                "market_coverage": {
                    "unique_skills": validation_result[1],
                    "unique_weeks": validation_result[2],
                    "unique_skill_categories": validation_result[3],
                    "unique_skill_subcategories": validation_result[4],
                    "total_skill_job_instances": validation_result[5]
                },
                "demand_metrics": {
                    "avg_penetration_rate": float(validation_result[6]) if validation_result[6] is not None else 0.0,
                    "avg_jobs_per_skill_week": float(validation_result[7]) if validation_result[7] is not None else 0.0,
                    "avg_sample_size": float(validation_result[20]) if validation_result[20] is not None else 0.0
                },
                "salary_intelligence": {
                    "salary_coverage_percentage": salary_coverage,
                    "overall_avg_salary": float(validation_result[9]) if validation_result[9] is not None else 0.0,
                    "avg_salary_premium": float(validation_result[10]) if validation_result[10] is not None else 0.0,
                    "avg_salary_sample_size": float(validation_result[11]) if validation_result[11] is not None else 0.0
                },
                "trend_analysis": {
                    "growing_skills": validation_result[12],
                    "declining_skills": validation_result[13],
                    "stable_skills": validation_result[14],
                    "new_skills": validation_result[15],
                    "growing_percentage": growing_percentage,
                    "declining_percentage": declining_percentage,
                    "trend_distribution": trend_stats
                },
                "work_arrangement_intelligence": {
                    "avg_remote_percentage": float(validation_result[16]) if validation_result[16] is not None else 0.0,
                    "remote_friendly_skills": validation_result[17]
                },
                "quality_metrics": {
                    "overall_avg_confidence": float(validation_result[18]) if validation_result[18] is not None else 0.0,
                    "overall_completeness": float(validation_result[19]) if validation_result[19] is not None else 0.0,
                    "quality_issues": quality_issues
                },
                "temporal_coverage": {
                    "earliest_week": str(validation_result[21]),
                    "latest_week": str(validation_result[22]),
                    "weeks_covered": validation_result[2]
                },
                "top_categories_analysis": top_categories_stats
            }

        finally:
            cursor.close()


@asset(
    deps=["analytics_fact_job_postings", "analytics_dim_company", "analytics_dim_date"],
    description="Create weekly company hiring aggregate fact table for competitive analysis",
    group_name="3b_analytics_facts_aggregates_analysis",
    kinds={"snowflake", "SQL"}
)
def analytics_fact_company_hiring_weekly(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build weekly company hiring aggregates from job postings for competitive intelligence.

    This asset creates comprehensive weekly company hiring intelligence by aggregating job postings
    by company and week to enable responsive competitive analysis and market positioning.

    Business Intelligence Capabilities:
    - Company hiring velocity tracking and trend analysis
    - Competitive benchmarking across companies in same industry/size
    - Compensation strategy analysis (salary ranges, transparency rates)
    - Work arrangement policy intelligence (remote, hybrid, onsite percentages)
    - Role distribution analysis (entry vs senior, management ratios)
    - Market position scoring and hiring competitiveness metrics

    Data Quality Rules:
    - Include only companies with valid COMPANY_KEY from dimension lookup
    - Filter active job postings (IS_ACTIVE_POSTING = TRUE)
    - Require minimum 2 jobs per company-week for trend reliability
    - Use salary data only where SALARY_CONFIDENCE >= 0.6
    - Apply LLM confidence filtering (LLM_OVERALL_CONFIDENCE >= 0.5)

    Grain: One record per company per week
    Aggregation Level: Weekly (Sunday-Saturday weeks)
    Retention: 104 weeks (2 years) for competitive trend analysis

    Returns:
        Dict containing processing statistics and competitive intelligence metrics
    """

    # Ensure the table exists using Schema-as-Code pattern
    table_name = ensure_object_exists("tables/analytics_fact_company_hiring_weekly.sql", snowflake, context)

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting weekly company hiring aggregation from fact job postings")

            # Step 1: Clear existing data for complete refresh
            context.log.info("Clearing existing company hiring weekly data")
            cursor.execute(f"TRUNCATE TABLE {table_name}")

            # Step 2: Build company hiring aggregates with comprehensive competitive intelligence
            context.log.info("Building weekly company hiring aggregates with competitive analysis and trend intelligence")

            build_sql = f"""
            INSERT INTO {table_name} (
                company_hiring_key,
                week_key,
                company_key,
                jobs_posted_count,
                active_jobs_count,
                new_jobs_this_week,
                week_over_week_change,
                hiring_trend_direction,
                avg_salary_offered,
                median_salary_offered,
                salary_range_width,
                remote_jobs_percentage,
                hybrid_jobs_percentage,
                on_site_jobs_percentage,
                entry_level_percentage,
                senior_level_percentage,
                management_roles_percentage,
                created_timestamp,
                week_start_date
            )
            WITH quality_company_jobs AS (
                SELECT
                    fjp.COMPANY_KEY,
                    fjp.FIRST_POSTED_DATE,
                    dd.WEEK_KEY,
                    dd.WEEK_BEGINNING_DATE as week_start_date,
                    fjp.IS_ACTIVE_POSTING,
                    fjp.SALARY_MIDPOINT_ANNUAL_USD,
                    fjp.SALARY_CONFIDENCE,
                    fjp.WORK_TYPE,
                    fjp.SENIORITY_LEVEL,
                    fjp.LLM_OVERALL_CONFIDENCE
                FROM BETTERJOBS_DB.ANALYTICS.FACT_JOB_POSTINGS fjp
                INNER JOIN BETTERJOBS_DB.ANALYTICS.DIM_DATE dd
                    ON fjp.FIRST_POSTED_DATE = dd.FULL_DATE
                WHERE fjp.COMPANY_KEY IS NOT NULL
                  AND fjp.COMPANY_KEY != 'COMP_UNKNOWN'
                  AND fjp.LLM_OVERALL_CONFIDENCE >= 0.5
                  AND fjp.FIRST_POSTED_DATE >= CURRENT_DATE - 730  -- 2 years of data
            ),

            company_weekly_base AS (
                SELECT
                    week_key,
                    week_start_date,
                    COMPANY_KEY,

                    -- Core hiring metrics
                    COUNT(*) as jobs_posted_count,
                    COUNT(CASE WHEN IS_ACTIVE_POSTING THEN 1 END) as active_jobs_count,
                    COUNT(CASE WHEN DATE_TRUNC('week', FIRST_POSTED_DATE) = week_start_date THEN 1 END) as new_jobs_this_week,

                                        -- Salary analysis (with confidence filtering) - using only SALARY_MIDPOINT_ANNUAL_USD per table definition
                    AVG(CASE WHEN SALARY_CONFIDENCE >= 0.6 AND SALARY_MIDPOINT_ANNUAL_USD > 0
                             THEN SALARY_MIDPOINT_ANNUAL_USD END) as avg_salary_offered,
                    MEDIAN(CASE WHEN SALARY_CONFIDENCE >= 0.6 AND SALARY_MIDPOINT_ANNUAL_USD > 0
                               THEN SALARY_MIDPOINT_ANNUAL_USD END) as median_salary_offered,
                    -- Calculate salary range width from the midpoint spread (approximation since we don't have min/max in this table)
                    (MAX(CASE WHEN SALARY_CONFIDENCE >= 0.6 AND SALARY_MIDPOINT_ANNUAL_USD > 0
                              THEN SALARY_MIDPOINT_ANNUAL_USD END) -
                     MIN(CASE WHEN SALARY_CONFIDENCE >= 0.6 AND SALARY_MIDPOINT_ANNUAL_USD > 0
                              THEN SALARY_MIDPOINT_ANNUAL_USD END)) as salary_range_width,

                    -- Work arrangement patterns
                    COUNT(CASE WHEN WORK_TYPE = 'Remote' THEN 1 END)::FLOAT / COUNT(*) * 100 as remote_jobs_percentage,
                    COUNT(CASE WHEN WORK_TYPE = 'Hybrid' THEN 1 END)::FLOAT / COUNT(*) * 100 as hybrid_jobs_percentage,
                    COUNT(CASE WHEN WORK_TYPE = 'On-site' THEN 1 END)::FLOAT / COUNT(*) * 100 as on_site_jobs_percentage,

                    -- Role distribution patterns
                    COUNT(CASE WHEN LOWER(SENIORITY_LEVEL) LIKE '%entry%' OR LOWER(SENIORITY_LEVEL) LIKE '%junior%'
                               THEN 1 END)::FLOAT / COUNT(*) * 100 as entry_level_percentage,
                    COUNT(CASE WHEN LOWER(SENIORITY_LEVEL) LIKE '%senior%' OR LOWER(SENIORITY_LEVEL) LIKE '%staff%'
                                OR LOWER(SENIORITY_LEVEL) LIKE '%principal%'
                               THEN 1 END)::FLOAT / COUNT(*) * 100 as senior_level_percentage,
                    COUNT(CASE WHEN LOWER(SENIORITY_LEVEL) LIKE '%manager%' OR LOWER(SENIORITY_LEVEL) LIKE '%director%'
                                OR LOWER(SENIORITY_LEVEL) LIKE '%vp%'
                               THEN 1 END)::FLOAT / COUNT(*) * 100 as management_roles_percentage

                FROM quality_company_jobs
                GROUP BY week_key, week_start_date, COMPANY_KEY
                HAVING COUNT(*) >= 2  -- Minimum statistical validity for company trends
            ),

            company_with_trends AS (
                SELECT cwb.*,
                       -- Previous week comparison for trend analysis
                       LAG(cwb.jobs_posted_count, 1) OVER (
                           PARTITION BY cwb.COMPANY_KEY
                           ORDER BY cwb.week_start_date
                       ) as prev_week_jobs,

                       -- Calculate week-over-week change
                       (cwb.jobs_posted_count - LAG(cwb.jobs_posted_count, 1) OVER (
                           PARTITION BY cwb.COMPANY_KEY
                           ORDER BY cwb.week_start_date
                       )) as week_over_week_change,

                       -- Trend direction classification
                       CASE
                           WHEN LAG(cwb.jobs_posted_count, 1) OVER (PARTITION BY cwb.COMPANY_KEY ORDER BY cwb.week_start_date) IS NULL THEN 'New'
                           WHEN ((cwb.jobs_posted_count::FLOAT / LAG(cwb.jobs_posted_count, 1) OVER (PARTITION BY cwb.COMPANY_KEY ORDER BY cwb.week_start_date)) - 1) * 100 > 20 THEN 'Accelerating'
                           WHEN ((cwb.jobs_posted_count::FLOAT / LAG(cwb.jobs_posted_count, 1) OVER (PARTITION BY cwb.COMPANY_KEY ORDER BY cwb.week_start_date)) - 1) * 100 > -10 THEN 'Stable'
                           ELSE 'Declining'
                       END as hiring_trend_direction
                FROM company_weekly_base cwb
            )

            SELECT
                'CH_' || cwt.week_key || '_' || cwt.COMPANY_KEY as company_hiring_key,
                cwt.week_key,
                cwt.COMPANY_KEY,
                cwt.jobs_posted_count,
                cwt.active_jobs_count,
                cwt.new_jobs_this_week,
                cwt.week_over_week_change,
                cwt.hiring_trend_direction,
                cwt.avg_salary_offered,
                cwt.median_salary_offered,
                cwt.salary_range_width,
                cwt.remote_jobs_percentage,
                cwt.hybrid_jobs_percentage,
                cwt.on_site_jobs_percentage,
                cwt.entry_level_percentage,
                cwt.senior_level_percentage,
                cwt.management_roles_percentage,
                CURRENT_TIMESTAMP as created_timestamp,
                cwt.week_start_date
            FROM company_with_trends cwt
            ORDER BY cwt.week_start_date DESC, cwt.jobs_posted_count DESC
            """

            cursor.execute(build_sql)
            rows_inserted = cursor.rowcount

            context.log.info(f"Successfully inserted {rows_inserted} weekly company hiring records")

            # Step 3: Validate data quality and gather competitive intelligence statistics
            context.log.info("Validating company hiring data quality and gathering competitive intelligence metrics")

            validation_sql = f"""
            SELECT
                COUNT(*) as total_company_weeks,
                COUNT(DISTINCT company_key) as unique_companies,
                COUNT(DISTINCT week_key) as unique_weeks,

                -- Hiring metrics
                SUM(jobs_posted_count) as total_jobs_tracked,
                AVG(jobs_posted_count) as avg_jobs_per_company_week,
                MAX(jobs_posted_count) as max_jobs_per_company_week,

                -- Salary analysis
                COUNT(CASE WHEN avg_salary_offered IS NOT NULL THEN 1 END) as company_weeks_with_salary,
                AVG(avg_salary_offered) as overall_avg_salary_offered,
                AVG(median_salary_offered) as overall_median_salary_offered,
                AVG(salary_range_width) as avg_salary_range_width,

                -- Work arrangement patterns
                AVG(remote_jobs_percentage) as avg_remote_percentage,
                AVG(hybrid_jobs_percentage) as avg_hybrid_percentage,
                AVG(on_site_jobs_percentage) as avg_onsite_percentage,

                -- Role distribution patterns
                AVG(entry_level_percentage) as avg_entry_level_percentage,
                AVG(senior_level_percentage) as avg_senior_level_percentage,
                AVG(management_roles_percentage) as avg_management_percentage,

                -- Trend analysis
                COUNT(CASE WHEN hiring_trend_direction = 'Accelerating' THEN 1 END) as accelerating_companies,
                COUNT(CASE WHEN hiring_trend_direction = 'Declining' THEN 1 END) as declining_companies,
                COUNT(CASE WHEN hiring_trend_direction = 'Stable' THEN 1 END) as stable_companies,
                COUNT(CASE WHEN hiring_trend_direction = 'New' THEN 1 END) as new_companies,

                -- Temporal coverage
                MIN(week_start_date) as earliest_week,
                MAX(week_start_date) as latest_week

            FROM {table_name}
            """

            cursor.execute(validation_sql)
            validation_result = cursor.fetchone()

            # Step 4: Analyze hiring trend distribution
            context.log.info("Analyzing company hiring trend distribution and competitive intelligence")

            cursor.execute(f"""
            SELECT
                hiring_trend_direction,
                COUNT(*) as company_week_count,
                AVG(jobs_posted_count) as avg_hiring_volume,
                AVG(avg_salary_offered) as avg_salary_by_trend,
                AVG(remote_jobs_percentage) as avg_remote_by_trend
            FROM {table_name}
            WHERE week_over_week_change IS NOT NULL
            GROUP BY hiring_trend_direction
            ORDER BY company_week_count DESC
            """)

            trend_stats = [
                {
                    "hiring_trend_direction": row[0],
                    "company_week_count": row[1],
                    "avg_hiring_volume": float(row[2]) if row[2] is not None else 0.0,
                    "avg_salary_by_trend": float(row[3]) if row[3] is not None else 0.0,
                    "avg_remote_by_trend": float(row[4]) if row[4] is not None else 0.0
                }
                for row in cursor.fetchall()
            ]

            # Step 5: Top hiring companies analysis
            cursor.execute(f"""
            SELECT
                dc.COMPANY_NAME,
                dc.COMPANY_SIZE_CATEGORY,
                dc.INDUSTRY,
                AVG(fch.jobs_posted_count) as avg_weekly_hiring,
                AVG(fch.avg_salary_offered) as avg_salary_offered,
                AVG(fch.remote_jobs_percentage) as avg_remote_percentage,
                COUNT(*) as weeks_tracked
            FROM {table_name} fch
            JOIN BETTERJOBS_DB.ANALYTICS.DIM_COMPANY dc ON fch.company_key = dc.company_key
            WHERE fch.week_start_date >= CURRENT_DATE - 30  -- Last 4 weeks
              AND dc.is_current = TRUE
            GROUP BY dc.COMPANY_NAME, dc.COMPANY_SIZE_CATEGORY, dc.INDUSTRY
            ORDER BY avg_weekly_hiring DESC
            LIMIT 20
            """)

            top_companies_stats = [
                {
                    "company_name": row[0],
                    "company_size_category": row[1],
                    "industry": row[2],
                    "avg_weekly_hiring": float(row[3]) if row[3] is not None else 0.0,
                    "avg_salary_offered": float(row[4]) if row[4] is not None else 0.0,
                    "avg_remote_percentage": float(row[5]) if row[5] is not None else 0.0,
                    "weeks_tracked": row[6]
                }
                for row in cursor.fetchall()
            ]

            # Step 6: Data quality validation checks
            quality_issues = []

            # Check for companies with impossible percentages
            cursor.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE remote_jobs_percentage < 0 OR remote_jobs_percentage > 100
                   OR hybrid_jobs_percentage < 0 OR hybrid_jobs_percentage > 100
                   OR on_site_jobs_percentage < 0 OR on_site_jobs_percentage > 100
            """)
            invalid_percentages = cursor.fetchone()[0]
            if invalid_percentages > 0:
                quality_issues.append(f"{invalid_percentages} company weeks with invalid work arrangement percentages")

            # Check for unrealistic salary ranges (should be non-negative)
            cursor.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE salary_range_width IS NOT NULL AND salary_range_width < 0
            """)
            negative_salary_ranges = cursor.fetchone()[0]
            if negative_salary_ranges > 0:
                quality_issues.append(f"{negative_salary_ranges} company weeks with negative salary ranges")

            # Check for inconsistent job counts
            cursor.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE new_jobs_this_week > jobs_posted_count
            """)
            inconsistent_job_counts = cursor.fetchone()[0]
            if inconsistent_job_counts > 0:
                quality_issues.append(f"{inconsistent_job_counts} company weeks with inconsistent job counts")

            if quality_issues:
                context.log.warning(f"Data quality issues detected: {', '.join(quality_issues)}")

            # Step 7: Calculate derived statistics
            total_company_weeks = validation_result[0]
            salary_coverage = (validation_result[6] / total_company_weeks * 100) if total_company_weeks > 0 else 0
            accelerating_percentage = (validation_result[16] / total_company_weeks * 100) if total_company_weeks > 0 else 0
            declining_percentage = (validation_result[17] / total_company_weeks * 100) if total_company_weeks > 0 else 0

            context.log.info(f"Company hiring validation: {total_company_weeks} total company-week records, "
                           f"{validation_result[1]} unique companies, {validation_result[2]} unique weeks")

            context.log.info(f"Competitive intelligence: {validation_result[3]} total jobs tracked, "
                           f"avg {validation_result[4]:.1f} jobs per company-week, max {validation_result[5]} jobs")

            context.log.info(f"Trend distribution: {accelerating_percentage:.1f}% accelerating, "
                           f"{declining_percentage:.1f}% declining, avg salary ${validation_result[7]:,.0f}")

            # Add metadata for Dagster UI (convert all numeric types properly for Dagster compatibility)
            context.add_output_metadata({
                "total_company_weeks": MetadataValue.int(int(total_company_weeks)),
                "unique_companies": MetadataValue.int(int(validation_result[1])),
                "unique_weeks": MetadataValue.int(int(validation_result[2])),
                "total_jobs_tracked": MetadataValue.int(int(validation_result[3])),
                "avg_jobs_per_company_week": MetadataValue.float(float(validation_result[4]) if validation_result[4] is not None else 0.0),
                "max_jobs_per_company_week": MetadataValue.int(int(validation_result[5])),
                "salary_coverage_percentage": MetadataValue.float(float(salary_coverage)),
                "overall_avg_salary_offered": MetadataValue.float(float(validation_result[7]) if validation_result[7] is not None else 0.0),
                "overall_median_salary_offered": MetadataValue.float(float(validation_result[8]) if validation_result[8] is not None else 0.0),
                "avg_salary_range_width": MetadataValue.float(float(validation_result[9]) if validation_result[9] is not None else 0.0),
                "avg_remote_percentage": MetadataValue.float(float(validation_result[10]) if validation_result[10] is not None else 0.0),
                "avg_hybrid_percentage": MetadataValue.float(float(validation_result[11]) if validation_result[11] is not None else 0.0),
                "avg_onsite_percentage": MetadataValue.float(float(validation_result[12]) if validation_result[12] is not None else 0.0),
                "avg_entry_level_percentage": MetadataValue.float(float(validation_result[13]) if validation_result[13] is not None else 0.0),
                "avg_senior_level_percentage": MetadataValue.float(float(validation_result[14]) if validation_result[14] is not None else 0.0),
                "avg_management_percentage": MetadataValue.float(float(validation_result[15]) if validation_result[15] is not None else 0.0),
                "accelerating_companies": MetadataValue.int(int(validation_result[16])),
                "declining_companies": MetadataValue.int(int(validation_result[17])),
                "stable_companies": MetadataValue.int(int(validation_result[18])),
                "new_companies": MetadataValue.int(int(validation_result[19])),
                "accelerating_percentage": MetadataValue.float(float(accelerating_percentage)),
                "declining_percentage": MetadataValue.float(float(declining_percentage)),
                "earliest_week": MetadataValue.text(str(validation_result[20])),
                "latest_week": MetadataValue.text(str(validation_result[21])),
                "trend_distribution": MetadataValue.json(trend_stats),
                "top_hiring_companies_last_4_weeks": MetadataValue.json(top_companies_stats),
                "quality_issues_count": MetadataValue.int(len(quality_issues))
            })

            return {
                "status": "success",
                "table_name": table_name,
                "rows_inserted": rows_inserted,
                "total_company_weeks": total_company_weeks,
                "competitive_coverage": {
                    "unique_companies": validation_result[1],
                    "unique_weeks": validation_result[2],
                    "total_jobs_tracked": validation_result[3],
                    "avg_jobs_per_company_week": float(validation_result[4]) if validation_result[4] is not None else 0.0,
                    "max_jobs_per_company_week": validation_result[5]
                },
                "salary_intelligence": {
                    "salary_coverage_percentage": salary_coverage,
                    "overall_avg_salary_offered": float(validation_result[7]) if validation_result[7] is not None else 0.0,
                    "overall_median_salary_offered": float(validation_result[8]) if validation_result[8] is not None else 0.0,
                    "avg_salary_range_width": float(validation_result[9]) if validation_result[9] is not None else 0.0
                },
                "work_arrangement_intelligence": {
                    "avg_remote_percentage": float(validation_result[10]) if validation_result[10] is not None else 0.0,
                    "avg_hybrid_percentage": float(validation_result[11]) if validation_result[11] is not None else 0.0,
                    "avg_onsite_percentage": float(validation_result[12]) if validation_result[12] is not None else 0.0
                },
                "role_distribution_intelligence": {
                    "avg_entry_level_percentage": float(validation_result[13]) if validation_result[13] is not None else 0.0,
                    "avg_senior_level_percentage": float(validation_result[14]) if validation_result[14] is not None else 0.0,
                    "avg_management_percentage": float(validation_result[15]) if validation_result[15] is not None else 0.0
                },
                "hiring_trend_analysis": {
                    "accelerating_companies": validation_result[16],
                    "declining_companies": validation_result[17],
                    "stable_companies": validation_result[18],
                    "new_companies": validation_result[19],
                    "accelerating_percentage": accelerating_percentage,
                    "declining_percentage": declining_percentage,
                    "trend_distribution": trend_stats
                },
                "quality_metrics": {
                    "quality_issues": quality_issues
                },
                "temporal_coverage": {
                    "earliest_week": str(validation_result[20]),
                    "latest_week": str(validation_result[21]),
                    "weeks_covered": validation_result[2]
                },
                "top_companies_analysis": top_companies_stats
            }

        finally:
            cursor.close()


@asset(
    deps=["analytics_fact_job_postings"],
    description="Create weekly market summary table for executive dashboard performance",
    group_name="3b_analytics_facts_aggregates_analysis",
    kinds={"snowflake", "SQL"}
)
def analytics_market_weekly_summary(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build weekly market summary table from FACT_JOB_POSTINGS for executive dashboard performance.

    This asset creates pre-aggregated weekly market metrics to ensure sub-second executive dashboard
    performance while minimizing storage overhead (~52 records per year).

    Processing Logic:
    1. Aggregate job postings by week (Sunday-Saturday) from FACT_JOB_POSTINGS
    2. Calculate core market metrics (job counts, velocity, growth rates)
    3. Compute salary intelligence using denormalized annual USD fields
    4. Analyze work arrangement trends from WORK_TYPE field
    5. Generate data quality metrics and sample size indicators
    6. Implement incremental processing for new weeks only (FUTURE ENHANCEMENT)

    Data Quality Rules:
    - Include only active job postings (IS_ACTIVE_POSTING = TRUE)
    - Use salary data only where SALARY_CONFIDENCE >= 0.6
    - Apply LLM confidence filtering (LLM_OVERALL_CONFIDENCE >= 0.5)
    - Require minimum 100 jobs per week for statistical validity
    - Validate week-over-week calculations for outlier detection

    Performance Features:
    - Weekly grain provides optimal balance of detail vs. performance
    - Pre-calculated metrics eliminate dashboard query complexity
    - Partitioned by WEEK_ENDING_DATE for efficient time-based queries
    - Clustered by WEEK_KEY for analytical access patterns

    Business Intelligence:
    - Executive KPI tracking: hiring velocity, market temperature, growth trends
    - Salary market intelligence: median/average compensation trends
    - Work arrangement insights: remote/hybrid/onsite adoption patterns
    - Data quality monitoring: completeness and confidence metrics

    Returns:
        Dict containing processing statistics and market summary metrics
    """

    # Ensure the table exists using Schema-as-Code pattern
    table_name = ensure_object_exists("tables/analytics_market_weekly_summary.sql", snowflake, context)

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting weekly market summary aggregation from fact job postings")

            # Step 1: Clear existing data for complete refresh
            context.log.info("Clearing existing market weekly summary data")
            cursor.execute(f"TRUNCATE TABLE {table_name}")

            # Step 2: Build market weekly summary with comprehensive metrics
            context.log.info("Building weekly market summary with executive dashboard metrics")

            build_sql = f"""
            INSERT INTO {table_name} (
                SUMMARY_KEY,
                WEEK_ENDING_DATE,
                WEEK_KEY,
                TOTAL_JOBS_POSTED,
                TOTAL_ACTIVE_JOBS,
                NEW_JOBS_POSTED,
                WEEK_OVER_WEEK_GROWTH_RATE,
                POSTING_VELOCITY_DAILY,
                MEDIAN_SALARY_ALL_ROLES,
                AVG_SALARY_ALL_ROLES,
                REMOTE_WORK_PERCENTAGE,
                HYBRID_WORK_PERCENTAGE,
                ONSITE_WORK_PERCENTAGE,
                SAMPLE_SIZE,
                DATA_QUALITY_SCORE,
                CREATED_TIMESTAMP
            )
            WITH weekly_job_data AS (
                SELECT
                    dd.WEEK_BEGINNING_DATE as week_start_date,
                    dd.WEEK_ENDING_DATE as week_ending_date,
                    dd.WEEK_KEY,

                    -- Job counting logic
                    COUNT(*) as total_jobs_posted,
                    COUNT(CASE WHEN IS_ACTIVE_POSTING THEN 1 END) as total_active_jobs,
                    COUNT(CASE WHEN DATE_TRUNC('week', FIRST_POSTED_DATE) = DATE_TRUNC('week', FIRST_POSTED_DATE) THEN 1 END) as new_jobs_posted,

                    -- Salary calculations (using denormalized fields for performance)
                    MEDIAN(CASE WHEN SALARY_CONFIDENCE >= 0.6 AND SALARY_MIDPOINT_ANNUAL_USD > 0
                                THEN SALARY_MIDPOINT_ANNUAL_USD END) as median_salary_all_roles,
                    AVG(CASE WHEN SALARY_CONFIDENCE >= 0.6 AND SALARY_MIDPOINT_ANNUAL_USD > 0
                             THEN SALARY_MIDPOINT_ANNUAL_USD END) as avg_salary_all_roles,

                    -- Work arrangement percentages
                    COUNT(CASE WHEN WORK_TYPE = 'Remote' THEN 1 END)::FLOAT / COUNT(*) * 100 as remote_work_percentage,
                    COUNT(CASE WHEN WORK_TYPE = 'Hybrid' THEN 1 END)::FLOAT / COUNT(*) * 100 as hybrid_work_percentage,
                    COUNT(CASE WHEN WORK_TYPE = 'On-site' THEN 1 END)::FLOAT / COUNT(*) * 100 as onsite_work_percentage,

                    -- Quality metrics
                    COUNT(*) as sample_size,
                    AVG(DATA_QUALITY_SCORE) as data_quality_score

                FROM BETTERJOBS_DB.ANALYTICS.FACT_JOB_POSTINGS
                WHERE IS_ACTIVE_POSTING = TRUE
                  AND LLM_OVERALL_CONFIDENCE >= 0.5
                  AND FIRST_POSTED_DATE >= CURRENT_DATE - 365  -- 1 year retention
                GROUP BY week_start_date, week_ending_date, week_key
                HAVING COUNT(*) >= 100  -- Minimum statistical validity
            ),

            weekly_with_trends AS (
                SELECT wjd.*,
                       LAG(wjd.total_jobs_posted, 1) OVER (ORDER BY wjd.week_start_date) as prev_week_jobs,
                       ((wjd.total_jobs_posted::FLOAT / LAG(wjd.total_jobs_posted, 1) OVER (ORDER BY wjd.week_start_date)) - 1) * 100 as week_over_week_growth_rate,
                       wjd.total_jobs_posted::FLOAT / 7 as posting_velocity_daily
                FROM weekly_job_data wjd
            )

            SELECT
                'MWS_' || week_key as SUMMARY_KEY,
                week_ending_date as WEEK_ENDING_DATE,
                week_key as WEEK_KEY,
                total_jobs_posted as TOTAL_JOBS_POSTED,
                total_active_jobs as TOTAL_ACTIVE_JOBS,
                new_jobs_posted as NEW_JOBS_POSTED,
                COALESCE(week_over_week_growth_rate, 0) as WEEK_OVER_WEEK_GROWTH_RATE,
                posting_velocity_daily as POSTING_VELOCITY_DAILY,
                ROUND(median_salary_all_roles, 0) as MEDIAN_SALARY_ALL_ROLES,
                ROUND(avg_salary_all_roles, 0) as AVG_SALARY_ALL_ROLES,
                ROUND(remote_work_percentage, 2) as REMOTE_WORK_PERCENTAGE,
                ROUND(hybrid_work_percentage, 2) as HYBRID_WORK_PERCENTAGE,
                ROUND(onsite_work_percentage, 2) as ONSITE_WORK_PERCENTAGE,
                sample_size as SAMPLE_SIZE,
                ROUND(data_quality_score, 3) as DATA_QUALITY_SCORE,
                CURRENT_TIMESTAMP as CREATED_TIMESTAMP
            FROM weekly_with_trends
            WHERE week_ending_date <= CURRENT_DATE  -- Don't include future weeks
            ORDER BY week_ending_date DESC
            """

            cursor.execute(build_sql)
            rows_inserted = cursor.rowcount

            context.log.info(f"Successfully inserted {rows_inserted} weekly market summary records")

            # Step 3: Validate data quality and gather statistics
            context.log.info("Validating market summary data quality and gathering executive metrics")

            validation_sql = f"""
            SELECT
                COUNT(*) as total_weeks,
                MIN(WEEK_ENDING_DATE) as earliest_week,
                MAX(WEEK_ENDING_DATE) as latest_week,
                AVG(TOTAL_JOBS_POSTED) as avg_weekly_jobs,
                AVG(MEDIAN_SALARY_ALL_ROLES) as avg_median_salary,
                AVG(REMOTE_WORK_PERCENTAGE) as avg_remote_percentage,
                AVG(WEEK_OVER_WEEK_GROWTH_RATE) as avg_growth_rate,
                AVG(DATA_QUALITY_SCORE) as avg_data_quality,

                -- Quality checks
                COUNT(CASE WHEN TOTAL_JOBS_POSTED < 100 THEN 1 END) as weeks_below_threshold,
                COUNT(CASE WHEN MEDIAN_SALARY_ALL_ROLES IS NULL THEN 1 END) as weeks_missing_salary,
                COUNT(CASE WHEN ABS(WEEK_OVER_WEEK_GROWTH_RATE) > 100 THEN 1 END) as weeks_extreme_growth

            FROM {table_name}
            """

            cursor.execute(validation_sql)
            validation_result = cursor.fetchone()

            # Step 4: Calculate derived statistics
            total_weeks = validation_result[0]

            context.log.info(f"Market summary validation: {total_weeks} weekly records, "
                           f"avg {validation_result[3]:.0f} jobs/week, avg growth {validation_result[6]:.1f}%")

            context.log.info(f"Executive metrics: avg median salary ${validation_result[4]:,.0f}, "
                           f"avg remote work {validation_result[5]:.1f}%")

            # Add metadata for Dagster UI
            context.add_output_metadata({
                "total_weeks": MetadataValue.int(int(total_weeks)),
                "earliest_week": MetadataValue.text(str(validation_result[1])),
                "latest_week": MetadataValue.text(str(validation_result[2])),
                "avg_weekly_jobs": MetadataValue.float(float(validation_result[3]) if validation_result[3] is not None else 0.0),
                "avg_median_salary": MetadataValue.float(float(validation_result[4]) if validation_result[4] is not None else 0.0),
                "avg_remote_percentage": MetadataValue.float(float(validation_result[5]) if validation_result[5] is not None else 0.0),
                "avg_growth_rate": MetadataValue.float(float(validation_result[6]) if validation_result[6] is not None else 0.0),
                "avg_data_quality": MetadataValue.float(float(validation_result[7]) if validation_result[7] is not None else 0.0),
                "weeks_below_threshold": MetadataValue.int(int(validation_result[8])),
                "weeks_missing_salary": MetadataValue.int(int(validation_result[9])),
                "weeks_extreme_growth": MetadataValue.int(int(validation_result[10]))
            })

            return {
                "status": "success",
                "table_name": table_name,
                "rows_inserted": rows_inserted,
                "total_weeks": total_weeks,
                "executive_metrics": {
                    "avg_weekly_jobs": float(validation_result[3]) if validation_result[3] is not None else 0.0,
                    "avg_median_salary": float(validation_result[4]) if validation_result[4] is not None else 0.0,
                    "avg_remote_percentage": float(validation_result[5]) if validation_result[5] is not None else 0.0,
                    "avg_growth_rate": float(validation_result[6]) if validation_result[6] is not None else 0.0,
                    "avg_data_quality": float(validation_result[7]) if validation_result[7] is not None else 0.0
                },
                "quality_metrics": {
                    "weeks_below_threshold": validation_result[8],
                    "weeks_missing_salary": validation_result[9],
                    "weeks_extreme_growth": validation_result[10]
                },
                "temporal_coverage": {
                    "earliest_week": str(validation_result[1]),
                    "latest_week": str(validation_result[2]),
                    "total_weeks": total_weeks
                }
            }

        finally:
            cursor.close()


@asset(
    deps=["analytics_fact_skills_demand_weekly", "analytics_fact_job_postings", "analytics_dim_skills", "analytics_dim_date"],
    description="Create skills trend analysis table for technology intelligence and market insights",
    group_name="3b_analytics_facts_aggregates_analysis",
    kinds={"snowflake", "SQL"}
)
def analytics_skills_trend_analysis(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Build skills trend analysis from FACT_SKILLS_DEMAND_WEEKLY with enhanced seniority breakdown.

    This asset creates comprehensive skills market intelligence by aggregating weekly skills demand
    data and adding unique insights like seniority distribution analysis that are not available
    in the base weekly fact table.

    Processing Logic:
    1. Source primary metrics from FACT_SKILLS_DEMAND_WEEKLY for efficiency
    2. Calculate monthly growth trends using 4-week rolling averages
    3. Add seniority distribution analysis by joining with FACT_JOB_POSTINGS
    4. Compute ranking changes week-over-week and month-over-month
    5. Generate unique analysis keys for each skill-week combination
    6. Apply data quality filtering and statistical validation

    Unique Value-Add (vs FACT_SKILLS_DEMAND_WEEKLY):
    - Monthly trend aggregations with year-over-year comparisons
    - Seniority distribution breakdown (entry/mid/senior demand by skill)
    - Enhanced ranking change analysis with historical context
    - Cross-skill competitive analysis and market share insights

    Data Quality Rules:
    - Source from high-confidence weekly aggregates (FACT_SKILLS_DEMAND_WEEKLY)
    - Require minimum 5 jobs per skill-week for statistical validity
    - Apply skill confidence filtering (avg_skill_confidence >= 0.5)
    - Validate growth rate calculations for outlier detection

    Returns:
        Dict containing processing statistics and skills trend metrics
    """

    # Ensure the table exists using Schema-as-Code pattern
    table_name = ensure_object_exists("tables/analytics_skills_trend_analysis.sql", snowflake, context)

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting skills trend analysis build from weekly skills demand data")

            # Step 1: Clear existing data for complete refresh
            context.log.info("Clearing existing skills trend analysis data")
            cursor.execute(f"TRUNCATE TABLE {table_name}")

            # Step 2: Build skills trend analysis with enhanced metrics
            context.log.info("Building skills trend analysis with seniority breakdown and enhanced metrics")

            build_sql = f"""
            INSERT INTO {table_name} (
                ANALYSIS_KEY,
                ANALYSIS_DATE,
                SKILL_NAME,
                WEEK_KEY,
                JOBS_REQUIRING_SKILL,
                MARKET_PENETRATION_RATE,
                DEMAND_RANK_OVERALL,
                DEMAND_GROWTH_WEEKLY,
                DEMAND_GROWTH_MONTHLY,
                RANK_CHANGE_WEEKLY,
                RANK_CHANGE_MONTHLY,
                AVERAGE_SALARY_WITH_SKILL,
                SALARY_PREMIUM_PERCENTAGE,
                REMOTE_AVAILABILITY_RATE,
                ENTRY_LEVEL_DEMAND,
                MID_LEVEL_DEMAND,
                MANAGER_LEVEL_DEMAND,
                SENIOR_LEVEL_DEMAND,
                SAMPLE_SIZE,
                DATA_QUALITY_SCORE,
                CREATED_TIMESTAMP
            )
            WITH weekly_skills_base AS (
                SELECT
                    fsdw.WEEK_KEY,
                    ds.CANONICAL_FORM as skill_canonical_form,  -- Use canonical form instead of SKILL_KEY
                    ds.SKILL_NAME,
                    ds.SKILL_CATEGORY,
                    fsdw.WEEK_START_DATE as analysis_date,

                    -- Aggregate metrics across all skill variants with same canonical form
                    SUM(fsdw.ACTIVE_JOBS_WITH_SKILL) as jobs_requiring_skill,
                    AVG(fsdw.SKILL_PENETRATION_RATE) as market_penetration_rate,
                    MIN(fsdw.SKILL_RANK_OVERALL) as demand_rank_overall,  -- Best rank among variants
                    AVG(fsdw.AVG_SALARY_MIDPOINT_ANNUAL_USD) as average_salary_with_skill,
                    AVG(fsdw.SALARY_PREMIUM_PERCENTAGE) as salary_premium_percentage,
                    AVG(fsdw.REMOTE_SKILL_PERCENTAGE) as remote_availability_rate,
                    SUM(fsdw.SAMPLE_SIZE) as sample_size,
                    AVG(fsdw.AVG_SKILL_CONFIDENCE) as data_quality_score,
                    AVG(fsdw.WEEK_OVER_WEEK_GROWTH_RATE) as demand_growth_weekly

                FROM BETTERJOBS_DB.ANALYTICS.FACT_SKILLS_DEMAND_WEEKLY fsdw
                INNER JOIN BETTERJOBS_DB.ANALYTICS.DIM_SKILLS ds ON fsdw.SKILL_KEY = ds.SKILL_KEY
                WHERE fsdw.AVG_SKILL_CONFIDENCE >= 0.5
                  AND fsdw.SAMPLE_SIZE >= 5
                  AND fsdw.WEEK_START_DATE >= CURRENT_DATE - 365  -- 1 year retention
                  AND ds.CANONICAL_FORM IS NOT NULL
                GROUP BY fsdw.WEEK_KEY, ds.CANONICAL_FORM, ds.SKILL_NAME, ds.SKILL_CATEGORY, fsdw.WEEK_START_DATE
                HAVING SUM(fsdw.SAMPLE_SIZE) >= 5  -- Ensure consolidated skills have sufficient sample size
            ),

            skills_with_monthly_trends AS (
                SELECT wsb.*,
                       -- Monthly growth calculation (4-week comparison)
                       LAG(wsb.jobs_requiring_skill, 4) OVER (
                           PARTITION BY wsb.skill_canonical_form
                           ORDER BY wsb.analysis_date
                       ) as jobs_4_weeks_ago,

                       CASE WHEN LAG(wsb.jobs_requiring_skill, 4) OVER (
                                PARTITION BY wsb.skill_canonical_form ORDER BY wsb.analysis_date) > 0
                            THEN ((wsb.jobs_requiring_skill::FLOAT /
                                  LAG(wsb.jobs_requiring_skill, 4) OVER (
                                      PARTITION BY wsb.skill_canonical_form ORDER BY wsb.analysis_date)) - 1) * 100
                            ELSE NULL END as demand_growth_monthly
                FROM weekly_skills_base wsb
            ),

            skills_with_ranking_changes AS (
                SELECT swmt.*,
                       -- Weekly ranking changes
                       (swmt.demand_rank_overall - LAG(swmt.demand_rank_overall, 1) OVER (
                           PARTITION BY swmt.skill_canonical_form ORDER BY swmt.analysis_date
                       )) as rank_change_weekly,

                       -- Monthly ranking changes (4-week comparison)
                       (swmt.demand_rank_overall - LAG(swmt.demand_rank_overall, 4) OVER (
                           PARTITION BY swmt.skill_canonical_form ORDER BY swmt.analysis_date
                       )) as rank_change_monthly
                FROM skills_with_monthly_trends swmt
            ),

            seniority_breakdown AS (
                SELECT
                    dd.WEEK_KEY as week_key,
                    ds.CANONICAL_FORM as skill_canonical_form,
                    COUNT(CASE WHEN LOWER(fjp.SENIORITY_LEVEL) LIKE '%entry%' THEN 1 END) as entry_level_jobs,
                    COUNT(CASE WHEN LOWER(fjp.SENIORITY_LEVEL) LIKE '%junior%' OR LOWER(fjp.SENIORITY_LEVEL) LIKE '%mid%' OR LOWER(fjp.SENIORITY_LEVEL) LIKE '%intermediate%' THEN 1 END) as mid_level_jobs,
                    COUNT(CASE WHEN LOWER(fjp.SENIORITY_LEVEL) LIKE '%senior%' OR LOWER(fjp.SENIORITY_LEVEL) LIKE '%staff%' OR LOWER(fjp.SENIORITY_LEVEL) LIKE '%principal%' THEN 1 END) as senior_level_jobs,
                    COUNT(CASE WHEN LOWER(fjp.SENIORITY_LEVEL) LIKE '%manager%' OR LOWER(fjp.SENIORITY_LEVEL) LIKE '%director%' OR LOWER(fjp.SENIORITY_LEVEL) LIKE '%vp%' THEN 1 END) as manager_level_jobs
                FROM BETTERJOBS_DB.ANALYTICS.FACT_JOB_POSTINGS fjp
                INNER JOIN BETTERJOBS_DB.ANALYTICS.JOB_SKILLS_BRIDGE jsb
                    ON fjp.JOB_POSTING_KEY = jsb.JOB_POSTING_KEY
                INNER JOIN BETTERJOBS_DB.ANALYTICS.DIM_SKILLS ds
                    ON jsb.SKILL_KEY = ds.SKILL_KEY
                INNER JOIN BETTERJOBS_DB.ANALYTICS.DIM_DATE dd
                    ON fjp.FIRST_POSTED_DATE = dd.FULL_DATE
                WHERE fjp.IS_ACTIVE_POSTING = TRUE
                    AND fjp.LLM_OVERALL_CONFIDENCE >= 0.5
                    AND fjp.FIRST_POSTED_DATE >= CURRENT_DATE - 365
                    AND ds.CANONICAL_FORM IS NOT NULL
                GROUP BY dd.WEEK_KEY, ds.CANONICAL_FORM
            ),

            skills_with_seniority AS (
                SELECT swrc.*,
                       -- Seniority breakdown from job postings
                       COALESCE(sb.entry_level_jobs, 0) as entry_level_demand,
                       COALESCE(sb.mid_level_jobs, 0) as mid_level_demand,
                       COALESCE(sb.senior_level_jobs, 0) as senior_level_demand,
                       COALESCE(sb.manager_level_jobs, 0) as manager_level_demand
                FROM skills_with_ranking_changes swrc
                LEFT JOIN seniority_breakdown sb
                    ON swrc.skill_canonical_form = sb.skill_canonical_form
                    AND swrc.week_key = sb.week_key
            )

            SELECT
                LOWER('sta_' || sws.week_key || '_' || REPLACE(sws.skill_canonical_form, ' ', '_')) as ANALYSIS_KEY,
                sws.analysis_date as ANALYSIS_DATE,
                sws.skill_canonical_form as SKILL_NAME,
                sws.week_key as WEEK_KEY,
                sws.jobs_requiring_skill as JOBS_REQUIRING_SKILL,
                sws.market_penetration_rate as MARKET_PENETRATION_RATE,
                sws.demand_rank_overall as DEMAND_RANK_OVERALL,
                sws.demand_growth_weekly as DEMAND_GROWTH_WEEKLY,
                sws.demand_growth_monthly as DEMAND_GROWTH_MONTHLY,
                sws.rank_change_weekly as RANK_CHANGE_WEEKLY,
                sws.rank_change_monthly as RANK_CHANGE_MONTHLY,
                ROUND(sws.average_salary_with_skill, 0) as AVERAGE_SALARY_WITH_SKILL,
                ROUND(sws.salary_premium_percentage, 2) as SALARY_PREMIUM_PERCENTAGE,
                ROUND(sws.remote_availability_rate, 2) as REMOTE_AVAILABILITY_RATE,
                sws.entry_level_demand as ENTRY_LEVEL_DEMAND,
                sws.mid_level_demand as MID_LEVEL_DEMAND,
                sws.senior_level_demand as SENIOR_LEVEL_DEMAND,
                sws.manager_level_demand as MANAGER_LEVEL_DEMAND,
                sws.sample_size as SAMPLE_SIZE,
                ROUND(sws.data_quality_score, 3) as DATA_QUALITY_SCORE,
                CURRENT_TIMESTAMP as CREATED_TIMESTAMP
            FROM skills_with_seniority sws
            ORDER BY sws.analysis_date DESC, sws.demand_rank_overall ASC
            """

            cursor.execute(build_sql)
            rows_inserted = cursor.rowcount

            context.log.info(f"Successfully inserted {rows_inserted} skills trend analysis records")

            # Step 3: Validate data quality and gather skill intelligence statistics
            context.log.info("Validating skills trend analysis data quality and gathering market intelligence")

            validation_sql = f"""
            SELECT
                COUNT(*) as total_skill_weeks,
                COUNT(DISTINCT SKILL_NAME) as unique_skills,
                COUNT(DISTINCT WEEK_KEY) as weeks_covered,
                MIN(ANALYSIS_DATE) as earliest_week,
                MAX(ANALYSIS_DATE) as latest_week,
                AVG(JOBS_REQUIRING_SKILL) as avg_skill_demand,
                AVG(MARKET_PENETRATION_RATE) as avg_penetration_rate,
                AVG(AVERAGE_SALARY_WITH_SKILL) as avg_skill_salary,
                AVG(REMOTE_AVAILABILITY_RATE) as avg_remote_rate,
                AVG(DATA_QUALITY_SCORE) as avg_data_quality,

                -- Growth and trend analysis
                AVG(DEMAND_GROWTH_WEEKLY) as avg_weekly_growth,
                AVG(DEMAND_GROWTH_MONTHLY) as avg_monthly_growth,
                COUNT(CASE WHEN DEMAND_GROWTH_WEEKLY > 10 THEN 1 END) as high_growth_skills,
                COUNT(CASE WHEN DEMAND_GROWTH_WEEKLY < -10 THEN 1 END) as declining_skills,

                -- Seniority distribution
                AVG(ENTRY_LEVEL_DEMAND) as avg_entry_demand,
                AVG(MID_LEVEL_DEMAND) as avg_mid_level_demand,
                AVG(SENIOR_LEVEL_DEMAND) as avg_senior_demand,
                AVG(MANAGER_LEVEL_DEMAND) as avg_manager_demand,

                -- Quality checks
                COUNT(CASE WHEN JOBS_REQUIRING_SKILL < 5 THEN 1 END) as records_below_threshold,
                COUNT(CASE WHEN AVERAGE_SALARY_WITH_SKILL IS NULL THEN 1 END) as records_missing_salary,
                COUNT(CASE WHEN ABS(DEMAND_GROWTH_WEEKLY) > 200 THEN 1 END) as records_extreme_growth

            FROM {table_name}
            """

            cursor.execute(validation_sql)
            validation_result = cursor.fetchone()

            # Step 4: Calculate derived statistics
            total_skill_weeks = validation_result[0]
            unique_skills = validation_result[1]
            weeks_covered = validation_result[2]

            context.log.info(f"Skills trend analysis validation: {total_skill_weeks} skill-week records, "
                           f"{unique_skills} unique skills, {weeks_covered} weeks covered")

            # Handle null values from aggregate functions when no data exists
            avg_demand = validation_result[5] if validation_result[5] is not None else 0.0
            avg_penetration = validation_result[6] if validation_result[6] is not None else 0.0
            avg_salary = validation_result[7] if validation_result[7] is not None else 0.0

            context.log.info(f"Skills intelligence: avg demand {avg_demand:.1f} jobs/skill, "
                           f"avg penetration {avg_penetration:.1f}%, avg salary ${avg_salary:,.0f}")

            context.log.info(f"Trend analysis: {validation_result[11]} high-growth skills, "
                           f"{validation_result[12]} declining skills")

            # Add metadata for Dagster UI - with null safety for all values
            context.add_output_metadata({
                "total_skill_weeks": MetadataValue.int(int(total_skill_weeks)),
                "unique_skills": MetadataValue.int(int(unique_skills)),
                "weeks_covered": MetadataValue.int(int(weeks_covered)),
                "earliest_week": MetadataValue.text(str(validation_result[3]) if validation_result[3] is not None else "N/A"),
                "latest_week": MetadataValue.text(str(validation_result[4]) if validation_result[4] is not None else "N/A"),
                "avg_skill_demand": MetadataValue.float(float(validation_result[5]) if validation_result[5] is not None else 0.0),
                "avg_penetration_rate": MetadataValue.float(float(validation_result[6]) if validation_result[6] is not None else 0.0),
                "avg_skill_salary": MetadataValue.float(float(validation_result[7]) if validation_result[7] is not None else 0.0),
                "avg_remote_rate": MetadataValue.float(float(validation_result[8]) if validation_result[8] is not None else 0.0),
                "avg_data_quality": MetadataValue.float(float(validation_result[9]) if validation_result[9] is not None else 0.0),
                "avg_weekly_growth": MetadataValue.float(float(validation_result[10]) if validation_result[10] is not None else 0.0),
                "avg_monthly_growth": MetadataValue.float(float(validation_result[11]) if validation_result[11] is not None else 0.0),
                "high_growth_skills": MetadataValue.int(int(validation_result[12] if validation_result[12] is not None else 0)),
                "declining_skills": MetadataValue.int(int(validation_result[13] if validation_result[13] is not None else 0)),
                "records_below_threshold": MetadataValue.int(int(validation_result[17] if validation_result[17] is not None else 0)),
                "records_missing_salary": MetadataValue.int(int(validation_result[18] if validation_result[18] is not None else 0)),
                "records_extreme_growth": MetadataValue.int(int(validation_result[19] if validation_result[19] is not None else 0))
            })

            return {
                "status": "success",
                "table_name": table_name,
                "rows_inserted": rows_inserted,
                "skills_intelligence": {
                    "total_skill_weeks": total_skill_weeks,
                    "unique_skills": unique_skills,
                    "weeks_covered": weeks_covered,
                    "avg_skill_demand": float(validation_result[5]) if validation_result[5] is not None else 0.0,
                    "avg_penetration_rate": float(validation_result[6]) if validation_result[6] is not None else 0.0,
                    "avg_skill_salary": float(validation_result[7]) if validation_result[7] is not None else 0.0,
                    "avg_remote_rate": float(validation_result[8]) if validation_result[8] is not None else 0.0
                },
                "trend_analysis": {
                    "avg_weekly_growth": float(validation_result[10]) if validation_result[10] is not None else 0.0,
                    "avg_monthly_growth": float(validation_result[11]) if validation_result[11] is not None else 0.0,
                    "high_growth_skills": validation_result[12] if validation_result[12] is not None else 0,
                    "declining_skills": validation_result[13] if validation_result[13] is not None else 0
                },
                "seniority_distribution": {
                    "avg_entry_demand": float(validation_result[14]) if validation_result[14] is not None else 0.0,
                    "avg_mid_level_demand": float(validation_result[15]) if validation_result[15] is not None else 0.0,
                    "avg_senior_demand": float(validation_result[16]) if validation_result[16] is not None else 0.0,
                    "avg_manager_demand": float(validation_result[17]) if validation_result[17] is not None else 0.0
                },
                "quality_metrics": {
                    "records_below_threshold": validation_result[18] if validation_result[18] is not None else 0,
                    "records_missing_salary": validation_result[19] if validation_result[19] is not None else 0,
                    "records_extreme_growth": validation_result[20] if validation_result[20] is not None else 0,
                    "avg_data_quality": float(validation_result[9]) if validation_result[9] is not None else 0.0
                },
                "temporal_coverage": {
                    "earliest_week": str(validation_result[3]) if validation_result[3] is not None else "N/A",
                    "latest_week": str(validation_result[4]) if validation_result[4] is not None else "N/A",
                    "weeks_covered": weeks_covered
                }
            }

        finally:
            cursor.close()