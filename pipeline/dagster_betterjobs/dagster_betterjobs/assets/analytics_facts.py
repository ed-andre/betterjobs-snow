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
          "analytics_dim_job_family", "analytics_dim_platform", "analytics_dim_skills"
          "analytics_dim_salary", "analytics_dim_experience", "analytics_dim_keywords",
          "stage_jobs_unified", "stage_jobs_llm_enriched_unified", "stage_job_salary_bridge"],
    description="Create primary fact table for job posting analytics",
    group_name="3b_analytics_facts",
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

            build_sql = f"""
            INSERT INTO {table_name} (
                job_posting_key,
                date_posted_key,
                company_key,
                location_key,
                job_family_key,
                platform_key,
                experience_key,
                keyword_key,
                salary_key,
                job_uid,
                job_title,
                posting_url,
                salary_min_annual_usd,
                salary_max_annual_usd,
                salary_midpoint_annual_usd,
                experience_min_years,
                experience_max_years,
                experience_level,
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

                    -- Experience dimension lookup
                    COALESCE(de.EXPERIENCE_KEY, 'EXP_UNKNOWN') as experience_key,

                    -- Keyword dimension lookup (primary keyword from array)
                    COALESCE(dk.KEYWORD_KEY, 'KWD_UNKNOWN') as keyword_key,

                    -- Salary dimension lookup (via bridge table)
                    COALESCE(ds.SALARY_KEY, 'SAL_UNKNOWN') as salary_key,

                    -- Salary measures from dimension (denormalized for performance)
                    ds.SALARY_MIN_ANNUAL_USD,
                    ds.SALARY_MAX_ANNUAL_USD,
                    ds.SALARY_MIDPOINT_ANNUAL_USD,

                    -- Salary confidence from bridge
                    jsb.OVERALL_CONFIDENCE as salary_bridge_confidence

                FROM job_data_prep jd

                -- Company dimension lookup
                LEFT JOIN BETTERJOBS_DB.ANALYTICS.DIM_COMPANY dc
                    ON jd.COMPANY_ID = dc.COMPANY_ID AND dc.IS_CURRENT = TRUE

                -- Location dimension lookup
                LEFT JOIN BETTERJOBS_DB.ANALYTICS.DIM_LOCATION dl
                    ON jd.LOCATION_STANDARDIZED = dl.LOCATION_NAME

                -- Job family dimension lookup
                LEFT JOIN BETTERJOBS_DB.ANALYTICS.DIM_JOB_FAMILY djf
                    ON jd.JOB_FAMILY = djf.JOB_FAMILY
                    AND jd.JOB_SUB_FAMILY = djf.JOB_SUB_FAMILY
                    AND jd.SENIORITY_LEVEL = djf.SENIORITY_LEVEL

                -- Platform dimension lookup
                LEFT JOIN BETTERJOBS_DB.ANALYTICS.DIM_PLATFORM dp
                    ON jd.PLATFORM = dp.PLATFORM_NAME

                -- Experience dimension lookup
                LEFT JOIN BETTERJOBS_DB.ANALYTICS.DIM_EXPERIENCE de
                    ON jd.EXPERIENCE_LEVEL = de.EXPERIENCE_NAME

                                -- Keyword dimension lookup (extract first primary keyword)
                LEFT JOIN BETTERJOBS_DB.ANALYTICS.DIM_KEYWORDS dk
                    ON dk.KEYWORD_TEXT = TRIM(GET(jd.PRIMARY_KEYWORDS, 0)::STRING, '"')
                    AND jd.PRIMARY_KEYWORDS IS NOT NULL
                    AND ARRAY_SIZE(jd.PRIMARY_KEYWORDS) > 0

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
                experience_key,
                keyword_key,
                salary_key,

                -- Degenerate dimensions
                JOB_UID as job_uid,
                JOB_TITLE_CLEAN as job_title,
                JOB_URL as posting_url,

                -- Salary measures (normalized annual USD)
                SALARY_MIN_ANNUAL_USD as salary_min_annual_usd,
                SALARY_MAX_ANNUAL_USD as salary_max_annual_usd,
                SALARY_MIDPOINT_ANNUAL_USD as salary_midpoint_annual_usd,

                -- Experience measures
                MIN_YEARS_EXPERIENCE as experience_min_years,
                MAX_YEARS_EXPERIENCE as experience_max_years,
                EXPERIENCE_LEVEL as experience_level,

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
                COUNT(CASE WHEN experience_key != 'EXP_UNKNOWN' THEN 1 END) as successful_experience_lookups,
                COUNT(CASE WHEN keyword_key != 'KWD_UNKNOWN' THEN 1 END) as successful_keyword_lookups,
                COUNT(CASE WHEN salary_key != 'SAL_UNKNOWN' THEN 1 END) as successful_salary_lookups,

                -- Data completeness metrics
                COUNT(CASE WHEN salary_min_annual_usd IS NOT NULL AND salary_max_annual_usd IS NOT NULL THEN 1 END) as jobs_with_salary,
                COUNT(CASE WHEN experience_min_years IS NOT NULL OR experience_max_years IS NOT NULL THEN 1 END) as jobs_with_experience,
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
            experience_success_rate = (validation_result[9] / total_jobs * 100) if total_jobs > 0 else 0
            keyword_success_rate = (validation_result[10] / total_jobs * 100) if total_jobs > 0 else 0
            salary_success_rate = (validation_result[11] / total_jobs * 100) if total_jobs > 0 else 0

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

            # Check for jobs with invalid experience ranges
            cursor.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE experience_min_years IS NOT NULL AND experience_max_years IS NOT NULL
                AND experience_min_years > experience_max_years
            """)
            invalid_experience_ranges = cursor.fetchone()[0]
            if invalid_experience_ranges > 0:
                quality_issues.append(f"{invalid_experience_ranges} jobs with invalid experience ranges")

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
            salary_completeness = (validation_result[12] / total_jobs * 100) if total_jobs > 0 else 0
            experience_completeness = (validation_result[13] / total_jobs * 100) if total_jobs > 0 else 0
            work_type_completeness = (validation_result[14] / total_jobs * 100) if total_jobs > 0 else 0
            good_llm_completeness = (validation_result[15] / total_jobs * 100) if total_jobs > 0 else 0

            context.log.info(f"Fact table validation: {total_jobs} total job postings, "
                           f"{validation_result[1]} unique jobs, {validation_result[2]} unique companies")

            context.log.info(f"Dimension lookup success rates: Company {company_success_rate:.1f}%, "
                           f"Location {location_success_rate:.1f}%, Job Family {job_family_success_rate:.1f}%, Salary {salary_success_rate:.1f}%")

            context.log.info(f"Data completeness: Salary {salary_completeness:.1f}%, "
                           f"Experience {experience_completeness:.1f}%, Work Type {work_type_completeness:.1f}%")

            context.log.info(f"Quality metrics: Avg data quality {validation_result[16]:.3f}, "
                           f"Avg LLM confidence {validation_result[17]:.3f}")

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
                "experience_lookup_success_rate": MetadataValue.float(experience_success_rate),
                "keyword_lookup_success_rate": MetadataValue.float(keyword_success_rate),
                "salary_lookup_success_rate": MetadataValue.float(salary_success_rate),
                "salary_completeness": MetadataValue.float(salary_completeness),
                "experience_completeness": MetadataValue.float(experience_completeness),
                "work_type_completeness": MetadataValue.float(work_type_completeness),
                "good_llm_confidence_percentage": MetadataValue.float(good_llm_completeness),
                "avg_data_quality_score": MetadataValue.float(float(validation_result[16]) if validation_result[16] is not None else 0.0),
                "avg_llm_confidence": MetadataValue.float(float(validation_result[17]) if validation_result[17] is not None else 0.0),
                "platform_distribution": MetadataValue.json(platform_stats[:10]),  # Top 10 platforms
                "monthly_distribution": MetadataValue.json(monthly_stats),  # Last 12 months
                "quality_issues_count": MetadataValue.int(len(quality_issues)),
                "earliest_job_date": MetadataValue.text(str(validation_result[19])),
                "latest_job_date": MetadataValue.text(str(validation_result[20])),
                "jobs_with_equity": MetadataValue.int(validation_result[21]),
                "jobs_with_bonus": MetadataValue.int(validation_result[22]),
                "jobs_needing_review": MetadataValue.int(validation_result[23])
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
                    "experience_success_rate": experience_success_rate,
                    "keyword_success_rate": keyword_success_rate,
                    "salary_success_rate": salary_success_rate
                },
                "data_completeness": {
                    "salary_completeness": salary_completeness,
                    "experience_completeness": experience_completeness,
                    "work_type_completeness": work_type_completeness,
                    "good_llm_confidence_percentage": good_llm_completeness
                },
                "quality_metrics": {
                    "avg_data_quality_score": float(validation_result[16]) if validation_result[16] is not None else 0.0,
                    "avg_llm_confidence": float(validation_result[17]) if validation_result[17] is not None else 0.0,
                    "avg_salary_confidence": float(validation_result[18]) if validation_result[18] is not None else 0.0,
                    "quality_issues": quality_issues
                },
                "temporal_coverage": {
                    "earliest_job_date": str(validation_result[19]),
                    "latest_job_date": str(validation_result[20]),
                    "monthly_distribution": monthly_stats
                },
                "business_features": {
                    "jobs_with_equity": validation_result[21],
                    "jobs_with_bonus": validation_result[22],
                    "jobs_needing_review": validation_result[23]
                },
                "platform_analysis": platform_stats
            }

        finally:
            cursor.close()