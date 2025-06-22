"""
Phase 1: Salary Normalization Assets

Assets for extracting, standardizing, and normalizing salary data from LLM-enriched
JOBS_LLM_ENRICHED table to solve the critical data quality issue where salary
values with different periods (hourly, annually, monthly, weekly) are mixed
without normalization.

Dependencies:
- stage_jobs_llm_enriched: Source of LLM-extracted salary data
- stage_jobs_unified: Source of job metadata

Output Tables:
- SALARY_NORMALIZED: Master salary table with all values normalized to annual USD
- JOB_SALARY_BRIDGE: Many-to-many job-salary relationships
"""

import json
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime
import hashlib

from dagster import (
    asset,
    AssetExecutionContext,
    MetadataValue
)

from dagster_betterjobs.resources import SnowflakeResource
from dagster_betterjobs.utils.schema_utils import ensure_object_exists


@asset(
    deps=["stage_jobs_llm_enriched_unified"],
    description="Extract and flatten salary data from LLM enriched jobs for normalization",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"}
)
def stage_salary_raw_extraction(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Extract salary data from JOBS_LLM_ENRICHED and prepare for normalization.

    Processing Steps:
    1. Extract all salary data with non-NULL min OR max values
    2. Identify salary confidence and quality indicators
    3. Detect statistical outliers requiring review
    4. Prepare data for period/currency normalization
    5. Track extraction statistics and data quality metrics
    """

    conn = snowflake.get_connection()
    cursor = None

    stats = {
        "extraction_timestamp": datetime.now().isoformat(),
        "salary_records_extracted": 0,
        "unique_jobs_with_salary": 0,
        "hourly_salary_count": 0,
        "annual_salary_count": 0,
        "monthly_salary_count": 0,
        "weekly_salary_count": 0,
        "other_period_count": 0,
        "extraction_errors": 0
    }

    try:
        cursor = conn.cursor()

        context.log.info("🔍 Starting salary raw extraction...")

        # Create the raw extraction view using schema-as-code pattern
        view_name = ensure_object_exists("views/stage_salary_raw_extraction.sql", snowflake, context)
        context.log.info(f"✅ Created {view_name} view using schema-as-code")

        # Get extraction statistics
        cursor.execute("""
        SELECT
            COUNT(*) as total_salary_records,
            COUNT(DISTINCT JOB_UID) as unique_jobs,
            COUNT(CASE WHEN SALARY_PERIOD = 'hourly' THEN 1 END) as hourly_count,
            COUNT(CASE WHEN SALARY_PERIOD = 'annually' THEN 1 END) as annual_count,
            COUNT(CASE WHEN SALARY_PERIOD = 'monthly' THEN 1 END) as monthly_count,
            COUNT(CASE WHEN SALARY_PERIOD = 'weekly' THEN 1 END) as weekly_count,
            COUNT(CASE WHEN SALARY_PERIOD NOT IN ('hourly', 'annually', 'monthly', 'weekly') THEN 1 END) as other_count,
            COUNT(CASE WHEN POTENTIAL_OUTLIER_FLAG = TRUE THEN 1 END) as potential_outliers
        FROM BETTERJOBS_DB.STAGE.SALARY_RAW_EXTRACTION
        """)

        result = cursor.fetchone()
        if result:
            stats.update({
                "salary_records_extracted": result[0],
                "unique_jobs_with_salary": result[1],
                "hourly_salary_count": result[2],
                "annual_salary_count": result[3],
                "monthly_salary_count": result[4],
                "weekly_salary_count": result[5],
                "other_period_count": result[6],
                "potential_outliers": result[7]
            })

        # Get period distribution for analysis
        cursor.execute("""
        SELECT
            SALARY_PERIOD,
            COUNT(*) as frequency,
            AVG(COALESCE(SALARY_MIN, SALARY_MAX)) as avg_salary_value,
            MIN(COALESCE(SALARY_MIN, SALARY_MAX)) as min_salary_value,
            MAX(COALESCE(SALARY_MIN, SALARY_MAX)) as max_salary_value
        FROM BETTERJOBS_DB.STAGE.SALARY_RAW_EXTRACTION
        GROUP BY SALARY_PERIOD
        ORDER BY frequency DESC
        """)

        period_data = cursor.fetchall()
        if period_data:
            columns = [desc[0] for desc in cursor.description]
            # Convert Decimal objects to float for JSON serialization
            period_list = []
            for row in period_data:
                row_dict = {}
                for col, val in zip(columns, row):
                    if hasattr(val, '__float__'):  # Convert Decimal to float
                        row_dict[col] = float(val) if val is not None else None
                    else:
                        row_dict[col] = val
                period_list.append(row_dict)
            stats["period_distribution"] = period_list

        context.log.info(f"""
        🎯 Salary Raw Extraction Complete:
        • Total Salary Records: {stats['salary_records_extracted']:,}
        • Jobs with Salary Data: {stats['unique_jobs_with_salary']:,}
        • Hourly Periods: {stats['hourly_salary_count']:,}
        • Annual Periods: {stats['annual_salary_count']:,}
        • Monthly Periods: {stats['monthly_salary_count']:,}
        • Weekly Periods: {stats['weekly_salary_count']:,}
        • Other Periods: {stats['other_period_count']:,}
        • Potential Outliers: {stats.get('potential_outliers', 0):,}
        """)

        # Add metadata for Dagster UI
        context.add_output_metadata({
            "salary_records_extracted": MetadataValue.int(stats["salary_records_extracted"]),
            "unique_jobs_with_salary": MetadataValue.int(stats["unique_jobs_with_salary"]),
            "hourly_salary_count": MetadataValue.int(stats["hourly_salary_count"]),
            "annual_salary_count": MetadataValue.int(stats["annual_salary_count"]),
            "monthly_salary_count": MetadataValue.int(stats["monthly_salary_count"]),
            "weekly_salary_count": MetadataValue.int(stats["weekly_salary_count"]),
            "other_period_count": MetadataValue.int(stats["other_period_count"]),
            "period_distribution": MetadataValue.json(stats.get("period_distribution", []))
        })

        return stats

    except Exception as e:
        context.log.error(f"❌ Salary raw extraction failed: {str(e)}")
        stats["extraction_errors"] = 1
        stats["error_message"] = str(e)
        raise

    finally:
        if cursor:
            cursor.close()


@asset(
    deps=["stage_salary_raw_extraction"],
    description="Create normalized salary master table with annual USD conversion",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"}
)
def stage_salary_normalized(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Apply salary normalization rules and convert all salaries to annual USD.

    Processing Steps:
    1. Apply period conversion factors (hourly * 2080, monthly * 12, etc.)
    2. Detect and flag statistical outliers ($5M/year, $1/hour, etc.)
    3. Calculate confidence scores based on conversion complexity
    4. Generate market percentiles for salary ranges
    5. Create canonical salary range identifiers
    """

    conn = snowflake.get_connection()
    cursor = None

    stats = {
        "normalization_timestamp": datetime.now().isoformat(),
        "normalized_salaries": 0,
        "outliers_flagged": 0,
        "manual_review_flagged": 0,
        "high_confidence_salaries": 0,
        "normalization_errors": 0
    }

    try:
        cursor = conn.cursor()

        context.log.info("🔄 Starting salary normalization...")

        # Ensure the table exists using schema-as-code pattern
        table_name = ensure_object_exists("tables/stage_salary_normalized.sql", snowflake, context)
        context.log.info(f"✅ Ensured {table_name} table exists using schema-as-code")

        # Clear existing normalized data
        cursor.execute("DELETE FROM BETTERJOBS_DB.STAGE.SALARY_NORMALIZED")
        context.log.info("🧹 Cleared existing SALARY_NORMALIZED data")

        # Create normalized salary data
        normalization_sql = """
        INSERT INTO BETTERJOBS_DB.STAGE.SALARY_NORMALIZED (
            SALARY_ID,
            SALARY_RANGE_NAME,
            SALARY_MIN_ORIGINAL,
            SALARY_MAX_ORIGINAL,
            SALARY_PERIOD_ORIGINAL,
            SALARY_CURRENCY_ORIGINAL,
            SALARY_MIN_ANNUAL_USD,
            SALARY_MAX_ANNUAL_USD,
            SALARY_MIDPOINT_ANNUAL_USD,
            NORMALIZATION_FACTOR,
            CONFIDENCE_SCORE,
            OUTLIER_FLAG,
            MANUAL_REVIEW_FLAG,
            FREQUENCY_COUNT,
            CREATED_TIMESTAMP,
            UPDATED_TIMESTAMP
        )
        WITH salary_normalization AS (
            SELECT
                -- Original values
                SALARY_MIN,
                SALARY_MAX,
                SALARY_PERIOD,
                SALARY_CURRENCY,

                -- Conversion factors for annual USD
                CASE
                    WHEN SALARY_PERIOD = 'hourly' THEN 2080      -- 40 hours/week * 52 weeks
                    WHEN SALARY_PERIOD = 'weekly' THEN 52
                    WHEN SALARY_PERIOD = 'monthly' THEN 12
                    WHEN SALARY_PERIOD = 'annually' THEN 1
                    WHEN SALARY_PERIOD = 'yearly' THEN 1
                    WHEN SALARY_PERIOD = 'annual' THEN 1
                    ELSE 1  -- Default to annual for unknown periods
                END as CONVERSION_FACTOR,

                -- Normalized values (convert to annual USD)
                COALESCE(SALARY_MIN, SALARY_MAX) *
                CASE
                    WHEN SALARY_PERIOD = 'hourly' THEN 2080
                    WHEN SALARY_PERIOD = 'weekly' THEN 52
                    WHEN SALARY_PERIOD = 'monthly' THEN 12
                    WHEN SALARY_PERIOD = 'annually' THEN 1
                    WHEN SALARY_PERIOD = 'yearly' THEN 1
                    WHEN SALARY_PERIOD = 'annual' THEN 1
                    ELSE 1
                END as SALARY_MIN_ANNUAL,

                COALESCE(SALARY_MAX, SALARY_MIN) *
                CASE
                    WHEN SALARY_PERIOD = 'hourly' THEN 2080
                    WHEN SALARY_PERIOD = 'weekly' THEN 52
                    WHEN SALARY_PERIOD = 'monthly' THEN 12
                    WHEN SALARY_PERIOD = 'annually' THEN 1
                    WHEN SALARY_PERIOD = 'yearly' THEN 1
                    WHEN SALARY_PERIOD = 'annual' THEN 1
                    ELSE 1
                END as SALARY_MAX_ANNUAL,

                -- Count frequency for deduplication
                COUNT(*) as FREQUENCY_COUNT

            FROM BETTERJOBS_DB.STAGE.SALARY_RAW_EXTRACTION
            WHERE SALARY_CURRENCY IN ('USD', 'US', 'DOLLAR', 'DOLLARS')  -- Only USD for now
            GROUP BY 1, 2, 3, 4
        ),

        salary_with_outliers AS (
            SELECT
                *,
                (SALARY_MIN_ANNUAL + SALARY_MAX_ANNUAL) / 2 as SALARY_MIDPOINT_ANNUAL,

                -- Outlier detection (statistical boundaries)
                CASE
                    WHEN SALARY_MIN_ANNUAL > 500000 OR SALARY_MAX_ANNUAL > 500000 THEN TRUE  -- Over $500K
                    WHEN SALARY_MIN_ANNUAL < 15000 OR SALARY_MAX_ANNUAL < 15000 THEN TRUE    -- Under $15K
                    WHEN SALARY_MAX_ANNUAL / SALARY_MIN_ANNUAL > 3 THEN TRUE                 -- Range too wide
                    ELSE FALSE
                END as IS_OUTLIER,

                -- Confidence scoring
                CASE
                    WHEN SALARY_PERIOD IN ('annually', 'annual', 'yearly') THEN 1.0          -- No conversion needed
                    WHEN SALARY_PERIOD IN ('hourly', 'monthly', 'weekly') THEN 0.9           -- Standard conversions
                    WHEN SALARY_CURRENCY != 'USD' THEN 0.7                                   -- Currency conversion uncertainty
                    ELSE 0.5                                                                  -- Unknown period
                END as BASE_CONFIDENCE_SCORE

            FROM salary_normalization
        )

        SELECT
            -- Generate unique ID
            'SAL_' || SUBSTR(SHA2(CONCAT(
                COALESCE(SALARY_MIN::STRING, ''),
                COALESCE(SALARY_MAX::STRING, ''),
                SALARY_PERIOD,
                SALARY_CURRENCY
            )), 1, 12) as SALARY_ID,

            -- Human readable name
            CASE
                WHEN SALARY_MIN_ANNUAL = SALARY_MAX_ANNUAL THEN
                    '$' || ROUND(SALARY_MIN_ANNUAL/1000) || 'K Annual USD'
                ELSE
                    '$' || ROUND(SALARY_MIN_ANNUAL/1000) || 'K-$' || ROUND(SALARY_MAX_ANNUAL/1000) || 'K Annual USD'
            END as SALARY_RANGE_NAME,

            -- Original values
            SALARY_MIN,
            SALARY_MAX,
            SALARY_PERIOD,
            SALARY_CURRENCY,

            -- Normalized values
            SALARY_MIN_ANNUAL,
            SALARY_MAX_ANNUAL,
            SALARY_MIDPOINT_ANNUAL,
            CONVERSION_FACTOR,

            -- Quality indicators
            BASE_CONFIDENCE_SCORE * CASE WHEN IS_OUTLIER THEN 0.5 ELSE 1.0 END as CONFIDENCE_SCORE,
            IS_OUTLIER,
            IS_OUTLIER OR BASE_CONFIDENCE_SCORE < 0.8 as NEEDS_MANUAL_REVIEW,

            FREQUENCY_COUNT,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP

        FROM salary_with_outliers
        WHERE SALARY_MIN_ANNUAL IS NOT NULL
          AND SALARY_MAX_ANNUAL IS NOT NULL
          AND SALARY_MIN_ANNUAL > 0
          AND SALARY_MAX_ANNUAL > 0
        """

        cursor.execute(normalization_sql)
        normalized_count = cursor.rowcount
        stats["normalized_salaries"] = normalized_count

        context.log.info(f"✅ Normalized {normalized_count:,} salary ranges")

        # Get normalization statistics
        cursor.execute("""
        SELECT
            COUNT(*) as total_normalized,
            COUNT(CASE WHEN OUTLIER_FLAG = TRUE THEN 1 END) as outliers,
            COUNT(CASE WHEN MANUAL_REVIEW_FLAG = TRUE THEN 1 END) as manual_review,
            COUNT(CASE WHEN CONFIDENCE_SCORE >= 0.9 THEN 1 END) as high_confidence,
            AVG(CONFIDENCE_SCORE) as avg_confidence,
            AVG(SALARY_MIDPOINT_ANNUAL_USD) as avg_salary_annual,
            MIN(SALARY_MIDPOINT_ANNUAL_USD) as min_salary_annual,
            MAX(SALARY_MIDPOINT_ANNUAL_USD) as max_salary_annual
        FROM BETTERJOBS_DB.STAGE.SALARY_NORMALIZED
        """)

        result = cursor.fetchone()
        if result:
            stats.update({
                "normalized_salaries": int(result[0]) if result[0] else 0,
                "outliers_flagged": int(result[1]) if result[1] else 0,
                "manual_review_flagged": int(result[2]) if result[2] else 0,
                "high_confidence_salaries": int(result[3]) if result[3] else 0,
                "avg_confidence_score": float(result[4]) if result[4] else 0.0,
                "avg_salary_annual": int(float(result[5])) if result[5] else 0,
                "min_salary_annual": int(float(result[6])) if result[6] else 0,
                "max_salary_annual": int(float(result[7])) if result[7] else 0
            })

        # Calculate market percentiles
        cursor.execute("""
        UPDATE BETTERJOBS_DB.STAGE.SALARY_NORMALIZED
        SET MARKET_PERCENTILE = subq.percentile_rank
        FROM (
            SELECT
                SALARY_ID,
                ROUND(PERCENT_RANK() OVER (ORDER BY SALARY_MIDPOINT_ANNUAL_USD) * 100) as percentile_rank
            FROM BETTERJOBS_DB.STAGE.SALARY_NORMALIZED
        ) subq
        WHERE BETTERJOBS_DB.STAGE.SALARY_NORMALIZED.SALARY_ID = subq.SALARY_ID
        """)

        context.log.info("✅ Calculated market percentiles")

        context.log.info(f"""
        🎯 Salary Normalization Complete:
        • Total Normalized Salaries: {stats['normalized_salaries']:,}
        • Outliers Flagged: {stats['outliers_flagged']:,}
        • Manual Review Needed: {stats['manual_review_flagged']:,}
        • High Confidence (>90%): {stats['high_confidence_salaries']:,}
        • Average Annual Salary: ${stats.get('avg_salary_annual', 0):,.0f}
        • Salary Range: ${stats.get('min_salary_annual', 0):,.0f} - ${stats.get('max_salary_annual', 0):,.0f}
        """)

        # Add metadata for Dagster UI
        context.add_output_metadata({
            "normalized_salaries": MetadataValue.int(stats["normalized_salaries"]),
            "outliers_flagged": MetadataValue.int(stats["outliers_flagged"]),
            "manual_review_flagged": MetadataValue.int(stats["manual_review_flagged"]),
            "high_confidence_salaries": MetadataValue.int(stats["high_confidence_salaries"]),
            "avg_confidence_score": MetadataValue.float(stats.get("avg_confidence_score", 0)),
            "avg_salary_annual": MetadataValue.int(stats.get("avg_salary_annual", 0)),
            "salary_range": MetadataValue.text(f"${stats.get('min_salary_annual', 0):,.0f} - ${stats.get('max_salary_annual', 0):,.0f}")
        })

        return stats

    except Exception as e:
        context.log.error(f"❌ Salary normalization failed: {str(e)}")
        stats["normalization_errors"] = 1
        stats["error_message"] = str(e)
        raise

    finally:
        if cursor:
            cursor.close()


@asset(
    deps=["stage_salary_normalized", "stage_jobs_unified"],
    description="Create job-to-salary bridge relationships",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"}
)
def stage_job_salary_bridge(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Link jobs to normalized salary data through bridge table.

    Processing Steps:
    1. Match jobs to normalized salary ranges
    2. Calculate combined confidence scores
    3. Flag relationships needing manual review
    4. Track data lineage and validation status
    """

    conn = snowflake.get_connection()
    cursor = None

    stats = {
        "bridge_timestamp": datetime.now().isoformat(),
        "job_salary_links": 0,
        "high_confidence_links": 0,
        "needs_review_links": 0,
        "bridge_errors": 0
    }

    try:
        cursor = conn.cursor()

        context.log.info("🔗 Starting job-salary bridge creation...")

        # Ensure the table exists using schema-as-code pattern
        table_name = ensure_object_exists("tables/stage_job_salary_bridge.sql", snowflake, context)
        context.log.info(f"✅ Ensured {table_name} table exists using schema-as-code")

        # Clear existing bridge data
        cursor.execute("DELETE FROM BETTERJOBS_DB.STAGE.JOB_SALARY_BRIDGE")
        context.log.info("🧹 Cleared existing JOB_SALARY_BRIDGE data")

        # Create job-salary bridge relationships
        bridge_sql = """
        INSERT INTO BETTERJOBS_DB.STAGE.JOB_SALARY_BRIDGE (
            BRIDGE_ID,
            JOB_UID,
            SALARY_ID,
            OVERALL_CONFIDENCE,
            SALARY_SOURCE,
            EXTRACTION_METHOD,
            NEEDS_REVIEW,
            VALIDATION_STATUS,
            CREATED_TIMESTAMP
        )
        SELECT
            -- Generate unique bridge ID
            'BRIDGE_SAL_' || SUBSTR(SHA2(CONCAT(sre.JOB_UID, sn.SALARY_ID)), 1, 12) as BRIDGE_ID,

            sre.JOB_UID,
            sn.SALARY_ID,

            -- Combined confidence score
            sn.CONFIDENCE_SCORE *
            CASE
                WHEN sre.SALARY_COMPLETENESS = 'complete_range' THEN 1.0
                WHEN sre.SALARY_COMPLETENESS IN ('min_only', 'max_only') THEN 0.8
                ELSE 0.5
            END as OVERALL_CONFIDENCE,

            'llm_extracted' as SALARY_SOURCE,
            'automated_normalization' as EXTRACTION_METHOD,

            -- Flag for review
            sn.MANUAL_REVIEW_FLAG OR sn.OUTLIER_FLAG OR sre.POTENTIAL_OUTLIER_FLAG as NEEDS_REVIEW,

            CASE
                WHEN sn.MANUAL_REVIEW_FLAG OR sn.OUTLIER_FLAG OR sre.POTENTIAL_OUTLIER_FLAG THEN 'needs_review'
                WHEN sn.CONFIDENCE_SCORE >= 0.9 THEN 'validated'
                ELSE 'pending'
            END as VALIDATION_STATUS,

            CURRENT_TIMESTAMP

        FROM BETTERJOBS_DB.STAGE.SALARY_RAW_EXTRACTION sre
        JOIN BETTERJOBS_DB.STAGE.SALARY_NORMALIZED sn ON (
            sn.SALARY_MIN_ORIGINAL = sre.SALARY_MIN
            AND sn.SALARY_MAX_ORIGINAL = sre.SALARY_MAX
            AND sn.SALARY_PERIOD_ORIGINAL = sre.SALARY_PERIOD
            AND sn.SALARY_CURRENCY_ORIGINAL = sre.SALARY_CURRENCY
        )
        """

        cursor.execute(bridge_sql)
        bridge_count = cursor.rowcount
        stats["job_salary_links"] = bridge_count

        context.log.info(f"✅ Created {bridge_count:,} job-salary bridge relationships")

        # Get bridge statistics
        cursor.execute("""
        SELECT
            COUNT(*) as total_links,
            COUNT(CASE WHEN OVERALL_CONFIDENCE >= 0.9 THEN 1 END) as high_confidence,
            COUNT(CASE WHEN NEEDS_REVIEW = TRUE THEN 1 END) as needs_review,
            AVG(OVERALL_CONFIDENCE) as avg_confidence
        FROM BETTERJOBS_DB.STAGE.JOB_SALARY_BRIDGE
        """)

        result = cursor.fetchone()
        if result:
            stats.update({
                "job_salary_links": result[0],
                "high_confidence_links": result[1],
                "needs_review_links": result[2],
                "avg_overall_confidence": round(result[3], 3) if result[3] else 0
            })

        # Get coverage statistics
        cursor.execute("""
        SELECT
            COUNT(DISTINCT ju.JOB_UID) as total_jobs,
            COUNT(DISTINCT jsb.JOB_UID) as jobs_with_salary,
            ROUND((COUNT(DISTINCT jsb.JOB_UID)::FLOAT / COUNT(DISTINCT ju.JOB_UID)) * 100, 2) as coverage_percentage
        FROM BETTERJOBS_DB.STAGE.JOBS_UNIFIED ju
        LEFT JOIN BETTERJOBS_DB.STAGE.JOB_SALARY_BRIDGE jsb ON ju.JOB_UID = jsb.JOB_UID
        """)

        coverage_result = cursor.fetchone()
        if coverage_result:
            stats.update({
                "total_jobs": coverage_result[0],
                "jobs_with_salary": coverage_result[1],
                "salary_coverage_percentage": coverage_result[2]
            })

        context.log.info(f"""
        🎯 Job-Salary Bridge Complete:
        • Total Bridge Links: {stats['job_salary_links']:,}
        • High Confidence Links: {stats['high_confidence_links']:,}
        • Needs Review: {stats['needs_review_links']:,}
        • Average Confidence: {stats.get('avg_overall_confidence', 0):.3f}
        • Salary Coverage: {stats.get('salary_coverage_percentage', 0):.1f}% of jobs
        """)

        # Add metadata for Dagster UI
        context.add_output_metadata({
            "job_salary_links": MetadataValue.int(stats["job_salary_links"]),
            "high_confidence_links": MetadataValue.int(stats["high_confidence_links"]),
            "needs_review_links": MetadataValue.int(stats["needs_review_links"]),
            "avg_overall_confidence": MetadataValue.float(stats.get("avg_overall_confidence", 0)),
            "salary_coverage_percentage": MetadataValue.float(stats.get("salary_coverage_percentage", 0)),
            "total_jobs": MetadataValue.int(stats.get("total_jobs", 0)),
            "jobs_with_salary": MetadataValue.int(stats.get("jobs_with_salary", 0))
        })

        return stats

    except Exception as e:
        context.log.error(f"❌ Job-salary bridge creation failed: {str(e)}")
        stats["bridge_errors"] = 1
        stats["error_message"] = str(e)
        raise

    finally:
        if cursor:
            cursor.close()