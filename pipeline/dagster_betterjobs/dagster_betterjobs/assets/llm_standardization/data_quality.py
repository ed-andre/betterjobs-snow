"""
Data Quality and Validation Assets for LLM Standardization

This module implements comprehensive data quality monitoring, validation,
and alerting for the LLM standardization pipeline.
"""

from dagster import asset, AssetExecutionContext, FreshnessPolicy
from dagster_snowflake import SnowflakeResource
from typing import Dict, Any, List
import uuid
from datetime import datetime


@asset(
    deps=["stage_skills_normalized", "stage_keywords_normalized", "stage_locations_normalized",
          "stage_job_skills_bridge", "stage_job_keywords_bridge", "stage_job_locations_bridge"],
    description="Comprehensive data quality validation for LLM standardization",
    group_name="data_quality_governance",
    kinds={"snowflake", "python", "SQL"}
)
def stage_llm_data_quality_validation(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource
) -> Dict[str, Any]:
    """
    Execute comprehensive data quality validation across all normalized assets.

    Validation Categories:
    1. Referential Integrity - Foreign key consistency, orphaned records
    2. Data Completeness - Coverage analysis, missing values
    3. Data Consistency - Duplicate detection, conflicting values
    4. Data Accuracy - Confidence score validation, manual review flags
    5. Data Freshness - Last update tracking, staleness detection
    6. Business Logic Validation - Domain-specific rules and constraints

    Output: Detailed validation report with issues categorized by severity
    """

    context.log.info("Starting comprehensive data quality validation")

    # Clear existing validation results
    clear_sql = "DELETE FROM BETTERJOBS_DB.STAGE.LLM_QUALITY_VALIDATION_RESULTS;"

    # Core validation queries
    validation_sql = """
    INSERT INTO BETTERJOBS_DB.STAGE.LLM_QUALITY_VALIDATION_RESULTS (
        VALIDATION_ID,
        VALIDATION_CATEGORY,
        ISSUE_TYPE,
        ISSUE_COUNT,
        SEVERITY,
        DESCRIPTION,
        AFFECTED_RECORDS,
        STATUS,
        CREATED_TIMESTAMP,
        CREATED_BY
    )
    WITH
    -- 1. Referential Integrity Checks
    orphaned_skills AS (
        SELECT
            'orphaned_skills' as issue_type,
            COUNT(*) as issue_count,
            ARRAY_AGG(sn.SKILL_ID) as affected_records
        FROM BETTERJOBS_DB.STAGE.SKILLS_NORMALIZED sn
        LEFT JOIN BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE jsb ON sn.SKILL_ID = jsb.SKILL_ID
        WHERE jsb.SKILL_ID IS NULL
    ),

    orphaned_keywords AS (
        SELECT
            'orphaned_keywords' as issue_type,
            COUNT(*) as issue_count,
            ARRAY_AGG(kn.KEYWORD_ID) as affected_records
        FROM BETTERJOBS_DB.STAGE.KEYWORDS_NORMALIZED kn
        LEFT JOIN BETTERJOBS_DB.STAGE.JOB_KEYWORDS_BRIDGE jkb ON kn.KEYWORD_ID = jkb.KEYWORD_ID
        WHERE jkb.KEYWORD_ID IS NULL
    ),

    orphaned_locations AS (
        SELECT
            'orphaned_locations' as issue_type,
            COUNT(*) as issue_count,
            ARRAY_AGG(ln.LOCATION_ID) as affected_records
        FROM BETTERJOBS_DB.STAGE.LOCATIONS_NORMALIZED ln
        LEFT JOIN BETTERJOBS_DB.STAGE.JOB_LOCATIONS_BRIDGE jlb ON ln.LOCATION_ID = jlb.LOCATION_ID
        WHERE jlb.LOCATION_ID IS NULL
    ),

    -- 2. Data Completeness Checks
    coverage_metrics AS (
        SELECT
            COUNT(DISTINCT ju.JOB_UID) as total_jobs,
            COUNT(DISTINCT jsb.JOB_UID) as jobs_with_skills,
            COUNT(DISTINCT jkb.JOB_UID) as jobs_with_keywords,
            COUNT(DISTINCT jlb.JOB_UID) as jobs_with_locations,
            ROUND((COUNT(DISTINCT jsb.JOB_UID)::FLOAT / COUNT(DISTINCT ju.JOB_UID)) * 100, 2) as skills_coverage_pct,
            ROUND((COUNT(DISTINCT jkb.JOB_UID)::FLOAT / COUNT(DISTINCT ju.JOB_UID)) * 100, 2) as keywords_coverage_pct,
            ROUND((COUNT(DISTINCT jlb.JOB_UID)::FLOAT / COUNT(DISTINCT ju.JOB_UID)) * 100, 2) as locations_coverage_pct
        FROM BETTERJOBS_DB.STAGE.JOBS_UNIFIED ju
        LEFT JOIN BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE jsb ON ju.JOB_UID = jsb.JOB_UID
        LEFT JOIN BETTERJOBS_DB.STAGE.JOB_KEYWORDS_BRIDGE jkb ON ju.JOB_UID = jkb.JOB_UID
        LEFT JOIN BETTERJOBS_DB.STAGE.JOB_LOCATIONS_BRIDGE jlb ON ju.JOB_UID = jlb.JOB_UID
        WHERE ju.IS_ENGLISH = TRUE
    ),

    -- 3. Data Consistency Checks
    skill_duplicates AS (
        SELECT
            'skill_duplicates' as issue_type,
            COUNT(*) - COUNT(DISTINCT SKILL_NAME_CLEAN) as issue_count,
            ARRAY_AGG(DISTINCT SKILL_NAME_CLEAN) as affected_records
        FROM BETTERJOBS_DB.STAGE.SKILLS_NORMALIZED
        GROUP BY SKILL_NAME_CLEAN
        HAVING COUNT(*) > 1
    ),

    -- 4. Data Accuracy Checks
    low_confidence_skills AS (
        SELECT
            'low_confidence_skills' as issue_type,
            COUNT(*) as issue_count,
            ARRAY_AGG(SKILL_ID) as affected_records
        FROM BETTERJOBS_DB.STAGE.SKILLS_NORMALIZED
        WHERE CONFIDENCE_SCORE < 0.5
    ),

    low_confidence_locations AS (
        SELECT
            'low_confidence_locations' as issue_type,
            COUNT(*) as issue_count,
            ARRAY_AGG(LOCATION_ID) as affected_records
        FROM BETTERJOBS_DB.STAGE.LOCATIONS_NORMALIZED
        WHERE CONFIDENCE_SCORE < 0.5
    ),

    -- 5. Manual Review Queue Items (TODO: Review manual review and confidence score thresholds)
    items_needing_review AS (
        SELECT
            'items_needing_review' as issue_type,
            (
                (SELECT COUNT(*) FROM BETTERJOBS_DB.STAGE.SKILLS_NORMALIZED WHERE MANUAL_REVIEW_FLAG = TRUE) +
                (SELECT COUNT(*) FROM BETTERJOBS_DB.STAGE.LOCATIONS_NORMALIZED WHERE MANUAL_REVIEW_FLAG = TRUE) +
                (SELECT COUNT(*) FROM BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE WHERE NEEDS_REVIEW = TRUE) +
                (SELECT COUNT(*) FROM BETTERJOBS_DB.STAGE.JOB_LOCATIONS_BRIDGE WHERE NEEDS_REVIEW = TRUE)
            ) as issue_count,
            ARRAY_CONSTRUCT('manual_review_items_detected') as affected_records
    )

    -- Combine all validation results
    SELECT
        CONCAT('val_', issue_type, '_', DATE_PART(epoch_second, CURRENT_TIMESTAMP())) as validation_id,
        'referential_integrity' as validation_category,
        issue_type,
        issue_count,
        CASE
            WHEN issue_count = 0 THEN 'RESOLVED'
            WHEN issue_count > 100 THEN 'HIGH'
            WHEN issue_count > 10 THEN 'MEDIUM'
            ELSE 'LOW'
        END as severity,
        CASE issue_type
            WHEN 'orphaned_skills' THEN 'Skills exist without job relationships'
            WHEN 'orphaned_keywords' THEN 'Keywords exist without job relationships'
            WHEN 'orphaned_locations' THEN 'Locations exist without job relationships'
        END as description,
        affected_records,
        CASE WHEN issue_count = 0 THEN 'RESOLVED' ELSE 'ACTIVE' END as status,
        CURRENT_TIMESTAMP,
        'system'
    FROM (
        SELECT * FROM orphaned_skills WHERE issue_count > 0
        UNION ALL
        SELECT * FROM orphaned_keywords WHERE issue_count > 0
        UNION ALL
        SELECT * FROM orphaned_locations WHERE issue_count > 0
    )

    UNION ALL

    -- Coverage validation results
    SELECT
        CONCAT('val_coverage_', coverage_type, '_', DATE_PART(epoch_second, CURRENT_TIMESTAMP())) as validation_id,
        'data_completeness' as validation_category,
        coverage_type as issue_type,
        100 - coverage_pct as issue_count,
        CASE
            WHEN coverage_pct >= 85 THEN 'RESOLVED'
            WHEN coverage_pct >= 70 THEN 'MEDIUM'
            ELSE 'HIGH'
        END as severity,
        CONCAT(coverage_type, ' coverage is ', coverage_pct, '% (target: 85%+)') as description,
        ARRAY_CONSTRUCT(coverage_pct) as affected_records,
        CASE WHEN coverage_pct >= 85 THEN 'RESOLVED' ELSE 'ACTIVE' END as status,
        CURRENT_TIMESTAMP,
        'system'
    FROM (
        SELECT 'skills_coverage' as coverage_type, skills_coverage_pct as coverage_pct FROM coverage_metrics
        UNION ALL
        SELECT 'keywords_coverage' as coverage_type, keywords_coverage_pct as coverage_pct FROM coverage_metrics
        UNION ALL
        SELECT 'locations_coverage' as coverage_type, locations_coverage_pct as coverage_pct FROM coverage_metrics
    )
    WHERE coverage_pct < 85

    UNION ALL

    -- Data accuracy results
    SELECT
        CONCAT('val_', issue_type, '_', DATE_PART(epoch_second, CURRENT_TIMESTAMP())) as validation_id,
        'data_accuracy' as validation_category,
        issue_type,
        issue_count,
        CASE
            WHEN issue_count = 0 THEN 'RESOLVED'
            WHEN issue_count > 50 THEN 'HIGH'
            WHEN issue_count > 10 THEN 'MEDIUM'
            ELSE 'LOW'
        END as severity,
        CASE issue_type
            WHEN 'low_confidence_skills' THEN 'Skills with confidence score below 0.5'
            WHEN 'low_confidence_locations' THEN 'Locations with confidence score below 0.5'
            WHEN 'items_needing_review' THEN 'Items flagged for manual review'
        END as description,
        affected_records,
        CASE WHEN issue_count = 0 THEN 'RESOLVED' ELSE 'ACTIVE' END as status,
        CURRENT_TIMESTAMP,
        'system'
    FROM (
        SELECT * FROM low_confidence_skills WHERE issue_count > 0
        UNION ALL
        SELECT * FROM low_confidence_locations WHERE issue_count > 0
        UNION ALL
        SELECT * FROM items_needing_review WHERE issue_count > 0
    );
    """

    try:
        with snowflake.get_connection() as conn:
            # Clear existing validation results
            context.log.info("Clearing existing validation results")
            cursor = conn.cursor()
            cursor.execute(clear_sql)
            cursor.close()

            # Run validation checks
            context.log.info("Executing comprehensive validation checks")
            cursor = conn.cursor()
            cursor.execute(validation_sql)
            cursor.close()

            # Get validation summary
            summary_sql = """
            SELECT
                VALIDATION_CATEGORY,
                COUNT(*) as total_issues,
                COUNT(CASE WHEN SEVERITY = 'HIGH' THEN 1 END) as high_severity,
                COUNT(CASE WHEN SEVERITY = 'MEDIUM' THEN 1 END) as medium_severity,
                COUNT(CASE WHEN SEVERITY = 'LOW' THEN 1 END) as low_severity,
                COUNT(CASE WHEN STATUS = 'RESOLVED' THEN 1 END) as resolved_issues
            FROM BETTERJOBS_DB.STAGE.LLM_QUALITY_VALIDATION_RESULTS
            WHERE CREATED_TIMESTAMP >= DATEADD(minute, -5, CURRENT_TIMESTAMP)
            GROUP BY VALIDATION_CATEGORY
            ORDER BY total_issues DESC;
            """

            cursor = conn.cursor()
            cursor.execute(summary_sql)
            validation_summary = [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
            cursor.close()

            # Get overall statistics
            stats_sql = """
            SELECT
                COUNT(*) as total_validations,
                COUNT(CASE WHEN STATUS = 'ACTIVE' THEN 1 END) as active_issues,
                COUNT(CASE WHEN SEVERITY = 'HIGH' THEN 1 END) as high_priority_issues,
                COUNT(CASE WHEN STATUS = 'RESOLVED' THEN 1 END) as resolved_issues
            FROM BETTERJOBS_DB.STAGE.LLM_QUALITY_VALIDATION_RESULTS
            WHERE CREATED_TIMESTAMP >= DATEADD(minute, -5, CURRENT_TIMESTAMP);
            """

            cursor = conn.cursor()
            cursor.execute(stats_sql)
            overall_stats = dict(zip([desc[0] for desc in cursor.description], cursor.fetchone()))
            cursor.close()

            # Convert Decimal to float for JSON serialization
            for key, value in overall_stats.items():
                if hasattr(value, 'to_eng_string'):  # Decimal object
                    overall_stats[key] = float(value)

            for summary in validation_summary:
                for key, value in summary.items():
                    if hasattr(value, 'to_eng_string'):  # Decimal object
                        summary[key] = float(value)

            context.log.info(f"Data quality validation completed:")
            context.log.info(f"  - Total validations: {overall_stats['TOTAL_VALIDATIONS']}")
            context.log.info(f"  - Active issues: {overall_stats['ACTIVE_ISSUES']}")
            context.log.info(f"  - High priority issues: {overall_stats['HIGH_PRIORITY_ISSUES']}")
            context.log.info(f"  - Resolved issues: {overall_stats['RESOLVED_ISSUES']}")

            if validation_summary:
                context.log.info("Validation summary by category:")
                for summary in validation_summary:
                    context.log.info(f"  - {summary['VALIDATION_CATEGORY']}: {summary['TOTAL_ISSUES']} issues " +
                                   f"(High: {summary['HIGH_SEVERITY']}, Medium: {summary['MEDIUM_SEVERITY']}, Low: {summary['LOW_SEVERITY']})")

            return {
                "status": "success",
                **overall_stats,
                "validation_summary": validation_summary,
                "timestamp": context.run_id
            }

    except Exception as e:
        context.log.error(f"Error in data quality validation: {str(e)}")
        raise

# TODO: Review freshness policy. Currently set to 7 days.
@asset(
    deps=["stage_llm_data_quality_validation"],
    description="Generate operational quality metrics and KPIs",
    group_name="data_quality_governance",
    kinds={"snowflake", "python", "SQL"},
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=10080)
)
def stage_llm_quality_metrics(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource
) -> Dict[str, Any]:
    """
    Generate comprehensive quality metrics for operational monitoring.

    Metrics Categories:
    1. Coverage KPIs - Job coverage percentages by category
    2. Quality KPIs - Confidence score distributions and trends
    3. Performance KPIs - Processing times and throughput metrics
    4. Accuracy KPIs - Manual review rates and error detection
    5. Operational KPIs - Data freshness and pipeline health

    Output: Structured metrics for dashboard consumption and alerting
    """

    context.log.info("Starting quality metrics generation")

    # Clear existing metrics for today
    clear_sql = """
    DELETE FROM BETTERJOBS_DB.STAGE.LLM_QUALITY_METRICS
    WHERE DATE(CALCULATION_TIMESTAMP) = CURRENT_DATE;
    """

    # Generate comprehensive quality metrics
    metrics_sql = """
    INSERT INTO BETTERJOBS_DB.STAGE.LLM_QUALITY_METRICS (
        METRIC_ID,
        METRIC_CATEGORY,
        METRIC_NAME,
        METRIC_VALUE,
        METRIC_THRESHOLD_MIN,
        METRIC_THRESHOLD_MAX,
        METRIC_STATUS,
        DATA_SOURCE,
        TIME_PERIOD,
        CALCULATION_TIMESTAMP,
        CALCULATION_METHOD,
        NOTES,
        CREATED_TIMESTAMP,
        CREATED_BY
    )
    WITH
    -- Coverage Metrics
    coverage_metrics AS (
        SELECT
            COUNT(DISTINCT ju.JOB_UID) as total_jobs,
            COUNT(DISTINCT jsb.JOB_UID) as jobs_with_skills,
            COUNT(DISTINCT jkb.JOB_UID) as jobs_with_keywords,
            COUNT(DISTINCT jlb.JOB_UID) as jobs_with_locations,
            ROUND((COUNT(DISTINCT jsb.JOB_UID)::FLOAT / COUNT(DISTINCT ju.JOB_UID)) * 100, 2) as skills_coverage_pct,
            ROUND((COUNT(DISTINCT jkb.JOB_UID)::FLOAT / COUNT(DISTINCT ju.JOB_UID)) * 100, 2) as keywords_coverage_pct,
            ROUND((COUNT(DISTINCT jlb.JOB_UID)::FLOAT / COUNT(DISTINCT ju.JOB_UID)) * 100, 2) as locations_coverage_pct
        FROM BETTERJOBS_DB.STAGE.JOBS_UNIFIED ju
        LEFT JOIN BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE jsb ON ju.JOB_UID = jsb.JOB_UID
        LEFT JOIN BETTERJOBS_DB.STAGE.JOB_KEYWORDS_BRIDGE jkb ON ju.JOB_UID = jkb.JOB_UID
        LEFT JOIN BETTERJOBS_DB.STAGE.JOB_LOCATIONS_BRIDGE jlb ON ju.JOB_UID = jlb.JOB_UID
        WHERE ju.IS_ENGLISH = TRUE
    ),

    -- Quality Metrics
    quality_metrics AS (
        SELECT
            'skills' as data_source,
            COUNT(*) as total_items,
            AVG(CONFIDENCE_SCORE) as avg_confidence,
            COUNT(CASE WHEN CONFIDENCE_SCORE >= 0.8 THEN 1 END) as high_confidence_items,
            COUNT(CASE WHEN CONFIDENCE_SCORE < 0.5 THEN 1 END) as low_confidence_items,
            COUNT(CASE WHEN MANUAL_REVIEW_FLAG = TRUE THEN 1 END) as manual_review_items
        FROM BETTERJOBS_DB.STAGE.SKILLS_NORMALIZED

        UNION ALL

        SELECT
            'keywords' as data_source,
            COUNT(*) as total_items,
            AVG(CONFIDENCE_SCORE) as avg_confidence,
            COUNT(CASE WHEN CONFIDENCE_SCORE >= 0.8 THEN 1 END) as high_confidence_items,
            COUNT(CASE WHEN CONFIDENCE_SCORE < 0.5 THEN 1 END) as low_confidence_items,
            0 as manual_review_items  -- Keywords don't have manual review flag
        FROM BETTERJOBS_DB.STAGE.KEYWORDS_NORMALIZED

        UNION ALL

        SELECT
            'locations' as data_source,
            COUNT(*) as total_items,
            AVG(CONFIDENCE_SCORE) as avg_confidence,
            COUNT(CASE WHEN CONFIDENCE_SCORE >= 0.8 THEN 1 END) as high_confidence_items,
            COUNT(CASE WHEN CONFIDENCE_SCORE < 0.5 THEN 1 END) as low_confidence_items,
            COUNT(CASE WHEN MANUAL_REVIEW_FLAG = TRUE THEN 1 END) as manual_review_items
        FROM BETTERJOBS_DB.STAGE.LOCATIONS_NORMALIZED
    ),

    -- Bridge Quality Metrics
    bridge_quality AS (
        SELECT
            'job_skills_bridge' as data_source,
            COUNT(*) as total_relationships,
            AVG(OVERALL_CONFIDENCE) as avg_confidence,
            COUNT(CASE WHEN OVERALL_CONFIDENCE >= 0.8 THEN 1 END) as high_confidence_relationships,
            COUNT(CASE WHEN NEEDS_REVIEW = TRUE THEN 1 END) as needs_review_relationships
        FROM BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE

        UNION ALL

        SELECT
            'job_keywords_bridge' as data_source,
            COUNT(*) as total_relationships,
            AVG(OVERALL_CONFIDENCE) as avg_confidence,
            COUNT(CASE WHEN OVERALL_CONFIDENCE >= 0.8 THEN 1 END) as high_confidence_relationships,
            0 as needs_review_relationships  -- Keywords bridge doesn't have needs_review
        FROM BETTERJOBS_DB.STAGE.JOB_KEYWORDS_BRIDGE

        UNION ALL

        SELECT
            'job_locations_bridge' as data_source,
            COUNT(*) as total_relationships,
            AVG(OVERALL_CONFIDENCE) as avg_confidence,
            COUNT(CASE WHEN OVERALL_CONFIDENCE >= 0.8 THEN 1 END) as high_confidence_relationships,
            COUNT(CASE WHEN NEEDS_REVIEW = TRUE THEN 1 END) as needs_review_relationships
        FROM BETTERJOBS_DB.STAGE.JOB_LOCATIONS_BRIDGE
    )

    -- Coverage Metrics Results
    SELECT
        CONCAT('metric_cov_', metric_name, '_', DATE_PART(epoch_second, CURRENT_TIMESTAMP())) as metric_id,
        'coverage' as metric_category,
        metric_name,
        metric_value,
        85.0 as metric_threshold_min,  -- Target 85% coverage
        100.0 as metric_threshold_max,
        CASE
            WHEN metric_value >= 85 THEN 'GREEN'
            WHEN metric_value >= 70 THEN 'YELLOW'
            ELSE 'RED'
        END as metric_status,
        'overall' as data_source,
        'daily' as time_period,
        CURRENT_TIMESTAMP,
        'automated',
        CONCAT('Current coverage: ', metric_value, '%'),
        CURRENT_TIMESTAMP,
        'system'
    FROM (
        SELECT 'skills_coverage_pct' as metric_name, skills_coverage_pct as metric_value FROM coverage_metrics
        UNION ALL
        SELECT 'keywords_coverage_pct' as metric_name, keywords_coverage_pct as metric_value FROM coverage_metrics
        UNION ALL
        SELECT 'locations_coverage_pct' as metric_name, locations_coverage_pct as metric_value FROM coverage_metrics
    )

    UNION ALL

    -- Quality Metrics Results
    SELECT
        CONCAT('metric_qual_', data_source, '_conf_', DATE_PART(epoch_second, CURRENT_TIMESTAMP())) as metric_id,
        'quality' as metric_category,
        CONCAT(data_source, '_avg_confidence') as metric_name,
        avg_confidence,
        0.70 as metric_threshold_min,  -- Target 70% average confidence
        1.0 as metric_threshold_max,
        CASE
            WHEN avg_confidence >= 0.80 THEN 'GREEN'
            WHEN avg_confidence >= 0.70 THEN 'YELLOW'
            ELSE 'RED'
        END as metric_status,
        data_source,
        'daily' as time_period,
        CURRENT_TIMESTAMP,
        'automated',
        CONCAT('Average confidence: ', ROUND(avg_confidence, 3)),
        CURRENT_TIMESTAMP,
        'system'
    FROM quality_metrics

    UNION ALL

    -- High Confidence Rate
    SELECT
        CONCAT('metric_hconf_', data_source, '_', DATE_PART(epoch_second, CURRENT_TIMESTAMP())) as metric_id,
        'quality' as metric_category,
        CONCAT(data_source, '_high_confidence_rate') as metric_name,
        ROUND((high_confidence_items::FLOAT / total_items) * 100, 2) as metric_value,
        70.0 as metric_threshold_min,  -- Target 70% high confidence
        100.0 as metric_threshold_max,
        CASE
            WHEN (high_confidence_items::FLOAT / total_items) >= 0.70 THEN 'GREEN'
            WHEN (high_confidence_items::FLOAT / total_items) >= 0.50 THEN 'YELLOW'
            ELSE 'RED'
        END as metric_status,
        data_source,
        'daily' as time_period,
        CURRENT_TIMESTAMP,
        'automated',
        CONCAT('High confidence items: ', high_confidence_items, '/', total_items),
        CURRENT_TIMESTAMP,
        'system'
    FROM quality_metrics

    UNION ALL

    -- Bridge Quality Metrics
    SELECT
        CONCAT('metric_bridge_', data_source, '_conf_', DATE_PART(epoch_second, CURRENT_TIMESTAMP())) as metric_id,
        'performance' as metric_category,
        CONCAT(data_source, '_avg_confidence') as metric_name,
        avg_confidence,
        0.70 as metric_threshold_min,
        1.0 as metric_threshold_max,
        CASE
            WHEN avg_confidence >= 0.80 THEN 'GREEN'
            WHEN avg_confidence >= 0.70 THEN 'YELLOW'
            ELSE 'RED'
        END as metric_status,
        data_source,
        'daily' as time_period,
        CURRENT_TIMESTAMP,
        'automated',
        CONCAT('Bridge relationships: ', total_relationships, ', Avg confidence: ', ROUND(avg_confidence, 3)),
        CURRENT_TIMESTAMP,
        'system'
    FROM bridge_quality;
    """

    try:
        with snowflake.get_connection() as conn:
            # Clear existing metrics for today
            context.log.info("Clearing existing quality metrics for today")
            cursor = conn.cursor()
            cursor.execute(clear_sql)
            cursor.close()

            # Generate new metrics
            context.log.info("Generating comprehensive quality metrics")
            cursor = conn.cursor()
            cursor.execute(metrics_sql)
            cursor.close()

            # Update metrics history
            history_sql = """
            INSERT INTO BETTERJOBS_DB.STAGE.LLM_QUALITY_METRICS_HISTORY (
                HISTORY_ID,
                METRIC_ID,
                METRIC_VALUE,
                CALCULATION_TIMESTAMP,
                CREATED_TIMESTAMP
            )
            SELECT
                CONCAT('hist_', METRIC_ID, '_', DATE_PART(epoch_second, CURRENT_TIMESTAMP())) as history_id,
                METRIC_ID,
                METRIC_VALUE,
                CALCULATION_TIMESTAMP,
                CURRENT_TIMESTAMP
            FROM BETTERJOBS_DB.STAGE.LLM_QUALITY_METRICS
            WHERE DATE(CALCULATION_TIMESTAMP) = CURRENT_DATE;
            """

            cursor = conn.cursor()
            cursor.execute(history_sql)
            cursor.close()

            # Get metrics summary
            summary_sql = """
            SELECT
                METRIC_CATEGORY,
                COUNT(*) as total_metrics,
                COUNT(CASE WHEN METRIC_STATUS = 'GREEN' THEN 1 END) as green_metrics,
                COUNT(CASE WHEN METRIC_STATUS = 'YELLOW' THEN 1 END) as yellow_metrics,
                COUNT(CASE WHEN METRIC_STATUS = 'RED' THEN 1 END) as red_metrics,
                AVG(CASE WHEN METRIC_NAME LIKE '%coverage%' THEN METRIC_VALUE END) as avg_coverage,
                AVG(CASE WHEN METRIC_NAME LIKE '%confidence%' THEN METRIC_VALUE END) as avg_confidence
            FROM BETTERJOBS_DB.STAGE.LLM_QUALITY_METRICS
            WHERE DATE(CALCULATION_TIMESTAMP) = CURRENT_DATE
            GROUP BY METRIC_CATEGORY
            ORDER BY total_metrics DESC;
            """

            cursor = conn.cursor()
            cursor.execute(summary_sql)
            metrics_summary = [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
            cursor.close()

            # Get top issues
            issues_sql = """
            SELECT
                METRIC_NAME,
                METRIC_VALUE,
                METRIC_STATUS,
                DATA_SOURCE,
                NOTES
            FROM BETTERJOBS_DB.STAGE.LLM_QUALITY_METRICS
            WHERE DATE(CALCULATION_TIMESTAMP) = CURRENT_DATE
              AND METRIC_STATUS IN ('RED', 'YELLOW')
            ORDER BY
                CASE METRIC_STATUS WHEN 'RED' THEN 1 ELSE 2 END,
                METRIC_VALUE ASC
            LIMIT 10;
            """

            cursor = conn.cursor()
            cursor.execute(issues_sql)
            top_issues = [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
            cursor.close()

            # Convert Decimal to float for JSON serialization
            for summary in metrics_summary:
                for key, value in summary.items():
                    if hasattr(value, 'to_eng_string'):  # Decimal object
                        summary[key] = float(value)

            for issue in top_issues:
                for key, value in issue.items():
                    if hasattr(value, 'to_eng_string'):  # Decimal object
                        issue[key] = float(value)

            total_metrics = sum(s['TOTAL_METRICS'] for s in metrics_summary)
            green_metrics = sum(s['GREEN_METRICS'] for s in metrics_summary)

            context.log.info(f"Quality metrics generation completed:")
            context.log.info(f"  - Total metrics generated: {total_metrics}")
            context.log.info(f"  - Green (healthy) metrics: {green_metrics}")
            context.log.info(f"  - Issues requiring attention: {len(top_issues)}")

            if metrics_summary:
                context.log.info("Metrics summary by category:")
                for summary in metrics_summary:
                    context.log.info(f"  - {summary['METRIC_CATEGORY']}: {summary['TOTAL_METRICS']} metrics " +
                                   f"(Green: {summary['GREEN_METRICS']}, Yellow: {summary['YELLOW_METRICS']}, Red: {summary['RED_METRICS']})")

            return {
                "status": "success",
                "total_metrics": total_metrics,
                "green_metrics": green_metrics,
                "metrics_summary": metrics_summary,
                "top_issues": top_issues,
                "timestamp": context.run_id
            }

    except Exception as e:
        context.log.error(f"Error in quality metrics generation: {str(e)}")
        raise


@asset(
    deps=["stage_skills_normalized", "stage_keywords_normalized", "stage_locations_normalized",
          "stage_job_skills_bridge", "stage_job_keywords_bridge", "stage_job_locations_bridge"],
    description="Detailed coverage analysis and gap identification",
    group_name="data_quality_governance",
    kinds={"snowflake", "python", "SQL"}
)
def stage_llm_coverage_analysis(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource
) -> Dict[str, Any]:
    """
    Generate detailed coverage analysis and identify gaps in normalization.

    Analysis Categories:
    1. Job coverage by source, date range, and characteristics
    2. Skills coverage by job family, seniority, and industry
    3. Geographic coverage analysis and location gaps
    4. Keyword coverage by business domain and role type
    5. Temporal coverage trends and seasonality analysis

    Output: Coverage reports with gap analysis and improvement recommendations
    """

    context.log.info("Starting coverage analysis")

    # Clear existing coverage analysis
    clear_sql = "DELETE FROM BETTERJOBS_DB.STAGE.LLM_COVERAGE_ANALYSIS;"

    # Generate comprehensive coverage analysis
    coverage_sql = """
    INSERT INTO BETTERJOBS_DB.STAGE.LLM_COVERAGE_ANALYSIS (
        ANALYSIS_ID,
        ANALYSIS_TYPE,
        DIMENSION_NAME,
        DIMENSION_VALUE,
        TOTAL_JOBS,
        JOBS_WITH_SKILLS,
        JOBS_WITH_KEYWORDS,
        JOBS_WITH_LOCATIONS,
        SKILLS_COVERAGE_PCT,
        KEYWORDS_COVERAGE_PCT,
        LOCATIONS_COVERAGE_PCT,
        GAP_ANALYSIS,
        RECOMMENDATIONS,
        CREATED_TIMESTAMP,
        CREATED_BY
    )
    WITH
    -- Overall Coverage
    overall_coverage AS (
        SELECT
            'overall' as dimension_name,
            'all_jobs' as dimension_value,
            COUNT(DISTINCT ju.JOB_UID) as total_jobs,
            COUNT(DISTINCT jsb.JOB_UID) as jobs_with_skills,
            COUNT(DISTINCT jkb.JOB_UID) as jobs_with_keywords,
            COUNT(DISTINCT jlb.JOB_UID) as jobs_with_locations,
            ROUND((COUNT(DISTINCT jsb.JOB_UID)::FLOAT / COUNT(DISTINCT ju.JOB_UID)) * 100, 2) as skills_coverage_pct,
            ROUND((COUNT(DISTINCT jkb.JOB_UID)::FLOAT / COUNT(DISTINCT ju.JOB_UID)) * 100, 2) as keywords_coverage_pct,
            ROUND((COUNT(DISTINCT jlb.JOB_UID)::FLOAT / COUNT(DISTINCT ju.JOB_UID)) * 100, 2) as locations_coverage_pct
        FROM BETTERJOBS_DB.STAGE.JOBS_UNIFIED ju
        LEFT JOIN BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE jsb ON ju.JOB_UID = jsb.JOB_UID
        LEFT JOIN BETTERJOBS_DB.STAGE.JOB_KEYWORDS_BRIDGE jkb ON ju.JOB_UID = jkb.JOB_UID
        LEFT JOIN BETTERJOBS_DB.STAGE.JOB_LOCATIONS_BRIDGE jlb ON ju.JOB_UID = jlb.JOB_UID
        WHERE ju.IS_ENGLISH = TRUE
    ),

    -- Coverage by Company Size
    company_size_coverage AS (
        SELECT
            'company_size' as dimension_name,
            cp.COMPANY_SIZE_CATEGORY as dimension_value,
            COUNT(DISTINCT ju.JOB_UID) as total_jobs,
            COUNT(DISTINCT jsb.JOB_UID) as jobs_with_skills,
            COUNT(DISTINCT jkb.JOB_UID) as jobs_with_keywords,
            COUNT(DISTINCT jlb.JOB_UID) as jobs_with_locations,
            ROUND((COUNT(DISTINCT jsb.JOB_UID)::FLOAT / COUNT(DISTINCT ju.JOB_UID)) * 100, 2) as skills_coverage_pct,
            ROUND((COUNT(DISTINCT jkb.JOB_UID)::FLOAT / COUNT(DISTINCT ju.JOB_UID)) * 100, 2) as keywords_coverage_pct,
            ROUND((COUNT(DISTINCT jlb.JOB_UID)::FLOAT / COUNT(DISTINCT ju.JOB_UID)) * 100, 2) as locations_coverage_pct
        FROM BETTERJOBS_DB.STAGE.JOBS_UNIFIED ju
        JOIN BETTERJOBS_DB.STAGE.COMPANY_PROFILES cp ON ju.COMPANY_ID = cp.COMPANY_ID
        LEFT JOIN BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE jsb ON ju.JOB_UID = jsb.JOB_UID
        LEFT JOIN BETTERJOBS_DB.STAGE.JOB_KEYWORDS_BRIDGE jkb ON ju.JOB_UID = jkb.JOB_UID
        LEFT JOIN BETTERJOBS_DB.STAGE.JOB_LOCATIONS_BRIDGE jlb ON ju.JOB_UID = jlb.JOB_UID
        WHERE ju.IS_ENGLISH = TRUE
          AND cp.COMPANY_SIZE_CATEGORY IS NOT NULL
        GROUP BY cp.COMPANY_SIZE_CATEGORY
        HAVING COUNT(DISTINCT ju.JOB_UID) >= 10  -- Minimum threshold for meaningful analysis
    ),

    -- Coverage by Salary Range (using LLM enriched salary data)
    salary_coverage AS (
        SELECT
            'salary_range' as dimension_name,
            CASE
                WHEN annual_salary_midpoint <= 60000 THEN 'entry_0_60k'
                WHEN annual_salary_midpoint <= 100000 THEN 'mid_60_100k'
                WHEN annual_salary_midpoint <= 150000 THEN 'senior_100_150k'
                WHEN annual_salary_midpoint <= 200000 THEN 'staff_150_200k'
                ELSE 'executive_200k_plus'
            END as dimension_value,
            COUNT(DISTINCT ju.JOB_UID) as total_jobs,
            COUNT(DISTINCT jsb.JOB_UID) as jobs_with_skills,
            COUNT(DISTINCT jkb.JOB_UID) as jobs_with_keywords,
            COUNT(DISTINCT jlb.JOB_UID) as jobs_with_locations,
            ROUND((COUNT(DISTINCT jsb.JOB_UID)::FLOAT / COUNT(DISTINCT ju.JOB_UID)) * 100, 2) as skills_coverage_pct,
            ROUND((COUNT(DISTINCT jkb.JOB_UID)::FLOAT / COUNT(DISTINCT ju.JOB_UID)) * 100, 2) as keywords_coverage_pct,
            ROUND((COUNT(DISTINCT jlb.JOB_UID)::FLOAT / COUNT(DISTINCT ju.JOB_UID)) * 100, 2) as locations_coverage_pct
        FROM BETTERJOBS_DB.STAGE.JOBS_UNIFIED ju
        JOIN BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED jle ON ju.JOB_UID = jle.JOB_UID
        LEFT JOIN BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE jsb ON ju.JOB_UID = jsb.JOB_UID
        LEFT JOIN BETTERJOBS_DB.STAGE.JOB_KEYWORDS_BRIDGE jkb ON ju.JOB_UID = jkb.JOB_UID
        LEFT JOIN BETTERJOBS_DB.STAGE.JOB_LOCATIONS_BRIDGE jlb ON ju.JOB_UID = jlb.JOB_UID
        JOIN (
            -- Calculate annual salary midpoint with period normalization
            SELECT
                JOB_UID,
                CASE
                    -- Calculate midpoint from min/max
                    WHEN SALARY_MIN IS NOT NULL AND SALARY_MAX IS NOT NULL
                    THEN (SALARY_MIN + SALARY_MAX) / 2
                    WHEN SALARY_MIN IS NOT NULL THEN SALARY_MIN
                    WHEN SALARY_MAX IS NOT NULL THEN SALARY_MAX
                    ELSE NULL
                END *
                CASE
                    -- Convert to annual based on period
                    WHEN LOWER(SALARY_PERIOD) IN ('annually', 'yearly', 'year', 'per year') THEN 1
                    WHEN LOWER(SALARY_PERIOD) IN ('monthly', 'month', 'per month') THEN 12
                    WHEN LOWER(SALARY_PERIOD) IN ('weekly', 'week', 'per week') THEN 52
                    WHEN LOWER(SALARY_PERIOD) IN ('daily', 'day', 'per day') THEN 260  -- ~22 days/month * 12 months
                    WHEN LOWER(SALARY_PERIOD) IN ('hourly', 'hour', 'per hour') THEN 2080  -- 40 hours/week * 52 weeks
                    ELSE 1  -- Default to annual if period is unclear
                END as annual_salary_midpoint
            FROM BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED
            WHERE (SALARY_MIN IS NOT NULL OR SALARY_MAX IS NOT NULL)
              AND SALARY_CURRENCY = 'USD'  -- Focus on USD for consistent comparison
              AND (SALARY_MIN > 0 OR SALARY_MAX > 0)
        ) salary_calc ON ju.JOB_UID = salary_calc.JOB_UID
        WHERE ju.IS_ENGLISH = TRUE
          AND salary_calc.annual_salary_midpoint IS NOT NULL
          AND salary_calc.annual_salary_midpoint > 20000  -- Reasonable minimum annual salary
          AND salary_calc.annual_salary_midpoint < 1000000  -- Reasonable maximum to filter outliers
        GROUP BY
            CASE
                WHEN annual_salary_midpoint <= 60000 THEN 'entry_0_60k'
                WHEN annual_salary_midpoint <= 100000 THEN 'mid_60_100k'
                WHEN annual_salary_midpoint <= 150000 THEN 'senior_100_150k'
                WHEN annual_salary_midpoint <= 200000 THEN 'staff_150_200k'
                ELSE 'executive_200k_plus'
            END
        HAVING COUNT(DISTINCT ju.JOB_UID) >= 10
    )

    -- Overall Coverage Results
    SELECT
        CONCAT('cov_overall_', DATE_PART(epoch_second, CURRENT_TIMESTAMP())) as analysis_id,
        'overall' as analysis_type,
        dimension_name,
        dimension_value,
        total_jobs,
        jobs_with_skills,
        jobs_with_keywords,
        jobs_with_locations,
        skills_coverage_pct,
        keywords_coverage_pct,
        locations_coverage_pct,
        CASE
            WHEN skills_coverage_pct < 85 OR keywords_coverage_pct < 80 OR locations_coverage_pct < 75
            THEN 'Coverage below target thresholds'
            ELSE 'Coverage meets targets'
        END as gap_analysis,
        CASE
            WHEN skills_coverage_pct < 85 THEN 'Focus on improving LLM skills extraction quality'
            WHEN keywords_coverage_pct < 80 THEN 'Enhance keyword standardization rules'
            WHEN locations_coverage_pct < 75 THEN 'Improve location parsing and standardization'
            ELSE 'Maintain current quality levels'
        END as recommendations,
        CURRENT_TIMESTAMP,
        'system'
    FROM overall_coverage

    UNION ALL

    -- Company Size Coverage Results
    SELECT
        CONCAT('cov_company_', dimension_value, '_', DATE_PART(epoch_second, CURRENT_TIMESTAMP())) as analysis_id,
        'company_size' as analysis_type,
        dimension_name,
        dimension_value,
        total_jobs,
        jobs_with_skills,
        jobs_with_keywords,
        jobs_with_locations,
        skills_coverage_pct,
        keywords_coverage_pct,
        locations_coverage_pct,
        CASE
            WHEN skills_coverage_pct < 80 THEN CONCAT('Low skills coverage (', skills_coverage_pct, '%) for ', dimension_value)
            WHEN keywords_coverage_pct < 75 THEN CONCAT('Low keywords coverage (', keywords_coverage_pct, '%) for ', dimension_value)
            WHEN locations_coverage_pct < 70 THEN CONCAT('Low locations coverage (', locations_coverage_pct, '%) for ', dimension_value)
            ELSE 'Coverage acceptable for company size segment'
        END as gap_analysis,
        CASE
            WHEN dimension_value = 'startup_1_50' AND skills_coverage_pct < 80
            THEN 'Startups may have less structured job descriptions - enhance extraction rules'
            WHEN dimension_value = 'enterprise_5000plus' AND locations_coverage_pct < 70
            THEN 'Large companies may have multiple locations - improve multi-location parsing'
            ELSE 'Monitor trends and maintain quality'
        END as recommendations,
        CURRENT_TIMESTAMP,
        'system'
    FROM company_size_coverage

    UNION ALL

    -- Salary Range Coverage Results
    SELECT
        CONCAT('cov_salary_', dimension_value, '_', DATE_PART(epoch_second, CURRENT_TIMESTAMP())) as analysis_id,
        'salary_range' as analysis_type,
        dimension_name,
        dimension_value,
        total_jobs,
        jobs_with_skills,
        jobs_with_keywords,
        jobs_with_locations,
        skills_coverage_pct,
        keywords_coverage_pct,
        locations_coverage_pct,
        CASE
            WHEN skills_coverage_pct < 80 THEN CONCAT('Skills coverage gap for ', dimension_value, ' positions')
            WHEN keywords_coverage_pct < 75 THEN CONCAT('Keywords coverage gap for ', dimension_value, ' positions')
            ELSE 'Coverage acceptable for salary range'
        END as gap_analysis,
        CASE
            WHEN dimension_value = 'entry_0_60k' AND skills_coverage_pct < 80
            THEN 'Entry-level positions may need different extraction patterns'
            WHEN dimension_value = 'executive_200k_plus' AND keywords_coverage_pct < 75
            THEN 'Executive positions may require specialized keyword rules'
            ELSE 'Continue monitoring coverage patterns'
        END as recommendations,
        CURRENT_TIMESTAMP,
        'system'
    FROM salary_coverage;
    """

    try:
        with snowflake.get_connection() as conn:
            # Clear existing analysis
            context.log.info("Clearing existing coverage analysis")
            cursor = conn.cursor()
            cursor.execute(clear_sql)
            cursor.close()

            # Generate new coverage analysis
            context.log.info("Generating comprehensive coverage analysis")
            cursor = conn.cursor()
            cursor.execute(coverage_sql)
            cursor.close()

            # Get analysis summary
            summary_sql = """
            SELECT
                ANALYSIS_TYPE,
                COUNT(*) as total_segments,
                AVG(SKILLS_COVERAGE_PCT) as avg_skills_coverage,
                AVG(KEYWORDS_COVERAGE_PCT) as avg_keywords_coverage,
                AVG(LOCATIONS_COVERAGE_PCT) as avg_locations_coverage,
                COUNT(CASE WHEN SKILLS_COVERAGE_PCT < 85 THEN 1 END) as skills_gaps,
                COUNT(CASE WHEN KEYWORDS_COVERAGE_PCT < 80 THEN 1 END) as keywords_gaps,
                COUNT(CASE WHEN LOCATIONS_COVERAGE_PCT < 75 THEN 1 END) as locations_gaps
            FROM BETTERJOBS_DB.STAGE.LLM_COVERAGE_ANALYSIS
            GROUP BY ANALYSIS_TYPE
            ORDER BY total_segments DESC;
            """

            cursor = conn.cursor()
            cursor.execute(summary_sql)
            analysis_summary = [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
            cursor.close()

            # Get top gaps
            gaps_sql = """
            SELECT
                ANALYSIS_TYPE,
                DIMENSION_VALUE,
                TOTAL_JOBS,
                SKILLS_COVERAGE_PCT,
                KEYWORDS_COVERAGE_PCT,
                LOCATIONS_COVERAGE_PCT,
                GAP_ANALYSIS,
                RECOMMENDATIONS
            FROM BETTERJOBS_DB.STAGE.LLM_COVERAGE_ANALYSIS
            WHERE SKILLS_COVERAGE_PCT < 85 OR KEYWORDS_COVERAGE_PCT < 80 OR LOCATIONS_COVERAGE_PCT < 75
            ORDER BY TOTAL_JOBS DESC
            LIMIT 10;
            """

            cursor = conn.cursor()
            cursor.execute(gaps_sql)
            top_gaps = [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
            cursor.close()

            # Convert Decimal to float for JSON serialization
            for summary in analysis_summary:
                for key, value in summary.items():
                    if hasattr(value, 'to_eng_string'):  # Decimal object
                        summary[key] = float(value)

            for gap in top_gaps:
                for key, value in gap.items():
                    if hasattr(value, 'to_eng_string'):  # Decimal object
                        gap[key] = float(value)

            total_segments = sum(s['TOTAL_SEGMENTS'] for s in analysis_summary)
            coverage_gaps = len(top_gaps)

            context.log.info(f"Coverage analysis completed:")
            context.log.info(f"  - Total segments analyzed: {total_segments}")
            context.log.info(f"  - Coverage gaps identified: {coverage_gaps}")

            if analysis_summary:
                context.log.info("Coverage summary by analysis type:")
                for summary in analysis_summary:
                    context.log.info(f"  - {summary['ANALYSIS_TYPE']}: {summary['TOTAL_SEGMENTS']} segments " +
                                   f"(Skills: {summary['AVG_SKILLS_COVERAGE']:.1f}%, Keywords: {summary['AVG_KEYWORDS_COVERAGE']:.1f}%, Locations: {summary['AVG_LOCATIONS_COVERAGE']:.1f}%)")

            return {
                "status": "success",
                "total_segments": total_segments,
                "coverage_gaps": coverage_gaps,
                "analysis_summary": analysis_summary,
                "top_gaps": top_gaps,
                "timestamp": context.run_id
            }

    except Exception as e:
        context.log.error(f"Error in coverage analysis: {str(e)}")
        raise


@asset(
    deps=["stage_skills_normalized", "stage_keywords_normalized", "stage_locations_normalized",
          "stage_job_skills_bridge", "stage_job_keywords_bridge", "stage_job_locations_bridge"],
    description="Monitor confidence score distributions and quality trends",
    group_name="data_quality_governance",
    kinds={"snowflake", "python", "SQL"}
)
def stage_llm_confidence_monitoring(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource
) -> Dict[str, Any]:
    """
    Monitor confidence score distributions and quality trends.

    Monitoring Categories:
    1. Confidence score distribution analysis by category
    2. Low confidence item identification and categorization
    3. Manual review queue prioritization
    4. Quality improvement trend tracking
    5. Confidence correlation analysis across data types

    Output: Confidence analytics and quality improvement insights
    """

    context.log.info("Starting confidence monitoring")

    # Clear existing confidence monitoring
    clear_sql = "DELETE FROM BETTERJOBS_DB.STAGE.LLM_CONFIDENCE_MONITORING;"

    # Generate confidence monitoring data
    monitoring_sql = """
    INSERT INTO BETTERJOBS_DB.STAGE.LLM_CONFIDENCE_MONITORING (
        MONITORING_ID,
        DATA_SOURCE,
        CONFIDENCE_BUCKET,
        ITEM_COUNT,
        PERCENTAGE_OF_TOTAL,
        AVG_CONFIDENCE_IN_BUCKET,
        TREND_DIRECTION,
        QUALITY_RATING,
        ACTION_REQUIRED,
        CREATED_TIMESTAMP,
        CREATED_BY
    )
    WITH
    -- Skills Confidence Distribution
    skills_confidence AS (
        SELECT
            'skills' as data_source,
            CASE
                WHEN CONFIDENCE_SCORE >= 0.9 THEN 'excellent_90_100'
                WHEN CONFIDENCE_SCORE >= 0.8 THEN 'good_80_90'
                WHEN CONFIDENCE_SCORE >= 0.7 THEN 'acceptable_70_80'
                WHEN CONFIDENCE_SCORE >= 0.5 THEN 'low_50_70'
                ELSE 'poor_below_50'
            END as confidence_bucket,
            COUNT(*) as item_count,
            AVG(CONFIDENCE_SCORE) as avg_confidence_in_bucket
        FROM BETTERJOBS_DB.STAGE.SKILLS_NORMALIZED
        GROUP BY
            CASE
                WHEN CONFIDENCE_SCORE >= 0.9 THEN 'excellent_90_100'
                WHEN CONFIDENCE_SCORE >= 0.8 THEN 'good_80_90'
                WHEN CONFIDENCE_SCORE >= 0.7 THEN 'acceptable_70_80'
                WHEN CONFIDENCE_SCORE >= 0.5 THEN 'low_50_70'
                ELSE 'poor_below_50'
            END
    ),

    -- Keywords Confidence Distribution
    keywords_confidence AS (
        SELECT
            'keywords' as data_source,
            CASE
                WHEN CONFIDENCE_SCORE >= 0.9 THEN 'excellent_90_100'
                WHEN CONFIDENCE_SCORE >= 0.8 THEN 'good_80_90'
                WHEN CONFIDENCE_SCORE >= 0.7 THEN 'acceptable_70_80'
                WHEN CONFIDENCE_SCORE >= 0.5 THEN 'low_50_70'
                ELSE 'poor_below_50'
            END as confidence_bucket,
            COUNT(*) as item_count,
            AVG(CONFIDENCE_SCORE) as avg_confidence_in_bucket
        FROM BETTERJOBS_DB.STAGE.KEYWORDS_NORMALIZED
        GROUP BY
            CASE
                WHEN CONFIDENCE_SCORE >= 0.9 THEN 'excellent_90_100'
                WHEN CONFIDENCE_SCORE >= 0.8 THEN 'good_80_90'
                WHEN CONFIDENCE_SCORE >= 0.7 THEN 'acceptable_70_80'
                WHEN CONFIDENCE_SCORE >= 0.5 THEN 'low_50_70'
                ELSE 'poor_below_50'
            END
    ),

    -- Locations Confidence Distribution
    locations_confidence AS (
        SELECT
            'locations' as data_source,
            CASE
                WHEN CONFIDENCE_SCORE >= 0.9 THEN 'excellent_90_100'
                WHEN CONFIDENCE_SCORE >= 0.8 THEN 'good_80_90'
                WHEN CONFIDENCE_SCORE >= 0.7 THEN 'acceptable_70_80'
                WHEN CONFIDENCE_SCORE >= 0.5 THEN 'low_50_70'
                ELSE 'poor_below_50'
            END as confidence_bucket,
            COUNT(*) as item_count,
            AVG(CONFIDENCE_SCORE) as avg_confidence_in_bucket
        FROM BETTERJOBS_DB.STAGE.LOCATIONS_NORMALIZED
        GROUP BY
            CASE
                WHEN CONFIDENCE_SCORE >= 0.9 THEN 'excellent_90_100'
                WHEN CONFIDENCE_SCORE >= 0.8 THEN 'good_80_90'
                WHEN CONFIDENCE_SCORE >= 0.7 THEN 'acceptable_70_80'
                WHEN CONFIDENCE_SCORE >= 0.5 THEN 'low_50_70'
                ELSE 'poor_below_50'
            END
    ),

    -- Calculate totals for percentage calculations
    totals AS (
        SELECT
            'skills' as data_source,
            SUM(item_count) as total_items
        FROM skills_confidence

        UNION ALL

        SELECT
            'keywords' as data_source,
            SUM(item_count) as total_items
        FROM keywords_confidence

        UNION ALL

        SELECT
            'locations' as data_source,
            SUM(item_count) as total_items
        FROM locations_confidence
    )

    -- Skills Results
    SELECT
        CONCAT('conf_skills_', confidence_bucket, '_', DATE_PART(epoch_second, CURRENT_TIMESTAMP())) as monitoring_id,
        sc.data_source,
        sc.confidence_bucket,
        sc.item_count,
        ROUND((sc.item_count::FLOAT / t.total_items) * 100, 2) as percentage_of_total,
        sc.avg_confidence_in_bucket,
        'stable' as trend_direction,  -- TODO: Calculate actual trend from history
        CASE
            WHEN sc.confidence_bucket IN ('excellent_90_100', 'good_80_90') THEN 'excellent'
            WHEN sc.confidence_bucket = 'acceptable_70_80' THEN 'good'
            WHEN sc.confidence_bucket = 'low_50_70' THEN 'needs_improvement'
            ELSE 'critical'
        END as quality_rating,
        CASE
            WHEN sc.confidence_bucket = 'poor_below_50' THEN 'immediate_review_required'
            WHEN sc.confidence_bucket = 'low_50_70' THEN 'improve_standardization_rules'
            ELSE 'monitor_trends'
        END as action_required,
        CURRENT_TIMESTAMP,
        'system'
    FROM skills_confidence sc
    JOIN totals t ON sc.data_source = t.data_source

    UNION ALL

    -- Keywords Results
    SELECT
        CONCAT('conf_keywords_', confidence_bucket, '_', DATE_PART(epoch_second, CURRENT_TIMESTAMP())) as monitoring_id,
        kc.data_source,
        kc.confidence_bucket,
        kc.item_count,
        ROUND((kc.item_count::FLOAT / t.total_items) * 100, 2) as percentage_of_total,
        kc.avg_confidence_in_bucket,
        'stable' as trend_direction,
        CASE
            WHEN kc.confidence_bucket IN ('excellent_90_100', 'good_80_90') THEN 'excellent'
            WHEN kc.confidence_bucket = 'acceptable_70_80' THEN 'good'
            WHEN kc.confidence_bucket = 'low_50_70' THEN 'needs_improvement'
            ELSE 'critical'
        END as quality_rating,
        CASE
            WHEN kc.confidence_bucket = 'poor_below_50' THEN 'immediate_review_required'
            WHEN kc.confidence_bucket = 'low_50_70' THEN 'improve_standardization_rules'
            ELSE 'monitor_trends'
        END as action_required,
        CURRENT_TIMESTAMP,
        'system'
    FROM keywords_confidence kc
    JOIN totals t ON kc.data_source = t.data_source

    UNION ALL

    -- Locations Results
    SELECT
        CONCAT('conf_locations_', confidence_bucket, '_', DATE_PART(epoch_second, CURRENT_TIMESTAMP())) as monitoring_id,
        lc.data_source,
        lc.confidence_bucket,
        lc.item_count,
        ROUND((lc.item_count::FLOAT / t.total_items) * 100, 2) as percentage_of_total,
        lc.avg_confidence_in_bucket,
        'stable' as trend_direction,
        CASE
            WHEN lc.confidence_bucket IN ('excellent_90_100', 'good_80_90') THEN 'excellent'
            WHEN lc.confidence_bucket = 'acceptable_70_80' THEN 'good'
            WHEN lc.confidence_bucket = 'low_50_70' THEN 'needs_improvement'
            ELSE 'critical'
        END as quality_rating,
        CASE
            WHEN lc.confidence_bucket = 'poor_below_50' THEN 'immediate_review_required'
            WHEN lc.confidence_bucket = 'low_50_70' THEN 'improve_standardization_rules'
            ELSE 'monitor_trends'
        END as action_required,
        CURRENT_TIMESTAMP,
        'system'
    FROM locations_confidence lc
    JOIN totals t ON lc.data_source = t.data_source;
    """

    try:
        with snowflake.get_connection() as conn:
            # Clear existing monitoring data
            context.log.info("Clearing existing confidence monitoring data")
            cursor = conn.cursor()
            cursor.execute(clear_sql)
            cursor.close()

            # Generate new monitoring data
            context.log.info("Generating confidence monitoring analysis")
            cursor = conn.cursor()
            cursor.execute(monitoring_sql)
            cursor.close()

            # Get monitoring summary
            summary_sql = """
            SELECT
                DATA_SOURCE,
                SUM(ITEM_COUNT) as total_items,
                SUM(CASE WHEN QUALITY_RATING = 'excellent' THEN ITEM_COUNT ELSE 0 END) as excellent_items,
                SUM(CASE WHEN QUALITY_RATING = 'good' THEN ITEM_COUNT ELSE 0 END) as good_items,
                SUM(CASE WHEN QUALITY_RATING = 'needs_improvement' THEN ITEM_COUNT ELSE 0 END) as needs_improvement_items,
                SUM(CASE WHEN QUALITY_RATING = 'critical' THEN ITEM_COUNT ELSE 0 END) as critical_items,
                ROUND(AVG(AVG_CONFIDENCE_IN_BUCKET), 3) as overall_avg_confidence
            FROM BETTERJOBS_DB.STAGE.LLM_CONFIDENCE_MONITORING
            GROUP BY DATA_SOURCE
            ORDER BY total_items DESC;
            """

            cursor = conn.cursor()
            cursor.execute(summary_sql)
            monitoring_summary = [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
            cursor.close()

            # Get items requiring action
            action_sql = """
            SELECT
                DATA_SOURCE,
                CONFIDENCE_BUCKET,
                ITEM_COUNT,
                PERCENTAGE_OF_TOTAL,
                QUALITY_RATING,
                ACTION_REQUIRED
            FROM BETTERJOBS_DB.STAGE.LLM_CONFIDENCE_MONITORING
            WHERE ACTION_REQUIRED IN ('immediate_review_required', 'improve_standardization_rules')
            ORDER BY
                CASE ACTION_REQUIRED WHEN 'immediate_review_required' THEN 1 ELSE 2 END,
                PERCENTAGE_OF_TOTAL DESC;
            """

            cursor = conn.cursor()
            cursor.execute(action_sql)
            action_items = [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
            cursor.close()

            # Convert Decimal to float for JSON serialization
            for summary in monitoring_summary:
                for key, value in summary.items():
                    if hasattr(value, 'to_eng_string'):  # Decimal object
                        summary[key] = float(value)

            for item in action_items:
                for key, value in item.items():
                    if hasattr(value, 'to_eng_string'):  # Decimal object
                        item[key] = float(value)

            total_items_monitored = sum(s['TOTAL_ITEMS'] for s in monitoring_summary)
            critical_action_items = len([i for i in action_items if i['ACTION_REQUIRED'] == 'immediate_review_required'])

            context.log.info(f"Confidence monitoring completed:")
            context.log.info(f"  - Total items monitored: {total_items_monitored}")
            context.log.info(f"  - Critical action items: {critical_action_items}")

            if monitoring_summary:
                context.log.info("Confidence monitoring summary by data source:")
                for summary in monitoring_summary:
                    context.log.info(f"  - {summary['DATA_SOURCE']}: {summary['TOTAL_ITEMS']} items " +
                                   f"(Excellent: {summary['EXCELLENT_ITEMS']}, Critical: {summary['CRITICAL_ITEMS']}, Avg Confidence: {summary['OVERALL_AVG_CONFIDENCE']:.3f})")

            return {
                "status": "success",
                "total_items_monitored": total_items_monitored,
                "critical_action_items": critical_action_items,
                "monitoring_summary": monitoring_summary,
                "action_items": action_items,
                "timestamp": context.run_id
            }

    except Exception as e:
        context.log.error(f"Error in confidence monitoring: {str(e)}")
        raise



@asset(
    deps=["stage_llm_data_quality_validation", "stage_llm_confidence_monitoring"],
    description="Manual review queue management and prioritization",
    group_name="data_quality_governance",
    kinds={"snowflake", "python", "SQL"}
)
def stage_llm_manual_review_queue(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource
) -> Dict[str, Any]:
    """
    Manage manual review queue and human validation workflow.

    Queue Management Features:
    1. Automated queue population based on confidence thresholds
    2. Priority scoring based on business impact and data quality issues
    3. Review status tracking and assignment management
    4. Batch processing for efficient human review
    5. Feedback loop integration for continuous improvement

    Output: Prioritized review queue and validation tracking
    """

    context.log.info("Starting manual review queue management")

    # Clear existing review queue
    clear_sql = "DELETE FROM BETTERJOBS_DB.STAGE.LLM_MANUAL_REVIEW_QUEUE;"

    # Populate manual review queue
    queue_sql = """
    INSERT INTO BETTERJOBS_DB.STAGE.LLM_MANUAL_REVIEW_QUEUE (
        REVIEW_ID,
        ITEM_TYPE,
        ITEM_ID,
        DATA_SOURCE,
        REVIEW_REASON,
        PRIORITY_SCORE,
        CONFIDENCE_SCORE,
        ORIGINAL_VALUE,
        SUGGESTED_VALUE,
        BUSINESS_IMPACT,
        REVIEW_STATUS,
        ASSIGNED_TO,
        CREATED_TIMESTAMP,
        UPDATED_TIMESTAMP,
        CREATED_BY
    )
    WITH
    -- Skills requiring review
    skills_review AS (
        SELECT
            'skill' as item_type,
            SKILL_ID as item_id,
            SKILL_NAME as item_name,
            'skills' as data_source,
            CONFIDENCE_SCORE,
            FREQUENCY_COUNT,
            CASE
                WHEN CONFIDENCE_SCORE < 0.3 THEN 'very_low_confidence'
                WHEN CONFIDENCE_SCORE < 0.5 THEN 'low_confidence'
                WHEN MANUAL_REVIEW_FLAG = TRUE THEN 'manual_review_flagged'
                WHEN FREQUENCY_COUNT > 100 AND CONFIDENCE_SCORE < 0.7 THEN 'high_impact_low_confidence'
                ELSE 'other'
            END as review_reason,
            CASE
                WHEN FREQUENCY_COUNT > 500 THEN 'high'
                WHEN FREQUENCY_COUNT > 100 THEN 'medium'
                ELSE 'low'
            END as business_impact
        FROM BETTERJOBS_DB.STAGE.SKILLS_NORMALIZED
        WHERE CONFIDENCE_SCORE < 0.5
           OR MANUAL_REVIEW_FLAG = TRUE
           OR (FREQUENCY_COUNT > 100 AND CONFIDENCE_SCORE < 0.7)
    ),

    -- Locations requiring review
    locations_review AS (
        SELECT
            'location' as item_type,
            LOCATION_ID as item_id,
            LOCATION_NAME as item_name,
            'locations' as data_source,
            CONFIDENCE_SCORE,
            FREQUENCY_COUNT,
            CASE
                WHEN CONFIDENCE_SCORE < 0.3 THEN 'very_low_confidence'
                WHEN CONFIDENCE_SCORE < 0.5 THEN 'low_confidence'
                WHEN MANUAL_REVIEW_FLAG = TRUE THEN 'manual_review_flagged'
                WHEN FREQUENCY_COUNT > 50 AND CONFIDENCE_SCORE < 0.7 THEN 'high_impact_low_confidence'
                ELSE 'other'
            END as review_reason,
            CASE
                WHEN FREQUENCY_COUNT > 200 THEN 'high'
                WHEN FREQUENCY_COUNT > 50 THEN 'medium'
                ELSE 'low'
            END as business_impact
        FROM BETTERJOBS_DB.STAGE.LOCATIONS_NORMALIZED
        WHERE CONFIDENCE_SCORE < 0.5
           OR MANUAL_REVIEW_FLAG = TRUE
           OR (FREQUENCY_COUNT > 50 AND CONFIDENCE_SCORE < 0.7)
    ),

    -- Bridge relationships requiring review
    bridge_review AS (
        SELECT
            'bridge_relationship' as item_type,
            BRIDGE_ID as item_id,
            CONCAT(SKILL_CATEGORY, ':', ORIGINAL_TEXT) as item_name,
            'job_skills_bridge' as data_source,
            OVERALL_CONFIDENCE as confidence_score,
            1 as frequency_count,  -- Individual relationships don't have frequency
            CASE
                WHEN OVERALL_CONFIDENCE < 0.3 THEN 'very_low_confidence'
                WHEN OVERALL_CONFIDENCE < 0.5 THEN 'low_confidence'
                WHEN NEEDS_REVIEW = TRUE THEN 'manual_review_flagged'
                ELSE 'other'
            END as review_reason,
            'medium' as business_impact  -- Individual relationships have medium impact
        FROM BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE
        WHERE OVERALL_CONFIDENCE < 0.5
           OR NEEDS_REVIEW = TRUE
        LIMIT 1000  -- Limit bridge reviews to manageable number
    )

    -- Skills Review Items
    SELECT
        CONCAT('rev_skill_', item_id, '_', DATE_PART(epoch_second, CURRENT_TIMESTAMP())) as review_id,
        item_type,
        item_id,
        data_source,
        review_reason,
        -- Priority score calculation (higher = more urgent)
        CASE business_impact
            WHEN 'high' THEN 100
            WHEN 'medium' THEN 50
            ELSE 25
        END +
        CASE review_reason
            WHEN 'very_low_confidence' THEN 50
            WHEN 'low_confidence' THEN 30
            WHEN 'high_impact_low_confidence' THEN 40
            WHEN 'manual_review_flagged' THEN 35
            ELSE 10
        END +
        -- Frequency bonus
        LEAST(frequency_count / 10, 25) as priority_score,
        confidence_score,
        item_name as original_value,
        CASE
            WHEN review_reason = 'very_low_confidence' THEN 'Requires confidence improvement'
            WHEN review_reason = 'manual_review_flagged' THEN 'Manual review needed'
            ELSE 'Review standardization'
        END as suggested_value,
        business_impact,
        'PENDING' as review_status,
        NULL as assigned_to,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP as updated_timestamp,
        'system'
    FROM skills_review

    UNION ALL

    -- Locations Review Items
    SELECT
        CONCAT('rev_location_', item_id, '_', DATE_PART(epoch_second, CURRENT_TIMESTAMP())) as review_id,
        item_type,
        item_id,
        data_source,
        review_reason,
        CASE business_impact
            WHEN 'high' THEN 100
            WHEN 'medium' THEN 50
            ELSE 25
        END +
        CASE review_reason
            WHEN 'very_low_confidence' THEN 50
            WHEN 'low_confidence' THEN 30
            WHEN 'high_impact_low_confidence' THEN 40
            WHEN 'manual_review_flagged' THEN 35
            ELSE 10
        END +
        LEAST(frequency_count / 5, 25) as priority_score,
        confidence_score,
        item_name as original_value,
        CASE
            WHEN review_reason = 'very_low_confidence' THEN 'Requires confidence improvement'
            WHEN review_reason = 'manual_review_flagged' THEN 'Manual review needed'
            ELSE 'Review standardization'
        END as suggested_value,
        business_impact,
        'PENDING' as review_status,
        NULL as assigned_to,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP as updated_timestamp,
        'system'
    FROM locations_review

    UNION ALL

    -- Bridge Review Items (limited sample)
    SELECT
        CONCAT('rev_bridge_', item_id, '_', DATE_PART(epoch_second, CURRENT_TIMESTAMP())) as review_id,
        item_type,
        item_id,
        data_source,
        review_reason,
        CASE review_reason
            WHEN 'very_low_confidence' THEN 75
            WHEN 'low_confidence' THEN 50
            WHEN 'manual_review_flagged' THEN 60
            ELSE 25
        END as priority_score,
        confidence_score,
        item_name as original_value,
        CASE
            WHEN review_reason = 'very_low_confidence' THEN 'Requires confidence improvement'
            WHEN review_reason = 'manual_review_flagged' THEN 'Manual review needed'
            ELSE 'Review relationship'
        END as suggested_value,
        business_impact,
        'PENDING' as review_status,
        NULL as assigned_to,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP as updated_timestamp,
        'system'
    FROM bridge_review;
    """

    try:
        with snowflake.get_connection() as conn:
            # Clear existing queue
            context.log.info("Clearing existing manual review queue")
            cursor = conn.cursor()
            cursor.execute(clear_sql)
            cursor.close()

            # Populate new queue
            context.log.info("Populating manual review queue")
            cursor = conn.cursor()
            cursor.execute(queue_sql)
            cursor.close()

            # Get queue summary
            summary_sql = """
            SELECT
                ITEM_TYPE,
                COUNT(*) as total_items,
                COUNT(CASE WHEN BUSINESS_IMPACT = 'high' THEN 1 END) as high_impact_items,
                COUNT(CASE WHEN REVIEW_STATUS = 'PENDING' THEN 1 END) as pending_items,
                AVG(PRIORITY_SCORE) as avg_priority_score,
                AVG(CONFIDENCE_SCORE) as avg_confidence_score
            FROM BETTERJOBS_DB.STAGE.LLM_MANUAL_REVIEW_QUEUE
            GROUP BY ITEM_TYPE
            ORDER BY total_items DESC;
            """

            cursor = conn.cursor()
            cursor.execute(summary_sql)
            queue_summary = [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
            cursor.close()

            # Get high priority items
            priority_sql = """
            SELECT
                ITEM_TYPE,
                ORIGINAL_VALUE,
                DATA_SOURCE,
                CONFIDENCE_SCORE,
                PRIORITY_SCORE,
                REVIEW_REASON,
                BUSINESS_IMPACT,
                REVIEW_STATUS
            FROM BETTERJOBS_DB.STAGE.LLM_MANUAL_REVIEW_QUEUE
            ORDER BY PRIORITY_SCORE DESC
            LIMIT 20;
            """

            cursor = conn.cursor()
            cursor.execute(priority_sql)
            high_priority_items = [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
            cursor.close()

            # Convert Decimal to float for JSON serialization
            for summary in queue_summary:
                for key, value in summary.items():
                    if hasattr(value, 'to_eng_string'):  # Decimal object
                        summary[key] = float(value)

            for item in high_priority_items:
                for key, value in item.items():
                    if hasattr(value, 'to_eng_string'):  # Decimal object
                        item[key] = float(value)

            total_queue_items = sum(s['TOTAL_ITEMS'] for s in queue_summary)
            high_impact_items = sum(s['HIGH_IMPACT_ITEMS'] for s in queue_summary)

            context.log.info(f"Manual review queue management completed:")
            context.log.info(f"  - Total items in queue: {total_queue_items}")
            context.log.info(f"  - High impact items: {high_impact_items}")

            if queue_summary:
                context.log.info("Queue summary by item type:")
                for summary in queue_summary:
                    context.log.info(f"  - {summary['ITEM_TYPE']}: {summary['TOTAL_ITEMS']} items " +
                                   f"(High impact: {summary['HIGH_IMPACT_ITEMS']}, Pending: {summary['PENDING_ITEMS']}, Avg priority: {summary['AVG_PRIORITY_SCORE']:.1f})")

            return {
                "status": "success",
                "total_queue_items": total_queue_items,
                "high_impact_items": high_impact_items,
                "queue_summary": queue_summary,
                "high_priority_items": high_priority_items,
                "timestamp": context.run_id
            }

    except Exception as e:
        context.log.error(f"Error in manual review queue management: {str(e)}")
        raise