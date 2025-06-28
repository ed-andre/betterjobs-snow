"""
LLM Enrichment Coordination Asset

Lightweight coordinator for LLM enrichment monitoring and validation across all platforms.
Part of ENHANCEMENT-010: LLM Enrichment Asset Breakdown for parallel processing.

This asset monitors completion status of all platform LLM assets, aggregates
statistics, and validates cross-platform LLM enrichment quality.
"""

from typing import Dict, Any, List
from datetime import datetime

from dagster import (
    asset,
    AssetExecutionContext,
    MetadataValue
)

from dagster_betterjobs.transformations.llm_processing import (
    create_llm_enrichment_table_if_not_exists
)


@asset(
    group_name="2a_stage_cleaning_enrichment",
    kinds={"snowflake", "python"},
    required_resource_keys={"snowflake"},
    deps=[
        "stage_jobs_llm_enriched_bamboohr",
        "stage_jobs_llm_enriched_greenhouse",
        "stage_jobs_llm_enriched_workday",
        "stage_jobs_llm_enriched_smartrecruiters"
    ],
    description="Monitor and validate completion of all platform LLM enrichment assets"
)
def stage_jobs_llm_enriched_unified(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Lightweight coordinator for LLM enrichment monitoring and validation.

    This asset runs after all individual platform LLM assets complete and provides:
    - Cross-platform processing statistics aggregation
    - Quality validation across all platforms
    - Unified monitoring and reporting
    - Data consistency checks

    Returns:
        Dict with coordination statistics and validation results
    """

    # Get Snowflake connection
    conn = context.resources.snowflake.get_connection()
    cursor = None

    coordination_stats = {
        "coordination_timestamp": datetime.now().isoformat(),
        "platforms_coordinated": ["bamboohr", "greenhouse", "workday", "smartrecruiters"],
        "coordination_status": "completed",
        "total_jobs_processed": 0,
        "total_jobs_enriched": 0,
        "overall_success_rate": 0.0,
        "avg_confidence_score": 0.0,
        "platform_summaries": {},
        "data_quality_checks": {},
        "recommendations": []
    }

    try:
        cursor = conn.cursor()

        # Ensure LLM enrichment table exists
        create_llm_enrichment_table_if_not_exists(context)
        context.log.info("LLM enrichment table verified/created")

        context.log.info("🎯 Starting LLM enrichment coordination and validation...")

        # Aggregate statistics across all platforms
        platform_stats = aggregate_platform_statistics(cursor, context)
        coordination_stats.update(platform_stats)

        # Perform cross-platform quality validation
        quality_checks = perform_quality_validation(cursor, context)
        coordination_stats["data_quality_checks"] = quality_checks

        # Generate recommendations based on results
        recommendations = generate_recommendations(coordination_stats, context)
        coordination_stats["recommendations"] = recommendations

        # Log summary
        context.log.info(f"""
        🎯 LLM Enrichment Coordination Complete:
        • Total Jobs Processed: {coordination_stats['total_jobs_processed']:,}
        • Total Jobs Enriched: {coordination_stats['total_jobs_enriched']:,}
        • Overall Success Rate: {coordination_stats['overall_success_rate']:.1f}%
        • Average Confidence: {coordination_stats['avg_confidence_score']:.3f}
        • Platforms: {len(coordination_stats['platforms_coordinated'])}
        """)

        # Add coordination metadata for Dagster UI
        context.add_output_metadata({
            "total_jobs_processed": MetadataValue.int(coordination_stats["total_jobs_processed"]),
            "total_jobs_enriched": MetadataValue.int(coordination_stats["total_jobs_enriched"]),
            "overall_success_rate": MetadataValue.float(coordination_stats["overall_success_rate"]),
            "avg_confidence_score": MetadataValue.float(coordination_stats["avg_confidence_score"]),
            "platforms_coordinated": MetadataValue.int(len(coordination_stats["platforms_coordinated"])),
            "coordination_status": MetadataValue.text(coordination_stats["coordination_status"]),
            "quality_check_results": MetadataValue.json(coordination_stats["data_quality_checks"]),
            "recommendations": MetadataValue.json(coordination_stats["recommendations"])
        })

        return coordination_stats

    except Exception as e:
        context.log.error(f"❌ LLM enrichment coordination failed: {str(e)}")
        coordination_stats["coordination_status"] = "failed"
        coordination_stats["error"] = str(e)
        raise

    finally:
        if cursor:
            cursor.close()


def aggregate_platform_statistics(cursor, context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Aggregate processing statistics across all platforms.
    """

    context.log.info("📊 Aggregating platform statistics...")

    # Get overall enrichment statistics
    cursor.execute("""
    SELECT
        j.PLATFORM,
        COUNT(j.JOB_UID) as total_jobs,
        COUNT(llm.JOB_UID) as enriched_jobs,
        ROUND(COUNT(llm.JOB_UID)::FLOAT / COUNT(j.JOB_UID) * 100, 1) as success_rate,
        ROUND(AVG(llm.LLM_OVERALL_CONFIDENCE), 3) as avg_confidence,
        COUNT(CASE WHEN llm.LLM_OVERALL_CONFIDENCE < 0.6 THEN 1 END) as low_confidence_count,
        COUNT(CASE WHEN llm.LLM_NEEDS_MANUAL_REVIEW = TRUE THEN 1 END) as needs_review_count
    FROM BETTERJOBS_DB.STAGE.JOBS_UNIFIED j
    LEFT JOIN BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED llm ON j.JOB_UID = llm.JOB_UID
    WHERE j.IS_ENGLISH = TRUE
    GROUP BY j.PLATFORM
    ORDER BY j.PLATFORM
    """)

    platform_results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    platform_summaries = {}
    total_jobs = 0
    total_enriched = 0
    confidence_scores = []

    for row in platform_results:
        platform_data = dict(zip(columns, row))
        platform = platform_data['PLATFORM']

        platform_summaries[platform] = {
            "total_jobs": platform_data['TOTAL_JOBS'],
            "enriched_jobs": platform_data['ENRICHED_JOBS'],
            "success_rate": platform_data['SUCCESS_RATE'],
            "avg_confidence": platform_data['AVG_CONFIDENCE'] or 0.0,
            "low_confidence_count": platform_data['LOW_CONFIDENCE_COUNT'],
            "needs_review_count": platform_data['NEEDS_REVIEW_COUNT']
        }

        total_jobs += platform_data['TOTAL_JOBS']
        total_enriched += platform_data['ENRICHED_JOBS']
        if platform_data['AVG_CONFIDENCE']:
            confidence_scores.append(platform_data['AVG_CONFIDENCE'])

        context.log.info(f"📈 [{platform.upper()}] {platform_data['ENRICHED_JOBS']}/{platform_data['TOTAL_JOBS']} jobs enriched ({platform_data['SUCCESS_RATE']}%)")

    overall_success_rate = (total_enriched / total_jobs * 100) if total_jobs > 0 else 0.0
    avg_confidence = sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0.0

    return {
        "total_jobs_processed": total_jobs,
        "total_jobs_enriched": total_enriched,
        "overall_success_rate": overall_success_rate,
        "avg_confidence_score": avg_confidence,
        "platform_summaries": platform_summaries
    }


def perform_quality_validation(cursor, context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Perform cross-platform quality validation checks.
    """

    context.log.info("🔍 Performing quality validation checks...")

    quality_checks = {}

    # Check 1: Duplicate job UIDs across enrichment table
    cursor.execute("""
    SELECT COUNT(*) as total_records, COUNT(DISTINCT JOB_UID) as unique_job_uids
    FROM BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED
    """)

    result = cursor.fetchone()
    if result:
        total_records, unique_job_uids = result
        quality_checks["duplicate_check"] = {
            "total_records": total_records,
            "unique_job_uids": unique_job_uids,
            "has_duplicates": total_records != unique_job_uids,
            "duplicate_count": total_records - unique_job_uids if total_records != unique_job_uids else 0
        }

    # Check 2: Missing enrichment for English jobs
    cursor.execute("""
    SELECT
        COUNT(j.JOB_UID) as total_english_jobs,
        COUNT(llm.JOB_UID) as enriched_english_jobs,
        COUNT(j.JOB_UID) - COUNT(llm.JOB_UID) as missing_enrichment_count
    FROM BETTERJOBS_DB.STAGE.JOBS_UNIFIED j
    LEFT JOIN BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED llm ON j.JOB_UID = llm.JOB_UID
    WHERE j.IS_ENGLISH = TRUE
    AND j.JOB_DESCRIPTION_CLEAN IS NOT NULL
    AND LENGTH(j.JOB_DESCRIPTION_CLEAN) >= 100
    """)

    result = cursor.fetchone()
    if result:
        total_english, enriched_english, missing_count = result
        quality_checks["coverage_check"] = {
            "total_eligible_jobs": total_english,
            "enriched_jobs": enriched_english,
            "missing_enrichment_count": missing_count,
            "coverage_percentage": (enriched_english / total_english * 100) if total_english > 0 else 0.0
        }

    # Check 3: Confidence score distribution
    cursor.execute("""
    SELECT
        COUNT(*) as total_enriched,
        COUNT(CASE WHEN LLM_OVERALL_CONFIDENCE >= 0.8 THEN 1 END) as high_confidence,
        COUNT(CASE WHEN LLM_OVERALL_CONFIDENCE BETWEEN 0.6 AND 0.8 THEN 1 END) as medium_confidence,
        COUNT(CASE WHEN LLM_OVERALL_CONFIDENCE < 0.6 THEN 1 END) as low_confidence,
        ROUND(AVG(LLM_OVERALL_CONFIDENCE), 3) as avg_confidence
    FROM BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED
    """)

    result = cursor.fetchone()
    if result:
        total, high, medium, low, avg_conf = result
        quality_checks["confidence_distribution"] = {
            "total_enriched": total,
            "high_confidence_count": high,
            "medium_confidence_count": medium,
            "low_confidence_count": low,
            "high_confidence_percentage": (high / total * 100) if total > 0 else 0.0,
            "average_confidence": avg_conf
        }

    return quality_checks


def generate_recommendations(coordination_stats: Dict[str, Any], context: AssetExecutionContext) -> List[str]:
    """
    Generate recommendations based on coordination results.
    """

    recommendations = []

    # Recommendation based on success rate
    if coordination_stats["overall_success_rate"] < 95.0:
        recommendations.append(
            f"Consider investigating platforms with low success rates. "
            f"Overall success rate is {coordination_stats['overall_success_rate']:.1f}%, target is 95%+"
        )

    # Recommendation based on confidence scores
    if coordination_stats["avg_confidence_score"] < 0.7:
        recommendations.append(
            f"Average confidence score is {coordination_stats['avg_confidence_score']:.3f}, "
            f"consider reviewing prompt templates or adding validation rules."
        )

    # Recommendation based on quality checks
    quality_checks = coordination_stats.get("data_quality_checks", {})

    if quality_checks.get("duplicate_check", {}).get("has_duplicates", False):
        duplicate_count = quality_checks["duplicate_check"]["duplicate_count"]
        recommendations.append(f"Found {duplicate_count} duplicate records in enrichment table. Investigate data consistency.")

    coverage = quality_checks.get("coverage_check", {}).get("coverage_percentage", 0)
    if coverage < 90.0:
        recommendations.append(f"Enrichment coverage is {coverage:.1f}%, target is 90%+. Check for processing failures.")

    # Platform-specific recommendations
    for platform, stats in coordination_stats.get("platform_summaries", {}).items():
        if stats["success_rate"] < 90.0:
            recommendations.append(f"Platform {platform} has low success rate ({stats['success_rate']:.1f}%). Investigate platform-specific issues.")

        if stats["avg_confidence"] < 0.6:
            recommendations.append(f"Platform {platform} has low confidence scores ({stats['avg_confidence']:.3f}). Review job description quality.")

    if not recommendations:
        recommendations.append("All quality checks passed. LLM enrichment is performing well across all platforms.")

    return recommendations