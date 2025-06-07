"""
Stage Jobs Unified Combiner Asset

ENHANCEMENT-001: Lightweight combiner for individual platform assets.
This asset consolidates results from individual platform processing assets
and performs cross-platform validation and deduplication.

This asset:
- Validates cross-platform UID uniqueness
- Performs final quality checks and statistics
- Provides unified monitoring and metadata
- Handles cross-platform deduplication if needed

The actual processing is now done in parallel by individual platform assets.
"""

from datetime import datetime, date
from typing import Dict, Any, Optional

from dagster import (
    asset,
    AssetExecutionContext,
    MetadataValue,
    Config
)

from dagster_betterjobs.transformations.uid_generation import validate_uid_uniqueness, get_uid_collision_report


class StageJobsUnifiedConfig(Config):
    """Configuration for stage jobs unified combiner."""
    platforms_to_validate: list = ["bamboohr", "greenhouse", "workday", "smartrecruiters"]
    enable_cross_platform_deduplication: bool = True


@asset(
    group_name="stage_cleansing_enrichment_validation_transformation",
    kinds={"snowflake", "python", "validation"},
    required_resource_keys={"snowflake"},
    deps=[
        "stage_jobs_bamboohr",
        "stage_jobs_greenhouse",
        "stage_jobs_workday",
        "stage_jobs_smartrecruiters"
    ],
    description="Cross-platform validation and monitoring for unified stage jobs data"
)
def stage_jobs_unified(context: AssetExecutionContext, config: StageJobsUnifiedConfig) -> Dict[str, Any]:
    """
    Lightweight combiner for individual platform assets.

    This asset:
    1. Validates that all individual platform assets completed successfully
    2. Performs cross-platform UID validation and deduplication checks
    3. Generates unified statistics and monitoring metadata
    4. Provides final quality validation

    The actual data processing is done in parallel by individual platform assets.

    Returns:
        Dict with cross-platform validation results and unified statistics
    """

    # Get Snowflake connection
    conn = context.resources.snowflake.get_connection()
    database_name = "BETTERJOBS_DB"
    stage_schema = "STAGE"

    # Statistics tracking
    stats = {
        "platforms_validated": [],
        "cross_platform_validation": {},
        "unified_statistics": {},
        "processing_start": datetime.now().isoformat()
    }

    cursor = None

    try:
        cursor = conn.cursor()

        context.log.info("🔄 Starting cross-platform validation and monitoring...")

        # Validate each platform completed successfully by checking data exists
        platform_stats = {}
        total_jobs = 0
        total_companies = 0

        for platform in config.platforms_to_validate:
            context.log.info(f"📊 Validating platform: {platform}")

            # Check platform data exists for today
            cursor.execute(f"""
            SELECT
                COUNT(*) as job_count,
                COUNT(DISTINCT company_id) as company_count,
                COUNT(DISTINCT job_uid) as unique_uids,
                AVG(data_quality_score) as avg_quality_score,
                MIN(transformation_timestamp) as earliest_transform,
                MAX(transformation_timestamp) as latest_transform
            FROM {database_name}.{stage_schema}.jobs_unified
            WHERE platform = %s AND partition_date = %s
            """, (platform, date.today()))

            result = cursor.fetchone()
            if result and result[0] > 0:
                platform_stat = {
                    "job_count": result[0],
                    "company_count": result[1],
                    "unique_uids": result[2],
                    "avg_quality_score": float(result[3]) if result[3] else 0.0,
                    "earliest_transform": result[4],
                    "latest_transform": result[5],
                    "status": "success"
                }

                total_jobs += result[0]
                total_companies += result[1]

                context.log.info(f"✅ {platform}: {result[0]} jobs, {result[1]} companies, avg quality: {result[3]:.3f}")
                stats["platforms_validated"].append(platform)
            else:
                platform_stat = {
                    "job_count": 0,
                    "company_count": 0,
                    "unique_uids": 0,
                    "avg_quality_score": 0.0,
                    "status": "no_data"
                }
                context.log.warning(f"⚠️  {platform}: No data found for today's partition")

            platform_stats[platform] = platform_stat

        stats["unified_statistics"] = {
            "total_jobs": total_jobs,
            "total_companies": total_companies,
            "platforms_with_data": len(stats["platforms_validated"]),
            "platform_details": platform_stats
        }

        # Cross-platform UID validation
        context.log.info("🔍 Performing cross-platform UID validation...")

        cursor.execute(f"""
        SELECT job_uid
        FROM {database_name}.{stage_schema}.jobs_unified
        WHERE partition_date = %s
        """, (date.today(),))

        all_uids = [row[0] for row in cursor.fetchall()]

        if all_uids:
            # Convert to pandas for UID validation (reusing existing logic)
            import pandas as pd
            uid_df = pd.DataFrame({'job_uid': all_uids})

            cross_platform_validation = validate_uid_uniqueness(uid_df, uid_column='job_uid')
            stats["cross_platform_validation"] = cross_platform_validation

            if cross_platform_validation['is_valid']:
                context.log.info(f"✅ Cross-platform UID validation passed: {cross_platform_validation['unique_uids']} unique UIDs")
            else:
                context.log.error(f"❌ Cross-platform UID collisions detected: {cross_platform_validation['duplicate_count']} duplicates")

                # Log collision details for debugging
                if cross_platform_validation['duplicate_uids']:
                    sample_collisions = cross_platform_validation['duplicate_uids'][:5]
                    context.log.error(f"Sample collision UIDs: {sample_collisions}")

                    # Get details about colliding records
                    for uid in sample_collisions:
                        cursor.execute(f"""
                        SELECT platform, job_id, company_id, job_title_clean
                        FROM {database_name}.{stage_schema}.jobs_unified
                        WHERE job_uid = %s AND partition_date = %s
                        """, (uid, date.today()))

                        collision_records = cursor.fetchall()
                        context.log.error(f"UID {uid} collision details:")
                        for record in collision_records:
                            context.log.error(f"  Platform: {record[0]}, Job ID: {record[1]}, Company: {record[2]}, Title: {record[3]}")
        else:
            context.log.warning("No UIDs found for validation")
            stats["cross_platform_validation"] = {"is_valid": False, "unique_uids": 0, "duplicate_count": 0}

        # Cross-platform duplicate analysis and deduplication
        if config.enable_cross_platform_deduplication:
            context.log.info("🔍 Analyzing and removing cross-platform duplicates...")

            # First, identify jobs with same job_id across different platforms
            cursor.execute(f"""
            WITH cross_platform_duplicates AS (
                SELECT
                    job_id,
                    COUNT(DISTINCT platform) as platform_count,
                    MIN(transformation_timestamp) as earliest_transform,
                    MIN(platform) as earliest_platform
                FROM {database_name}.{stage_schema}.jobs_unified
                WHERE partition_date = %s
                GROUP BY job_id
                HAVING COUNT(DISTINCT platform) > 1
            ),
            duplicates_to_remove AS (
                SELECT j.job_uid, j.platform, j.job_id
                FROM {database_name}.{stage_schema}.jobs_unified j
                INNER JOIN cross_platform_duplicates cpd ON j.job_id = cpd.job_id
                WHERE j.partition_date = %s
                AND (j.transformation_timestamp > cpd.earliest_transform
                     OR (j.transformation_timestamp = cpd.earliest_transform AND j.platform > cpd.earliest_platform))
            )
            SELECT COUNT(*) as duplicates_to_remove_count
            FROM duplicates_to_remove
            """, (date.today(), date.today()))

            result = cursor.fetchone()
            duplicates_to_remove = result[0] if result else 0

            if duplicates_to_remove > 0:
                context.log.warning(f"🔄 Removing {duplicates_to_remove} cross-platform duplicate jobs...")

                # Get details of what we're removing before deletion
                cursor.execute(f"""
                WITH cross_platform_duplicates AS (
                    SELECT
                        job_id,
                        COUNT(DISTINCT platform) as platform_count,
                        MIN(transformation_timestamp) as earliest_transform,
                        MIN(platform) as earliest_platform
                    FROM {database_name}.{stage_schema}.jobs_unified
                    WHERE partition_date = %s
                    GROUP BY job_id
                    HAVING COUNT(DISTINCT platform) > 1
                ),
                duplicates_to_remove AS (
                    SELECT j.job_uid, j.platform, j.job_id, j.job_title_clean
                    FROM {database_name}.{stage_schema}.jobs_unified j
                    INNER JOIN cross_platform_duplicates cpd ON j.job_id = cpd.job_id
                    WHERE j.partition_date = %s
                    AND (j.transformation_timestamp > cpd.earliest_transform
                         OR (j.transformation_timestamp = cpd.earliest_transform AND j.platform > cpd.earliest_platform))
                )
                SELECT platform, job_id, job_title_clean
                FROM duplicates_to_remove
                LIMIT 10
                """, (date.today(), date.today()))

                sample_removals = cursor.fetchall()
                context.log.warning("Sample jobs being removed (keeping earliest by timestamp, then platform name):")
                for platform, job_id, job_title in sample_removals:
                    context.log.warning(f"  Removing: {platform} - {job_id} - {job_title}")

                # Actually remove the cross-platform duplicates
                cursor.execute(f"""
                WITH cross_platform_duplicates AS (
                    SELECT
                        job_id,
                        COUNT(DISTINCT platform) as platform_count,
                        MIN(transformation_timestamp) as earliest_transform,
                        MIN(platform) as earliest_platform
                    FROM {database_name}.{stage_schema}.jobs_unified
                    WHERE partition_date = %s
                    GROUP BY job_id
                    HAVING COUNT(DISTINCT platform) > 1
                ),
                duplicates_to_remove AS (
                    SELECT j.job_uid
                    FROM {database_name}.{stage_schema}.jobs_unified j
                    INNER JOIN cross_platform_duplicates cpd ON j.job_id = cpd.job_id
                    WHERE j.partition_date = %s
                    AND (j.transformation_timestamp > cpd.earliest_transform
                         OR (j.transformation_timestamp = cpd.earliest_transform AND j.platform > cpd.earliest_platform))
                )
                DELETE FROM {database_name}.{stage_schema}.jobs_unified
                WHERE job_uid IN (SELECT job_uid FROM duplicates_to_remove)
                """, (date.today(), date.today()))

                removed_count = cursor.rowcount
                conn.commit()

                context.log.info(f"✅ Successfully removed {removed_count} cross-platform duplicate jobs")
                stats["cross_platform_duplicates_removed"] = removed_count

                # Update total jobs count after deduplication
                cursor.execute(f"""
                SELECT COUNT(*)
                FROM {database_name}.{stage_schema}.jobs_unified
                WHERE partition_date = %s
                """, (date.today(),))

                final_job_count = cursor.fetchone()[0]
                stats["unified_statistics"]["total_jobs_after_deduplication"] = final_job_count
                context.log.info(f"📊 Final job count after cross-platform deduplication: {final_job_count}")

            else:
                context.log.info("✅ No cross-platform job_id duplicates detected")
                stats["cross_platform_duplicates_removed"] = 0

        # Final summary
        stats["processing_end"] = datetime.now().isoformat()

        successful_platforms = len(stats["platforms_validated"])
        total_platforms = len(config.platforms_to_validate)

        if successful_platforms == total_platforms:
            context.log.info(f"🎉 All {total_platforms} platforms processed successfully with {total_jobs} total jobs")
        else:
            context.log.warning(f"⚠️  Only {successful_platforms}/{total_platforms} platforms processed successfully")

                # Add Dagster metadata
        final_job_count = stats["unified_statistics"].get("total_jobs_after_deduplication", total_jobs)

        context.add_output_metadata({
            "total_jobs_initial": MetadataValue.int(total_jobs),
            "total_jobs_final": MetadataValue.int(final_job_count),
            "total_companies": MetadataValue.int(total_companies),
            "platforms_processed": MetadataValue.int(successful_platforms),
            "cross_platform_uid_valid": MetadataValue.bool(stats["cross_platform_validation"].get("is_valid", False)),
            "unique_uids": MetadataValue.int(stats["cross_platform_validation"].get("unique_uids", 0)),
            "cross_platform_duplicates_removed": MetadataValue.int(stats.get("cross_platform_duplicates_removed", 0)),
            "deduplication_enabled": MetadataValue.bool(config.enable_cross_platform_deduplication),
            "avg_processing_quality": MetadataValue.float(
                sum(p["avg_quality_score"] for p in platform_stats.values() if p["status"] == "success") / successful_platforms
                if successful_platforms > 0 else 0.0
            ),
            "platforms_with_data": MetadataValue.text(", ".join(stats["platforms_validated"])),
            "validation_timestamp": MetadataValue.text(datetime.now().isoformat())
        })

    except Exception as e:
        context.log.error(f"Error in cross-platform validation: {str(e)}")
        stats["error"] = str(e)
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return stats