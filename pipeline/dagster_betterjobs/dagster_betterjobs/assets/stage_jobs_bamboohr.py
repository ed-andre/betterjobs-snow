"""
Stage Jobs BambooHR Asset

Individual platform asset for BambooHR jobs processing.
Part of ENHANCEMENT-001: Asset Breakdown Strategy for parallel processing.

This asset:
1. Loads BambooHR RAW data
2. Applies complete transformation pipeline
3. Loads processed data to STAGE.jobs_unified table
4. Enables parallel processing with other platform assets
"""

from datetime import datetime
from typing import Dict, Any, Optional

from dagster import (
    asset,
    AssetExecutionContext,
    MetadataValue,
    Config
)

from dagster_betterjobs.transformations.stage_processing import (
    PlatformProcessingConfig,
    create_stage_table_if_not_exists,
    process_platform_jobs,
    get_platform_statistics,
    check_cross_platform_conflicts
)

from dagster_betterjobs.transformations.watermark_management import (
    WatermarkConfig,
    get_watermark,
    load_incremental_raw_data,
    upsert_platform_data_to_snowflake,
    get_processing_stats
)


class StageBambooHRConfig(Config):
    """Configuration for BambooHR stage processing."""
    process_language_detection: bool = True
    language_confidence_threshold: float = 0.8
    include_non_english: bool = True
    max_records: Optional[int] = None

    # Incremental processing configuration
    enable_incremental: bool = True
    overlap_hours: int = 2
    force_full_refresh: bool = False
    max_incremental_days: int = 7


@asset(
    group_name="stage_cleansing_enrichment_validation_transformation",
    kinds={"snowflake", "python", "transformation"},
    required_resource_keys={"snowflake"},
    deps=["bamboohr_company_jobs_discovery"],
    description="Processed BambooHR jobs data loaded to STAGE.jobs_unified"
)
def stage_jobs_bamboohr(context: AssetExecutionContext, config: StageBambooHRConfig) -> Dict[str, Any]:
    """
    Process BambooHR jobs data through complete transformation pipeline.

    This asset:
    1. Loads BambooHR RAW data
    2. Applies text cleaning, language detection, and platform mapping
    3. Generates deterministic UIDs
    4. Validates data quality
    5. Loads to STAGE.jobs_unified table

    Returns:
        Processing statistics and metadata
    """
    platform = "bamboohr"

    # Convert Dagster config to processing configs
    processing_config = PlatformProcessingConfig(
        process_language_detection=config.process_language_detection,
        language_confidence_threshold=config.language_confidence_threshold,
        include_non_english=config.include_non_english,
        max_records=config.max_records
    )

    watermark_config = WatermarkConfig(
        enable_incremental=config.enable_incremental,
        overlap_hours=config.overlap_hours,
        force_full_refresh=config.force_full_refresh,
        max_incremental_days=config.max_incremental_days,
        min_expected_prefixes=15  # Expected distinct company name prefixes for partition validation
    )

    # Get Snowflake connection
    conn = context.resources.snowflake.get_connection()

    # Initialize statistics
    stats = {
        "platform": platform,
        "processing_start": datetime.now().isoformat(),
        "raw_jobs": 0,
        "processed_jobs": 0,
        "loaded_jobs": 0,
        "uid_validation": {},
        "platform_statistics": {}
    }

    try:
        # Ensure stage table exists
        create_stage_table_if_not_exists(conn)
        context.log.info(f"[{platform}] Stage table verified/created")

        # Get watermark for incremental processing
        watermark = get_watermark(platform, conn, context, watermark_config)
        stats["watermark_used"] = watermark.isoformat() if watermark else None
        stats["processing_mode"] = "incremental" if watermark else "full_refresh"

        # Load raw data incrementally
        context.log.info(f"[{platform}] Loading {'incremental' if watermark else 'full'} raw data...")
        raw_df = load_incremental_raw_data(platform, watermark, conn, context, processing_config.max_records)
        stats["raw_jobs"] = len(raw_df)

        if raw_df.empty:
            context.log.warning(f"[{platform}] No raw data found, skipping processing")
            stats["processing_end"] = datetime.now().isoformat()
            return stats

        # Process platform jobs
        context.log.info(f"[{platform}] Processing jobs through transformation pipeline...")
        processed_df, processing_stats = process_platform_jobs(
            platform, raw_df, processing_config, context
        )

        # Update stats with processing results
        stats.update(processing_stats)

        if processed_df.empty:
            context.log.warning(f"[{platform}] No jobs survived processing pipeline")
            stats["processing_end"] = datetime.now().isoformat()
            return stats

        # Check for cross-platform conflicts before loading
        context.log.info(f"[{platform}] Checking for cross-platform job_uid conflicts...")
        conflict_check = check_cross_platform_conflicts(processed_df, platform, conn, context)
        stats["cross_platform_conflicts"] = conflict_check

        # Load to Snowflake using MERGE (upsert)
        context.log.info(f"[{platform}] Upserting processed data to Snowflake...")
        loaded_count = upsert_platform_data_to_snowflake(processed_df, platform, conn, context)
        stats["loaded_jobs"] = loaded_count

        # Get final platform and processing statistics
        platform_stats = get_platform_statistics(platform, conn)
        processing_stats = get_processing_stats(platform, conn, context, watermark)
        stats["platform_statistics"] = platform_stats
        stats.update(processing_stats)

        stats["processing_end"] = datetime.now().isoformat()

        context.log.info(f"[{platform}] Processing complete: {loaded_count} jobs {'upserted' if watermark else 'loaded'}")

        # Add Dagster metadata
        context.add_output_metadata({
            "platform": MetadataValue.text(platform),
            "processing_mode": MetadataValue.text(stats["processing_mode"]),
            "watermark_used": MetadataValue.text(stats.get("watermark_used", "None")),
            "raw_jobs": MetadataValue.int(stats["raw_jobs"]),
            "processed_jobs": MetadataValue.int(stats["processed_jobs"]),
            "loaded_jobs": MetadataValue.int(stats["loaded_jobs"]),
            "unique_uids": MetadataValue.int(stats["uid_validation"].get("unique_uids", 0)),
            "uid_collision_free": MetadataValue.bool(stats["uid_validation"].get("is_valid", False)),
            "cross_platform_conflicts": MetadataValue.int(conflict_check.get("conflict_count", 0)),
            "cross_platform_conflicts_found": MetadataValue.bool(conflict_check.get("conflicts_found", False)),
            "company_count": MetadataValue.int(platform_stats.get("company_count", 0)),
            "avg_quality_score": MetadataValue.float(platform_stats.get("avg_quality_score", 0.0)),
            "incremental_records": MetadataValue.int(stats.get("incremental_records", 0)),
            "processing_duration": MetadataValue.text(
                f"{datetime.fromisoformat(stats['processing_end']) - datetime.fromisoformat(stats['processing_start'])}"
            )
        })

    except Exception as e:
        context.log.error(f"[{platform}] Error in processing: {str(e)}")
        stats["error"] = str(e)
        raise

    finally:
        if conn:
            conn.close()

    return stats