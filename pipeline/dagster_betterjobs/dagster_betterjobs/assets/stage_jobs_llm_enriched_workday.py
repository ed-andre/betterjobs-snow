"""
Workday Platform LLM Enrichment Asset

Platform-specific LLM enrichment for Workday jobs using shared processing logic.
Part of ENHANCEMENT-010: LLM Enrichment Asset Breakdown for parallel processing.

This asset processes only Workday jobs for AI-powered information extraction,
enabling parallel processing across all ATS platforms.
"""

from typing import Dict, Any

from dagster import (
    asset,
    AssetExecutionContext,
    Config
)

from dagster_betterjobs.transformations.llm_processing import (
    process_platform_llm_enrichment,
    create_platform_metadata
)


class LLMEnrichmentConfig(Config):
    """Configuration for LLM enrichment processing."""
    batch_size: int = 15  # Standard batch size for Workday jobs
    delay_between_batches: float = 1.0  # Rate limiting between batches
    max_retries: int = 3  # Maximum retry attempts for failed API calls
    max_description_length: int = 8000  # Token limit for job descriptions
    confidence_threshold: float = 0.6  # Threshold for low-confidence flagging
    limit_jobs: int = None  # Limit for testing (None = process all)
    enable_validation_pass: bool = True  # Enable second-pass validation for low confidence
    processing_mode: str = "new_only"  # "new_only", "all", "failed_only"


@asset(
    group_name="stage_cleansing_enrichment_validation_transformation",
    kinds={"snowflake", "python", "gemini"},
    required_resource_keys={"snowflake", "gemini"},
    deps=["stage_jobs_unified"],
    description="Extract structured information from Workday job descriptions using Gemini LLM"
)
def stage_jobs_llm_enriched_workday(context: AssetExecutionContext, config: LLMEnrichmentConfig) -> Dict[str, Any]:
    """
    Enrich Workday job data with AI-extracted information using Gemini LLM.

    This asset processes jobs from the Workday platform and extracts
    structured information including salary, skills, experience requirements,
    work arrangements, and job classifications using the Gemini API.

    Uses shared processing logic from llm_processing.py for DRY implementation.

    Returns:
        Dict with processing statistics and results
    """

    # Get resources
    conn = context.resources.snowflake.get_connection()
    gemini = context.resources.gemini

    context.log.info("🚀 [WORKDAY] Starting platform-specific LLM enrichment...")

    # Process using shared logic
    stats = process_platform_llm_enrichment(
        platform="workday",
        context=context,
        config=config,
        conn=conn,
        gemini=gemini
    )

    # Add metadata for Dagster UI
    metadata = create_platform_metadata(stats, "workday")
    context.add_output_metadata(metadata)

    context.log.info(f"✅ [WORKDAY] LLM enrichment completed successfully")

    return stats