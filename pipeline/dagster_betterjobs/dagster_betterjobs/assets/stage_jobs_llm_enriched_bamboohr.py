"""
BambooHR Platform LLM Enrichment Asset

Platform-specific LLM enrichment for BambooHR jobs using shared processing logic.
Part of ENHANCEMENT-031: Company-based partitioned LLM enrichment for parallel processing.

This asset processes BambooHR jobs partitioned by company name (A-Z, 0-9, other)
for AI-powered information extraction, enabling maximum parallel processing.
"""

from typing import Dict, Any

from dagster import (
    asset,
    AssetExecutionContext,
    MetadataValue
)

from dagster_betterjobs.transformations.llm_processing import (
    process_platform_llm_enrichment,
    create_platform_metadata,
    create_llm_enrichment_table_if_not_exists,
    PartitionedLLMEnrichmentConfig
)
from dagster_betterjobs.partitions import llm_company_partitions


@asset(
    group_name="2a_stage_cleaning_enrichment",
    kinds={"snowflake", "python", "gemini"},
    required_resource_keys={"snowflake", "gemini"},
    deps=["stage_jobs_unified"],
    partitions_def=llm_company_partitions,
    description="Extract structured information from BambooHR job descriptions using Gemini LLM (partitioned by company)"
)
def stage_jobs_llm_enriched_bamboohr(context: AssetExecutionContext, config: PartitionedLLMEnrichmentConfig) -> Dict[str, Any]:
    """
    Enrich BambooHR job data with AI-extracted information using Gemini LLM.

    This asset processes jobs from the BambooHR platform partitioned by company name
    and extracts structured information including salary, skills, experience requirements,
    work arrangements, and job classifications using the Gemini API.

    Uses shared processing logic from llm_processing.py for DRY implementation.

    Returns:
        Dict with processing statistics and results
    """

    # Get partition key for company-based filtering
    partition_key = context.partition_key

    # Get resources
    conn = context.resources.snowflake.get_connection()
    gemini = context.resources.gemini

    # Ensure LLM enrichment table exists
    create_llm_enrichment_table_if_not_exists(context)

    context.log.info(f"🚀 [BAMBOOHR] Starting partitioned LLM enrichment for companies: {partition_key}")

    # Process using shared logic with partition filtering
    stats = process_platform_llm_enrichment(
        platform="bamboohr",
        context=context,
        config=config,
        conn=conn,
        gemini=gemini,
        partition_key=partition_key
    )

    # Add metadata for Dagster UI
    metadata = create_platform_metadata(stats, "bamboohr")
    metadata["partition_key"] = MetadataValue.text(partition_key)
    context.add_output_metadata(metadata)

    context.log.info(f"✅ [BAMBOOHR] Partitioned LLM enrichment completed successfully for {partition_key}")

    return stats