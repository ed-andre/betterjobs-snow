"""
Shared LLM Processing Module

This module contains all the common LLM enrichment processing logic extracted from
the monolithic stage_jobs_llm_enriched asset to enable DRY implementation of
platform-specific parallel processing.

Provides reusable functions for:
- Platform-specific job querying and filtering
- Gemini API batch processing with retry logic
- LLM data extraction and validation
- Database insertion and error handling
- Statistics tracking and monitoring

Used by individual platform LLM assets for parallel processing.
"""

import json
import time
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Optional

from dagster import AssetExecutionContext, MetadataValue
from dagster_betterjobs.transformations.llm_prompts import (
    JobExtractionPrompts,
    PromptFormatter
)


def process_platform_llm_enrichment(
    platform: str,
    context: AssetExecutionContext,
    config,
    conn,
    gemini
) -> Dict[str, Any]:
    """
    Shared LLM enrichment processing logic for individual platforms.

    This function contains all the existing LLM processing logic but filters
    jobs by platform for parallel processing. Implements complete DRY approach.

    Args:
        platform: Platform name (bamboohr, greenhouse, workday, smartrecruiters)
        context: Dagster execution context
        config: LLM enrichment configuration
        conn: Snowflake connection
        gemini: Gemini resource

    Returns:
        Dict with processing statistics and results
    """

    # Initialize enhanced statistics tracking with platform prefix
    stats = {
        "processing_start": datetime.now().isoformat(),
        "platform": platform,
        "jobs_processed": 0,
        "jobs_successful": 0,
        "jobs_failed": 0,
        "llm_extraction_failures": 0,      # NEW: Track LLM API failures
        "database_insertion_failures": 0,   # NEW: Track database insertion failures
        "failed_job_records": [],          # NEW: Track all failed records
        "insertion_error_summary": {},     # NEW: Track insertion error types
        "batches_processed": 0,            # NEW: Track batch processing
        "batches_with_failures": 0,       # NEW: Track batches that had insertion failures
        "api_calls_made": 0,
        "total_tokens_estimated": 0,
        "avg_confidence_score": 0.0,
        "low_confidence_count": 0,
        "validation_passes_performed": 0,
        "error_summary": {}
    }

    database_name = "BETTERJOBS_DB"
    stage_schema = "STAGE"

    cursor = None

    try:
        cursor = conn.cursor()

        context.log.info(f"🚀 [{platform.upper()}] Starting LLM enrichment processing...")

        # Get platform-specific jobs to process
        jobs_df = get_platform_jobs_for_processing(
            cursor, platform, config, database_name, stage_schema, context
        )

        if len(jobs_df) == 0:
            context.log.info(f"✅ [{platform.upper()}] No jobs to process")
            return stats

        context.log.info(f"📊 [{platform.upper()}] Found {len(jobs_df)} jobs to process with LLM")

        # Initialize prompt templates
        prompts = JobExtractionPrompts()
        formatter = PromptFormatter()

        # Process jobs in batches with platform-specific configuration
        batch_size = config.batch_size
        total_batches = (len(jobs_df) + batch_size - 1) // batch_size

        context.log.info(f"🔄 [{platform.upper()}] Processing {len(jobs_df)} jobs in {total_batches} batches (batch size: {batch_size})")

        # Process batches with resilient per-batch database storage
        total_records_saved = 0
        for batch_idx in range(0, len(jobs_df), batch_size):
            batch_jobs = jobs_df.iloc[batch_idx:batch_idx + batch_size]
            current_batch = (batch_idx // batch_size) + 1

            context.log.info(f"🔄 [{platform.upper()}] Processing batch {current_batch}/{total_batches} ({len(batch_jobs)} jobs)")

            # Process batch
            batch_results = process_llm_batch(
                batch_jobs, prompts, formatter, gemini, config, context, stats, platform
            )

            # Store batch results immediately after processing with resilient insertion
            if batch_results:
                insertion_stats = insert_llm_batch_results_resilient(
                    context, cursor, batch_results, database_name, stage_schema, platform
                )

                # Aggregate insertion statistics
                stats["database_insertion_failures"] += insertion_stats["failed_insertions"]
                stats["failed_job_records"].extend(insertion_stats["failed_records"])

                # Merge error summaries
                for error_type, count in insertion_stats["error_summary"].items():
                    stats["insertion_error_summary"][error_type] = stats["insertion_error_summary"].get(error_type, 0) + count

                if insertion_stats["failed_insertions"] > 0:
                    stats["batches_with_failures"] += 1

                total_records_saved += insertion_stats["successful_insertions"]
                context.log.info(f"💾 [{platform.upper()}] Saved batch {current_batch} results: {insertion_stats['successful_insertions']}/{insertion_stats['total_records']} records")

            stats["batches_processed"] += 1

            # Rate limiting between batches
            if current_batch < total_batches:
                time.sleep(config.delay_between_batches)

        context.log.info(f"✅ [{platform.upper()}] Total LLM enrichment records saved: {total_records_saved}")

        # Calculate final statistics
        finalize_platform_statistics(cursor, stats, database_name, stage_schema, platform, context)

        # Calculate enhanced success rates
        total_llm_failures = stats["llm_extraction_failures"] + stats["database_insertion_failures"]
        overall_success_rate = ((stats["jobs_processed"] - total_llm_failures) / stats["jobs_processed"]) * 100 if stats["jobs_processed"] > 0 else 0
        extraction_success_rate = (stats["jobs_successful"] / stats["jobs_processed"]) * 100 if stats["jobs_processed"] > 0 else 0

        context.log.info(f"""
        🎯 [{platform.upper()}] Resilient LLM Processing Complete:
        • Jobs Processed: {stats['jobs_processed']}
        • LLM Extraction Success: {stats['jobs_successful']} ({extraction_success_rate:.1f}%)
        • Database Insertion Failures: {stats['database_insertion_failures']}
        • Overall Success Rate: {overall_success_rate:.1f}%
        • Batches with Failures: {stats['batches_with_failures']}/{stats['batches_processed']}
        • Average Confidence: {stats['avg_confidence_score']:.3f}
        • Low Confidence Jobs: {stats['low_confidence_count']}
        • API Calls Made: {stats['api_calls_made']}
        • Estimated Tokens: {stats['total_tokens_estimated']:,}
        """)

        return stats

    except Exception as e:
        context.log.error(f"❌ [{platform.upper()}] LLM enrichment failed: {str(e)}")
        raise

    finally:
        if cursor:
            cursor.close()


def get_platform_jobs_for_processing(
    cursor,
    platform: str,
    config,
    database_name: str,
    stage_schema: str,
    context: AssetExecutionContext
) -> pd.DataFrame:
    """
    Get platform-specific jobs for LLM processing based on configuration.
    """

    # Determine which jobs to process based on processing mode
    context.log.info(f"📋 [{platform.upper()}] Processing mode: {config.processing_mode}")

    if config.processing_mode == "new_only":
        # Process jobs that don't have LLM enrichment yet
        jobs_query = f"""
        SELECT
            j.JOB_UID,
            j.JOB_TITLE_CLEAN,
            j.JOB_DESCRIPTION_CLEAN,
            j.COMPANY_NAME_CLEAN,
            j.PLATFORM,
            j.IS_ENGLISH
        FROM {database_name}.{stage_schema}.JOBS_UNIFIED j
        LEFT JOIN {database_name}.{stage_schema}.JOBS_LLM_ENRICHED llm
            ON j.JOB_UID = llm.JOB_UID
        WHERE j.PLATFORM = '{platform}'
            AND j.IS_ENGLISH = TRUE
            AND j.JOB_DESCRIPTION_CLEAN IS NOT NULL
            AND LENGTH(j.JOB_DESCRIPTION_CLEAN) >= 100
            AND llm.JOB_UID IS NULL
        """
    elif config.processing_mode == "failed_only":
        # Reprocess jobs that failed previously
        jobs_query = f"""
        SELECT
            j.JOB_UID,
            j.JOB_TITLE_CLEAN,
            j.JOB_DESCRIPTION_CLEAN,
            j.COMPANY_NAME_CLEAN,
            j.PLATFORM,
            j.IS_ENGLISH
        FROM {database_name}.{stage_schema}.JOBS_UNIFIED j
        INNER JOIN {database_name}.{stage_schema}.JOBS_LLM_ENRICHED llm
            ON j.JOB_UID = llm.JOB_UID
        WHERE j.PLATFORM = '{platform}'
            AND j.IS_ENGLISH = TRUE
            AND j.JOB_DESCRIPTION_CLEAN IS NOT NULL
            AND LENGTH(j.JOB_DESCRIPTION_CLEAN) >= 100
            AND llm.LLM_OVERALL_CONFIDENCE < 0.3  -- Very low confidence indicates failure
        """
    else:  # "all"
        # Process all English jobs for this platform
        jobs_query = f"""
        SELECT
            j.JOB_UID,
            j.JOB_TITLE_CLEAN,
            j.JOB_DESCRIPTION_CLEAN,
            j.COMPANY_NAME_CLEAN,
            j.PLATFORM,
            j.IS_ENGLISH
        FROM {database_name}.{stage_schema}.JOBS_UNIFIED j
        WHERE j.PLATFORM = '{platform}'
            AND j.IS_ENGLISH = TRUE
            AND j.JOB_DESCRIPTION_CLEAN IS NOT NULL
            AND LENGTH(j.JOB_DESCRIPTION_CLEAN) >= 100
        """

    # Add limit if specified
    if config.limit_jobs:
        jobs_query += f" LIMIT {config.limit_jobs}"

    context.log.info(f"🔍 [{platform.upper()}] Loading jobs for LLM processing...")
    cursor.execute(jobs_query)

    # Convert to DataFrame for easier processing
    columns = [desc[0] for desc in cursor.description]
    jobs_data = cursor.fetchall()
    jobs_df = pd.DataFrame(jobs_data, columns=columns)

    return jobs_df


def process_llm_batch(
    batch_jobs: pd.DataFrame,
    prompts: JobExtractionPrompts,
    formatter: PromptFormatter,
    gemini,
    config,
    context: AssetExecutionContext,
    stats: Dict[str, Any],
    platform: str
) -> List[Dict[str, Any]]:
    """
    Process a batch of jobs through LLM extraction.
    """

    batch_results = []

    for idx, job in batch_jobs.iterrows():
        job_start_time = time.time()

        try:
            # Format job description for LLM processing
            formatted_description = formatter.format_job_description(
                job['JOB_DESCRIPTION_CLEAN'],
                max_length=config.max_description_length
            )

            # Get comprehensive extraction prompt
            extraction_prompt = prompts.get_comprehensive_extraction_prompt()
            full_prompt = extraction_prompt.format(job_description=formatted_description)

            # Estimate tokens (rough approximation: 4 characters per token)
            estimated_tokens = len(full_prompt) // 4
            stats["total_tokens_estimated"] += estimated_tokens

            # Extract with retry logic
            extracted_data = extract_job_data_with_retry(
                context=context,
                gemini=gemini,
                prompt=full_prompt,
                job_uid=job['JOB_UID'],
                max_retries=config.max_retries,
                platform=platform
            )

            stats["api_calls_made"] += 1

            if extracted_data:
                # Calculate processing time
                processing_time = time.time() - job_start_time

                # Calculate overall confidence
                overall_confidence = formatter.calculate_average_confidence(extracted_data)

                # Identify low confidence fields
                low_confidence_fields = formatter.get_low_confidence_fields(
                    extracted_data,
                    threshold=config.confidence_threshold
                )

                # Prepare database record
                llm_record = prepare_llm_record(
                    job_uid=job['JOB_UID'],
                    extracted_data=extracted_data,
                    processing_time=processing_time,
                    estimated_tokens=estimated_tokens,
                    overall_confidence=overall_confidence,
                    low_confidence_fields=low_confidence_fields
                )

                batch_results.append(llm_record)
                stats["jobs_successful"] += 1

                if overall_confidence < config.confidence_threshold:
                    stats["low_confidence_count"] += 1

                context.log.debug(f"✅ [{platform.upper()}] Processed job {job['JOB_UID']}: confidence={overall_confidence:.3f}")

            else:
                stats["jobs_failed"] += 1
                stats["llm_extraction_failures"] += 1  # Track LLM extraction failure
                context.log.warning(f"❌ [{platform.upper()}] Failed to extract data for job {job['JOB_UID']}")

        except Exception as e:
            stats["jobs_failed"] += 1
            stats["llm_extraction_failures"] += 1  # Track LLM extraction failure
            error_type = type(e).__name__
            stats["error_summary"][error_type] = stats["error_summary"].get(error_type, 0) + 1
            context.log.error(f"❌ [{platform.upper()}] Error processing job {job['JOB_UID']}: {str(e)}")

        stats["jobs_processed"] += 1

    return batch_results


def extract_job_data_with_retry(
    context: AssetExecutionContext,
    gemini,
    prompt: str,
    job_uid: str,
    max_retries: int = 3,
    platform: str = ""
) -> Optional[Dict[str, Any]]:
    """
    Extract structured data from job description using Gemini with retry logic.

    Adapted from the proven retry patterns in retry_failed_company_urls.py
    """

    for attempt in range(max_retries):
        try:
            with gemini.get_model(context) as model:
                start_time = time.time()
                response = model.generate_content(prompt)
                elapsed_time = time.time() - start_time

                context.log.debug(f"[{platform.upper()}] Gemini response for {job_uid} in {elapsed_time:.2f}s (attempt {attempt + 1})")

                # Parse JSON response using the prompt formatter
                formatter = PromptFormatter()
                extracted_data = formatter.validate_extraction_response(response.text)

                # Validate that we got the expected structure
                if validate_extraction_structure(extracted_data):
                    return extracted_data
                else:
                    context.log.warning(f"[{platform.upper()}] Invalid extraction structure for {job_uid} (attempt {attempt + 1})")
                    if attempt == max_retries - 1:
                        return None

        except json.JSONDecodeError as e:
            context.log.warning(f"[{platform.upper()}] JSON decode error for {job_uid} (attempt {attempt + 1}): {str(e)}")
            if attempt == max_retries - 1:
                return None

        except Exception as e:
            context.log.error(f"[{platform.upper()}] Gemini API error for {job_uid} (attempt {attempt + 1}): {str(e)}")
            if attempt == max_retries - 1:
                return None

        # Exponential backoff for retries
        if attempt < max_retries - 1:
            time.sleep(2 ** attempt)

    return None


def validate_extraction_structure(data: Dict[str, Any]) -> bool:
    """
    Validate that the extracted data has the expected structure.
    """
    required_sections = ['salary_info', 'experience_requirements', 'skills', 'work_arrangement', 'job_classification']

    for section in required_sections:
        if section not in data:
            return False
        if not isinstance(data[section], dict):
            return False
        if 'confidence' not in data[section]:
            return False

    return True


def prepare_llm_record(
    job_uid: str,
    extracted_data: Dict[str, Any],
    processing_time: float,
    estimated_tokens: int,
    overall_confidence: float,
    low_confidence_fields: List[str]
) -> Dict[str, Any]:
    """
    Prepare a database record from extracted LLM data.
    """

    # Extract salary information
    salary_info = extracted_data.get('salary_info', {})

    # Extract experience requirements
    experience_req = extracted_data.get('experience_requirements', {})

    # Extract skills
    skills = extracted_data.get('skills', {})
    technical_skills = skills.get('technical_skills', {})
    soft_skills = skills.get('soft_skills', [])
    skills_confidence = skills.get('confidence', 0.0)

    # Extract work arrangement
    work_arrangement = extracted_data.get('work_arrangement', {})

    # Extract job classification
    job_classification = extracted_data.get('job_classification', {})

    return {
        'job_uid': job_uid,

        # Salary information
        'salary_min': salary_info.get('salary_min'),
        'salary_max': salary_info.get('salary_max'),
        'salary_currency': salary_info.get('salary_currency', 'USD'),
        'salary_period': salary_info.get('salary_period'),
        'salary_type': salary_info.get('salary_type'),
        'equity_mentioned': salary_info.get('equity_mentioned', False),
        'bonus_mentioned': salary_info.get('bonus_mentioned', False),
        'salary_confidence': salary_info.get('confidence', 0.0),

        # Experience requirements
        'min_years_experience': experience_req.get('min_years_experience'),
        'max_years_experience': experience_req.get('max_years_experience'),
        'experience_level': experience_req.get('experience_level'),
        'specific_technologies_years': json.dumps(experience_req.get('specific_technologies_years', {})),
        'education_requirements': json.dumps(experience_req.get('education_requirements', [])),
        'certifications': json.dumps(experience_req.get('certifications', [])),
        'experience_confidence': experience_req.get('confidence', 0.0),

        # Skills
        'technical_skills': json.dumps(technical_skills),
        'soft_skills': json.dumps(soft_skills),
        'skills_confidence': skills_confidence,

        # Work arrangement
        'work_type': work_arrangement.get('work_type'),
        'remote_flexibility': work_arrangement.get('remote_flexibility'),
        'travel_requirements': work_arrangement.get('travel_requirements'),
        'office_locations': json.dumps(work_arrangement.get('office_locations', [])),
        'timezone_requirements': work_arrangement.get('timezone_requirements'),
        'work_arrangement_confidence': work_arrangement.get('confidence', 0.0),

        # Job classification
        'job_family': job_classification.get('job_family'),
        'job_sub_family': job_classification.get('job_sub_family'),
        'seniority_level': job_classification.get('seniority_level'),
        'primary_keywords': json.dumps(job_classification.get('primary_keywords', [])),
        'industry_keywords': json.dumps(job_classification.get('industry_keywords', [])),
        'role_type': job_classification.get('role_type'),
        'team_size': job_classification.get('team_size'),
        'classification_confidence': job_classification.get('confidence', 0.0),

        # LLM processing metadata
        'llm_overall_confidence': overall_confidence,
        'llm_needs_manual_review': overall_confidence < 0.6,  # Flag low confidence for review
        'extraction_confidence_avg': overall_confidence,
        'keyword_quality_score': None,  # Future implementation
        'validation_status': 'pending'
    }


def insert_llm_batch_results_resilient(
    context: AssetExecutionContext,
    cursor,
    batch_results: List[Dict[str, Any]],
    database_name: str,
    stage_schema: str,
    platform: str
) -> Dict[str, Any]:
    """
    Insert LLM batch results with individual record error handling.

    This function prevents single problematic records from failing entire batches.
    Returns comprehensive processing statistics including failed records.

    Args:
        context: Dagster execution context
        cursor: Snowflake cursor
        batch_results: List of LLM extraction results to insert
        database_name: Snowflake database name
        stage_schema: Snowflake schema name
        platform: Platform name for logging

    Returns:
        Dict with insertion statistics and failed record details
    """

    insertion_stats = {
        "total_records": len(batch_results),
        "successful_insertions": 0,
        "failed_insertions": 0,
        "failed_records": [],
        "error_summary": {}
    }

    if not batch_results:
        context.log.info(f"[{platform.upper()}] No batch results to insert")
        return insertion_stats

    # Build single row insert query
    insert_query = f"""
    INSERT INTO {database_name}.{stage_schema}.JOBS_LLM_ENRICHED (
        JOB_UID, SALARY_MIN, SALARY_MAX, SALARY_CURRENCY, SALARY_PERIOD, SALARY_TYPE,
        EQUITY_MENTIONED, BONUS_MENTIONED, SALARY_CONFIDENCE,
        MIN_YEARS_EXPERIENCE, MAX_YEARS_EXPERIENCE, EXPERIENCE_LEVEL,
        SPECIFIC_TECHNOLOGIES_YEARS, EDUCATION_REQUIREMENTS, CERTIFICATIONS, EXPERIENCE_CONFIDENCE,
        TECHNICAL_SKILLS, SOFT_SKILLS, SKILLS_CONFIDENCE,
        WORK_TYPE, REMOTE_FLEXIBILITY, TRAVEL_REQUIREMENTS, OFFICE_LOCATIONS, TIMEZONE_REQUIREMENTS, WORK_ARRANGEMENT_CONFIDENCE,
        JOB_FAMILY, JOB_SUB_FAMILY, SENIORITY_LEVEL, PRIMARY_KEYWORDS, INDUSTRY_KEYWORDS,
        ROLE_TYPE, TEAM_SIZE, CLASSIFICATION_CONFIDENCE,
        LLM_OVERALL_CONFIDENCE, LLM_NEEDS_MANUAL_REVIEW, EXTRACTION_CONFIDENCE_AVG,
        KEYWORD_QUALITY_SCORE, VALIDATION_STATUS
    ) SELECT
        %(job_uid)s, %(salary_min)s, %(salary_max)s, %(salary_currency)s, %(salary_period)s, %(salary_type)s,
        %(equity_mentioned)s, %(bonus_mentioned)s, %(salary_confidence)s,
        %(min_years_experience)s, %(max_years_experience)s, %(experience_level)s,
        PARSE_JSON(%(specific_technologies_years)s), PARSE_JSON(%(education_requirements)s), PARSE_JSON(%(certifications)s), %(experience_confidence)s,
        PARSE_JSON(%(technical_skills)s), PARSE_JSON(%(soft_skills)s), %(skills_confidence)s,
        %(work_type)s, %(remote_flexibility)s, %(travel_requirements)s, PARSE_JSON(%(office_locations)s), %(timezone_requirements)s, %(work_arrangement_confidence)s,
        %(job_family)s, %(job_sub_family)s, %(seniority_level)s, PARSE_JSON(%(primary_keywords)s), PARSE_JSON(%(industry_keywords)s),
        %(role_type)s, %(team_size)s, %(classification_confidence)s,
        %(llm_overall_confidence)s, %(llm_needs_manual_review)s, %(extraction_confidence_avg)s,
        %(keyword_quality_score)s, %(validation_status)s
    """

    # Process each record individually with error isolation
    for record in batch_results:
        try:
            # Attempt individual record insertion
            cursor.execute(insert_query, record)
            insertion_stats["successful_insertions"] += 1

        except Exception as e:
            # Log individual record failure and continue
            error_type = type(e).__name__
            error_msg = str(e)

            failed_record_info = {
                "job_uid": record.get("job_uid", "unknown"),
                "error_type": error_type,
                "error_message": error_msg,
                "record_data": record  # For debugging
            }

            insertion_stats["failed_records"].append(failed_record_info)
            insertion_stats["failed_insertions"] += 1
            insertion_stats["error_summary"][error_type] = insertion_stats["error_summary"].get(error_type, 0) + 1

            context.log.warning(f"[{platform.upper()}] Failed to insert record {record.get('job_uid', 'unknown')}: {error_msg}")

    # Log comprehensive batch results
    success_rate = (insertion_stats["successful_insertions"] / insertion_stats["total_records"]) * 100
    context.log.info(f"[{platform.upper()}] Batch insertion complete: {insertion_stats['successful_insertions']}/{insertion_stats['total_records']} successful ({success_rate:.1f}%)")

    if insertion_stats["failed_insertions"] > 0:
        context.log.warning(f"[{platform.upper()}] {insertion_stats['failed_insertions']} records failed insertion - continuing with next batch")
        for error_type, count in insertion_stats["error_summary"].items():
            context.log.warning(f"[{platform.upper()}] {error_type}: {count} failures")

    return insertion_stats


# Keep the original function as an alias for backward compatibility
def insert_llm_batch_results(
    context: AssetExecutionContext,
    cursor,
    batch_results: List[Dict[str, Any]],
    database_name: str,
    stage_schema: str,
    platform: str
) -> None:
    """
    Legacy function maintained for backward compatibility.
    Now uses resilient insertion internally.
    """
    insertion_stats = insert_llm_batch_results_resilient(
        context, cursor, batch_results, database_name, stage_schema, platform
    )

    # Maintain original behavior: raise exception if ALL records failed
    if insertion_stats["successful_insertions"] == 0 and insertion_stats["failed_insertions"] > 0:
        context.log.error(f"[{platform.upper()}] All records in batch failed to insert")
        raise Exception(f"Complete batch insertion failure for platform {platform}")


def finalize_platform_statistics(
    cursor,
    stats: Dict[str, Any],
    database_name: str,
    stage_schema: str,
    platform: str,
    context: AssetExecutionContext
) -> None:
    """
    Calculate final statistics for platform processing.
    """

    if stats["jobs_successful"] > 0:
        # Get average confidence from database for this platform
        cursor.execute(f"""
        SELECT AVG(LLM_OVERALL_CONFIDENCE)
        FROM {database_name}.{stage_schema}.JOBS_LLM_ENRICHED llm
        INNER JOIN {database_name}.{stage_schema}.JOBS_UNIFIED j ON llm.JOB_UID = j.JOB_UID
        WHERE j.PLATFORM = '{platform}'
        AND llm.LLM_PROCESSING_TIMESTAMP >= CURRENT_DATE()
        """)

        result = cursor.fetchone()
        if result and result[0]:
            stats["avg_confidence_score"] = float(result[0])


def create_platform_metadata(stats: Dict[str, Any], platform: str) -> Dict[str, MetadataValue]:
    """
    Create enhanced Dagster metadata for platform-specific resilient LLM processing.
    """

    extraction_success_rate = (stats["jobs_successful"] / stats["jobs_processed"]) * 100 if stats["jobs_processed"] > 0 else 0
    total_llm_failures = stats["llm_extraction_failures"] + stats["database_insertion_failures"]
    overall_success_rate = ((stats["jobs_processed"] - total_llm_failures) / stats["jobs_processed"]) * 100 if stats["jobs_processed"] > 0 else 0
    processing_time_minutes = (datetime.now() - datetime.fromisoformat(stats["processing_start"])).total_seconds() / 60 if stats["processing_start"] else 0

    metadata = {
        # Core processing metrics
        f"{platform}_jobs_processed": MetadataValue.int(stats["jobs_processed"]),
        f"{platform}_extraction_success_rate": MetadataValue.float(extraction_success_rate),
        f"{platform}_overall_success_rate": MetadataValue.float(overall_success_rate),
        f"{platform}_avg_confidence_score": MetadataValue.float(stats["avg_confidence_score"]),
        f"{platform}_low_confidence_count": MetadataValue.int(stats["low_confidence_count"]),

        # API and performance metrics
        f"{platform}_api_calls_made": MetadataValue.int(stats["api_calls_made"]),
        f"{platform}_estimated_tokens": MetadataValue.int(stats["total_tokens_estimated"]),
        f"{platform}_processing_time_minutes": MetadataValue.float(processing_time_minutes),

        # Enhanced resilient processing metrics
        f"{platform}_database_insertion_failures": MetadataValue.int(stats["database_insertion_failures"]),
        f"{platform}_batches_processed": MetadataValue.int(stats["batches_processed"]),
        f"{platform}_batches_with_failures": MetadataValue.int(stats["batches_with_failures"]),
        f"{platform}_failed_records_count": MetadataValue.int(len(stats["failed_job_records"]))
    }

    # Add error summary if there are failures
    if stats["insertion_error_summary"]:
        for error_type, count in stats["insertion_error_summary"].items():
            metadata[f"{platform}_error_{error_type.lower()}_count"] = MetadataValue.int(count)

    return metadata


def coordinate_llm_enrichment_completion(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Lightweight coordinator for LLM enrichment monitoring and validation.

    This function aggregates statistics and validates cross-platform
    LLM enrichment quality after all individual platform assets complete.
    """

    # This will be implemented in the coordinator asset
    # For now, return basic coordination stats
    return {
        "coordination_timestamp": datetime.now().isoformat(),
        "platforms_coordinated": ["bamboohr", "greenhouse", "workday", "smartrecruiters"],
        "coordination_status": "completed"
    }