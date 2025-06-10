"""
Stage Jobs LLM Enriched Asset

PHASE-2: LLM Integration for structured information extraction from job descriptions.

This asset leverages the existing Gemini resource to extract structured information
from job descriptions including:
- Salary information
- Experience requirements
- Technical skills and soft skills
- Work arrangement details
- Job classification and keywords

Uses proven patterns from retry_failed_company_urls.py for batch processing
and error handling with the Gemini API.
"""

import json
import time
import pandas as pd
from datetime import datetime, date
from typing import Dict, Any, List, Optional

from dagster import (
    asset,
    AssetExecutionContext,
    MetadataValue,
    Config
)

from dagster_betterjobs.transformations.llm_prompts import (
    JobExtractionPrompts,
    PromptFormatter
)


class LLMEnrichmentConfig(Config):
    """Configuration for LLM enrichment processing."""
    batch_size: int = 15  # Proven optimal batch size from existing Gemini usage
    delay_between_batches: float = 1.0  # Rate limiting between batches
    max_retries: int = 3  # Maximum retry attempts for failed API calls
    max_description_length: int = 8000  # Token limit for job descriptions
    confidence_threshold: float = 0.6  # Threshold for low-confidence flagging
    limit_jobs: Optional[int] = None  # Limit for testing (None = process all)
    enable_validation_pass: bool = True  # Enable second-pass validation for low confidence
    processing_mode: str = "new_only"  # "new_only", "all", "failed_only"


@asset(
    group_name="stage_cleansing_enrichment_validation_transformation",
    kinds={"snowflake", "python", "gemini"},
    required_resource_keys={"snowflake", "gemini"},
    deps=["stage_jobs_unified"],
    description="Extract structured information from job descriptions using Gemini LLM"
)
def stage_jobs_llm_enriched(context: AssetExecutionContext, config: LLMEnrichmentConfig) -> Dict[str, Any]:
    """
    Enrich job data with AI-extracted information using Gemini LLM.

    This asset processes jobs from the stage_jobs_unified table and extracts
    structured information including salary, skills, experience requirements,
    work arrangements, and job classifications using the Gemini API.

    Uses proven batch processing patterns from existing Gemini integrations
    with comprehensive error handling and quality validation.

    Returns:
        Dict with processing statistics and results
    """

    # Get resources
    conn = context.resources.snowflake.get_connection()
    gemini = context.resources.gemini

    # Initialize statistics tracking
    stats = {
        "processing_start": datetime.now().isoformat(),
        "jobs_processed": 0,
        "jobs_successful": 0,
        "jobs_failed": 0,
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

        # Create the LLM enriched table if it doesn't exist
        context.log.info("🏗️  Creating LLM enriched table schema...")
        create_llm_table_query = f"""
        CREATE TABLE IF NOT EXISTS {database_name}.{stage_schema}.JOBS_LLM_ENRICHED (
            -- Primary key (matches jobs_unified.job_uid)
            JOB_UID STRING PRIMARY KEY,

            -- Salary information (8 fields)
            SALARY_MIN NUMBER,
            SALARY_MAX NUMBER,
            SALARY_CURRENCY STRING DEFAULT 'USD',
            SALARY_PERIOD STRING, -- hourly, annually, monthly
            SALARY_TYPE STRING,   -- base, total, contract
            EQUITY_MENTIONED BOOLEAN DEFAULT FALSE,
            BONUS_MENTIONED BOOLEAN DEFAULT FALSE,
            SALARY_CONFIDENCE FLOAT,

            -- Experience requirements (6 fields)
            MIN_YEARS_EXPERIENCE NUMBER,
            MAX_YEARS_EXPERIENCE NUMBER,
            EXPERIENCE_LEVEL STRING, -- Entry, Mid, Senior, Executive
            SPECIFIC_TECHNOLOGIES_YEARS VARIANT, -- JSON object with tech-to-years mapping
            EDUCATION_REQUIREMENTS VARIANT, -- Array of education requirements
            CERTIFICATIONS VARIANT, -- Array of certification names
            EXPERIENCE_CONFIDENCE FLOAT,

            -- Technical & soft skills (3 consolidated fields)
            TECHNICAL_SKILLS VARIANT, -- JSON object with categories
            SOFT_SKILLS VARIANT, -- Array of soft skills
            SKILLS_CONFIDENCE FLOAT,

            -- Work arrangement (6 fields)
            WORK_TYPE STRING, -- Remote, Hybrid, On-site
            REMOTE_FLEXIBILITY STRING,
            TRAVEL_REQUIREMENTS STRING,
            OFFICE_LOCATIONS VARIANT, -- Array of locations
            TIMEZONE_REQUIREMENTS STRING, -- Timezone preference text
            WORK_ARRANGEMENT_CONFIDENCE FLOAT,

            -- Job classification (8 fields)
            JOB_FAMILY STRING, -- Engineering, Data, Product, etc.
            JOB_SUB_FAMILY STRING,
            SENIORITY_LEVEL STRING,
            PRIMARY_KEYWORDS VARIANT, -- Array of keywords
            INDUSTRY_KEYWORDS VARIANT, -- Array of industry terms
            ROLE_TYPE STRING, -- IC, Manager, Director, VP
            TEAM_SIZE STRING, -- Team size description
            CLASSIFICATION_CONFIDENCE FLOAT,

            -- LLM processing metadata (schema-compliant)
            LLM_PROCESSED BOOLEAN DEFAULT TRUE,
            LLM_PROCESSING_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
            LLM_MODEL_VERSION STRING DEFAULT 'gemini-2.5-flash-preview-05-20',
            LLM_OVERALL_CONFIDENCE FLOAT,
            LLM_NEEDS_MANUAL_REVIEW BOOLEAN DEFAULT FALSE,

            -- Quality validation (schema-compliant)
            EXTRACTION_CONFIDENCE_AVG FLOAT,     -- Average of all confidence scores
            KEYWORD_QUALITY_SCORE FLOAT,         -- SQL-based keyword validation score
            VALIDATION_STATUS STRING DEFAULT 'pending', -- pending, validated, needs_review
            LAST_UPDATED TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
        )
        """

        cursor.execute(create_llm_table_query)
        context.log.info("✅ LLM enriched table schema created/verified")

        # Determine which jobs to process based on processing mode
        context.log.info(f"📋 Processing mode: {config.processing_mode}")

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
            WHERE j.IS_ENGLISH = TRUE
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
            WHERE j.IS_ENGLISH = TRUE
                AND j.JOB_DESCRIPTION_CLEAN IS NOT NULL
                AND LENGTH(j.JOB_DESCRIPTION_CLEAN) >= 100
                AND llm.LLM_OVERALL_CONFIDENCE < 0.3  -- Very low confidence indicates failure
            """
        else:  # "all"
            # Process all English jobs
            jobs_query = f"""
            SELECT
                j.JOB_UID,
                j.JOB_TITLE_CLEAN,
                j.JOB_DESCRIPTION_CLEAN,
                j.COMPANY_NAME_CLEAN,
                j.PLATFORM,
                j.IS_ENGLISH
            FROM {database_name}.{stage_schema}.JOBS_UNIFIED j
            WHERE j.IS_ENGLISH = TRUE
                AND j.JOB_DESCRIPTION_CLEAN IS NOT NULL
                AND LENGTH(j.JOB_DESCRIPTION_CLEAN) >= 100
            """

        # Add limit if specified
        if config.limit_jobs:
            jobs_query += f" LIMIT {config.limit_jobs}"

        context.log.info("🔍 Loading jobs for LLM processing...")
        cursor.execute(jobs_query)

        # Convert to DataFrame for easier processing
        columns = [desc[0] for desc in cursor.description]
        jobs_data = cursor.fetchall()
        jobs_df = pd.DataFrame(jobs_data, columns=columns)

        context.log.info(f"📊 Found {len(jobs_df)} jobs to process with LLM")

        if len(jobs_df) == 0:
            context.log.info("✅ No jobs to process")
            return stats

        # Initialize prompt templates
        prompts = JobExtractionPrompts()
        formatter = PromptFormatter()

        # Process jobs in batches
        batch_size = config.batch_size
        total_batches = (len(jobs_df) + batch_size - 1) // batch_size

        context.log.info(f"🚀 Starting LLM processing: {len(jobs_df)} jobs in {total_batches} batches")

        for batch_idx in range(0, len(jobs_df), batch_size):
            batch_jobs = jobs_df.iloc[batch_idx:batch_idx + batch_size]
            current_batch = (batch_idx // batch_size) + 1

            context.log.info(f"🔄 Processing batch {current_batch}/{total_batches} ({len(batch_jobs)} jobs)")

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
                        max_retries=config.max_retries
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

                        context.log.debug(f"✅ Processed job {job['JOB_UID']}: confidence={overall_confidence:.3f}")

                    else:
                        stats["jobs_failed"] += 1
                        context.log.warning(f"❌ Failed to extract data for job {job['JOB_UID']}")

                except Exception as e:
                    stats["jobs_failed"] += 1
                    error_type = type(e).__name__
                    stats["error_summary"][error_type] = stats["error_summary"].get(error_type, 0) + 1
                    context.log.error(f"❌ Error processing job {job['JOB_UID']}: {str(e)}")

                stats["jobs_processed"] += 1

            # Bulk insert batch results
            if batch_results:
                insert_llm_batch_results(context, cursor, batch_results, database_name, stage_schema)
                context.log.info(f"💾 Saved batch {current_batch} results: {len(batch_results)} records")

            # Rate limiting between batches
            if current_batch < total_batches:
                time.sleep(config.delay_between_batches)

        # Calculate final statistics
        if stats["jobs_successful"] > 0:
            # Get average confidence from database
            cursor.execute(f"""
            SELECT AVG(LLM_OVERALL_CONFIDENCE)
            FROM {database_name}.{stage_schema}.JOBS_LLM_ENRICHED
            WHERE LLM_PROCESSING_TIMESTAMP >= CURRENT_DATE()
            """)

            result = cursor.fetchone()
            if result and result[0]:
                stats["avg_confidence_score"] = float(result[0])

        # Success rate calculation
        success_rate = (stats["jobs_successful"] / stats["jobs_processed"]) * 100 if stats["jobs_processed"] > 0 else 0

        context.log.info(f"""
        🎯 LLM Processing Complete:
        • Jobs Processed: {stats['jobs_processed']}
        • Success Rate: {success_rate:.1f}%
        • Average Confidence: {stats['avg_confidence_score']:.3f}
        • Low Confidence Jobs: {stats['low_confidence_count']}
        • API Calls Made: {stats['api_calls_made']}
        • Estimated Tokens: {stats['total_tokens_estimated']:,}
        """)

        # Add metadata for Dagster UI
        context.add_output_metadata({
            "jobs_processed": MetadataValue.int(stats["jobs_processed"]),
            "success_rate": MetadataValue.float(success_rate),
            "avg_confidence_score": MetadataValue.float(stats["avg_confidence_score"]),
            "low_confidence_count": MetadataValue.int(stats["low_confidence_count"]),
            "api_calls_made": MetadataValue.int(stats["api_calls_made"]),
            "estimated_tokens": MetadataValue.int(stats["total_tokens_estimated"]),
            "processing_time_minutes": MetadataValue.float((datetime.now() - datetime.fromisoformat(stats["processing_start"])).total_seconds() / 60) if stats["processing_start"] else 0
        })

        return stats

    except Exception as e:
        context.log.error(f"❌ LLM enrichment failed: {str(e)}")
        raise

    finally:
        if cursor:
            cursor.close()


def extract_job_data_with_retry(
    context: AssetExecutionContext,
    gemini,
    prompt: str,
    job_uid: str,
    max_retries: int = 3
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

                context.log.debug(f"Gemini response for {job_uid} in {elapsed_time:.2f}s (attempt {attempt + 1})")

                # Parse JSON response using the prompt formatter
                formatter = PromptFormatter()
                extracted_data = formatter.validate_extraction_response(response.text)

                # Validate that we got the expected structure
                if validate_extraction_structure(extracted_data):
                    return extracted_data
                else:
                    context.log.warning(f"Invalid extraction structure for {job_uid} (attempt {attempt + 1})")
                    if attempt == max_retries - 1:
                        return None

        except json.JSONDecodeError as e:
            context.log.warning(f"JSON decode error for {job_uid} (attempt {attempt + 1}): {str(e)}")
            if attempt == max_retries - 1:
                return None

        except Exception as e:
            context.log.error(f"Gemini API error for {job_uid} (attempt {attempt + 1}): {str(e)}")
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

        # Experience requirements (updated to match schema)
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

        # Work arrangement (updated to match schema)
        'work_type': work_arrangement.get('work_type'),
        'remote_flexibility': work_arrangement.get('remote_flexibility'),
        'travel_requirements': work_arrangement.get('travel_requirements'),
        'office_locations': json.dumps(work_arrangement.get('office_locations', [])),
        'timezone_requirements': work_arrangement.get('timezone_requirements'),
        'work_arrangement_confidence': work_arrangement.get('confidence', 0.0),

        # Job classification (updated to match schema)
        'job_family': job_classification.get('job_family'),
        'job_sub_family': job_classification.get('job_sub_family'),
        'seniority_level': job_classification.get('seniority_level'),
        'primary_keywords': json.dumps(job_classification.get('primary_keywords', [])),
        'industry_keywords': json.dumps(job_classification.get('industry_keywords', [])),
        'role_type': job_classification.get('role_type'),
        'team_size': job_classification.get('team_size'),
        'classification_confidence': job_classification.get('confidence', 0.0),

        # LLM processing metadata (schema-compliant)
        'llm_overall_confidence': overall_confidence,
        'llm_needs_manual_review': overall_confidence < 0.6,  # Flag low confidence for review
        'extraction_confidence_avg': overall_confidence,
        'keyword_quality_score': None,  # Future implementation
        'validation_status': 'pending'
    }


def insert_llm_batch_results(
    context: AssetExecutionContext,
    cursor,
    batch_results: List[Dict[str, Any]],
    database_name: str,
    stage_schema: str
) -> None:
    """
    Bulk insert LLM extraction results into Snowflake.
    """

    if not batch_results:
        return

    # Build single row insert query using VALUES with individual execution
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

    try:
        # Execute individual inserts instead of executemany for complex queries
        for record in batch_results:
            cursor.execute(insert_query, record)
        context.log.debug(f"Inserted {len(batch_results)} LLM records")
    except Exception as e:
        context.log.error(f"Failed to insert LLM batch results: {str(e)}")
        raise