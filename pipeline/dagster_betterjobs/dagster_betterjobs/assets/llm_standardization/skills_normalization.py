"""
Phase 1: Skills Normalization Assets

Assets for extracting, standardizing, and normalizing skills data from LLM-enriched
VARIANT columns into proper relational structures for analytics.

Dependencies:
- stage_jobs_llm_enriched: Source of LLM-extracted skills data
- stage_jobs_unified: Source of job metadata

Output Tables:
- SKILLS_NORMALIZED: Master skills table with standardized names
- JOB_SKILLS_BRIDGE: Many-to-many job-skill relationships
"""

import json
import pandas as pd
from typing import Dict, Any, List, Union
from datetime import datetime
from pathlib import Path
from decimal import Decimal
import time

from dagster import (
    asset,
    AssetExecutionContext,
    MetadataValue
)

from dagster_betterjobs.resources import SnowflakeResource
from dagster_betterjobs.utils.schema_utils import ensure_object_exists, execute_sql_file
from dagster_betterjobs.transformations.llm_prompts import JobExtractionPrompts, PromptFormatter
from dagster_gemini import GeminiResource


PROJECT_ROOT = Path(__file__).resolve().parents[5]  # Go up 6 levels to project root


def _convert_decimal(obj: Union[Decimal, List, Dict, Any]) -> Union[float, List, Dict, Any]:
    """Convert Decimal objects to float for JSON serialization."""
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, list):
        return [_convert_decimal(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: _convert_decimal(value) for key, value in obj.items()}
    return obj


@asset(
    deps=["stage_jobs_llm_enriched_unified"],
    description="Extract and flatten skills from LLM VARIANT columns using schema-as-code",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"}
)
def stage_llm_skills_raw_extraction(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Extract skills from LLM enrichment and map to Lightcast taxonomy.

    Uses schema-as-code approach with canonical view definition from SQL file.

    Processes:
    - technical_skills: Flattens array and maps to Lightcast taxonomy
    - soft_skills: Extracts array values (not mapped to Lightcast)

    Output: Raw skills with Lightcast taxonomy mapping and confidence scores
    """

    conn = snowflake.get_connection()
    cursor = None

    stats = {
        "extraction_timestamp": datetime.now().isoformat(),
        "skills_extracted": 0,
        "technical_skills_count": 0,
        "soft_skills_count": 0,
        "lightcast_mapped_skills": 0,
        "high_confidence_matches": 0,  # confidence >= 0.9
        "unique_jobs_processed": 0,
        "extraction_errors": 0,
        "schema_as_code": True
    }

    try:
        cursor = conn.cursor()

        context.log.info("🔍 Starting LLM skills raw extraction with Lightcast mapping...")

        # 🔧 SCHEMA-AS-CODE: Ensure view exists using canonical SQL file
        view_name = ensure_object_exists("views/stage_skills_raw_extraction.sql", snowflake, context)
        context.log.info(f"✅ Skills raw extraction view ready: {view_name}")

        # Get extraction statistics
        cursor.execute(f"""
        SELECT
            COUNT(*) as total_skills,
            COUNT(DISTINCT JOB_UID) as unique_jobs,
            COUNT(CASE WHEN SKILL_SOURCE = 'technical_skills' THEN 1 END) as technical_count,
            COUNT(CASE WHEN SKILL_SOURCE = 'soft_skills' THEN 1 END) as soft_count,
            COUNT(CASE WHEN LIGHTCAST_SKILL_ID IS NOT NULL THEN 1 END) as lightcast_mapped,
            COUNT(CASE WHEN LIGHTCAST_MATCH_CONFIDENCE >= 0.9 THEN 1 END) as high_confidence
        FROM {view_name}
        """)

        result = cursor.fetchone()
        if result:
            stats.update({
                "skills_extracted": result[0],
                "unique_jobs_processed": result[1],
                "technical_skills_count": result[2],
                "soft_skills_count": result[3],
                "lightcast_mapped_skills": result[4],
                "high_confidence_matches": result[5]
            })

        # Sample top skills with Lightcast mapping
        cursor.execute(f"""
        SELECT
            SKILL_SOURCE,
            SKILL_NAME_ORIGINAL,
            LIGHTCAST_SKILL_NAME,
            LIGHTCAST_SUBCATEGORY_NAME,
            LIGHTCAST_CATEGORY_NAME,
            LIGHTCAST_MATCH_CONFIDENCE,
            COUNT(*) as frequency
        FROM {view_name}
        WHERE LIGHTCAST_SKILL_ID IS NOT NULL
        GROUP BY 1,2,3,4,5,6
        ORDER BY frequency DESC
        LIMIT 20
        """)

        sample_data = cursor.fetchall()
        if sample_data:
            columns = [desc[0] for desc in cursor.description]
            stats["top_skills_sample"] = _convert_decimal([dict(zip(columns, row)) for row in sample_data])

        context.log.info(f"""
        🎯 Skills Raw Extraction Complete (Lightcast Integration):
        • Total Skills Extracted: {stats['skills_extracted']:,}
        • Unique Jobs Processed: {stats['unique_jobs_processed']:,}
        • Technical Skills: {stats['technical_skills_count']:,}
        • Soft Skills: {stats['soft_skills_count']:,}
        • Lightcast Mapped: {stats['lightcast_mapped_skills']:,}
        • High Confidence: {stats['high_confidence_matches']:,}
        • View: {view_name}
        """)

        # Add metadata for Dagster UI
        context.add_output_metadata({
            "skills_extracted": MetadataValue.int(stats["skills_extracted"]),
            "unique_jobs_processed": MetadataValue.int(stats["unique_jobs_processed"]),
            "technical_skills_count": MetadataValue.int(stats["technical_skills_count"]),
            "soft_skills_count": MetadataValue.int(stats["soft_skills_count"]),
            "lightcast_mapped_skills": MetadataValue.int(stats["lightcast_mapped_skills"]),
            "high_confidence_matches": MetadataValue.int(stats["high_confidence_matches"]),
            "schema_as_code": MetadataValue.bool(True),
            "view_name": MetadataValue.text(view_name),
            "top_skills_sample": MetadataValue.json(stats.get("top_skills_sample", []))
        })

        return stats

    except Exception as e:
        context.log.error(f"❌ Skills raw extraction failed: {str(e)}")
        stats["extraction_errors"] = 1
        stats["error_message"] = str(e)
        raise

    finally:
        if cursor:
            cursor.close()




@asset(
    deps=["stage_llm_skills_raw_extraction"],
    description="Process orphaned skills through Gemini API for Lightcast taxonomy categorization",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "gemini"},

)
def stage_manual_skill_taxonomy(context: AssetExecutionContext, snowflake: SnowflakeResource, gemini: GeminiResource) -> Dict[str, Any]:
    """
    Second-pass categorization of orphaned or uncategorized skills using Gemini API.

    Maps skills to Lightcast taxonomy categories and subcategories, storing results
    in a dedicated lookup table for future reference.

    Features:
    - Bulk processing of uncategorized skills through Gemini API
    - Direct Gemini API integration
    - Confidence scoring for mappings
    - Persistent storage of results
    """

    conn = snowflake.get_connection()
    cursor = None

    stats = {
        "processing_timestamp": datetime.now().isoformat(),
        "skills_processed": 0,
        "high_confidence_mappings": 0,
        "low_confidence_mappings": 0,
        "avg_confidence_score": 0.0,
        "schema_as_code": True,
        "api_calls_made": 0,
        "extraction_failures": 0
    }

    try:
        cursor = conn.cursor()

        context.log.info("🔍 Starting manual skill taxonomy categorization...")

        taxonomy_table = ensure_object_exists("tables/stage_manual_skill_taxonomy.sql", snowflake, context)
        context.log.info(f"✅ Manual skill taxonomy table ready: {taxonomy_table}")

        # Extract uncategorized skills
        context.log.info("🔄 Extracting uncategorized skills...")
        cursor.execute(f"""
        WITH processed_skills AS (
            SELECT DISTINCT SKILL_NAME
            FROM {taxonomy_table}
            WHERE LIGHTCAST_CATEGORY_NAME IS NOT NULL
        )
        SELECT DISTINCT
            SKILL_NAME_ORIGINAL as SKILL_NAME,
            COUNT(*) as occurrence_count
        FROM BETTERJOBS_DB.STAGE.SKILLS_RAW_EXTRACTION sre
        WHERE LIGHTCAST_SKILL_NAME IS NULL
        AND NOT EXISTS (
            SELECT 1
            FROM processed_skills ps
            WHERE ps.SKILL_NAME = sre.SKILL_NAME_ORIGINAL
        )
        GROUP BY 1
        ORDER BY 2 DESC
        """)

        uncategorized_skills = cursor.fetchall()
        if not uncategorized_skills:
            context.log.info("✅ No uncategorized skills found for processing")
            return stats

        context.log.info(f"Found {len(uncategorized_skills)} uncategorized skills for processing")

        # Extract Lightcast taxonomy
        context.log.info("🔄 Extracting Lightcast taxonomy...")
        cursor.execute("""
        SELECT DISTINCT
            CATEGORY_NAME,
            SUBCATEGORY_NAME
        FROM BETTERJOBS_DB.STAGE.SKILL_2_SKILL
        WHERE LATEST_VERSION = TRUE
        ORDER BY CATEGORY_NAME, SUBCATEGORY_NAME
        """)

        taxonomy_rows = cursor.fetchall()
        taxonomy_pairs = [f"{row[0]} ⇢ {row[1]}" for row in taxonomy_rows]
        taxonomy_text = "\n".join(taxonomy_pairs)

        batch_size = 100  # Process 100 skills per API call for maximum efficiency
        prompts = JobExtractionPrompts()
        formatter = PromptFormatter()

        for i in range(0, len(uncategorized_skills), batch_size):
            batch = uncategorized_skills[i:i + batch_size]
            context.log.info(f"Processing batch {i//batch_size + 1} of {(len(uncategorized_skills)-1)//batch_size + 1}")

            # Create a list of skill names for bulk processing
            skill_names = [skill[0] for skill in batch]
            skill_list = "\n".join([f"- {skill}" for skill in skill_names])

            try:
                prompt_template = prompts.get_skill_taxonomy_prompt()
                prompt = prompt_template.format(
                    skill_name=skill_list,  # Pass the entire list of skills
                    taxonomy_list=taxonomy_text
                )

                max_retries = 2
                for attempt in range(max_retries):
                    try:
                        with gemini.get_model(context) as model:
                            stats["api_calls_made"] += 1

                            response = model.generate_content(
                                prompt,
                                generation_config={
                                    "temperature": 0.1,
                                    "candidate_count": 1
                                }
                            )

                            response_text = response.text if hasattr(response, 'text') else str(response)
                            parsed = formatter.validate_extraction_response(response_text)

                            if not parsed:
                                raise ValueError("Empty response from Gemini")

                            if "skill_mappings" not in parsed:  # Note: Changed to skill_mappings (plural)
                                raise ValueError("Missing skill_mappings in response")

                            mappings = parsed["skill_mappings"]  # Expect a list of mappings
                            if not isinstance(mappings, list):
                                raise ValueError(f"skill_mappings is not a list: {type(mappings)}")

                            # Validate and prepare all mappings for bulk insert
                            valid_mappings = []
                            skipped_count = 0
                            for mapping in mappings:
                                if not isinstance(mapping, dict):
                                    skipped_count += 1
                                    continue

                                required_fields = ["skill_name", "lightcast_category", "lightcast_subcategory", "match_confidence"]
                                missing_fields = [f for f in required_fields if f not in mapping]
                                if missing_fields:
                                    context.log.warning(f"Skipping individual record for skill '{mapping.get('skill_name', 'unknown')}' due to missing fields: {missing_fields}")
                                    skipped_count += 1
                                    continue

                                valid_mappings.append(mapping)

                            if skipped_count > 0:
                                context.log.info(f"Batch summary: {len(valid_mappings)} valid mappings will be processed, {skipped_count} records were skipped")

                            if valid_mappings:
                                # Bulk insert using ARRAY_CONSTRUCT and FLATTEN
                                cursor.execute(f"""
                                INSERT INTO {taxonomy_table} (
                                    SKILL_NAME,
                                    LIGHTCAST_CATEGORY_NAME,
                                    LIGHTCAST_SUBCATEGORY_NAME,
                                    MATCH_CONFIDENCE,
                                    SOURCE,
                                    VERSION
                                )
                                SELECT
                                    value:skill_name::STRING,
                                    value:lightcast_category::STRING,
                                    value:lightcast_subcategory::STRING,
                                    value:match_confidence::FLOAT,
                                    'gemini_auto',
                                    'v1'
                                FROM TABLE(FLATTEN(input => PARSE_JSON(%s)))
                                """, (json.dumps(valid_mappings),))

                                # Update stats
                                stats["skills_processed"] += len(valid_mappings)
                                stats["high_confidence_mappings"] += sum(1 for m in valid_mappings if m["match_confidence"] >= 0.7)
                                stats["low_confidence_mappings"] += sum(1 for m in valid_mappings if m["match_confidence"] < 0.7)

                            break

                    except Exception as api_err:
                        context.log.warning(f"Attempt {attempt + 1} failed for batch: {str(api_err)}")
                        if attempt == max_retries - 1:
                            raise
                        time.sleep(1.5 ** attempt)

            except Exception as e:
                stats["extraction_failures"] += len(skill_names)
                context.log.error(f"Error processing batch: {str(e)}")
                continue

        if stats["skills_processed"] > 0:
            cursor.execute(f"""
            SELECT AVG(MATCH_CONFIDENCE)
            FROM {taxonomy_table}
            WHERE SOURCE = 'gemini_auto'
            AND VERSION = 'v1'
            """)
            avg_confidence = cursor.fetchone()
            if avg_confidence and avg_confidence[0]:
                stats["avg_confidence_score"] = float(avg_confidence[0])

        # Log final statistics
        context.log.info(f"""
        ✅ Manual Skill Taxonomy Processing Complete:
        • Skills Processed: {stats['skills_processed']:,}
        • API Batch Calls Made: {stats['api_calls_made']:,}
        • High Confidence Mappings: {stats['high_confidence_mappings']:,}
        • Low Confidence Mappings: {stats['low_confidence_mappings']:,}
        • Extraction Failures: {stats['extraction_failures']:,}
        • Average Confidence: {stats['avg_confidence_score']:.3f}
        """)

        # Add metadata for Dagster UI
        context.add_output_metadata({
            "skills_processed": MetadataValue.int(stats["skills_processed"]),
            "api_batch_calls": MetadataValue.int(stats["api_calls_made"]),
            "high_confidence_mappings": MetadataValue.int(stats["high_confidence_mappings"]),
            "low_confidence_mappings": MetadataValue.int(stats["low_confidence_mappings"]),
            "extraction_failures": MetadataValue.int(stats["extraction_failures"]),
            "avg_confidence_score": MetadataValue.float(stats["avg_confidence_score"]),
            "processing_note": MetadataValue.text(
                "API batch calls represent HTTP requests to Gemini. Each batch processes multiple skills. "
                "Gemini internal metrics may show higher counts due to token-level processing."
            )
        })

        return stats

    except Exception as e:
        context.log.error(f"❌ Manual skill taxonomy processing failed: {str(e)}")
        stats["error_message"] = str(e)
        raise

    finally:
        if cursor:
            cursor.close()



@asset(
    deps=["stage_llm_skills_raw_extraction", "stage_manual_skill_taxonomy"],
    description="Apply standardization rules to create skills master table (standardization only) using schema-as-code",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"}
)
def stage_skills_normalized(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Apply standardization rules and create skills master table.


    ENHANCEMENT-038: Updated to use Lightcast taxonomy instead of custom taxonomy.
    Uses LIGHTCAST_CATEGORY_NAME and LIGHTCAST_SUBCATEGORY_NAME from SKILLS_RAW_EXTRACTION
    with fallback to MANUAL_SKILL_TAXONOMY for orphaned skills.

    Uses schema-as-code approach with canonical table definitions from SQL files.

    Processing:
    - Apply standardization rules with confidence scoring
    - Use Lightcast taxonomy for categorization
    - Fall back to manual taxonomy for orphaned skills
    - Calculate frequency and trend metrics from raw skills
    - Preserve original LLM confidence scores
    - Flag low-confidence items for manual review
    - NO consolidation logic (moved to separate asset)

    Output: Standardized skills master table (one record per unique skill)
    """

    conn = snowflake.get_connection()
    cursor = None

    stats = {
        "normalization_timestamp": datetime.now().isoformat(),
        "skills_normalized": 0,
        "high_confidence_skills": 0,
        "low_confidence_skills": 0,
        "unique_skill_categories": 0,
        "avg_confidence_score": 0.0,
        "lightcast_mapped_skills": 0,
        "manual_taxonomy_mapped_skills": 0,
        "unmapped_skills": 0,
        "schema_as_code": True
    }

    try:
        cursor = conn.cursor()

        context.log.info("🎯 Starting skills normalization with Lightcast taxonomy...")

        # 🔧 SCHEMA-AS-CODE: Ensure skills normalized table exists using canonical SQL file
        skills_table_name = ensure_object_exists("tables/stage_skills_normalized.sql", snowflake, context)
        context.log.info(f"✅ Skills normalized table ready: {skills_table_name}")

        # 🔧 SCHEMA-AS-CODE: Ensure manual skill taxonomy table exists
        manual_taxonomy_table = ensure_object_exists("tables/stage_manual_skill_taxonomy.sql", snowflake, context)
        context.log.info(f"✅ Manual skill taxonomy table ready: {manual_taxonomy_table}")

        # Clear existing data for fresh normalization
        cursor.execute(f"DELETE FROM {skills_table_name}")

        # ENHANCEMENT-038: Updated to use Lightcast taxonomy
        context.log.info("🔄 Starting skills standardization with Lightcast taxonomy...")

        # Get table names dynamically using schema-as-code
        skills_view = ensure_object_exists("views/stage_skills_raw_extraction.sql", snowflake, context)
        rules_table = ensure_object_exists("tables/stage_skill_standardization_rules.sql", snowflake, context)
        unified_jobs_table = ensure_object_exists("tables/stage_jobs_unified.sql", snowflake, context)
        llm_enriched_table = ensure_object_exists("tables/stage_jobs_llm_enriched.sql", snowflake, context)

        # Direct standardization with Lightcast taxonomy
        standardization_sql = f"""
        INSERT INTO {skills_table_name} (
            SKILL_ID,
            SKILL_NAME,
            SKILL_NAME_CLEAN,
            SKILL_NAME_ORIGINAL,
            SKILL_CATEGORY,
            SKILL_SUBCATEGORY,
            SKILL_TYPE,
            ORIGINAL_VARIANTS,
            FREQUENCY_COUNT,
            FIRST_SEEN_DATE,
            LAST_SEEN_DATE,
            CONFIDENCE_SCORE,
            MANUAL_REVIEW_FLAG,
            CANONICAL_FORM
        )
        WITH standardized_skills AS (
            SELECT
                sre.SKILL_NAME_ORIGINAL as skill_name,
                -- Use Lightcast taxonomy with fallback to manual taxonomy and then 'Unknown'
                COALESCE(
                    sre.LIGHTCAST_CATEGORY_NAME,
                    mst.LIGHTCAST_CATEGORY_NAME,
                    'Unknown'
                ) as skill_category,
                COALESCE(
                    sre.LIGHTCAST_SUBCATEGORY_NAME,
                    mst.LIGHTCAST_SUBCATEGORY_NAME,
                    'Unknown'
                ) as skill_subcategory,
                CASE
                    WHEN sre.SKILL_SOURCE IN ('soft_skills') THEN 'soft'
                    ELSE 'technical'
                END as skill_type,
                ARRAY_AGG(DISTINCT sre.SKILL_NAME_ORIGINAL) as original_variants,
                COUNT(*) as frequency_count,
                MIN(ju.DATE_RETRIEVED::DATE) as first_seen_date,
                MAX(ju.DATE_RETRIEVED::DATE) as last_seen_date,
                -- Preserve original LLM confidence scores
                AVG(COALESCE(
                    mst.MATCH_CONFIDENCE, -- added first to prevent using 0.5 for skills that didn't get mapped in first pass
                    sre.LIGHTCAST_MATCH_CONFIDENCE,
                    lle.SKILLS_CONFIDENCE,
                    0.5
                )) as confidence_score,
                -- Track taxonomy source for statistics
                CASE
                    WHEN sre.LIGHTCAST_CATEGORY_NAME IS NOT NULL THEN 'lightcast_direct'
                    WHEN mst.LIGHTCAST_CATEGORY_NAME IS NOT NULL THEN 'manual_taxonomy'
                    ELSE 'unknown'
                END as taxonomy_source
            FROM {skills_view} sre
            LEFT JOIN {manual_taxonomy_table} mst
                ON LOWER(sre.SKILL_NAME_ORIGINAL) = LOWER(mst.SKILL_NAME)
            LEFT JOIN {llm_enriched_table} lle
                ON sre.JOB_UID = lle.JOB_UID
            JOIN {unified_jobs_table} ju
                ON sre.JOB_UID = ju.JOB_UID
            WHERE LENGTH(sre.SKILL_NAME_RAW) >= 2  -- Filter out single characters
              AND ju.IS_ENGLISH = TRUE            -- Only English jobs. Handling foreign language skills would be a headache now
            GROUP BY 1, 2, 3, 4, 10
            HAVING COUNT(*) >= 1  -- Include all skills
        )
        SELECT
            CONCAT('skill_', ROW_NUMBER() OVER (ORDER BY frequency_count DESC)) as skill_id,
            skill_name,
            LOWER(TRIM(skill_name)) as skill_name_clean,
            original_variants[0]::STRING as skill_name_original,
            skill_category,
            skill_subcategory,
            skill_type,
            original_variants,
            frequency_count,
            first_seen_date,
            last_seen_date,
            confidence_score,
            confidence_score < 0.5 as manual_review_flag,
            skill_name as canonical_form  -- Same as skill_name for standardized skills
        FROM standardized_skills
        """

        cursor.execute(standardization_sql)
        context.log.info("✅ Successfully applied standardization rules and inserted skills with Lightcast taxonomy")

        # Update stats to reflect standardization-only processing
        stats.update({
            "consolidation_enabled": False,
            "standardization_only": True,
            "processing_method": "direct_sql_standardization_with_lightcast"
        })

        # Get normalization statistics
        cursor.execute(f"""
        SELECT
            COUNT(*) as total_skills,
            COUNT(CASE WHEN CONFIDENCE_SCORE >= 0.8 THEN 1 END) as high_confidence,
            COUNT(CASE WHEN CONFIDENCE_SCORE < 0.5 THEN 1 END) as low_confidence,
            COUNT(DISTINCT SKILL_CATEGORY) as unique_categories,
            AVG(CONFIDENCE_SCORE) as avg_confidence
        FROM {skills_table_name}
        """)

        result = cursor.fetchone()
        if result:
            stats.update({
                "skills_normalized": result[0],
                "high_confidence_skills": result[1],
                "low_confidence_skills": result[2],
                "unique_skill_categories": result[3],
                "avg_confidence_score": result[4]
            })

        # Get taxonomy source breakdown
        cursor.execute(f"""
        WITH taxonomy_sources AS (
            SELECT
                CASE
                    WHEN s.SKILL_CATEGORY = 'Unknown' THEN 'unmapped'
                    WHEN EXISTS (
                        SELECT 1 FROM {manual_taxonomy_table} m
                        WHERE LOWER(s.SKILL_NAME) = LOWER(m.SKILL_NAME)
                    ) THEN 'manual_taxonomy'
                    ELSE 'lightcast_direct'
                END as taxonomy_source,
                COUNT(*) as skill_count
            FROM {skills_table_name} s
            GROUP BY 1
        )
        SELECT taxonomy_source, skill_count
        FROM taxonomy_sources
        ORDER BY skill_count DESC
        """)

        taxonomy_results = cursor.fetchall()
        if taxonomy_results:
            for row in taxonomy_results:
                source, count = row
                if source == 'lightcast_direct':
                    stats["lightcast_mapped_skills"] = count
                elif source == 'manual_taxonomy':
                    stats["manual_taxonomy_mapped_skills"] = count
                elif source == 'unmapped':
                    stats["unmapped_skills"] = count

        # Get category breakdown
        cursor.execute(f"""
        SELECT SKILL_CATEGORY, COUNT(*) as skill_count
        FROM {skills_table_name}
        GROUP BY SKILL_CATEGORY
        ORDER BY skill_count DESC
        """)

        category_results = cursor.fetchall()
        if category_results:
            columns = [desc[0] for desc in cursor.description]
            stats["category_breakdown"] = [dict(zip(columns, row)) for row in category_results]

        context.log.info(f"""
        🎯 Skills Standardization Complete with Lightcast Taxonomy - ENHANCEMENT-038:
        • Skills Standardized: {stats['skills_normalized']:,}
        • Lightcast Mapped Skills: {stats.get('lightcast_mapped_skills', 0):,}
        • Manual Taxonomy Mapped: {stats.get('manual_taxonomy_mapped_skills', 0):,}
        • Unmapped Skills: {stats.get('unmapped_skills', 0):,}
        • High Confidence: {stats['high_confidence_skills']:,}
        • Low Confidence: {stats['low_confidence_skills']:,}
        • Unique Categories: {stats['unique_skill_categories']}
        • Average Confidence: {stats['avg_confidence_score']:.3f}
        • Processing Method: {stats.get('processing_method', 'standardization')}
        • Skills Table: {skills_table_name}
        """)

        # Add metadata for standardization with Lightcast taxonomy
        metadata = {
            "skills_normalized": MetadataValue.int(stats["skills_normalized"]),
            "lightcast_mapped_skills": MetadataValue.int(stats.get("lightcast_mapped_skills", 0)),
            "manual_taxonomy_mapped_skills": MetadataValue.int(stats.get("manual_taxonomy_mapped_skills", 0)),
            "unmapped_skills": MetadataValue.int(stats.get("unmapped_skills", 0)),
            "high_confidence_skills": MetadataValue.int(stats["high_confidence_skills"]),
            "low_confidence_skills": MetadataValue.int(stats["low_confidence_skills"]),
            "unique_skill_categories": MetadataValue.int(stats["unique_skill_categories"]),
            "avg_confidence_score": MetadataValue.float(stats["avg_confidence_score"]),
            "category_breakdown": MetadataValue.json(stats.get("category_breakdown", [])),
            "schema_as_code": MetadataValue.bool(True),
            "skills_table_name": MetadataValue.text(skills_table_name),
            "standardization_only": MetadataValue.bool(True),
            "processing_method": MetadataValue.text(stats.get("processing_method", "direct_sql_standardization_with_lightcast")),
            "enhancement_038": MetadataValue.bool(True),  # Flag for tracking this enhancement
            "consolidation_enabled": MetadataValue.bool(False)
        }

        context.add_output_metadata(metadata)

        return stats

    except Exception as e:
        context.log.error(f"❌ Skills normalization failed: {str(e)}")
        stats["error_message"] = str(e)
        raise

    finally:
        if cursor:
            cursor.close()


@asset(
    deps=["stage_skills_normalized", "stage_jobs_unified"],
    description="Create job-skill relationships using normalized skills with context tracking using schema-as-code",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"}
)
def stage_job_skills_bridge(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Map jobs to normalized skills with rich context.

    ENHANCEMENT-035: Updated to use normalized skills from dedicated asset.

    Uses schema-as-code approach with canonical table definition from SQL file.

    Features:
    - Source tracking (technical_skills vs soft_skills)
    - Context classification (required vs preferred vs nice-to-have)
    - Experience level inference
    - Confidence scoring for skill-job associations
    """

    conn = snowflake.get_connection()
    cursor = None

    stats = {
        "bridge_timestamp": datetime.now().isoformat(),
        "total_relationships": 0,
        "unique_jobs_with_skills": 0,
        "unique_skills_used": 0,
        "avg_skills_per_job": 0.0,
        "high_confidence_relationships": 0,
        "schema_as_code": True
    }

    try:
        cursor = conn.cursor()

        context.log.info("🔗 Creating job-skills bridge relationships using normalized skills ...")

        # 🔧 SCHEMA-AS-CODE: Ensure bridge table exists using canonical SQL file
        bridge_table_name = ensure_object_exists("tables/stage_job_skills_bridge.sql", snowflake, context)
        context.log.info(f"✅ Job skills bridge table ready: {bridge_table_name}")
        context.log.info("📊 Using normalized skills with lightcast taxonomy for improved variant matching")

        # Get required table names dynamically
        skills_view = ensure_object_exists("views/stage_skills_raw_extraction.sql", snowflake, context)
        skills_normalized_table = ensure_object_exists("tables/stage_skills_normalized.sql", snowflake, context)
        rules_table = ensure_object_exists("tables/stage_skill_standardization_rules.sql", snowflake, context)
        jobs_llm_table = ensure_object_exists("tables/stage_jobs_llm_enriched.sql", snowflake, context)
        jobs_unified_table = ensure_object_exists("tables/stage_jobs_unified.sql", snowflake, context)

        context.log.info(f"🔄 Cleared existing data for fresh creation of {bridge_table_name}")
        # Clear existing relationships for fresh creation
        cursor.execute(f"DELETE FROM {bridge_table_name}")


        context.log.info(f"🔄 Inserting job-skill relationships for {bridge_table_name}")
        # Create job-skill relationships - Simplified approach
        bridge_sql = f"""
        INSERT INTO {bridge_table_name} (
            BRIDGE_ID,
            JOB_UID,
            SKILL_ID,
            SKILL_SOURCE, -- same as SKILL_TYPE in STAGE.SKILLS_NORMALIZED
            SKILL_CATEGORY,
            ORIGINAL_TEXT,
            EXTRACTION_CONFIDENCE,
            STANDARDIZATION_CONFIDENCE,
            OVERALL_CONFIDENCE,
            SKILL_CONTEXT,
            PROCESSING_METHOD,
            NEEDS_REVIEW
        )
        WITH skill_matches AS (
            -- Match skills from raw extraction to normalized skills using original variants
            SELECT DISTINCT
                sre.JOB_UID,
                sn.SKILL_ID,
                sre.SKILL_SOURCE,
                sn.SKILL_CATEGORY,
                sre.SKILL_NAME_ORIGINAL,
                COALESCE(sn.CONFIDENCE_SCORE, lle.LLM_OVERALL_CONFIDENCE, 0.5) as extraction_confidence,
                sn.CONFIDENCE_SCORE as standardization_confidence
            FROM {skills_view} sre
            JOIN {skills_normalized_table} sn
                ON (
                    sn.SKILL_NAME = sre.SKILL_NAME_ORIGINAL

                )
            LEFT JOIN {jobs_llm_table} lle
                ON sre.JOB_UID = lle.JOB_UID
            JOIN {jobs_unified_table} ju
                ON sre.JOB_UID = ju.JOB_UID
            WHERE ju.IS_ENGLISH = TRUE
        )
        SELECT
            CONCAT('bridge_', ROW_NUMBER() OVER (ORDER BY JOB_UID, SKILL_ID)) as bridge_id,
            JOB_UID,
            SKILL_ID,
            SKILL_SOURCE,
            SKILL_CATEGORY,
            SKILL_NAME_ORIGINAL,
            extraction_confidence,
            standardization_confidence,
            (extraction_confidence + standardization_confidence) / 2.0 as overall_confidence,

            -- Infer context based on source
            CASE
                WHEN SKILL_SOURCE = 'technical_skills' THEN 'required'
                WHEN SKILL_SOURCE = 'soft_skills' THEN 'preferred'
                ELSE 'unknown'
            END as skill_context,

            'llm_auto_normalized' as processing_method,
            CASE WHEN standardization_confidence < 0.5 THEN TRUE ELSE FALSE END as needs_review

        FROM skill_matches
        """

        cursor.execute(bridge_sql)

        # Get bridge statistics
        cursor.execute(f"""
        SELECT
            COUNT(*) as total_relationships,
            COUNT(DISTINCT JOB_UID) as unique_jobs,
            COUNT(DISTINCT SKILL_ID) as unique_skills,
            COUNT(CASE WHEN OVERALL_CONFIDENCE >= 0.8 THEN 1 END) as high_confidence
        FROM {bridge_table_name}
        """)

        result = cursor.fetchone()
        if result:
            stats.update({
                "total_relationships": result[0],
                "unique_jobs_with_skills": result[1],
                "unique_skills_used": result[2],
                "high_confidence_relationships": result[3]
            })

        # Calculate average skills per job separately
        cursor.execute(f"""
        SELECT AVG(skill_count) as avg_skills_per_job
        FROM (
            SELECT JOB_UID, COUNT(*) as skill_count
            FROM {bridge_table_name}
            GROUP BY JOB_UID
        ) job_skill_counts
        """)

        avg_result = cursor.fetchone()
        if avg_result and avg_result[0]:
            stats["avg_skills_per_job"] = float(avg_result[0])

        # Get source breakdown
        cursor.execute(f"""
        SELECT
            SKILL_SOURCE,
            COUNT(*) as relationship_count,
            COUNT(DISTINCT JOB_UID) as jobs_count,
            AVG(OVERALL_CONFIDENCE) as avg_confidence
        FROM {bridge_table_name}
        WHERE SKILL_SOURCE IN ('technical_skills', 'soft_skills')
        GROUP BY SKILL_SOURCE
        ORDER BY relationship_count DESC
        """)

        source_results = cursor.fetchall()
        if source_results:
            columns = [desc[0] for desc in cursor.description]
            stats["source_breakdown"] = [dict(zip(columns, row)) for row in source_results]

        # Foreign key constraints are defined in the table creation script (stage_job_skills_bridge.sql)
        # No need to add them here - they are created when the table is created

        context.log.info(f"""
        🔗 Job-Skills Bridge Complete (Schema-as-Code):
        • Total Relationships: {stats['total_relationships']:,}
        • Unique Jobs with Skills: {stats['unique_jobs_with_skills']:,}
        • Unique Skills Used: {stats['unique_skills_used']:,}
        • Average Skills per Job: {stats['avg_skills_per_job']:.1f}
        • High Confidence Relationships: {stats['high_confidence_relationships']:,}
        • Bridge Table: {bridge_table_name}
        """)

        # Add metadata
        context.add_output_metadata({
            "total_relationships": MetadataValue.int(stats["total_relationships"]),
            "unique_jobs_with_skills": MetadataValue.int(stats["unique_jobs_with_skills"]),
            "unique_skills_used": MetadataValue.int(stats["unique_skills_used"]),
            "avg_skills_per_job": MetadataValue.float(stats["avg_skills_per_job"]),
            "high_confidence_relationships": MetadataValue.int(stats["high_confidence_relationships"]),
            "source_breakdown": MetadataValue.json(stats.get("source_breakdown", [])),
            "schema_as_code": MetadataValue.bool(True),
            "bridge_table_name": MetadataValue.text(bridge_table_name)
        })

        return stats

    except Exception as e:
        context.log.error(f"❌ Job-skills bridge creation failed: {str(e)}")
        stats["error_message"] = str(e)
        raise

    finally:
        if cursor:
            cursor.close()

