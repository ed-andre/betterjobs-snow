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
from typing import Dict, Any, List
from datetime import datetime
from pathlib import Path

from dagster import (
    asset,
    AssetExecutionContext,
    MetadataValue
)

from dagster_betterjobs.resources import SnowflakeResource
from dagster_betterjobs.utils.skill_consolidation import (
    SkillData,
    ConsolidationConfig,
    consolidate_skill_variants,
    get_consolidation_summary
)
from dagster_betterjobs.utils.schema_utils import ensure_object_exists, execute_sql_file


PROJECT_ROOT = Path(__file__).resolve().parents[5]  # Go up 6 levels to project root


@asset(
    deps=["stage_jobs_llm_enriched_unified"],
    description="Extract and flatten skills from LLM VARIANT columns using schema-as-code",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"}
)
def stage_llm_skills_raw_extraction(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Extract all skills from VARIANT columns and flatten into workable format.

    Uses schema-as-code approach with canonical view definition from SQL file.

    Processes:
    - technical_skills: Flattens nested JSON by category
    - soft_skills: Extracts array values

    Note: PRIMARY_KEYWORDS are handled separately in keywords normalization pipeline.

    Output: Raw skills with source tracking and confidence scores
    """

    conn = snowflake.get_connection()
    cursor = None

    stats = {
        "extraction_timestamp": datetime.now().isoformat(),
        "skills_extracted": 0,
        "technical_skills_count": 0,
        "soft_skills_count": 0,
        "unique_jobs_processed": 0,
        "extraction_errors": 0,
        "schema_as_code": True
    }

    try:
        cursor = conn.cursor()

        context.log.info("🔍 Starting LLM skills raw extraction with schema-as-code...")

        # 🔧 SCHEMA-AS-CODE: Ensure view exists using canonical SQL file
        view_name = ensure_object_exists("views/stage_skills_raw_extraction.sql", snowflake, context)
        context.log.info(f"✅ Skills raw extraction view ready: {view_name}")

        # Get extraction statistics
        cursor.execute(f"""
        SELECT
            COUNT(*) as total_skills,
            COUNT(DISTINCT JOB_UID) as unique_jobs,
            COUNT(CASE WHEN SKILL_SOURCE = 'technical_skills' THEN 1 END) as technical_count,
            COUNT(CASE WHEN SKILL_SOURCE = 'soft_skills' THEN 1 END) as soft_count
        FROM {view_name}
        """)

        result = cursor.fetchone()
        if result:
            stats.update({
                "skills_extracted": result[0],
                "unique_jobs_processed": result[1],
                "technical_skills_count": result[2],
                "soft_skills_count": result[3]
            })

        # Sample some data for validation
        cursor.execute(f"""
        SELECT SKILL_SOURCE, SKILL_CATEGORY, SKILL_NAME_ORIGINAL, COUNT(*) as frequency
        FROM {view_name}
        GROUP BY SKILL_SOURCE, SKILL_CATEGORY, SKILL_NAME_ORIGINAL
        ORDER BY frequency DESC
        LIMIT 20
        """)

        sample_data = cursor.fetchall()
        if sample_data:
            columns = [desc[0] for desc in cursor.description]
            stats["top_skills_sample"] = [dict(zip(columns, row)) for row in sample_data]

        context.log.info(f"""
        🎯 Skills Raw Extraction Complete (Schema-as-Code):
        • Total Skills Extracted: {stats['skills_extracted']:,}
        • Unique Jobs Processed: {stats['unique_jobs_processed']:,}
        • Technical Skills: {stats['technical_skills_count']:,}
        • Soft Skills: {stats['soft_skills_count']:,}
        • View: {view_name}
        """)

        # Add metadata for Dagster UI
        context.add_output_metadata({
            "skills_extracted": MetadataValue.int(stats["skills_extracted"]),
            "unique_jobs_processed": MetadataValue.int(stats["unique_jobs_processed"]),
            "technical_skills_count": MetadataValue.int(stats["technical_skills_count"]),
            "soft_skills_count": MetadataValue.int(stats["soft_skills_count"]),
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
    description="Maintain skill standardization rules and aliases using schema-as-code",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"}
)
def stage_skills_standardization_rules(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Create and maintain comprehensive skill standardization rules.

    Uses schema-as-code approach with canonical table definition from SQL file.

    Features:
    - Technical skill standardization (e.g., JS -> JavaScript)
    - Soft skill normalization (e.g., communication -> Communication Skills)
    - Common abbreviation expansions
    - Confidence scoring based on pattern matching
    - Manual override support
    """

    conn = snowflake.get_connection()
    cursor = None

    stats = {
        "rules_timestamp": datetime.now().isoformat(),
        "total_rules_found": 0,
        "categories_covered": 0,
        "high_confidence_rules": 0,
        "rules_table_created": False,
        "data_populated": False,
        "schema_as_code": True
    }

    try:
        cursor = conn.cursor()

        context.log.info("🔧 Setting up skill standardization rules with schema-as-code...")

        # 🔧 SCHEMA-AS-CODE: Ensure table exists using canonical SQL file
        table_name = ensure_object_exists("tables/stage_skill_standardization_rules.sql", snowflake, context)
        context.log.info(f"✅ Skill standardization rules table ready: {table_name}")
        stats["rules_table_created"] = True

        # Always reload data to ensure latest rules from SQL file
        context.log.info("🔄 Clearing existing data and reloading skill standardization rules...")

        # Clear existing data
        cursor.execute(f"DELETE FROM {table_name}")
        context.log.info("🗑️ Cleared existing skill standardization rules")

        # Execute data population script using standardized utility
        insert_file_path = PROJECT_ROOT / "pipeline" / "sql" / "data_population" / "insert_skill_standardization_rules.sql"

        if insert_file_path.exists():
            result = execute_sql_file(snowflake, str(insert_file_path), context)

            if result["status"] == "success":
                context.log.info(f"✅ Successfully executed skill standardization rules data population")
                stats["data_populated"] = True
            else:
                context.log.error(f"❌ Failed to populate skill standardization rules: {result.get('error', 'Unknown error')}")
                stats["data_populated"] = False
        else:
            context.log.warning(f"Data population file not found: {insert_file_path}")
            stats["data_populated"] = False

        # Get statistics on existing rules
        cursor.execute(f"""
        SELECT
            COUNT(*) as total_rules,
            COUNT(DISTINCT SKILL_CATEGORY) as categories,
            COUNT(CASE WHEN CONFIDENCE_SCORE >= 0.9 THEN 1 END) as high_confidence
        FROM {table_name}
        """)

        result = cursor.fetchone()
        if result:
            stats.update({
                "total_rules_found": result[0],
                "categories_covered": result[1],
                "high_confidence_rules": result[2]
            })

        context.log.info(f"""
        📋 Skill Standardization Rules Status (Schema-as-Code):
        • Total Rules Found: {stats['total_rules_found']}
        • Categories Covered: {stats['categories_covered']}
        • High Confidence Rules: {stats['high_confidence_rules']}
        • Table: {table_name}
        • Data Populated: {stats['data_populated']}
        """)

        # Add metadata
        context.add_output_metadata({
            "total_rules_found": MetadataValue.int(stats["total_rules_found"]),
            "categories_covered": MetadataValue.int(stats["categories_covered"]),
            "high_confidence_rules": MetadataValue.int(stats["high_confidence_rules"]),
            "rules_table_created": MetadataValue.bool(stats["rules_table_created"]),
            "schema_as_code": MetadataValue.bool(True),
            "table_name": MetadataValue.text(table_name),
            "data_populated": MetadataValue.bool(stats["data_populated"])
        })

        return stats

    except Exception as e:
        context.log.error(f"❌ Skills standardization rules check failed: {str(e)}")
        stats["error_message"] = str(e)
        raise

    finally:
        if cursor:
            cursor.close()


@asset(
    deps=["stage_llm_skills_raw_extraction", "stage_skills_standardization_rules"],
    description="Create normalized skills master table with market intelligence using schema-as-code",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"}
)
def stage_skills_normalized(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Apply standardization rules and create skills master table.

    Uses schema-as-code approach with canonical table definitions from SQL files.

    Processing:
    - Apply standardization rules with confidence scoring
    - Deduplicate skill variations
    - Calculate frequency and trend metrics
    - Flag low-confidence items for manual review

    Output: Clean skills master table for analytics
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
        "schema_as_code": True
    }

    try:
        cursor = conn.cursor()

        context.log.info("🎯 Starting skills normalization with schema-as-code...")

        # 🔧 SCHEMA-AS-CODE: Ensure skills normalized table exists using canonical SQL file
        skills_table_name = ensure_object_exists("tables/stage_skills_normalized.sql", snowflake, context)
        context.log.info(f"✅ Skills normalized table ready: {skills_table_name}")

        # 🔧 SCHEMA-AS-CODE: Ensure skill family mapping table exists using canonical SQL file
        family_mapping_table = ensure_object_exists("tables/stage_skill_family_mapping.sql", snowflake, context)
        context.log.info(f"✅ Skill family mapping table ready: {family_mapping_table}")

        # Always reload family mapping data to ensure latest mappings from SQL file
        context.log.info("🔄 Clearing existing data and reloading skill family mappings...")

        # Clear existing family mapping data
        cursor.execute(f"DELETE FROM {family_mapping_table}")
        context.log.info("🗑️ Cleared existing skill family mappings")

        # Execute family mapping data population script
        family_insert_file_path = PROJECT_ROOT / "pipeline" / "sql" / "data_population" / "insert_skill_family_mappings.sql"

        if family_insert_file_path.exists():
            result = execute_sql_file(snowflake, str(family_insert_file_path), context)

            if result["status"] == "success":
                context.log.info(f"✅ Successfully executed skill family mappings data population")
            else:
                context.log.error(f"❌ Failed to populate skill family mappings: {result.get('error', 'Unknown error')}")
        else:
            context.log.warning(f"Family mapping data population file not found: {family_insert_file_path}")

        # Check if family mappings exist after population
        cursor.execute(f"SELECT COUNT(*) FROM {family_mapping_table}")
        mapping_count = cursor.fetchone()[0]

        if mapping_count == 0:
            context.log.warning(f"""
            ⚠️  No skill family mappings found in {family_mapping_table}.
            Family mappings are required for proper skill categorization.
            """)
        else:
            context.log.info(f"✅ Found {mapping_count} skill family mappings")

        # Clear existing data for fresh normalization
        cursor.execute(f"DELETE FROM {skills_table_name}")

        # ENHANCEMENT-023: Intelligent Skills Variant Consolidation
        context.log.info("🔄 Starting skills consolidation process...")

        # Step 1: Get skills data for consolidation
        # Get skills view and other table names dynamically
        skills_view = ensure_object_exists("views/stage_skills_raw_extraction.sql", snowflake, context)
        rules_table = ensure_object_exists("tables/stage_skill_standardization_rules.sql", snowflake, context)
        unified_jobs_table = ensure_object_exists("tables/stage_jobs_unified.sql", snowflake, context)

        pre_consolidation_sql = f"""
        WITH skill_aggregation AS (
            SELECT
                COALESCE(sr.STANDARDIZED_NAME, sre.SKILL_NAME_ORIGINAL) as skill_name,
                COALESCE(sr.SKILL_CATEGORY, sre.SKILL_CATEGORY) as skill_category,
                COALESCE(sr.SKILL_SUBCATEGORY, 'uncategorized') as skill_subcategory,
                COALESCE(sfm.SKILL_FAMILY, 'general') as skill_family,
                CASE
                    WHEN sre.SKILL_CATEGORY IN ('soft') THEN 'soft'
                    ELSE 'technical'
                END as skill_type,
                ARRAY_AGG(DISTINCT sre.SKILL_NAME_ORIGINAL) as original_variants,
                COUNT(*) as frequency_count,
                MIN(ju.DATE_RETRIEVED::DATE) as first_seen_date,
                MAX(ju.DATE_RETRIEVED::DATE) as last_seen_date,
                AVG(COALESCE(sr.CONFIDENCE_SCORE, 0.5)) as confidence_score
            FROM {skills_view} sre
            LEFT JOIN {rules_table} sr
                ON LOWER(sre.SKILL_NAME_RAW) = LOWER(sr.PATTERN)
            LEFT JOIN {family_mapping_table} sfm
                ON sre.SKILL_CATEGORY = sfm.SKILL_CATEGORY AND sfm.IS_ACTIVE = TRUE
            JOIN {unified_jobs_table} ju ON sre.JOB_UID = ju.JOB_UID
            WHERE LENGTH(sre.SKILL_NAME_RAW) >= 2  -- Filter out single characters
              AND ju.IS_ENGLISH = TRUE            -- Only English jobs
            GROUP BY 1, 2, 3, 4, 5
            HAVING COUNT(*) >= 1  -- include all skills for now but should limit eventually to what shows up more than once
        )
        SELECT * FROM skill_aggregation
        ORDER BY frequency_count DESC
        """

        cursor.execute(pre_consolidation_sql)
        pre_consolidation_results = cursor.fetchall()

        # Convert to SkillData objects for consolidation
        original_skills = {}
        for row in pre_consolidation_results:
            skill_name = row[0]
            skill_data = SkillData(
                skill_name=skill_name,
                skill_category=row[1],
                skill_subcategory=row[2],
                frequency_count=row[6],  # Fixed: frequency_count is at index 6
                confidence_score=float(row[9]),  # Fixed: confidence_score is at index 9
                original_variants=json.loads(row[5]) if isinstance(row[5], str) else row[5],  # Fixed: original_variants is at index 5
                first_seen_date=str(row[7]) if row[7] else None,  # Fixed: first_seen_date is at index 7
                last_seen_date=str(row[8]) if row[8] else None   # Fixed: last_seen_date is at index 8
            )
            original_skills[skill_name] = skill_data

        context.log.info(f"📊 Pre-consolidation: {len(original_skills)} unique skills")

        # Step 2: Apply consolidation
        consolidation_config = ConsolidationConfig(
            enabled=True,
            preferred_form="singular",
            min_frequency_threshold=2
        )

        consolidated_skills = consolidate_skill_variants(original_skills, consolidation_config)

        # Step 3: Generate consolidation summary
        consolidation_summary = get_consolidation_summary(original_skills, consolidated_skills)

        context.log.info(f"""
        ✅ Skills Consolidation Complete:
        • Original Skills: {consolidation_summary['original_skill_count']:,}
        • Consolidated Skills: {consolidation_summary['consolidated_skill_count']:,}
        • Skills Merged: {consolidation_summary['skills_merged']:,}
        • Consolidation Ratio: {consolidation_summary['consolidation_ratio']:.2%}
        """)

                        # Step 4: Bulk insert consolidated skills using staging approach
        context.log.info("🔄 Starting bulk insert of consolidated skills...")

        # Create temporary staging table
        cursor.execute("""
        CREATE OR REPLACE TEMPORARY TABLE SKILLS_STAGING (
            SKILL_ID STRING,
            SKILL_NAME STRING,
            SKILL_NAME_CLEAN STRING,
            SKILL_NAME_ORIGINAL STRING,
            SKILL_CATEGORY STRING,
            SKILL_SUBCATEGORY STRING,
            SKILL_FAMILY STRING,
            SKILL_TYPE STRING,
            ORIGINAL_VARIANTS_JSON STRING,
            FREQUENCY_COUNT INTEGER,
            FIRST_SEEN_DATE DATE,
            LAST_SEEN_DATE DATE,
            CONFIDENCE_SCORE FLOAT,
            MANUAL_REVIEW_FLAG BOOLEAN,
            CANONICAL_FORM STRING
        )
        """)

        # Prepare all data for bulk insert
        staging_data = []
        for skill_name, skill_data in consolidated_skills.items():
            skill_id = f"skill_{hash(skill_name) % 100000:05d}"

            staging_record = (
                skill_id,
                skill_name,
                skill_name.lower().strip(),
                skill_data.original_variants[0] if skill_data.original_variants else skill_name,
                skill_data.skill_category,
                skill_data.skill_subcategory,
                'general',
                'technical' if skill_data.skill_category not in ['soft', 'keyword'] else
                'soft' if skill_data.skill_category == 'soft' else 'business',
                json.dumps(skill_data.original_variants),  # Store as JSON string temporarily
                skill_data.frequency_count,
                skill_data.first_seen_date,
                skill_data.last_seen_date,
                skill_data.confidence_score,
                skill_data.confidence_score < 0.5,
                skill_name
            )
            staging_data.append(staging_record)

        # Bulk insert into staging table
        staging_insert_sql = """
        INSERT INTO SKILLS_STAGING VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cursor.executemany(staging_insert_sql, staging_data)
        context.log.info(f"✅ Bulk inserted {len(staging_data)} records into staging table")

        # Insert into final table with VARIANT conversion
        final_insert_sql = f"""
        INSERT INTO {skills_table_name} (
            SKILL_ID,
            SKILL_NAME,
            SKILL_NAME_CLEAN,
            SKILL_NAME_ORIGINAL,
            SKILL_CATEGORY,
            SKILL_SUBCATEGORY,
            SKILL_FAMILY,
            SKILL_TYPE,
            ORIGINAL_VARIANTS,
            FREQUENCY_COUNT,
            FIRST_SEEN_DATE,
            LAST_SEEN_DATE,
            CONFIDENCE_SCORE,
            MANUAL_REVIEW_FLAG,
            CANONICAL_FORM
        )
        SELECT
            SKILL_ID,
            SKILL_NAME,
            SKILL_NAME_CLEAN,
            SKILL_NAME_ORIGINAL,
            SKILL_CATEGORY,
            SKILL_SUBCATEGORY,
            SKILL_FAMILY,
            SKILL_TYPE,
            PARSE_JSON(ORIGINAL_VARIANTS_JSON) as ORIGINAL_VARIANTS,
            FREQUENCY_COUNT,
            FIRST_SEEN_DATE,
            LAST_SEEN_DATE,
            CONFIDENCE_SCORE,
            MANUAL_REVIEW_FLAG,
            CANONICAL_FORM
        FROM SKILLS_STAGING
        """

        cursor.execute(final_insert_sql)
        context.log.info("✅ Bulk inserted all skills into final table with VARIANT conversion")

        # Update stats to include consolidation metrics
        stats.update({
            "consolidation_enabled": True,
            "original_skill_count": consolidation_summary['original_skill_count'],
            "consolidated_skill_count": consolidation_summary['consolidated_skill_count'],
            "skills_merged": consolidation_summary['skills_merged'],
            "consolidation_ratio": consolidation_summary['consolidation_ratio'],
            "merge_examples": consolidation_summary['merge_examples']
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
        🎯 Skills Normalization Complete (Schema-as-Code):
        • Skills Normalized: {stats['skills_normalized']:,}
        • High Confidence: {stats['high_confidence_skills']:,}
        • Low Confidence: {stats['low_confidence_skills']:,}
        • Unique Categories: {stats['unique_skill_categories']}
        • Average Confidence: {stats['avg_confidence_score']:.3f}
        • Consolidation Ratio: {stats.get('consolidation_ratio', 0):.2%}
        • Skills Merged: {stats.get('skills_merged', 0):,}
        • Skills Table: {skills_table_name}
        • Family Mapping Table: {family_mapping_table}
        """)

        # Add metadata including consolidation metrics
        metadata = {
            "skills_normalized": MetadataValue.int(stats["skills_normalized"]),
            "high_confidence_skills": MetadataValue.int(stats["high_confidence_skills"]),
            "low_confidence_skills": MetadataValue.int(stats["low_confidence_skills"]),
            "unique_skill_categories": MetadataValue.int(stats["unique_skill_categories"]),
            "avg_confidence_score": MetadataValue.float(stats["avg_confidence_score"]),
            "category_breakdown": MetadataValue.json(stats.get("category_breakdown", [])),
            "schema_as_code": MetadataValue.bool(True),
            "skills_table_name": MetadataValue.text(skills_table_name),
            "family_mapping_table": MetadataValue.text(family_mapping_table)
        }

        # Add consolidation metrics if available
        if stats.get("consolidation_enabled"):
            metadata.update({
                "consolidation_enabled": MetadataValue.bool(True),
                "original_skill_count": MetadataValue.int(stats.get("original_skill_count", 0)),
                "consolidated_skill_count": MetadataValue.int(stats.get("consolidated_skill_count", 0)),
                "skills_merged": MetadataValue.int(stats.get("skills_merged", 0)),
                "consolidation_ratio": MetadataValue.float(stats.get("consolidation_ratio", 0)),
                "merge_examples": MetadataValue.json(stats.get("merge_examples", []))
            })

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
    description="Create job-skill relationships with context tracking using schema-as-code",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"}
)
def stage_job_skills_bridge(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Map jobs to normalized skills with rich context.

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

        context.log.info("🔗 Creating job-skills bridge relationships with schema-as-code...")

        # 🔧 SCHEMA-AS-CODE: Ensure bridge table exists using canonical SQL file
        bridge_table_name = ensure_object_exists("tables/stage_job_skills_bridge.sql", snowflake, context)
        context.log.info(f"✅ Job skills bridge table ready: {bridge_table_name}")

        # Get required table names dynamically
        skills_view = ensure_object_exists("views/stage_skills_raw_extraction.sql", snowflake, context)
        skills_normalized_table = ensure_object_exists("tables/stage_skills_normalized.sql", snowflake, context)
        rules_table = ensure_object_exists("tables/stage_skill_standardization_rules.sql", snowflake, context)
        jobs_llm_table = ensure_object_exists("tables/stage_jobs_llm_enriched.sql", snowflake, context)
        jobs_unified_table = ensure_object_exists("tables/stage_jobs_unified.sql", snowflake, context)

        # Clear existing relationships for fresh creation
        cursor.execute(f"DELETE FROM {bridge_table_name}")

        # Create job-skill relationships - Simplified approach
        bridge_sql = f"""
        INSERT INTO {bridge_table_name} (
            BRIDGE_ID,
            JOB_UID,
            SKILL_ID,
            SKILL_SOURCE,
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
            -- First pass: Match skills that were standardized
            SELECT
                sre.JOB_UID,
                sn.SKILL_ID,
                sre.SKILL_SOURCE,
                sn.SKILL_CATEGORY,
                sre.SKILL_NAME_ORIGINAL,
                COALESCE(lle.LLM_OVERALL_CONFIDENCE, 0.7) as extraction_confidence,
                sn.CONFIDENCE_SCORE as standardization_confidence,
                sr.PATTERN as matched_pattern
            FROM {skills_view} sre
            JOIN {rules_table} sr
                ON LOWER(sre.SKILL_NAME_RAW) = LOWER(sr.PATTERN)
            JOIN {skills_normalized_table} sn
                ON sn.SKILL_NAME = sr.STANDARDIZED_NAME
            LEFT JOIN {jobs_llm_table} lle
                ON sre.JOB_UID = lle.JOB_UID
            JOIN {jobs_unified_table} ju
                ON sre.JOB_UID = ju.JOB_UID
            WHERE ju.IS_ENGLISH = TRUE

            UNION ALL

            -- Second pass: Match skills that weren't standardized (direct match)
            SELECT
                sre.JOB_UID,
                sn.SKILL_ID,
                sre.SKILL_SOURCE,
                sn.SKILL_CATEGORY,
                sre.SKILL_NAME_ORIGINAL,
                COALESCE(lle.LLM_OVERALL_CONFIDENCE, 0.7) as extraction_confidence,
                sn.CONFIDENCE_SCORE as standardization_confidence,
                NULL as matched_pattern
            FROM {skills_view} sre
            JOIN {skills_normalized_table} sn
                ON sn.SKILL_NAME = sre.SKILL_NAME_ORIGINAL
            LEFT JOIN {jobs_llm_table} lle
                ON sre.JOB_UID = lle.JOB_UID
            JOIN {jobs_unified_table} ju
                ON sre.JOB_UID = ju.JOB_UID
            LEFT JOIN {rules_table} sr
                ON LOWER(sre.SKILL_NAME_RAW) = LOWER(sr.PATTERN)
            WHERE ju.IS_ENGLISH = TRUE
              AND sr.PATTERN IS NULL  -- Only get non-standardized matches
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

            'llm_auto' as processing_method,
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

        # Add foreign key constraints (best effort - may fail if constraint already exists)
        try:
            cursor.execute(f"""
            ALTER TABLE {bridge_table_name}
            ADD CONSTRAINT FK_JOB_SKILLS_JOB_UID
            FOREIGN KEY (JOB_UID) REFERENCES {jobs_unified_table}(JOB_UID)
            """)

            cursor.execute(f"""
            ALTER TABLE {bridge_table_name}
            ADD CONSTRAINT FK_JOB_SKILLS_SKILL_ID
            FOREIGN KEY (SKILL_ID) REFERENCES {skills_normalized_table}(SKILL_ID)
            """)

            context.log.info("✅ Added foreign key constraints")

        except Exception as fk_error:
            context.log.warning(f"Foreign key constraints may already exist: {fk_error}")

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