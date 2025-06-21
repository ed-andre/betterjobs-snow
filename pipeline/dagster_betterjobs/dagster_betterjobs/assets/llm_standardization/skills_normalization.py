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

from dagster import (
    asset,
    AssetExecutionContext,
    MetadataValue
)

from dagster_betterjobs.resources import SnowflakeResource


@asset(
    deps=["stage_jobs_llm_enriched_unified"],
    description="Extract and flatten skills from LLM VARIANT columns",
    group_name="llm_standardization",
    kinds={"snowflake", "python", "SQL"}
)
def stage_llm_skills_raw_extraction(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Extract all skills from VARIANT columns and flatten into workable format.

    Processes:
    - technical_skills: Flattens nested JSON by category
    - soft_skills: Extracts array values
    - primary_keywords: Treats as skills for standardization

    Output: Raw skills with source tracking and confidence scores
    """

    conn = snowflake.get_connection()
    cursor = None

    stats = {
        "extraction_timestamp": datetime.now().isoformat(),
        "skills_extracted": 0,
        "technical_skills_count": 0,
        "soft_skills_count": 0,
        "primary_keywords_count": 0,
        "unique_jobs_processed": 0,
        "extraction_errors": 0
    }

    try:
        cursor = conn.cursor()

        context.log.info("🔍 Starting LLM skills raw extraction...")

        # Create or replace the raw extraction view
        extraction_sql = """
        CREATE OR REPLACE VIEW BETTERJOBS_DB.STAGE.SKILLS_RAW_EXTRACTION AS
        WITH         TECHNICAL_SKILLS_EXPLODED AS (
            -- Extract technical skills by category
            SELECT
                jle.JOB_UID,
                'technical_skills' as SKILL_SOURCE,
                SKILL_CATEGORY.KEY::STRING as SKILL_CATEGORY,
                TRIM(LOWER(SKILL_NAME.VALUE::STRING)) as SKILL_NAME_RAW,
                SKILL_NAME.VALUE::STRING as SKILL_NAME_ORIGINAL
            FROM BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED jle,
            LATERAL FLATTEN(input => jle.TECHNICAL_SKILLS) SKILL_CATEGORY,
            LATERAL FLATTEN(input => SKILL_CATEGORY.VALUE) SKILL_NAME
            WHERE jle.TECHNICAL_SKILLS IS NOT NULL
              AND SKILL_CATEGORY.VALUE IS NOT NULL
              AND IS_ARRAY(SKILL_CATEGORY.VALUE)
              AND ARRAY_SIZE(SKILL_CATEGORY.VALUE) > 0
              AND LENGTH(TRIM(SKILL_NAME.VALUE::STRING)) > 1
              AND LOWER(TRIM(SKILL_NAME.VALUE::STRING)) NOT IN ('null', 'none', 'n/a', '')
        ),

        SOFT_SKILLS_EXPLODED AS (
            -- Extract soft skills
            SELECT
                jle.JOB_UID,
                'soft_skills' as SKILL_SOURCE,
                'soft' as SKILL_CATEGORY,
                TRIM(LOWER(SKILL.VALUE::STRING)) as SKILL_NAME_RAW,
                SKILL.VALUE::STRING as SKILL_NAME_ORIGINAL
            FROM BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED jle,
            LATERAL FLATTEN(input => jle.SOFT_SKILLS) SKILL
            WHERE jle.SOFT_SKILLS IS NOT NULL
              AND SKILL.VALUE IS NOT NULL
              AND LENGTH(TRIM(SKILL.VALUE::STRING)) > 1
              AND LOWER(TRIM(SKILL.VALUE::STRING)) NOT IN ('null', 'none', 'n/a', '')
        ),

        PRIMARY_KEYWORDS_EXPLODED AS (
            -- Extract primary keywords as skills
            SELECT
                jle.JOB_UID,
                'primary_keywords' as SKILL_SOURCE,
                'keyword' as SKILL_CATEGORY,
                TRIM(LOWER(KEYWORD.VALUE::STRING)) as SKILL_NAME_RAW,
                KEYWORD.VALUE::STRING as SKILL_NAME_ORIGINAL
            FROM BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED jle,
            LATERAL FLATTEN(input => jle.PRIMARY_KEYWORDS) KEYWORD
            WHERE jle.PRIMARY_KEYWORDS IS NOT NULL
              AND KEYWORD.VALUE IS NOT NULL
              AND LENGTH(TRIM(KEYWORD.VALUE::STRING)) > 1
              AND LOWER(TRIM(KEYWORD.VALUE::STRING)) NOT IN ('null', 'none', 'n/a', '')
        )

        SELECT * FROM TECHNICAL_SKILLS_EXPLODED
        UNION ALL
        SELECT * FROM SOFT_SKILLS_EXPLODED
        UNION ALL
        SELECT * FROM PRIMARY_KEYWORDS_EXPLODED
        """

        cursor.execute(extraction_sql)
        context.log.info("✅ Created SKILLS_RAW_EXTRACTION view")

        # Get extraction statistics
        cursor.execute("""
        SELECT
            COUNT(*) as total_skills,
            COUNT(DISTINCT JOB_UID) as unique_jobs,
            COUNT(CASE WHEN SKILL_SOURCE = 'technical_skills' THEN 1 END) as technical_count,
            COUNT(CASE WHEN SKILL_SOURCE = 'soft_skills' THEN 1 END) as soft_count,
            COUNT(CASE WHEN SKILL_SOURCE = 'primary_keywords' THEN 1 END) as keywords_count
        FROM BETTERJOBS_DB.STAGE.SKILLS_RAW_EXTRACTION
        """)

        result = cursor.fetchone()
        if result:
            stats.update({
                "skills_extracted": result[0],
                "unique_jobs_processed": result[1],
                "technical_skills_count": result[2],
                "soft_skills_count": result[3],
                "primary_keywords_count": result[4]
            })

        # Sample some data for validation
        cursor.execute("""
        SELECT SKILL_SOURCE, SKILL_CATEGORY, SKILL_NAME_ORIGINAL, COUNT(*) as frequency
        FROM BETTERJOBS_DB.STAGE.SKILLS_RAW_EXTRACTION
        GROUP BY SKILL_SOURCE, SKILL_CATEGORY, SKILL_NAME_ORIGINAL
        ORDER BY frequency DESC
        LIMIT 20
        """)

        sample_data = cursor.fetchall()
        if sample_data:
            columns = [desc[0] for desc in cursor.description]
            stats["top_skills_sample"] = [dict(zip(columns, row)) for row in sample_data]

        context.log.info(f"""
        🎯 Skills Raw Extraction Complete:
        • Total Skills Extracted: {stats['skills_extracted']:,}
        • Unique Jobs Processed: {stats['unique_jobs_processed']:,}
        • Technical Skills: {stats['technical_skills_count']:,}
        • Soft Skills: {stats['soft_skills_count']:,}
        • Primary Keywords: {stats['primary_keywords_count']:,}
        """)

        # Add metadata for Dagster UI
        context.add_output_metadata({
            "skills_extracted": MetadataValue.int(stats["skills_extracted"]),
            "unique_jobs_processed": MetadataValue.int(stats["unique_jobs_processed"]),
            "technical_skills_count": MetadataValue.int(stats["technical_skills_count"]),
            "soft_skills_count": MetadataValue.int(stats["soft_skills_count"]),
            "primary_keywords_count": MetadataValue.int(stats["primary_keywords_count"]),
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
    description="Maintain skill standardization rules and aliases",
    group_name="llm_standardization",
    kinds={"snowflake", "python", "SQL"}
)
def stage_skills_standardization_rules(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Ensure skill standardization rules table exists and has basic rules.

    Note: Rules should be managed via separate migration scripts or database initialization,
    not hardcoded in this asset. This asset only ensures the table exists and validates
    that some rules are present.
    """

    conn = snowflake.get_connection()
    cursor = None

    stats = {
        "rules_timestamp": datetime.now().isoformat(),
        "total_rules_found": 0,
        "categories_covered": 0,
        "high_confidence_rules": 0,
        "rules_table_created": False
    }

    try:
        cursor = conn.cursor()

        context.log.info("📋 Checking skill standardization rules...")

        # Create standardization rules table if it doesn't exist
        create_rules_table_sql = """
        CREATE TABLE IF NOT EXISTS BETTERJOBS_DB.STAGE.SKILL_STANDARDIZATION_RULES (
            RULE_ID STRING PRIMARY KEY,
            PATTERN STRING,                                 -- Pattern to match (regex or exact)
            STANDARDIZED_NAME STRING,                       -- Standard form
            SKILL_CATEGORY STRING,                          -- Correct category
            SKILL_SUBCATEGORY STRING,                       -- Correct subcategory
            CONFIDENCE_SCORE FLOAT DEFAULT 1.0,
            RULE_TYPE STRING DEFAULT 'exact_match',         -- exact_match, regex_pattern, fuzzy_match
            CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
            UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
        ) CLUSTER BY (SKILL_CATEGORY, PATTERN)
        """

        cursor.execute(create_rules_table_sql)
        stats["rules_table_created"] = True
        context.log.info("✅ Ensured SKILL_STANDARDIZATION_RULES table exists")

        # Check if rules exist
        cursor.execute("SELECT COUNT(*) FROM BETTERJOBS_DB.STAGE.SKILL_STANDARDIZATION_RULES")
        rule_count = cursor.fetchone()[0]

        if rule_count == 0:
            context.log.warning("""
            ⚠️  No standardization rules found in SKILL_STANDARDIZATION_RULES table.

            Please run the setup script to populate initial rules:
            pipeline/sql/llm_standardization/insert_skill_standardization_rules.sql

            Or execute the INSERT statement provided in the documentation.
            """)
        else:
            context.log.info(f"✅ Found {rule_count} existing standardization rules")

        # Get statistics on existing rules
        cursor.execute("""
        SELECT
            COUNT(*) as total_rules,
            COUNT(DISTINCT SKILL_CATEGORY) as categories,
            COUNT(CASE WHEN CONFIDENCE_SCORE >= 0.9 THEN 1 END) as high_confidence
        FROM BETTERJOBS_DB.STAGE.SKILL_STANDARDIZATION_RULES
        """)

        result = cursor.fetchone()
        if result:
            stats.update({
                "total_rules_found": result[0],
                "categories_covered": result[1],
                "high_confidence_rules": result[2]
            })

        context.log.info(f"""
        📋 Skill Standardization Rules Status:
        • Total Rules Found: {stats['total_rules_found']}
        • Categories Covered: {stats['categories_covered']}
        • High Confidence Rules: {stats['high_confidence_rules']}
        """)

        # Add metadata
        context.add_output_metadata({
            "total_rules_found": MetadataValue.int(stats["total_rules_found"]),
            "categories_covered": MetadataValue.int(stats["categories_covered"]),
            "high_confidence_rules": MetadataValue.int(stats["high_confidence_rules"]),
            "rules_table_created": MetadataValue.bool(stats["rules_table_created"])
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
    description="Create normalized skills master table with market intelligence",
    group_name="llm_standardization",
    kinds={"snowflake", "python", "SQL"}
)
def stage_skills_normalized(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Apply standardization rules and create skills master table.

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
        "avg_confidence_score": 0.0
    }

    try:
        cursor = conn.cursor()

        context.log.info("🎯 Starting skills normalization...")

        # Create skills normalized table
        create_skills_table_sql = """
        CREATE TABLE IF NOT EXISTS BETTERJOBS_DB.STAGE.SKILLS_NORMALIZED (
            SKILL_ID STRING PRIMARY KEY,
            SKILL_NAME STRING NOT NULL,                    -- Standardized skill name
            SKILL_NAME_CLEAN STRING NOT NULL,              -- Cleaned version for matching
            SKILL_NAME_ORIGINAL STRING,                    -- Most common original variant

            -- Skill Classification
            SKILL_CATEGORY STRING NOT NULL,                -- languages, databases, cloud, frameworks, tools, soft
            SKILL_SUBCATEGORY STRING,                      -- backend_language, nosql_database, public_cloud, etc.
            SKILL_FAMILY STRING,                           -- development, data, devops, etc.
            SKILL_TYPE STRING DEFAULT 'technical',         -- technical, soft, business, certification

            -- Standardization & Deduplication
            ORIGINAL_VARIANTS VARIANT,                     -- JSON array of all variations found
            COMMON_ALIASES VARIANT,                        -- JSON array of known aliases (Dead column for now)
            CANONICAL_FORM STRING,                         -- Preferred canonical name

            -- Market Data
            FREQUENCY_COUNT INTEGER DEFAULT 0,             -- How often this skill appears
            FIRST_SEEN_DATE DATE,                          -- When first detected
            LAST_SEEN_DATE DATE,                           -- Most recent occurrence
            TREND_DIRECTION STRING,                        -- rising, stable, declining

            -- Quality & Confidence
            CONFIDENCE_SCORE FLOAT DEFAULT 1.0,            -- Confidence in standardization
            MANUAL_REVIEW_FLAG BOOLEAN DEFAULT FALSE,      -- Needs human review
            APPROVED_BY_ADMIN BOOLEAN DEFAULT FALSE,       -- Admin approved

            -- Audit Fields
            CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
            UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
            CREATED_BY STRING DEFAULT 'system'
        ) CLUSTER BY (SKILL_CATEGORY, SKILL_NAME)
        """

        cursor.execute(create_skills_table_sql)
        context.log.info("✅ Ensured SKILLS_NORMALIZED table exists")

        # Check if skill family mapping table exists
        cursor.execute("""
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'STAGE'
          AND TABLE_NAME = 'SKILL_FAMILY_MAPPING'
          AND TABLE_CATALOG = 'BETTERJOBS_DB'
        """)

        family_table_exists = cursor.fetchone()[0] > 0

        # Create skill family mapping table if needed
        create_family_mapping_sql = """
        CREATE TABLE IF NOT EXISTS BETTERJOBS_DB.STAGE.SKILL_FAMILY_MAPPING (
            MAPPING_ID STRING PRIMARY KEY,
            SKILL_CATEGORY STRING NOT NULL,             -- Category to map from
            SKILL_FAMILY STRING NOT NULL,               -- Family to map to
            FAMILY_DESCRIPTION STRING,                  -- Description of the family
            PRIORITY INTEGER DEFAULT 1,                 -- Priority for overlapping mappings
            IS_ACTIVE BOOLEAN DEFAULT TRUE,             -- Enable/disable mapping
            CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
            UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
        ) CLUSTER BY (SKILL_CATEGORY, IS_ACTIVE)
        """

        cursor.execute(create_family_mapping_sql)

        if family_table_exists:
            context.log.info("✅ SKILL_FAMILY_MAPPING table already exists")
        else:
            context.log.info("✅ Created SKILL_FAMILY_MAPPING table")

        # Check if family mappings exist
        cursor.execute("SELECT COUNT(*) FROM BETTERJOBS_DB.STAGE.SKILL_FAMILY_MAPPING")
        mapping_count = cursor.fetchone()[0]

        if mapping_count == 0:
            context.log.warning("""
            ⚠️  No skill family mappings found in SKILL_FAMILY_MAPPING table.

            Please run the setup script to populate mappings:
            pipeline/sql/llm_standardization/insert_skill_family_mappings.sql

            Family mappings are required for proper skill categorization.
            """)
        else:
            context.log.info(f"✅ Found {mapping_count} existing skill family mappings")

        # Clear existing data for fresh normalization
        cursor.execute("DELETE FROM BETTERJOBS_DB.STAGE.SKILLS_NORMALIZED")

        # Normalize and populate skills
        normalization_sql = """
        INSERT INTO BETTERJOBS_DB.STAGE.SKILLS_NORMALIZED (
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
        WITH skill_aggregation AS (
            SELECT
                COALESCE(sr.STANDARDIZED_NAME, sre.SKILL_NAME_ORIGINAL) as skill_name,
                COALESCE(sr.SKILL_CATEGORY, sre.SKILL_CATEGORY) as skill_category,
                COALESCE(sr.SKILL_SUBCATEGORY, 'uncategorized') as skill_subcategory,
                COALESCE(sfm.SKILL_FAMILY, 'general') as skill_family,
                CASE
                    WHEN sre.SKILL_CATEGORY IN ('soft') THEN 'soft'
                    WHEN sre.SKILL_CATEGORY IN ('keyword') THEN 'business'
                    ELSE 'technical'
                END as skill_type,
                ARRAY_AGG(DISTINCT sre.SKILL_NAME_ORIGINAL) as original_variants,
                COUNT(*) as frequency_count,
                MIN(ju.DATE_RETRIEVED::DATE) as first_seen_date,
                MAX(ju.DATE_RETRIEVED::DATE) as last_seen_date,
                AVG(COALESCE(sr.CONFIDENCE_SCORE, 0.5)) as confidence_score
            FROM BETTERJOBS_DB.STAGE.SKILLS_RAW_EXTRACTION sre
            LEFT JOIN BETTERJOBS_DB.STAGE.SKILL_STANDARDIZATION_RULES sr
                ON LOWER(sre.SKILL_NAME_RAW) = LOWER(sr.PATTERN)
            LEFT JOIN BETTERJOBS_DB.STAGE.SKILL_FAMILY_MAPPING sfm
                ON sre.SKILL_CATEGORY = sfm.SKILL_CATEGORY AND sfm.IS_ACTIVE = TRUE
            JOIN BETTERJOBS_DB.STAGE.JOBS_UNIFIED ju ON sre.JOB_UID = ju.JOB_UID
            WHERE LENGTH(sre.SKILL_NAME_RAW) >= 2  -- Filter out single characters
              AND ju.IS_ENGLISH = TRUE            -- Only English jobs
            GROUP BY 1, 2, 3, 4, 5
            HAVING COUNT(*) >= 2  -- Only include skills appearing at least twice
        )
        SELECT
            CONCAT('skill_', ROW_NUMBER() OVER (ORDER BY frequency_count DESC)) as skill_id,
            skill_name,
            LOWER(TRIM(skill_name)) as skill_name_clean,
            original_variants[0]::STRING as skill_name_original,
            skill_category,
            skill_subcategory,
            skill_family,
            skill_type,
            original_variants,
            frequency_count,
            first_seen_date,
            last_seen_date,
            confidence_score,
            CASE WHEN confidence_score < 0.6 THEN TRUE ELSE FALSE END as manual_review_flag,
            skill_name as canonical_form
        FROM skill_aggregation
        """

        cursor.execute(normalization_sql)

        # Get normalization statistics
        cursor.execute("""
        SELECT
            COUNT(*) as total_skills,
            COUNT(CASE WHEN CONFIDENCE_SCORE >= 0.8 THEN 1 END) as high_confidence,
            COUNT(CASE WHEN CONFIDENCE_SCORE < 0.5 THEN 1 END) as low_confidence,
            COUNT(DISTINCT SKILL_CATEGORY) as unique_categories,
            AVG(CONFIDENCE_SCORE) as avg_confidence
        FROM BETTERJOBS_DB.STAGE.SKILLS_NORMALIZED
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
        cursor.execute("""
        SELECT SKILL_CATEGORY, COUNT(*) as skill_count
        FROM BETTERJOBS_DB.STAGE.SKILLS_NORMALIZED
        GROUP BY SKILL_CATEGORY
        ORDER BY skill_count DESC
        """)

        category_results = cursor.fetchall()
        if category_results:
            columns = [desc[0] for desc in cursor.description]
            stats["category_breakdown"] = [dict(zip(columns, row)) for row in category_results]

        context.log.info(f"""
        🎯 Skills Normalization Complete:
        • Skills Normalized: {stats['skills_normalized']:,}
        • High Confidence: {stats['high_confidence_skills']:,}
        • Low Confidence: {stats['low_confidence_skills']:,}
        • Unique Categories: {stats['unique_skill_categories']}
        • Average Confidence: {stats['avg_confidence_score']:.3f}
        """)

        # Add metadata
        context.add_output_metadata({
            "skills_normalized": MetadataValue.int(stats["skills_normalized"]),
            "high_confidence_skills": MetadataValue.int(stats["high_confidence_skills"]),
            "low_confidence_skills": MetadataValue.int(stats["low_confidence_skills"]),
            "unique_skill_categories": MetadataValue.int(stats["unique_skill_categories"]),
            "avg_confidence_score": MetadataValue.float(stats["avg_confidence_score"]),
            "category_breakdown": MetadataValue.json(stats.get("category_breakdown", []))
        })

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
    description="Create job-skill relationships with context tracking",
    group_name="llm_standardization",
    kinds={"snowflake", "python", "SQL"}
)
def stage_job_skills_bridge(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Map jobs to normalized skills with rich context.

    Features:
    - Source tracking (technical_skills vs soft_skills vs keywords)
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
        "high_confidence_relationships": 0
    }

    try:
        cursor = conn.cursor()

        context.log.info("🔗 Creating job-skills bridge relationships...")

        # Create job skills bridge table
        create_bridge_table_sql = """
        CREATE TABLE IF NOT EXISTS BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE (
            BRIDGE_ID STRING PRIMARY KEY,
            JOB_UID STRING NOT NULL,                       -- FK to JOBS_UNIFIED
            SKILL_ID STRING NOT NULL,                      -- FK to SKILLS_NORMALIZED

            -- Source Information
            SKILL_SOURCE STRING NOT NULL,                  -- 'technical_skills', 'soft_skills', 'primary_keywords'
            SKILL_CATEGORY STRING NOT NULL,                -- Denormalized for performance
            ORIGINAL_TEXT STRING,                          -- Original text from LLM

            -- Confidence & Quality
            EXTRACTION_CONFIDENCE FLOAT,                   -- LLM extraction confidence
            STANDARDIZATION_CONFIDENCE FLOAT,              -- Skill matching confidence
            OVERALL_CONFIDENCE FLOAT,                      -- Combined confidence score

            -- Context
            SKILL_CONTEXT STRING,                          -- required, preferred, nice-to-have


            -- Processing Metadata
            PROCESSING_METHOD STRING DEFAULT 'llm_auto',   -- llm_auto, manual_override, admin_correction
            NEEDS_REVIEW BOOLEAN DEFAULT FALSE,

            -- Audit Fields
            CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
            CREATED_BY STRING DEFAULT 'system'
        ) CLUSTER BY (JOB_UID, SKILL_CATEGORY)
        """

        cursor.execute(create_bridge_table_sql)
        context.log.info("✅ Created JOB_SKILLS_BRIDGE table")

        # Clear existing relationships for fresh creation
        cursor.execute("DELETE FROM BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE")

        # Create job-skill relationships - Simplified approach
        bridge_sql = """
        INSERT INTO BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE (
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
            FROM BETTERJOBS_DB.STAGE.SKILLS_RAW_EXTRACTION sre
            JOIN BETTERJOBS_DB.STAGE.SKILL_STANDARDIZATION_RULES sr
                ON LOWER(sre.SKILL_NAME_RAW) = LOWER(sr.PATTERN)
            JOIN BETTERJOBS_DB.STAGE.SKILLS_NORMALIZED sn
                ON sn.SKILL_NAME = sr.STANDARDIZED_NAME
            LEFT JOIN BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED lle
                ON sre.JOB_UID = lle.JOB_UID
            JOIN BETTERJOBS_DB.STAGE.JOBS_UNIFIED ju
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
            FROM BETTERJOBS_DB.STAGE.SKILLS_RAW_EXTRACTION sre
            JOIN BETTERJOBS_DB.STAGE.SKILLS_NORMALIZED sn
                ON sn.SKILL_NAME = sre.SKILL_NAME_ORIGINAL
            LEFT JOIN BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED lle
                ON sre.JOB_UID = lle.JOB_UID
            JOIN BETTERJOBS_DB.STAGE.JOBS_UNIFIED ju
                ON sre.JOB_UID = ju.JOB_UID
            LEFT JOIN BETTERJOBS_DB.STAGE.SKILL_STANDARDIZATION_RULES sr
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
                WHEN SKILL_SOURCE = 'primary_keywords' THEN 'context'
                ELSE 'unknown'
            END as skill_context,

            'llm_auto' as processing_method,
            CASE WHEN standardization_confidence < 0.6 THEN TRUE ELSE FALSE END as needs_review

        FROM skill_matches
        """

        cursor.execute(bridge_sql)

        # Get bridge statistics
        cursor.execute("""
        SELECT
            COUNT(*) as total_relationships,
            COUNT(DISTINCT JOB_UID) as unique_jobs,
            COUNT(DISTINCT SKILL_ID) as unique_skills,
            COUNT(CASE WHEN OVERALL_CONFIDENCE >= 0.8 THEN 1 END) as high_confidence
        FROM BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE
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
        cursor.execute("""
        SELECT AVG(skill_count) as avg_skills_per_job
        FROM (
            SELECT JOB_UID, COUNT(*) as skill_count
            FROM BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE
            GROUP BY JOB_UID
        ) job_skill_counts
        """)

        avg_result = cursor.fetchone()
        if avg_result and avg_result[0]:
            stats["avg_skills_per_job"] = float(avg_result[0])

        # Get source breakdown
        cursor.execute("""
        SELECT
            SKILL_SOURCE,
            COUNT(*) as relationship_count,
            COUNT(DISTINCT JOB_UID) as jobs_count,
            AVG(OVERALL_CONFIDENCE) as avg_confidence
        FROM BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE
        GROUP BY SKILL_SOURCE
        ORDER BY relationship_count DESC
        """)

        source_results = cursor.fetchall()
        if source_results:
            columns = [desc[0] for desc in cursor.description]
            stats["source_breakdown"] = [dict(zip(columns, row)) for row in source_results]

        # Add foreign key constraints (best effort - may fail if constraint already exists)
        try:
            cursor.execute("""
            ALTER TABLE BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE
            ADD CONSTRAINT FK_JOB_SKILLS_JOB_UID
            FOREIGN KEY (JOB_UID) REFERENCES BETTERJOBS_DB.STAGE.JOBS_UNIFIED(JOB_UID)
            """)

            cursor.execute("""
            ALTER TABLE BETTERJOBS_DB.STAGE.JOB_SKILLS_BRIDGE
            ADD CONSTRAINT FK_JOB_SKILLS_SKILL_ID
            FOREIGN KEY (SKILL_ID) REFERENCES BETTERJOBS_DB.STAGE.SKILLS_NORMALIZED(SKILL_ID)
            """)

            context.log.info("✅ Added foreign key constraints")

        except Exception as fk_error:
            context.log.warning(f"Foreign key constraints may already exist: {fk_error}")

        context.log.info(f"""
        🔗 Job-Skills Bridge Complete:
        • Total Relationships: {stats['total_relationships']:,}
        • Unique Jobs with Skills: {stats['unique_jobs_with_skills']:,}
        • Unique Skills Used: {stats['unique_skills_used']:,}
        • Average Skills per Job: {stats['avg_skills_per_job']:.1f}
        • High Confidence Relationships: {stats['high_confidence_relationships']:,}
        """)

        # Add metadata
        context.add_output_metadata({
            "total_relationships": MetadataValue.int(stats["total_relationships"]),
            "unique_jobs_with_skills": MetadataValue.int(stats["unique_jobs_with_skills"]),
            "unique_skills_used": MetadataValue.int(stats["unique_skills_used"]),
            "avg_skills_per_job": MetadataValue.float(stats["avg_skills_per_job"]),
            "high_confidence_relationships": MetadataValue.int(stats["high_confidence_relationships"]),
            "source_breakdown": MetadataValue.json(stats.get("source_breakdown", []))
        })

        return stats

    except Exception as e:
        context.log.error(f"❌ Job-skills bridge creation failed: {str(e)}")
        stats["error_message"] = str(e)
        raise

    finally:
        if cursor:
            cursor.close()