"""
Phase 2: Keywords Normalization Assets

Assets for extracting, standardizing, and normalizing keywords data from LLM-enriched
VARIANT columns into proper relational structures for analytics.

Dependencies:
- stage_jobs_llm_enriched_unified: Source of LLM-extracted keywords data
- stage_jobs_unified: Source of job metadata

Output Tables:
- KEYWORDS_NORMALIZED: Master keywords table with standardized names
- JOB_KEYWORDS_BRIDGE: Many-to-many job-keyword relationships

Keyword Sources Processed:
- industry_keywords: Industry classification terms (arrays)
- role_type: Role hierarchy classification (single string values)

Note: primary_keywords are processed in Phase 1 as they overlap with technical skills.
This phase focuses on industry_keywords and role_type classification.
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
    description="Extract and flatten keywords from LLM VARIANT columns",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"}
)
def stage_llm_keywords_raw_extraction(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Extract all keywords from VARIANT columns and flatten into workable format.

    Processes:
    - industry_keywords: Business domain and industry classification terms from arrays
    - role_type: Role hierarchy classification from single string values

    Note: primary_keywords are handled in Phase 1 skills normalization

    Output: Raw keywords with source tracking and frequency metrics
    """

    conn = snowflake.get_connection()
    cursor = None

    stats = {
        "extraction_timestamp": datetime.now().isoformat(),
        "keywords_extracted": 0,
        "industry_keywords_count": 0,
        "role_type_keywords_count": 0,
        "unique_jobs_processed": 0,
        "extraction_errors": 0
    }

    try:
        cursor = conn.cursor()

        context.log.info("🔍 Starting LLM keywords raw extraction...")

        # Create or replace the raw extraction view
        extraction_sql = """
        CREATE OR REPLACE VIEW BETTERJOBS_DB.STAGE.KEYWORDS_RAW_EXTRACTION AS
        WITH INDUSTRY_KEYWORDS_EXPLODED AS (
            -- Extract industry classification keywords from array
            SELECT
                jle.JOB_UID,
                'industry_keywords' as KEYWORD_SOURCE,
                'industry' as KEYWORD_TYPE,
                TRIM(LOWER(KEYWORD.VALUE::STRING)) as KEYWORD_TEXT_RAW,
                KEYWORD.VALUE::STRING as KEYWORD_TEXT_ORIGINAL
            FROM BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED jle,
            LATERAL FLATTEN(input => jle.INDUSTRY_KEYWORDS) KEYWORD
            WHERE jle.INDUSTRY_KEYWORDS IS NOT NULL
              AND IS_ARRAY(jle.INDUSTRY_KEYWORDS)
              AND ARRAY_SIZE(jle.INDUSTRY_KEYWORDS) > 0
              AND KEYWORD.VALUE IS NOT NULL
              AND LENGTH(TRIM(KEYWORD.VALUE::STRING)) > 1
              AND LOWER(TRIM(KEYWORD.VALUE::STRING)) NOT IN ('null', 'none', 'n/a', '')
        ),

        ROLE_TYPE_EXPLODED AS (
            -- Extract role type from single string value (not an array)
            SELECT
                jle.JOB_UID,
                'role_type' as KEYWORD_SOURCE,
                'role_type' as KEYWORD_TYPE,
                TRIM(LOWER(jle.ROLE_TYPE)) as KEYWORD_TEXT_RAW,
                jle.ROLE_TYPE as KEYWORD_TEXT_ORIGINAL
            FROM BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED jle
            WHERE jle.ROLE_TYPE IS NOT NULL
              AND LENGTH(TRIM(jle.ROLE_TYPE)) > 1
              AND LOWER(TRIM(jle.ROLE_TYPE)) NOT IN ('null', 'none', 'n/a', '')
        )

        SELECT * FROM INDUSTRY_KEYWORDS_EXPLODED
        UNION ALL
        SELECT * FROM ROLE_TYPE_EXPLODED
        """

        cursor.execute(extraction_sql)
        context.log.info("✅ Created KEYWORDS_RAW_EXTRACTION view")

        # Get extraction statistics
        cursor.execute("""
        SELECT
            COUNT(*) as total_keywords,
            COUNT(DISTINCT JOB_UID) as unique_jobs,
            COUNT(CASE WHEN KEYWORD_SOURCE = 'industry_keywords' THEN 1 END) as industry_count,
            COUNT(CASE WHEN KEYWORD_SOURCE = 'role_type' THEN 1 END) as role_type_count
        FROM BETTERJOBS_DB.STAGE.KEYWORDS_RAW_EXTRACTION
        """)

        result = cursor.fetchone()
        if result:
            stats.update({
                "keywords_extracted": result[0],
                "unique_jobs_processed": result[1],
                "industry_keywords_count": result[2],
                "role_type_keywords_count": result[3]
            })

        # Sample some data for validation
        cursor.execute("""
        SELECT KEYWORD_SOURCE, KEYWORD_TYPE, KEYWORD_TEXT_ORIGINAL, COUNT(*) as frequency
        FROM BETTERJOBS_DB.STAGE.KEYWORDS_RAW_EXTRACTION
        GROUP BY KEYWORD_SOURCE, KEYWORD_TYPE, KEYWORD_TEXT_ORIGINAL
        ORDER BY frequency DESC
        LIMIT 20
        """)

        sample_data = cursor.fetchall()
        if sample_data:
            columns = [desc[0] for desc in cursor.description]
            stats["top_keywords_sample"] = [dict(zip(columns, row)) for row in sample_data]

        context.log.info(f"""
        🎯 Keywords Raw Extraction Complete:
        • Total Keywords Extracted: {stats['keywords_extracted']:,}
        • Unique Jobs Processed: {stats['unique_jobs_processed']:,}
        • Industry Keywords: {stats['industry_keywords_count']:,}
        • Role Type Keywords: {stats['role_type_keywords_count']:,}
        """)

        # Add metadata for Dagster UI
        context.add_output_metadata({
            "keywords_extracted": MetadataValue.int(stats["keywords_extracted"]),
            "unique_jobs_processed": MetadataValue.int(stats["unique_jobs_processed"]),
            "industry_keywords_count": MetadataValue.int(stats["industry_keywords_count"]),
            "role_type_keywords_count": MetadataValue.int(stats["role_type_keywords_count"]),
            "top_keywords_sample": MetadataValue.json(stats.get("top_keywords_sample", []))
        })

        return stats

    except Exception as e:
        context.log.error(f"❌ Keywords raw extraction failed: {str(e)}")
        stats["extraction_errors"] = 1
        stats["error_message"] = str(e)
        raise

    finally:
        if cursor:
            cursor.close()


@asset(
    description="Maintain keyword standardization rules and aliases",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"}
)
def stage_keywords_standardization_rules(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Create and maintain comprehensive keyword standardization rules.

    Features:
    - Industry keyword standardization (FinTech -> Financial Technology)
    - Role type normalization (IC -> Individual Contributor)
    - Common abbreviation expansions
    - Confidence scoring based on pattern matching
    - Manual override support
    """

    conn = snowflake.get_connection()
    cursor = None

    stats = {
        "update_timestamp": datetime.now().isoformat(),
        "rules_loaded": 0,
        "rules_errors": 0
    }

    try:
        cursor = conn.cursor()

        context.log.info("🔧 Setting up keyword standardization rules...")

        # Create keyword standardization rules table if not exists
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS BETTERJOBS_DB.STAGE.KEYWORD_STANDARDIZATION_RULES (
            RULE_ID STRING PRIMARY KEY,
            PATTERN STRING NOT NULL,                         -- Pattern to match (regex or exact)
            STANDARDIZED_TEXT STRING NOT NULL,               -- Standard form
            KEYWORD_TYPE STRING NOT NULL,                    -- industry, role_type, company_stage, etc.
            KEYWORD_CATEGORY STRING,                         -- specific category within type
            CONFIDENCE_SCORE FLOAT DEFAULT 1.0,
            RULE_TYPE STRING DEFAULT 'exact_match',          -- exact_match, regex_pattern, fuzzy_match
            IS_ACTIVE BOOLEAN DEFAULT TRUE,
            CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
            UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
        ) CLUSTER BY (KEYWORD_TYPE, IS_ACTIVE)
        """

        cursor.execute(create_table_sql)
        context.log.info("✅ Created/verified KEYWORD_STANDARDIZATION_RULES table")

        # Load standardization rules from SQL file
        # This will be populated from the SQL configuration files
        context.log.info("📋 Keyword standardization rules table ready for configuration")

        # Get current rule count
        cursor.execute("""
        SELECT
            COUNT(*) as total_rules,
            COUNT(CASE WHEN IS_ACTIVE THEN 1 END) as active_rules,
            COUNT(DISTINCT KEYWORD_TYPE) as types_covered
        FROM BETTERJOBS_DB.STAGE.KEYWORD_STANDARDIZATION_RULES
        """)

        result = cursor.fetchone()
        if result:
            stats.update({
                "rules_loaded": result[0],
                "active_rules": result[1],
                "types_covered": result[2]
            })

        context.log.info(f"""
        🎯 Keyword Standardization Rules Status:
        • Total Rules: {stats['rules_loaded']:,}
        • Active Rules: {stats.get('active_rules', 0):,}
        • Types Covered: {stats.get('types_covered', 0):,}
        """)

        # Add metadata for Dagster UI
        context.add_output_metadata({
            "rules_loaded": MetadataValue.int(stats["rules_loaded"]),
            "active_rules": MetadataValue.int(stats.get("active_rules", 0)),
            "types_covered": MetadataValue.int(stats.get("types_covered", 0))
        })

        return stats

    except Exception as e:
        context.log.error(f"❌ Keyword standardization rules setup failed: {str(e)}")
        stats["rules_errors"] = 1
        stats["error_message"] = str(e)
        raise

    finally:
        if cursor:
            cursor.close()


@asset(
    description="Manage keyword type and category classifications",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"}
)
def stage_keyword_type_mapping(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Create and maintain keyword type and category classifications.

    Classification Categories:
    - Industry Types: technology, healthcare, finance, retail, manufacturing
    - Company Stage: startup, growth, enterprise, public, non_profit
    - Role Hierarchy: individual_contributor, manager, director, executive
    - Function Types: engineering, sales, marketing, operations, support
    - Work Style: remote_friendly, hybrid, on_site, distributed
    """

    conn = snowflake.get_connection()
    cursor = None

    stats = {
        "update_timestamp": datetime.now().isoformat(),
        "mappings_loaded": 0,
        "mapping_errors": 0
    }

    try:
        cursor = conn.cursor()

        context.log.info("🗂️ Setting up keyword type mappings...")

        # Create keyword type mapping table if not exists
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS BETTERJOBS_DB.STAGE.KEYWORD_TYPE_MAPPING (
            MAPPING_ID STRING PRIMARY KEY,
            KEYWORD_TEXT STRING NOT NULL,                    -- Keyword to classify
            KEYWORD_TYPE STRING NOT NULL,                    -- industry, role_type, company_stage, etc.
            KEYWORD_CATEGORY STRING NOT NULL,                -- specific category within type
            CATEGORY_DESCRIPTION STRING,                     -- Human-readable description
            CONFIDENCE_SCORE FLOAT DEFAULT 1.0,             -- Confidence in classification
            BUSINESS_RELEVANCE STRING DEFAULT 'medium',     -- high, medium, low
            IS_ACTIVE BOOLEAN DEFAULT TRUE,
            CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
            UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
        ) CLUSTER BY (KEYWORD_TYPE, IS_ACTIVE)
        """

        cursor.execute(create_table_sql)
        context.log.info("✅ Created/verified KEYWORD_TYPE_MAPPING table")

        # Load type mappings from SQL file
        # This will be populated from the SQL configuration files
        context.log.info("📋 Keyword type mapping table ready for configuration")

        # Get current mapping count
        cursor.execute("""
        SELECT
            COUNT(*) as total_mappings,
            COUNT(CASE WHEN IS_ACTIVE THEN 1 END) as active_mappings,
            COUNT(DISTINCT KEYWORD_TYPE) as types_defined,
            COUNT(DISTINCT KEYWORD_CATEGORY) as categories_defined
        FROM BETTERJOBS_DB.STAGE.KEYWORD_TYPE_MAPPING
        """)

        result = cursor.fetchone()
        if result:
            stats.update({
                "mappings_loaded": result[0],
                "active_mappings": result[1],
                "types_defined": result[2],
                "categories_defined": result[3]
            })

        context.log.info(f"""
        🎯 Keyword Type Mapping Status:
        • Total Mappings: {stats['mappings_loaded']:,}
        • Active Mappings: {stats.get('active_mappings', 0):,}
        • Types Defined: {stats.get('types_defined', 0):,}
        • Categories Defined: {stats.get('categories_defined', 0):,}
        """)

        # Add metadata for Dagster UI
        context.add_output_metadata({
            "mappings_loaded": MetadataValue.int(stats["mappings_loaded"]),
            "active_mappings": MetadataValue.int(stats.get("active_mappings", 0)),
            "types_defined": MetadataValue.int(stats.get("types_defined", 0)),
            "categories_defined": MetadataValue.int(stats.get("categories_defined", 0))
        })

        return stats

    except Exception as e:
        context.log.error(f"❌ Keyword type mapping setup failed: {str(e)}")
        stats["mapping_errors"] = 1
        stats["error_message"] = str(e)
        raise

    finally:
        if cursor:
            cursor.close()


@asset(
    deps=["stage_llm_keywords_raw_extraction", "stage_keywords_standardization_rules", "stage_keyword_type_mapping"],
    description="Create normalized keywords master table with market intelligence",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"}
)
def stage_keywords_normalized(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Apply standardization rules and create keywords master table.

    Processing:
    - Apply standardization rules with confidence scoring
    - Deduplicate keyword variations
    - Calculate frequency and trend metrics
    - Classify keywords by type and category
    - Flag low-confidence items for manual review

    Output: Clean keywords master table for analytics
    """

    conn = snowflake.get_connection()
    cursor = None

    stats = {
        "processing_timestamp": datetime.now().isoformat(),
        "keywords_normalized": 0,
        "industry_keywords": 0,
        "role_type_keywords": 0,
        "high_confidence_keywords": 0,
        "low_confidence_keywords": 0,
        "processing_errors": 0
    }

    try:
        cursor = conn.cursor()

        context.log.info("🔄 Starting keywords normalization process...")

        # Clear existing normalized data to rebuild
        cursor.execute("DELETE FROM BETTERJOBS_DB.STAGE.KEYWORDS_NORMALIZED")
        context.log.info("🗑️ Cleared existing KEYWORDS_NORMALIZED data")

        # Normalize and populate keywords
        normalization_sql = """
        INSERT INTO BETTERJOBS_DB.STAGE.KEYWORDS_NORMALIZED (
            KEYWORD_ID,
            KEYWORD_TEXT,
            KEYWORD_TEXT_CLEAN,
            KEYWORD_TYPE,
            KEYWORD_CATEGORY,
            ORIGINAL_VARIANTS,
            CANONICAL_FORM,
            FREQUENCY_COUNT,
            TREND_SCORE,
            CONFIDENCE_SCORE,
            APPROVED_BY_ADMIN,
            CREATED_TIMESTAMP,
            UPDATED_TIMESTAMP
        )
        WITH keyword_aggregation AS (
            SELECT
                -- Apply standardization rules or use original text
                COALESCE(ksr.STANDARDIZED_TEXT, kre.KEYWORD_TEXT_ORIGINAL) as keyword_text,

                -- Determine keyword type and category
                COALESCE(ksr.KEYWORD_TYPE, ktm.KEYWORD_TYPE, kre.KEYWORD_TYPE) as keyword_type,
                COALESCE(ksr.KEYWORD_CATEGORY, ktm.KEYWORD_CATEGORY, 'uncategorized') as keyword_category,

                -- Aggregate variants
                ARRAY_AGG(DISTINCT kre.KEYWORD_TEXT_ORIGINAL) as original_variants,

                -- Calculate frequency metrics
                COUNT(*) as frequency_count,
                COUNT(DISTINCT kre.JOB_UID) as unique_job_count,

                -- Calculate confidence score
                AVG(COALESCE(ksr.CONFIDENCE_SCORE, ktm.CONFIDENCE_SCORE, 0.5)) as confidence_score,

                -- Business relevance
                MAX(COALESCE(ktm.BUSINESS_RELEVANCE, 'medium')) as business_relevance

            FROM BETTERJOBS_DB.STAGE.KEYWORDS_RAW_EXTRACTION kre

            -- Left join with standardization rules
            LEFT JOIN BETTERJOBS_DB.STAGE.KEYWORD_STANDARDIZATION_RULES ksr
                ON LOWER(kre.KEYWORD_TEXT_RAW) = LOWER(ksr.PATTERN)
                AND ksr.IS_ACTIVE = TRUE

            -- Left join with type mappings
            LEFT JOIN BETTERJOBS_DB.STAGE.KEYWORD_TYPE_MAPPING ktm
                ON LOWER(kre.KEYWORD_TEXT_RAW) = LOWER(ktm.KEYWORD_TEXT)
                AND ktm.IS_ACTIVE = TRUE

            WHERE LENGTH(kre.KEYWORD_TEXT_RAW) >= 2  -- Filter out single characters

            GROUP BY 1, 2, 3
            HAVING COUNT(*) >= 2  -- Only include keywords appearing at least twice
        )

        SELECT
            CONCAT('keyword_', ROW_NUMBER() OVER (ORDER BY frequency_count DESC)) as keyword_id,
            keyword_text,
            LOWER(TRIM(keyword_text)) as keyword_text_clean,
            keyword_type,
            keyword_category,
            original_variants,
            keyword_text as canonical_form,  -- Use standardized text as canonical form
            frequency_count,
            -- Simple trend score based on frequency (can be enhanced later)
            CASE
                WHEN frequency_count >= 100 THEN 1.0
                WHEN frequency_count >= 50 THEN 0.7
                WHEN frequency_count >= 10 THEN 0.5
                ELSE 0.3
            END as trend_score,
            confidence_score,
            CASE WHEN confidence_score >= 0.8 THEN TRUE ELSE FALSE END as approved_by_admin,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        FROM keyword_aggregation
        ORDER BY frequency_count DESC
        """

        cursor.execute(normalization_sql)
        keywords_inserted = cursor.rowcount
        context.log.info(f"✅ Inserted {keywords_inserted:,} normalized keywords")

        # Get normalization statistics
        cursor.execute("""
        SELECT
            COUNT(*) as total_keywords,
            COUNT(CASE WHEN KEYWORD_TYPE = 'industry' THEN 1 END) as industry_count,
            COUNT(CASE WHEN KEYWORD_TYPE = 'role_type' THEN 1 END) as role_type_count,
            COUNT(CASE WHEN CONFIDENCE_SCORE >= 0.8 THEN 1 END) as high_confidence,
            COUNT(CASE WHEN CONFIDENCE_SCORE < 0.5 THEN 1 END) as low_confidence,
            AVG(CONFIDENCE_SCORE) as avg_confidence,
            AVG(FREQUENCY_COUNT) as avg_frequency
        FROM BETTERJOBS_DB.STAGE.KEYWORDS_NORMALIZED
        """)

        result = cursor.fetchone()
        if result:
            stats.update({
                "keywords_normalized": result[0],
                "industry_keywords": result[1],
                "role_type_keywords": result[2],
                "high_confidence_keywords": result[3],
                "low_confidence_keywords": result[4],
                "avg_confidence_score": float(result[5]) if result[5] else 0.0,
                "avg_frequency": float(result[6]) if result[6] else 0.0
            })

        # Sample normalized keywords for validation
        cursor.execute("""
        SELECT KEYWORD_TYPE, KEYWORD_TEXT, FREQUENCY_COUNT, CONFIDENCE_SCORE
        FROM BETTERJOBS_DB.STAGE.KEYWORDS_NORMALIZED
        ORDER BY FREQUENCY_COUNT DESC
        LIMIT 15
        """)

        sample_data = cursor.fetchall()
        if sample_data:
            columns = [desc[0] for desc in cursor.description]
            stats["top_normalized_keywords"] = [dict(zip(columns, row)) for row in sample_data]

        context.log.info(f"""
        🎯 Keywords Normalization Complete:
        • Keywords Normalized: {stats['keywords_normalized']:,}
        • Industry Keywords: {stats['industry_keywords']:,}
        • Role Type Keywords: {stats['role_type_keywords']:,}
        • High Confidence: {stats['high_confidence_keywords']:,}
        • Low Confidence: {stats['low_confidence_keywords']:,}
        • Average Confidence: {stats.get('avg_confidence_score', 0):.3f}
        • Average Frequency: {stats.get('avg_frequency', 0):.1f}
        """)

        # Add metadata for Dagster UI
        context.add_output_metadata({
            "keywords_normalized": MetadataValue.int(stats["keywords_normalized"]),
            "industry_keywords": MetadataValue.int(stats["industry_keywords"]),
            "role_type_keywords": MetadataValue.int(stats["role_type_keywords"]),
            "high_confidence_keywords": MetadataValue.int(stats["high_confidence_keywords"]),
            "low_confidence_keywords": MetadataValue.int(stats["low_confidence_keywords"]),
            "avg_confidence_score": MetadataValue.float(stats.get("avg_confidence_score", 0.0)),
            "avg_frequency": MetadataValue.float(stats.get("avg_frequency", 0.0)),
            "top_normalized_keywords": MetadataValue.json(stats.get("top_normalized_keywords", []))
        })

        return stats

    except Exception as e:
        context.log.error(f"❌ Keywords normalization failed: {str(e)}")
        stats["processing_errors"] = 1
        stats["error_message"] = str(e)
        raise

    finally:
        if cursor:
            cursor.close()


@asset(
    deps=["stage_keywords_normalized", "stage_jobs_unified"],
    description="Create job-keyword relationships with context tracking",
    group_name="2b_stage_llm_standardization_validation",
    kinds={"snowflake", "python", "SQL"}
)
def stage_job_keywords_bridge(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Map jobs to normalized keywords with rich context.

    Features:
    - Source tracking (industry_keywords vs role_type_keywords)
    - Context classification (primary vs secondary relevance)
    - Business relevance scoring
    - Confidence scoring for keyword-job associations
    - Processing method tracking
    """

    conn = snowflake.get_connection()
    cursor = None

    stats = {
        "processing_timestamp": datetime.now().isoformat(),
        "relationships_created": 0,
        "industry_relationships": 0,
        "role_type_relationships": 0,
        "high_confidence_relationships": 0,
        "unique_jobs_with_keywords": 0,
        "processing_errors": 0
    }

    try:
        cursor = conn.cursor()

        context.log.info("🔗 Starting job-keywords bridge creation...")

        # Clear existing bridge data to rebuild
        cursor.execute("DELETE FROM BETTERJOBS_DB.STAGE.JOB_KEYWORDS_BRIDGE")
        context.log.info("🗑️ Cleared existing JOB_KEYWORDS_BRIDGE data")

        # Create job-keyword relationships - Following skills bridge pattern
        bridge_sql = """
        INSERT INTO BETTERJOBS_DB.STAGE.JOB_KEYWORDS_BRIDGE (
            BRIDGE_ID,
            JOB_UID,
            KEYWORD_ID,
            KEYWORD_SOURCE,
            ORIGINAL_TEXT,
            EXTRACTION_CONFIDENCE,
            STANDARDIZATION_CONFIDENCE,
            OVERALL_CONFIDENCE,
            CREATED_TIMESTAMP
        )
        WITH keyword_matches AS (
            -- First pass: Match keywords that were standardized via rules
            SELECT
                kre.JOB_UID,
                kn.KEYWORD_ID,
                kre.KEYWORD_SOURCE,
                kre.KEYWORD_TEXT_ORIGINAL,
                COALESCE(lle.LLM_OVERALL_CONFIDENCE, 0.8) as extraction_confidence,
                kn.CONFIDENCE_SCORE as standardization_confidence,
                kr.PATTERN as matched_pattern
            FROM BETTERJOBS_DB.STAGE.KEYWORDS_RAW_EXTRACTION kre
            JOIN BETTERJOBS_DB.STAGE.KEYWORD_STANDARDIZATION_RULES kr
                ON LOWER(kre.KEYWORD_TEXT_RAW) = LOWER(kr.PATTERN)
            JOIN BETTERJOBS_DB.STAGE.KEYWORDS_NORMALIZED kn
                ON kn.KEYWORD_TEXT = kr.STANDARDIZED_TEXT
                AND kn.KEYWORD_TYPE = kre.KEYWORD_TYPE
            LEFT JOIN BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED lle
                ON kre.JOB_UID = lle.JOB_UID
            JOIN BETTERJOBS_DB.STAGE.JOBS_UNIFIED ju
                ON kre.JOB_UID = ju.JOB_UID
            WHERE ju.IS_ENGLISH = TRUE

            UNION ALL

            -- Second pass: Match keywords that weren't standardized (direct match)
            SELECT
                kre.JOB_UID,
                kn.KEYWORD_ID,
                kre.KEYWORD_SOURCE,
                kre.KEYWORD_TEXT_ORIGINAL,
                COALESCE(lle.LLM_OVERALL_CONFIDENCE, 0.8) as extraction_confidence,
                kn.CONFIDENCE_SCORE as standardization_confidence,
                NULL as matched_pattern
            FROM BETTERJOBS_DB.STAGE.KEYWORDS_RAW_EXTRACTION kre
            JOIN BETTERJOBS_DB.STAGE.KEYWORDS_NORMALIZED kn
                ON kn.KEYWORD_TEXT = kre.KEYWORD_TEXT_ORIGINAL
                AND kn.KEYWORD_TYPE = kre.KEYWORD_TYPE
            LEFT JOIN BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED lle
                ON kre.JOB_UID = lle.JOB_UID
            JOIN BETTERJOBS_DB.STAGE.JOBS_UNIFIED ju
                ON kre.JOB_UID = ju.JOB_UID
            LEFT JOIN BETTERJOBS_DB.STAGE.KEYWORD_STANDARDIZATION_RULES kr
                ON LOWER(kre.KEYWORD_TEXT_RAW) = LOWER(kr.PATTERN)
            WHERE ju.IS_ENGLISH = TRUE
              AND kr.PATTERN IS NULL  -- Only get non-standardized matches
        )
        SELECT
            CONCAT('kb_', ROW_NUMBER() OVER (ORDER BY JOB_UID, KEYWORD_ID)) as bridge_id,
            JOB_UID,
            KEYWORD_ID,
            KEYWORD_SOURCE,
            KEYWORD_TEXT_ORIGINAL,
            extraction_confidence,
            standardization_confidence,
            (extraction_confidence + standardization_confidence) / 2.0 as overall_confidence,
            CURRENT_TIMESTAMP

        FROM keyword_matches
        """

        cursor.execute(bridge_sql)
        relationships_created = cursor.rowcount
        context.log.info(f"✅ Created {relationships_created:,} job-keyword relationships")

        # Get bridge statistics
        cursor.execute("""
        SELECT
            COUNT(*) as total_relationships,
            COUNT(CASE WHEN KEYWORD_SOURCE = 'industry_keywords' THEN 1 END) as industry_relationships,
            COUNT(CASE WHEN KEYWORD_SOURCE = 'role_type' THEN 1 END) as role_type_relationships,
            COUNT(CASE WHEN OVERALL_CONFIDENCE >= 0.8 THEN 1 END) as high_confidence,
            COUNT(DISTINCT JOB_UID) as unique_jobs,
            AVG(OVERALL_CONFIDENCE) as avg_confidence
        FROM BETTERJOBS_DB.STAGE.JOB_KEYWORDS_BRIDGE
        """)

        result = cursor.fetchone()
        if result:
            stats.update({
                "relationships_created": result[0],
                "industry_relationships": result[1],
                "role_type_relationships": result[2],
                "high_confidence_relationships": result[3],
                "unique_jobs_with_keywords": result[4],
                "avg_confidence_score": float(result[5]) if result[5] else 0.0
            })

        # Calculate coverage metrics
        cursor.execute("""
        SELECT
            COUNT(DISTINCT ju.JOB_UID) as total_jobs,
            COUNT(DISTINCT jkb.JOB_UID) as jobs_with_keywords,
            ROUND((COUNT(DISTINCT jkb.JOB_UID)::FLOAT / COUNT(DISTINCT ju.JOB_UID)) * 100, 2) as coverage_percentage
        FROM BETTERJOBS_DB.STAGE.JOBS_UNIFIED ju
        LEFT JOIN BETTERJOBS_DB.STAGE.JOB_KEYWORDS_BRIDGE jkb ON ju.JOB_UID = jkb.JOB_UID
        """)

        coverage_result = cursor.fetchone()
        if coverage_result:
            stats.update({
                "total_jobs": coverage_result[0],
                "coverage_percentage": float(coverage_result[2]) if coverage_result[2] else 0.0
            })

        # Sample relationships for validation
        cursor.execute("""
        SELECT
            kn.KEYWORD_TYPE,
            kn.KEYWORD_TEXT,
            COUNT(*) as job_count,
            AVG(jkb.OVERALL_CONFIDENCE) as avg_confidence
        FROM BETTERJOBS_DB.STAGE.JOB_KEYWORDS_BRIDGE jkb
        JOIN BETTERJOBS_DB.STAGE.KEYWORDS_NORMALIZED kn ON jkb.KEYWORD_ID = kn.KEYWORD_ID
        GROUP BY kn.KEYWORD_TYPE, kn.KEYWORD_TEXT
        ORDER BY job_count DESC
        LIMIT 15
        """)

        sample_data = cursor.fetchall()
        if sample_data:
            columns = [desc[0] for desc in cursor.description]
            stats["top_keyword_relationships"] = [dict(zip(columns, row)) for row in sample_data]

        context.log.info(f"""
        🎯 Job-Keywords Bridge Complete:
        • Relationships Created: {stats['relationships_created']:,}
        • Industry Relationships: {stats['industry_relationships']:,}
        • Role Type Relationships: {stats['role_type_relationships']:,}
        • High Confidence: {stats['high_confidence_relationships']:,}
        • Jobs with Keywords: {stats['unique_jobs_with_keywords']:,}
        • Coverage: {stats.get('coverage_percentage', 0):.1f}%
        • Average Confidence: {stats.get('avg_confidence_score', 0):.3f}
        """)

        # Add metadata for Dagster UI
        context.add_output_metadata({
            "relationships_created": MetadataValue.int(stats["relationships_created"]),
            "industry_relationships": MetadataValue.int(stats["industry_relationships"]),
            "role_type_relationships": MetadataValue.int(stats["role_type_relationships"]),
            "high_confidence_relationships": MetadataValue.int(stats["high_confidence_relationships"]),
            "unique_jobs_with_keywords": MetadataValue.int(stats["unique_jobs_with_keywords"]),
            "coverage_percentage": MetadataValue.float(stats.get("coverage_percentage", 0.0)),
            "avg_confidence_score": MetadataValue.float(stats.get("avg_confidence_score", 0.0)),
            "top_keyword_relationships": MetadataValue.json(stats.get("top_keyword_relationships", []))
        })

        return stats

    except Exception as e:
        context.log.error(f"❌ Job-keywords bridge creation failed: {str(e)}")
        stats["processing_errors"] = 1
        stats["error_message"] = str(e)
        raise

    finally:
        if cursor:
            cursor.close()