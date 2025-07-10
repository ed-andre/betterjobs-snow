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
from pathlib import Path

from dagster import (
    asset,
    AssetExecutionContext,
    MetadataValue
)

from dagster_betterjobs.resources import SnowflakeResource
from dagster_betterjobs.utils.schema_utils import ensure_object_exists, execute_sql_file


PROJECT_ROOT = Path(__file__).resolve().parents[5]  # Go up 6 levels to project root

@asset(
    deps=["stage_jobs_llm_enriched_unified"],
    description="Extract and flatten keywords from LLM VARIANT columns using schema-as-code",
    group_name="2b_stage_normalization_keywords",
    kinds={"snowflake", "python", "SQL"}
)
def stage_llm_keywords_raw_extraction(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Extract all keywords from VARIANT columns and flatten into workable format.

    Uses schema-as-code approach with canonical view definition from SQL file.

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
        "extraction_errors": 0,
        "schema_as_code": True
    }

    try:
        cursor = conn.cursor()

        context.log.info("🔍 Starting LLM keywords raw extraction with schema-as-code...")

        # 🔧 SCHEMA-AS-CODE: Ensure view exists using canonical SQL file
        view_name = ensure_object_exists("views/stage_keywords_raw_extraction.sql", snowflake, context)
        context.log.info(f"✅ Keywords raw extraction view ready: {view_name}")

        # Get extraction statistics
        cursor.execute(f"""
        SELECT
            COUNT(*) as total_keywords,
            COUNT(DISTINCT JOB_UID) as unique_jobs,
            COUNT(CASE WHEN KEYWORD_SOURCE = 'primary_keywords' THEN 1 END) as primary_count,
            COUNT(CASE WHEN KEYWORD_SOURCE = 'industry_keywords' THEN 1 END) as industry_count,
            COUNT(CASE WHEN KEYWORD_SOURCE = 'role_type' THEN 1 END) as role_type_count
        FROM {view_name}
        """)

        result = cursor.fetchone()
        if result:
            stats.update({
                "keywords_extracted": result[0],
                "unique_jobs_processed": result[1],
                "primary_keywords_count": result[2],
                "industry_keywords_count": result[3],
                "role_type_keywords_count": result[4]
            })

        # Sample some data for validation
        cursor.execute(f"""
        SELECT KEYWORD_SOURCE, KEYWORD_TYPE, KEYWORD_TEXT_ORIGINAL, COUNT(*) as frequency
        FROM {view_name}
        GROUP BY KEYWORD_SOURCE, KEYWORD_TYPE, KEYWORD_TEXT_ORIGINAL
        ORDER BY frequency DESC
        LIMIT 20
        """)

        sample_data = cursor.fetchall()
        if sample_data:
            columns = [desc[0] for desc in cursor.description]
            stats["top_keywords_sample"] = [dict(zip(columns, row)) for row in sample_data]

        context.log.info(f"""
        🎯 Keywords Raw Extraction Complete (Schema-as-Code):
        • Total Keywords Extracted: {stats['keywords_extracted']:,}
        • Unique Jobs Processed: {stats['unique_jobs_processed']:,}
        • Primary Keywords: {stats['primary_keywords_count']:,}
        • Industry Keywords: {stats['industry_keywords_count']:,}
        • Role Type Keywords: {stats['role_type_keywords_count']:,}
        • View: {view_name}
        """)

        # Add metadata for Dagster UI
        context.add_output_metadata({
            "keywords_extracted": MetadataValue.int(stats["keywords_extracted"]),
            "unique_jobs_processed": MetadataValue.int(stats["unique_jobs_processed"]),
            "primary_keywords_count": MetadataValue.int(stats["primary_keywords_count"]),
            "industry_keywords_count": MetadataValue.int(stats["industry_keywords_count"]),
            "role_type_keywords_count": MetadataValue.int(stats["role_type_keywords_count"]),
            "schema_as_code": MetadataValue.bool(True),
            "view_name": MetadataValue.text(view_name),
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
    description="Maintain keyword standardization rules and aliases using schema-as-code",
    group_name="2b_stage_normalization_keywords",
    kinds={"snowflake", "python", "SQL"}
)
def stage_keywords_standardization_rules(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Create and maintain comprehensive keyword standardization rules.

    Uses schema-as-code approach with canonical table definition from SQL file.

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
        "rules_errors": 0,
        "data_populated": False,
        "schema_as_code": True
    }

    try:
        cursor = conn.cursor()

        context.log.info("🔧 Setting up keyword standardization rules with schema-as-code...")

        # 🔧 SCHEMA-AS-CODE: Ensure table exists using canonical SQL file
        table_name = ensure_object_exists("tables/stage_keyword_standardization_rules.sql", snowflake, context)
        context.log.info(f"✅ Keyword standardization rules table ready: {table_name}")

        # Always reload data to ensure latest rules from SQL file
        context.log.info("🔄 Clearing existing data and reloading keyword standardization rules...")

        # Clear existing data
        cursor.execute(f"DELETE FROM {table_name}")
        context.log.info("🗑️ Cleared existing keyword standardization rules")

        # Execute data population script using standardized utility
        insert_file_path = PROJECT_ROOT / "pipeline" / "sql" / "data_population" / "insert_keyword_standardization_rules.sql"

        if insert_file_path.exists():
            result = execute_sql_file(snowflake, str(insert_file_path), context)

            if result["status"] == "success":
                context.log.info(f"✅ Successfully executed keyword standardization rules data population")
                stats["data_populated"] = True
            else:
                context.log.error(f"❌ Failed to populate keyword standardization rules: {result.get('error', 'Unknown error')}")
                stats["data_populated"] = False
        else:
            context.log.warning(f"Data population file not found: {insert_file_path}")
            stats["data_populated"] = False

        # Get current rule count
        cursor.execute(f"""
        SELECT
            COUNT(*) as total_rules,
            COUNT(CASE WHEN IS_ACTIVE THEN 1 END) as active_rules,
            COUNT(DISTINCT KEYWORD_TYPE) as types_covered
        FROM {table_name}
        """)

        result = cursor.fetchone()
        if result:
            stats.update({
                "rules_loaded": result[0],
                "active_rules": result[1],
                "types_covered": result[2]
            })

        context.log.info(f"""
        🎯 Keyword Standardization Rules Status (Schema-as-Code):
        • Total Rules: {stats['rules_loaded']:,}
        • Active Rules: {stats.get('active_rules', 0):,}
        • Types Covered: {stats.get('types_covered', 0):,}
        • Table: {table_name}
        • Data Populated: {stats['data_populated']}
        """)

        # Add metadata for Dagster UI
        context.add_output_metadata({
            "rules_loaded": MetadataValue.int(stats["rules_loaded"]),
            "active_rules": MetadataValue.int(stats.get("active_rules", 0)),
            "types_covered": MetadataValue.int(stats.get("types_covered", 0)),
            "schema_as_code": MetadataValue.bool(True),
            "table_name": MetadataValue.text(table_name),
            "data_populated": MetadataValue.bool(stats["data_populated"])
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
    description="Manage keyword type and category classifications with data population",
    group_name="2b_stage_normalization_keywords",
    kinds={"snowflake", "python", "SQL"}
)
def stage_keyword_type_mapping(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Create and maintain keyword type and category classifications.

    Uses schema-as-code approach with canonical table definition and data population.

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
        "mapping_errors": 0,
        "data_populated": False,
        "schema_as_code": True
    }

    try:
        cursor = conn.cursor()

        context.log.info("🗂️ Setting up keyword type mappings with schema-as-code...")

        # 🔧 SCHEMA-AS-CODE: Ensure table exists using canonical SQL file
        table_name = ensure_object_exists("tables/stage_keyword_type_mapping.sql", snowflake, context)
        context.log.info(f"✅ Keyword type mapping table ready: {table_name}")

        # Always reload data to ensure latest mappings from SQL file
        context.log.info("🔄 Clearing existing data and reloading keyword type mappings...")

        # Clear existing data
        cursor.execute(f"DELETE FROM {table_name}")
        context.log.info("🗑️ Cleared existing keyword type mappings")

        # Execute data population script using standardized utility
        insert_file_path = PROJECT_ROOT / "pipeline" / "sql" / "data_population" / "insert_keyword_type_mappings.sql"

        if insert_file_path.exists():
            result = execute_sql_file(snowflake, str(insert_file_path), context)

            if result["status"] == "success":
                context.log.info(f"✅ Successfully executed keyword type mappings data population")
                stats["data_populated"] = True
            else:
                context.log.error(f"❌ Failed to populate keyword type mappings: {result.get('error', 'Unknown error')}")
                stats["data_populated"] = False
        else:
            context.log.warning(f"Data population file not found: {insert_file_path}")
            stats["data_populated"] = False

        # Get current mapping count
        cursor.execute(f"""
        SELECT
            COUNT(*) as total_mappings,
            COUNT(CASE WHEN IS_ACTIVE THEN 1 END) as active_mappings,
            COUNT(DISTINCT KEYWORD_TYPE) as types_defined,
            COUNT(DISTINCT KEYWORD_CATEGORY) as categories_defined
        FROM {table_name}
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
        🎯 Keyword Type Mapping Status (Schema-as-Code):
        • Total Mappings: {stats['mappings_loaded']:,}
        • Active Mappings: {stats.get('active_mappings', 0):,}
        • Types Defined: {stats.get('types_defined', 0):,}
        • Categories Defined: {stats.get('categories_defined', 0):,}
        • Table: {table_name}
        • Data Populated: {stats['data_populated']}
        """)

        # Add metadata for Dagster UI
        context.add_output_metadata({
            "mappings_loaded": MetadataValue.int(stats["mappings_loaded"]),
            "active_mappings": MetadataValue.int(stats.get("active_mappings", 0)),
            "types_defined": MetadataValue.int(stats.get("types_defined", 0)),
            "categories_defined": MetadataValue.int(stats.get("categories_defined", 0)),
            "schema_as_code": MetadataValue.bool(True),
            "table_name": MetadataValue.text(table_name),
            "data_populated": MetadataValue.bool(stats["data_populated"])
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
    description="Create normalized keywords master table with market intelligence using schema-as-code",
    group_name="2b_stage_normalization_keywords",
    kinds={"snowflake", "python", "SQL"}
)
def stage_keywords_normalized(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Apply standardization rules and create keywords master table.

    Uses schema-as-code approach for table creation and dependency management.

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
        "primary_keywords": 0,
        "industry_keywords": 0,
        "role_type_keywords": 0,
        "high_confidence_keywords": 0,
        "low_confidence_keywords": 0,
        "processing_errors": 0,
        "schema_as_code": True
    }

    try:
        cursor = conn.cursor()

        context.log.info("🔄 Starting keywords normalization process with schema-as-code...")

        # 🔧 SCHEMA-AS-CODE: Ensure all required objects exist
        normalized_table = ensure_object_exists("tables/stage_keywords_normalized.sql", snowflake, context)
        extraction_view = ensure_object_exists("views/stage_keywords_raw_extraction.sql", snowflake, context)
        rules_table = ensure_object_exists("tables/stage_keyword_standardization_rules.sql", snowflake, context)
        mapping_table = ensure_object_exists("tables/stage_keyword_type_mapping.sql", snowflake, context)

        context.log.info(f"✅ All keyword normalization objects ready")

        # Clear existing normalized data to rebuild
        cursor.execute(f"DELETE FROM {normalized_table}")
        context.log.info("🗑️ Cleared existing KEYWORDS_NORMALIZED data")

        # Normalize and populate keywords
        normalization_sql = f"""
        INSERT INTO {normalized_table} (
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
        WITH
        -- ENHANCEMENT-037: AI Keyword Category and Subcategory override
        ai_keyword_category AS (
            SELECT DISTINCT
                KEYWORD_TEXT_RAW,
                'Artificial Intelligence' as KEYWORD_CATEGORY
            FROM {extraction_view} kre
            WHERE kre.KEYWORD_TEXT_RAW ILIKE 'ai %'
                OR kre.KEYWORD_TEXT_RAW ILIKE '% ai'
                OR kre.KEYWORD_TEXT_RAW ILIKE 'ai-%'
        ),
        keyword_aggregation AS (
            SELECT
                -- Apply standardization rules or use original text
                COALESCE(ksr.STANDARDIZED_TEXT, kre.KEYWORD_TEXT_ORIGINAL) as keyword_text,

                -- Determine keyword type and category
                COALESCE(ksr.KEYWORD_TYPE, ktm.KEYWORD_TYPE, kre.KEYWORD_TYPE) as keyword_type,
                COALESCE(aikc.KEYWORD_CATEGORY, ksr.KEYWORD_CATEGORY, ktm.KEYWORD_CATEGORY, 'uncategorized') as keyword_category,

                -- Aggregate variants
                ARRAY_AGG(DISTINCT kre.KEYWORD_TEXT_ORIGINAL) as original_variants,

                -- Calculate frequency metrics
                COUNT(*) as frequency_count,
                COUNT(DISTINCT kre.JOB_UID) as unique_job_count,

                -- Calculate confidence score
                AVG(COALESCE(ksr.CONFIDENCE_SCORE, ktm.CONFIDENCE_SCORE, 0.5)) as confidence_score,

                -- Business relevance
                MAX(COALESCE(ktm.BUSINESS_RELEVANCE, 'medium')) as business_relevance

            FROM {extraction_view} kre

            -- Left join with standardization rules
            LEFT JOIN {rules_table} ksr
                ON LOWER(kre.KEYWORD_TEXT_RAW) = LOWER(ksr.PATTERN)
                AND ksr.IS_ACTIVE = TRUE

            -- Left join with type mappings
            LEFT JOIN {mapping_table} ktm
                ON LOWER(kre.KEYWORD_TEXT_RAW) = LOWER(ktm.KEYWORD_TEXT)
                AND ktm.IS_ACTIVE = TRUE

            -- Left join with AI keyword category
            LEFT JOIN ai_keyword_category aikc
                ON kre.KEYWORD_TEXT_RAW = aikc.KEYWORD_TEXT_RAW

            WHERE LENGTH(kre.KEYWORD_TEXT_RAW) >= 2  -- Filter out single characters

            GROUP BY 1, 2, 3
            HAVING COUNT(*) >= 1  -- Keeping all keywords for now even if they appear only once
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
            CASE WHEN confidence_score >= 0.5 THEN TRUE ELSE FALSE END as approved_by_admin, --
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        FROM keyword_aggregation
        ORDER BY frequency_count DESC
        """

        cursor.execute(normalization_sql)
        keywords_inserted = cursor.rowcount
        context.log.info(f"✅ Inserted {keywords_inserted:,} normalized keywords")

        # Get normalization statistics
        cursor.execute(f"""
        SELECT
            COUNT(*) as total_keywords,
            COUNT(CASE WHEN KEYWORD_TYPE = 'primary' THEN 1 END) as primary_count,
            COUNT(CASE WHEN KEYWORD_TYPE = 'industry' THEN 1 END) as industry_count,
            COUNT(CASE WHEN KEYWORD_TYPE = 'role_type' THEN 1 END) as role_type_count,
            COUNT(CASE WHEN CONFIDENCE_SCORE >= 0.8 THEN 1 END) as high_confidence,
            COUNT(CASE WHEN CONFIDENCE_SCORE >= 0.5 AND CONFIDENCE_SCORE < 0.8 THEN 1 END) as medium_confidence,
            COUNT(CASE WHEN CONFIDENCE_SCORE < 0.5 THEN 1 END) as low_confidence,
            AVG(CONFIDENCE_SCORE) as avg_confidence,
            AVG(FREQUENCY_COUNT) as avg_frequency
        FROM {normalized_table}
        """)

        result = cursor.fetchone()
        if result:
            stats.update({
                "keywords_normalized": result[0],
                "primary_keywords": result[1],
                "industry_keywords": result[2],
                "role_type_keywords": result[3],
                "high_confidence_keywords": result[4],
                "medium_confidence_keywords": result[5],
                "low_confidence_keywords": result[6],
                "avg_confidence_score": float(result[7]) if result[7] else 0.0,
                "avg_frequency": float(result[8]) if result[8] else 0.0
            })

        # Sample normalized keywords for validation
        cursor.execute(f"""
        SELECT KEYWORD_TYPE, KEYWORD_TEXT, FREQUENCY_COUNT, CONFIDENCE_SCORE
        FROM {normalized_table}
        ORDER BY FREQUENCY_COUNT DESC
        LIMIT 15
        """)

        sample_data = cursor.fetchall()
        if sample_data:
            columns = [desc[0] for desc in cursor.description]
            stats["top_normalized_keywords"] = [dict(zip(columns, row)) for row in sample_data]

        context.log.info(f"""
        🎯 Keywords Normalization Complete (Schema-as-Code):
        • Keywords Normalized: {stats['keywords_normalized']:,}
        • Primary Keywords: {stats['primary_keywords']:,}
        • Industry Keywords: {stats['industry_keywords']:,}
        • Role Type Keywords: {stats['role_type_keywords']:,}
        • High Confidence: {stats['high_confidence_keywords']:,}
        • Medium Confidence: {stats['medium_confidence_keywords']:,}
        • Low Confidence: {stats['low_confidence_keywords']:,}
        • Average Confidence: {stats.get('avg_confidence_score', 0):.3f}
        • Average Frequency: {stats.get('avg_frequency', 0):.1f}
        • Table: {normalized_table}
        """)

        # Add metadata for Dagster UI
        context.add_output_metadata({
            "keywords_normalized": MetadataValue.int(stats["keywords_normalized"]),
            "primary_keywords": MetadataValue.int(stats["primary_keywords"]),
            "industry_keywords": MetadataValue.int(stats["industry_keywords"]),
            "role_type_keywords": MetadataValue.int(stats["role_type_keywords"]),
            "high_confidence_keywords": MetadataValue.int(stats["high_confidence_keywords"]),
            "low_confidence_keywords": MetadataValue.int(stats["low_confidence_keywords"]),
            "avg_confidence_score": MetadataValue.float(stats.get("avg_confidence_score", 0.0)),
            "avg_frequency": MetadataValue.float(stats.get("avg_frequency", 0.0)),
            "schema_as_code": MetadataValue.bool(True),
            "normalized_table": MetadataValue.text(normalized_table),
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
    description="Create job-keyword relationships with context tracking using schema-as-code",
    group_name="2b_stage_normalization_keywords",
    kinds={"snowflake", "python", "SQL"}
)
def stage_job_keywords_bridge(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Map jobs to normalized keywords with rich context.

    Uses schema-as-code approach for table creation and dependency management.

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
        "primary_relationships": 0,
        "industry_relationships": 0,
        "role_type_relationships": 0,
        "high_confidence_relationships": 0,
        "unique_jobs_with_keywords": 0,
        "processing_errors": 0,
        "schema_as_code": True
    }

    try:
        cursor = conn.cursor()

        context.log.info("🔗 Starting job-keywords bridge creation with schema-as-code...")

        # 🔧 SCHEMA-AS-CODE: Ensure all required objects exist
        bridge_table = ensure_object_exists("tables/stage_job_keywords_bridge.sql", snowflake, context)
        extraction_view = ensure_object_exists("views/stage_keywords_raw_extraction.sql", snowflake, context)
        normalized_table = ensure_object_exists("tables/stage_keywords_normalized.sql", snowflake, context)
        rules_table = ensure_object_exists("tables/stage_keyword_standardization_rules.sql", snowflake, context)

        context.log.info(f"✅ All job-keywords bridge objects ready")

        # Clear existing bridge data to rebuild
        cursor.execute(f"DELETE FROM {bridge_table}")
        context.log.info("🗑️ Cleared existing JOB_KEYWORDS_BRIDGE data")

        # Create job-keyword relationships - Following skills bridge pattern
        bridge_sql = f"""
        INSERT INTO {bridge_table} (
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
            FROM {extraction_view} kre
            JOIN {rules_table} kr
                ON LOWER(kre.KEYWORD_TEXT_RAW) = LOWER(kr.PATTERN)
            JOIN {normalized_table} kn
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
            FROM {extraction_view} kre
            JOIN {normalized_table} kn
                ON kn.KEYWORD_TEXT = kre.KEYWORD_TEXT_ORIGINAL
                AND kn.KEYWORD_TYPE = kre.KEYWORD_TYPE
            LEFT JOIN BETTERJOBS_DB.STAGE.JOBS_LLM_ENRICHED lle
                ON kre.JOB_UID = lle.JOB_UID
            JOIN BETTERJOBS_DB.STAGE.JOBS_UNIFIED ju
                ON kre.JOB_UID = ju.JOB_UID
            LEFT JOIN {rules_table} kr
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
        cursor.execute(f"""
        SELECT
            COUNT(*) as total_relationships,
            COUNT(CASE WHEN KEYWORD_SOURCE = 'primary_keywords' THEN 1 END) as primary_relationships,
            COUNT(CASE WHEN KEYWORD_SOURCE = 'industry_keywords' THEN 1 END) as industry_relationships,
            COUNT(CASE WHEN KEYWORD_SOURCE = 'role_type' THEN 1 END) as role_type_relationships,
            COUNT(CASE WHEN OVERALL_CONFIDENCE >= 0.8 THEN 1 END) as high_confidence,
            COUNT(DISTINCT JOB_UID) as unique_jobs,
            AVG(OVERALL_CONFIDENCE) as avg_confidence
        FROM {bridge_table}
        """)

        result = cursor.fetchone()
        if result:
            stats.update({
                "relationships_created": result[0],
                "primary_relationships": result[1],
                "industry_relationships": result[2],
                "role_type_relationships": result[3],
                "high_confidence_relationships": result[4],
                "unique_jobs_with_keywords": result[5],
                "avg_confidence_score": float(result[6]) if result[6] else 0.0
            })

        # Calculate coverage metrics
        cursor.execute(f"""
        SELECT
            COUNT(DISTINCT ju.JOB_UID) as total_jobs,
            COUNT(DISTINCT jkb.JOB_UID) as jobs_with_keywords,
            ROUND((COUNT(DISTINCT jkb.JOB_UID)::FLOAT / COUNT(DISTINCT ju.JOB_UID)) * 100, 2) as coverage_percentage
        FROM BETTERJOBS_DB.STAGE.JOBS_UNIFIED ju
        LEFT JOIN {bridge_table} jkb ON ju.JOB_UID = jkb.JOB_UID
        """)

        coverage_result = cursor.fetchone()
        if coverage_result:
            stats.update({
                "total_jobs": coverage_result[0],
                "coverage_percentage": float(coverage_result[2]) if coverage_result[2] else 0.0
            })

        # Sample relationships for validation
        cursor.execute(f"""
        SELECT
            kn.KEYWORD_TYPE,
            kn.KEYWORD_TEXT,
            COUNT(*) as job_count,
            AVG(jkb.OVERALL_CONFIDENCE) as avg_confidence
        FROM {bridge_table} jkb
        JOIN {normalized_table} kn ON jkb.KEYWORD_ID = kn.KEYWORD_ID
        GROUP BY kn.KEYWORD_TYPE, kn.KEYWORD_TEXT
        ORDER BY job_count DESC
        LIMIT 15
        """)

        sample_data = cursor.fetchall()
        if sample_data:
            columns = [desc[0] for desc in cursor.description]
            stats["top_keyword_relationships"] = [dict(zip(columns, row)) for row in sample_data]

        context.log.info(f"""
        🎯 Job-Keywords Bridge Complete (Schema-as-Code):
        • Relationships Created: {stats['relationships_created']:,}
        • Primary Relationships: {stats['primary_relationships']:,}
        • Industry Relationships: {stats['industry_relationships']:,}
        • Role Type Relationships: {stats['role_type_relationships']:,}
        • High Confidence: {stats['high_confidence_relationships']:,}
        • Jobs with Keywords: {stats['unique_jobs_with_keywords']:,}
        • Coverage: {stats.get('coverage_percentage', 0):.1f}%
        • Average Confidence: {stats.get('avg_confidence_score', 0):.3f}
        • Bridge Table: {bridge_table}
        """)

        # Add metadata for Dagster UI
        context.add_output_metadata({
            "relationships_created": MetadataValue.int(stats["relationships_created"]),
            "primary_relationships": MetadataValue.int(stats["primary_relationships"]),
            "industry_relationships": MetadataValue.int(stats["industry_relationships"]),
            "role_type_relationships": MetadataValue.int(stats["role_type_relationships"]),
            "high_confidence_relationships": MetadataValue.int(stats["high_confidence_relationships"]),
            "unique_jobs_with_keywords": MetadataValue.int(stats["unique_jobs_with_keywords"]),
            "coverage_percentage": MetadataValue.float(stats.get("coverage_percentage", 0.0)),
            "avg_confidence_score": MetadataValue.float(stats.get("avg_confidence_score", 0.0)),
            "schema_as_code": MetadataValue.bool(True),
            "bridge_table": MetadataValue.text(bridge_table),
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