"""
Stage Processing Utilities

Shared processing logic extracted from stage_jobs_unified to enable
individual platform assets with maximum code reuse (DRY principle).

This module provides:
- Platform-specific data loading
- Common transformation pipeline
- Snowflake table operations
- Quality validation and statistics
"""

import pandas as pd
import json
from datetime import datetime, date
from typing import Dict, Any, Optional, Tuple

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from dagster import AssetExecutionContext

from dagster_betterjobs.transformations.text_cleaning import clean_text_fields, clean_job_description
from dagster_betterjobs.transformations.language_detection import LanguageDetector
from dagster_betterjobs.transformations.platform_mapping import PlatformMapper
from dagster_betterjobs.transformations.uid_generation import add_job_uids_to_dataframe, validate_uid_uniqueness, get_uid_collision_report
from dagster_betterjobs.utils.schema_utils import ensure_object_exists


class PlatformProcessingConfig:
    """Configuration for platform processing."""
    def __init__(
        self,
        process_language_detection: bool = True,
        language_confidence_threshold: float = 0.8,
        include_non_english: bool = True,
        max_records: Optional[int] = None
    ):
        self.process_language_detection = process_language_detection
        self.language_confidence_threshold = language_confidence_threshold
        self.include_non_english = include_non_english
        self.max_records = max_records


def create_stage_table_if_not_exists(context: AssetExecutionContext):
    """
    Create the unified stage table if it doesn't exist using schema-as-code approach.

    🔧 SCHEMA-AS-CODE IMPLEMENTATION 🔧
    This function now uses the canonical SQL definition from stage_jobs_unified.sql
    instead of hardcoded CREATE TABLE statements.
    """

    try:
        # 🔧 SCHEMA-AS-CODE: Ensure table exists using canonical SQL definition
        # Use the actual Snowflake resource from context instead of creating a wrapper
        table_fqn = ensure_object_exists("tables/stage_jobs_unified.sql", context.resources.snowflake, context)

        context.log.info(f"✅ SCHEMA-AS-CODE: Stage jobs unified table verified/created: {table_fqn}")

        return True
    except Exception as e:
        context.log.error(f"❌ Error creating stage table using schema-as-code: {str(e)}")
        raise


def load_platform_raw_data(
    platform: str,
    conn,
    context: AssetExecutionContext,
    max_records: Optional[int] = None,
    database_name: str = "BETTERJOBS_DB",
    raw_schema: str = "RAW"
) -> pd.DataFrame:
    """Load raw data for a specific platform."""
    raw_table_name = f"{platform}_jobs"

    query_sql = f"""
    SELECT
        j.*,
        m.company_name
    FROM {database_name}.{raw_schema}.{raw_table_name} j
    LEFT JOIN {database_name}.{raw_schema}.master_company_urls m
        ON j.company_id = m.company_id
    WHERE j.is_active = TRUE
    """

    if max_records:
        query_sql += f" LIMIT {max_records}"

    cursor = conn.cursor()
    try:
        cursor.execute(query_sql)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        if not results:
            context.log.warning(f"No data found in {raw_table_name}")
            return pd.DataFrame()

        # Convert to DataFrame and normalize column names
        raw_df = pd.DataFrame(results, columns=columns)
        raw_df.columns = raw_df.columns.str.lower()

        context.log.info(f"Loaded {len(raw_df)} jobs from {platform}")
        return raw_df

    finally:
        cursor.close()


def process_platform_jobs(
    platform: str,
    raw_df: pd.DataFrame,
    config: PlatformProcessingConfig,
    context: AssetExecutionContext
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Process jobs for a specific platform through the complete transformation pipeline.

    Returns:
        Tuple of (processed_dataframe, statistics_dict)
    """
    stats = {
        "platform": platform,
        "raw_jobs": len(raw_df),
        "processed_jobs": 0,
        "english_jobs": 0,
        "non_english_jobs": 0,
        "uid_validation": {},
        "processing_start": datetime.now().isoformat()
    }

    if raw_df.empty:
        stats["processing_end"] = datetime.now().isoformat()
        return pd.DataFrame(), stats

    # Initialize transformation modules
    language_detector = LanguageDetector(
        confidence_threshold=config.language_confidence_threshold
    )
    platform_mapper = PlatformMapper()

    # Apply text cleaning
    context.log.info(f"[{platform}] Applying text cleaning...")
    cleaned_df = clean_text_fields(raw_df)

    # Ensure required columns exist for language detection
    if 'job_description_clean' not in cleaned_df.columns:
        context.log.warning(f"[{platform}] job_description_clean column not found after text cleaning")
        # Check if we have job_description column
        if 'job_description' in cleaned_df.columns:
            context.log.info(f"[{platform}] Creating job_description_clean from job_description")
            cleaned_df['job_description_clean'] = cleaned_df['job_description'].apply(
                lambda x: clean_job_description(x) if pd.notnull(x) else ""
            )
        else:
            context.log.warning(f"[{platform}] No job_description column found, creating empty job_description_clean")
            cleaned_df['job_description_clean'] = ""

    # Apply language detection if enabled
    if config.process_language_detection:
        context.log.info(f"[{platform}] Performing language detection...")

        # Use multi-field detection for better accuracy and debug logging
        # This will trigger debug logging for target jobs if they exist
        def detect_language_row(row):
            job_id = str(row.get('job_id', ''))
            company_id = str(row.get('company_id', ''))
            job_title = str(row.get('job_title', '') or row.get('job_title_clean', ''))
            job_description = str(row.get('job_description_clean', ''))

            return language_detector.detect_language_multi_field(
                job_title=job_title,
                job_description=job_description,
                job_id=job_id,
                company_id=company_id,
                platform=platform
            )

        context.log.info(f"[{platform}] Applying multi-field language detection with debug logging...")
        language_results = cleaned_df.apply(detect_language_row, axis=1)

        # Extract results into separate columns
        cleaned_df['detected_language'] = [result['detected_language'] for result in language_results]
        cleaned_df['language_confidence'] = [result['language_confidence'] for result in language_results]
        cleaned_df['is_english'] = [result['is_english'] for result in language_results]
        cleaned_df['language_detection_method'] = [result['language_detection_method'] for result in language_results]
        cleaned_df['detection_source'] = [result['detection_source'] for result in language_results]

        # Filter English jobs if configured
        if not config.include_non_english:
            english_jobs = len(cleaned_df[cleaned_df['is_english'] == True])
            non_english_jobs = len(cleaned_df[cleaned_df['is_english'] == False])

            stats["english_jobs"] = english_jobs
            stats["non_english_jobs"] = non_english_jobs

            cleaned_df = cleaned_df[cleaned_df['is_english'] == True].copy()

            context.log.info(f"[{platform}] Language filtering: {english_jobs} English, {non_english_jobs} non-English jobs")

    # Apply platform field mapping
    context.log.info(f"[{platform}] Applying platform field mapping...")
    unified_df = platform_mapper.map_platform_data(cleaned_df, platform)

    # Generate deterministic UIDs for true uniqueness
    context.log.info(f"[{platform}] Generating deterministic UIDs...")
    unified_df = add_job_uids_to_dataframe(unified_df, uid_column='job_uid')

    # Validate UID uniqueness for this platform
    uid_validation = validate_uid_uniqueness(unified_df, uid_column='job_uid')
    stats["uid_validation"] = uid_validation

    if not uid_validation['is_valid']:
        context.log.warning(f"[{platform}] UID collisions detected: {uid_validation['duplicate_count']} duplicates")
        # Log sample collision UIDs
        if uid_validation['duplicate_uids']:
            sample_collisions = uid_validation['duplicate_uids'][:5]
            context.log.warning(f"[{platform}] Sample collision UIDs: {sample_collisions}")
    else:
        context.log.info(f"[{platform}] All {uid_validation['unique_uids']} UIDs are unique")

    # Add partition date and platform metadata
    unified_df['partition_date'] = date.today()
    unified_df['source_raw_table'] = f"{platform}_jobs"

    # Data quality validation
    context.log.info(f"[{platform}] Performing data quality validation...")

    # Calculate data quality score
    total_fields = len(unified_df.columns)
    unified_df['data_quality_score'] = unified_df.apply(
        lambda row: (total_fields - row.isna().sum()) / total_fields, axis=1
    )

    # Check for duplicates within platform
    initial_count = len(unified_df)
    duplicate_mask = unified_df.duplicated(subset=['job_id', 'platform', 'company_id'])
    duplicates_count = duplicate_mask.sum()

    if duplicates_count > 0:
        context.log.warning(f"[{platform}] Removing {duplicates_count} duplicate jobs (job_id + platform + company_id)")
        unified_df = unified_df.drop_duplicates(subset=['job_id', 'platform', 'company_id'], keep='first')
        final_count = len(unified_df)
        context.log.info(f"[{platform}] After deduplication: {final_count} jobs remaining")
    else:
        context.log.info(f"[{platform}] No duplicates found - all {initial_count} jobs are unique")

    stats["processed_jobs"] = len(unified_df)
    stats["processing_end"] = datetime.now().isoformat()

    context.log.info(f"[{platform}] Successfully processed {len(unified_df)} jobs")

    return unified_df, stats


def load_platform_data_to_snowflake(
    platform_df: pd.DataFrame,
    platform: str,
    conn,
    context: AssetExecutionContext,
    database_name: str = "BETTERJOBS_DB",
    stage_schema: str = "STAGE"
) -> int:
    """
    Load processed platform data to Snowflake using robust timestamp handling.

    Returns:
        Number of rows loaded
    """
    if platform_df.empty:
        context.log.info(f"[{platform}] No data to load")
        return 0

    # Prepare DataFrame for Snowflake loading
    expected_columns = [
        'job_id', 'job_uid', 'company_id', 'platform', 'job_title_clean', 'job_description_clean',
        'company_name_clean', 'location_standardized', 'job_url', 'date_posted', 'date_retrieved',
        'is_active', 'employment_status', 'department', 'detected_language', 'language_confidence',
        'is_english', 'language_detection_method', 'platform_specific_data', 'data_quality_score',
        'source_raw_table', 'raw_data', 'partition_date'
    ]

    # Filter DataFrame to only include expected columns (if they exist)
    available_columns = [col for col in expected_columns if col in platform_df.columns]
    filtered_df = platform_df[available_columns].copy()

    context.log.info(f"[{platform}] Loading {len(filtered_df)} rows with columns: {list(filtered_df.columns)}")

    # Handle data types for Snowflake
    for col in filtered_df.select_dtypes(include=['object']).columns:
        if col not in ['date_posted', 'date_retrieved', 'partition_date']:
            filtered_df[col] = filtered_df[col].fillna('').astype(str)

    # Convert date_retrieved to string to prevent timestamp corruption
    if 'date_retrieved' in filtered_df.columns:
        context.log.info(f"[{platform}] Converting date_retrieved to string to prevent timestamp corruption")
        filtered_df['date_retrieved'] = filtered_df['date_retrieved'].astype(str)

    # Handle boolean columns
    boolean_columns = ['is_active', 'is_english']
    for col in boolean_columns:
        if col in filtered_df.columns:
            filtered_df[col] = filtered_df[col].astype(bool)

    # Handle VARIANT columns (JSON data)
    variant_columns = ['platform_specific_data', 'raw_data']
    for col in variant_columns:
        if col in filtered_df.columns:
            filtered_df[col] = filtered_df[col].apply(
                lambda x: json.dumps(x) if isinstance(x, (dict, list)) else str(x) if pd.notnull(x) else None
            )

    # Create temporary table for bulk insert
    temp_table_name = f"temp_stage_jobs_{platform}_{int(datetime.now().timestamp())}"

    cursor = conn.cursor()
    try:
        # Use write_pandas to load to temporary table first
        success, num_chunks, num_rows, output = write_pandas(
            conn,
            filtered_df,
            temp_table_name,
            database=database_name,
            schema=stage_schema,
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=False
        )

        if not success:
            raise Exception(f"Failed to load data to temporary table: {output}")

        context.log.info(f"[{platform}] Loaded {num_rows} rows to temporary table {temp_table_name}")

        # Build dynamic INSERT SQL based on columns that actually exist in the DataFrame
        available_columns = list(filtered_df.columns)
        context.log.info(f"[{platform}] Available columns for insert: {available_columns}")

        # Define expected columns with their transformations
        column_mappings = {
            'job_uid': 'job_uid',
            'job_id': 'job_id',
            'company_id': 'company_id',
            'platform': 'platform',
            'job_title_clean': 'job_title_clean',
            'job_description_clean': 'job_description_clean',
            'company_name_clean': 'company_name_clean',
            'location_standardized': 'location_standardized',
            'job_url': 'job_url',
            'date_posted': 'TRY_CAST(date_posted AS DATE) as date_posted',
            'date_retrieved': 'TRY_CAST(date_retrieved AS TIMESTAMP_NTZ) as date_retrieved',
            'is_active': 'is_active',
            'employment_status': 'employment_status',
            'department': 'department',  # Optional column
            'detected_language': 'detected_language',
            'language_confidence': 'language_confidence',
            'is_english': 'is_english',
            'language_detection_method': 'language_detection_method',
            'platform_specific_data': 'TRY_PARSE_JSON(platform_specific_data) as platform_specific_data',
            'data_quality_score': 'data_quality_score',
            'source_raw_table': 'source_raw_table',
            'raw_data': 'TRY_PARSE_JSON(raw_data) as raw_data',
            'partition_date': 'TRY_CAST(partition_date AS DATE) as partition_date'
        }

        # Build select clause only for columns that exist
        select_columns = []
        insert_columns = []

        for col_name, col_expr in column_mappings.items():
            if col_name in available_columns:
                select_columns.append(col_expr)
                insert_columns.append(col_name)
            else:
                # Handle missing optional columns with NULL
                if col_name == 'department':
                    select_columns.append('NULL as department')
                    insert_columns.append(col_name)
                    context.log.warning(f"[{platform}] Column '{col_name}' not found, using NULL")

        # Add transformation_timestamp (always included)
        select_columns.append('CURRENT_TIMESTAMP as transformation_timestamp')
        insert_columns.append('transformation_timestamp')

        # Build the SELECT clause with proper formatting
        select_clause = ',\n            '.join(select_columns)

        insert_sql = f"""
        INSERT INTO {database_name}.{stage_schema}.jobs_unified ({', '.join(insert_columns)})
        SELECT
            {select_clause}
        FROM {database_name}.{stage_schema}.{temp_table_name}
        """

        cursor.execute(insert_sql)
        final_count = cursor.rowcount
        conn.commit()

        # Clean up temporary table
        cursor.execute(f"DROP TABLE {database_name}.{stage_schema}.{temp_table_name}")
        conn.commit()

        context.log.info(f"[{platform}] Successfully inserted {final_count} jobs with explicit timestamp casting")
        return final_count

    finally:
        cursor.close()


def get_platform_statistics(
    platform: str,
    conn,
    database_name: str = "BETTERJOBS_DB",
    stage_schema: str = "STAGE"
) -> Dict[str, Any]:
    """Get statistics for a specific platform from the stage table."""
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
        SELECT
            COUNT(*) as job_count,
            COUNT(DISTINCT company_id) as company_count,
            COUNT(DISTINCT job_uid) as unique_uids,
            AVG(data_quality_score) as avg_quality_score
        FROM {database_name}.{stage_schema}.jobs_unified
        WHERE platform = %s AND partition_date = %s
        """, (platform, date.today()))

        result = cursor.fetchone()
        if result:
            return {
                "job_count": result[0],
                "company_count": result[1],
                "unique_uids": result[2],
                "avg_quality_score": float(result[3]) if result[3] else 0.0
            }
        else:
            return {
                "job_count": 0,
                "company_count": 0,
                "unique_uids": 0,
                "avg_quality_score": 0.0
            }
    finally:
        cursor.close()


def clear_platform_partition(
    platform: str,
    conn,
    context: AssetExecutionContext,
    database_name: str = "BETTERJOBS_DB",
    stage_schema: str = "STAGE"
):
    """Clear existing data for a platform's today partition."""
    cursor = conn.cursor()
    try:
        delete_sql = f"""
        DELETE FROM {database_name}.{stage_schema}.jobs_unified
        WHERE partition_date = %s AND platform = %s
        """
        cursor.execute(delete_sql, (date.today(), platform))
        deleted_count = cursor.rowcount
        conn.commit()
        context.log.info(f"[{platform}] Cleared {deleted_count} existing records from today's partition")
    finally:
        cursor.close()


def check_cross_platform_conflicts(
    platform_df: pd.DataFrame,
    platform: str,
    conn,
    context: AssetExecutionContext,
    database_name: str = "BETTERJOBS_DB",
    stage_schema: str = "STAGE"
) -> Dict[str, Any]:
    """
    Check if any job_uids in the platform data would conflict with existing data from other platforms.

    Since job_uid is a deterministic hash of the composite key (job_id + platform + company_id + date_posted),
    a job_uid conflict indicates the exact same job appearing across platforms, which is a true duplicate.

    Returns:
        Dict with conflict analysis results
    """
    if platform_df.empty or 'job_uid' not in platform_df.columns:
        return {"conflicts_found": False, "conflict_count": 0, "conflicting_job_uids": []}

    # Get job_uids that this platform is about to insert
    platform_job_uids = platform_df['job_uid'].tolist()

    cursor = conn.cursor()
    try:
        if platform_job_uids:
            # Create a temp table with platform job_uids for efficient checking
            placeholders = ','.join(['%s'] * len(platform_job_uids))

            cursor.execute(f"""
            SELECT DISTINCT job_uid, platform, job_id, company_id
            FROM {database_name}.{stage_schema}.jobs_unified
            WHERE job_uid IN ({placeholders})
            AND platform != %s
            AND partition_date = %s
            """, platform_job_uids + [platform, date.today()])

            existing_conflicts = cursor.fetchall()

            if existing_conflicts:
                conflicting_job_uids = [row[0] for row in existing_conflicts]
                conflicting_details = {
                    row[0]: {
                        "existing_platform": row[1],
                        "job_id": row[2],
                        "company_id": row[3]
                    } for row in existing_conflicts
                }

                context.log.warning(f"[{platform}] Found {len(conflicting_job_uids)} job_uids that already exist in other platforms:")
                for job_uid in conflicting_job_uids[:5]:  # Show first 5
                    details = conflicting_details[job_uid]
                    context.log.warning(f"  job_uid '{job_uid}' (job_id: {details['job_id']}, company: {details['company_id']}) already exists in platform '{details['existing_platform']}'")

                return {
                    "conflicts_found": True,
                    "conflict_count": len(conflicting_job_uids),
                    "conflicting_job_uids": conflicting_job_uids[:10],  # Limit for metadata
                    "conflicting_details": {k: v for k, v in list(conflicting_details.items())[:10]}
                }
            else:
                context.log.info(f"[{platform}] No cross-platform job_uid conflicts detected")
                return {"conflicts_found": False, "conflict_count": 0, "conflicting_job_uids": []}
        else:
            return {"conflicts_found": False, "conflict_count": 0, "conflicting_job_uids": []}

    finally:
        cursor.close()