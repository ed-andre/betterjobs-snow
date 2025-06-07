"""
Stage Jobs Unified Asset

This asset creates a unified, cleaned, and standardized view of all job data
from the RAW layer platforms (BambooHR, Greenhouse, Workday, SmartRecruiters).

It combines data from all platform-specific RAW tables and applies:
- Text cleaning and standardization
- Language detection and filtering
- Platform-specific field mapping
- Data quality validation

Output: Clean, standardized unified job data ready for LLM processing in Phase 2.
"""

import pandas as pd
import json
from datetime import datetime, date
from typing import Dict, Any, Optional

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from dagster import (
    asset,
    AssetExecutionContext,
    MetadataValue,
    AssetMaterialization,
    Config
)

from dagster_betterjobs.transformations.text_cleaning import clean_text_fields, clean_job_description
from dagster_betterjobs.transformations.language_detection import LanguageDetector
from dagster_betterjobs.transformations.platform_mapping import PlatformMapper
from dagster_betterjobs.transformations.uid_generation import add_job_uids_to_dataframe, validate_uid_uniqueness, get_uid_collision_report


class StageJobsUnifiedConfig(Config):
    """Configuration for stage jobs unified processing."""
    process_language_detection: bool = True
    language_confidence_threshold: float = 0.8
    include_non_english: bool = True
    batch_size: int = 1000
    max_records: Optional[int] = None
    platforms_to_process: list = ["bamboohr", "greenhouse", "workday", "smartrecruiters"]


@asset(
    group_name="stage_cleansing_enrichment_validation_transformation",
    kinds={"snowflake", "python", "transformation"},
    required_resource_keys={"snowflake"},
    deps=[
        "bamboohr_company_jobs_discovery",
        "greenhouse_company_jobs_discovery",
        "workday_company_jobs_discovery",
        "smartrecruiters_company_jobs_discovery"
    ],
    description="Unified, cleaned, and standardized job data from all RAW platforms"
)
def stage_jobs_unified(context: AssetExecutionContext, config: StageJobsUnifiedConfig) -> Dict[str, Any]:
    """
    Create unified stage jobs table by combining and transforming all RAW platform data.

    This asset:
    1. Loads data from all RAW platform tables
    2. Applies text cleaning and standardization
    3. Performs language detection and filtering
    4. Maps platform-specific fields to unified schema
    5. Validates data quality
    6. Loads to Snowflake STAGE.JOBS_UNIFIED table

    Returns:
        Dict with processing statistics and metadata
    """

    # Initialize transformation modules
    language_detector = LanguageDetector(
        confidence_threshold=config.language_confidence_threshold
    )
    platform_mapper = PlatformMapper()

    # Get Snowflake connection using the existing pattern
    conn = context.resources.snowflake.get_connection()
    database_name = "BETTERJOBS_DB"
    raw_schema = "RAW"
    stage_schema = "STAGE"

    # Statistics tracking
    stats = {
        "total_raw_jobs": 0,
        "jobs_processed": 0,
        "jobs_loaded": 0,
        "english_jobs": 0,
        "non_english_jobs": 0,
        "failed_jobs": 0,
        "platforms_processed": [],
        "uid_validation": {},
        "processing_start": datetime.now().isoformat()
    }

    cursor = None

    try:
        # Create STAGE schema if it doesn't exist
        cursor = conn.cursor()
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database_name}.{stage_schema}")

        # Create unified jobs table if it doesn't exist (don't recreate every time)
        create_unified_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {database_name}.{stage_schema}.jobs_unified (
            -- Generated unique identifier (replaces composite primary key)
            job_uid STRING PRIMARY KEY,

            -- Core identifiers
            job_id STRING,
            company_id STRING,
            platform STRING,

            -- Standardized core fields
            job_title_clean STRING,
            job_description_clean STRING,
            company_name_clean STRING,
            location_standardized STRING,
            job_url STRING,

            -- Date fields (using TRY_CAST for robust string-to-timestamp conversion)
            date_posted DATE,
            date_retrieved TIMESTAMP_NTZ,

            -- Status and classification
            is_active BOOLEAN DEFAULT TRUE,
            employment_status STRING,
            department STRING,

            -- Language detection
            detected_language STRING,
            language_confidence FLOAT,
            is_english BOOLEAN,
            language_detection_method STRING,

            -- Platform-specific data (preserved as JSON)
            platform_specific_data VARIANT,

            -- Quality and metadata
            transformation_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
            data_quality_score FLOAT,

            -- Source tracking
            source_raw_table STRING,
            raw_data VARIANT,

            -- Partitioning
            partition_date DATE
        )
        """

        cursor.execute(create_unified_table_sql)
        conn.commit()
        context.log.info("Created/verified STAGE.jobs_unified table")

        # Process each platform
        all_unified_jobs = []

        for platform in config.platforms_to_process:
            context.log.info(f"Processing platform: {platform}")

            # Load RAW data for this platform
            raw_table_name = f"{platform}_jobs"

            try:
                # Query RAW data with JOIN to get company_name
                query_sql = f"""
                SELECT
                    j.*,
                    m.company_name
                FROM {database_name}.{raw_schema}.{raw_table_name} j
                LEFT JOIN {database_name}.{raw_schema}.master_company_urls m
                    ON j.company_id = m.company_id
                WHERE j.is_active = TRUE
                """

                if config.max_records:
                    query_sql += f" LIMIT {config.max_records}"

                cursor.execute(query_sql)
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

                if not results:
                    context.log.warning(f"No data found in {raw_table_name}")
                    continue

                # Convert to DataFrame
                raw_df = pd.DataFrame(results, columns=columns)
                stats["total_raw_jobs"] += len(raw_df)

                context.log.info(f"Loaded {len(raw_df)} jobs from {platform}")
                context.log.info(f"Available columns: {list(raw_df.columns)}")

                # Normalize column names to lowercase for consistent processing
                # Snowflake returns uppercase column names, but our transformations expect lowercase
                raw_df.columns = raw_df.columns.str.lower()
                context.log.info(f"Normalized columns: {list(raw_df.columns)}")

                # Apply text cleaning
                context.log.info("Applying text cleaning...")
                cleaned_df = clean_text_fields(raw_df)

                context.log.info(f"Columns after text cleaning: {list(cleaned_df.columns)}")

                # Ensure required columns exist for language detection
                if 'job_description_clean' not in cleaned_df.columns:
                    context.log.warning(f"job_description_clean column not found after text cleaning for {platform}")
                    # Check if we have job_description column
                    if 'job_description' in cleaned_df.columns:
                        context.log.info("Creating job_description_clean from job_description")
                        cleaned_df['job_description_clean'] = cleaned_df['job_description'].apply(
                            lambda x: clean_job_description(x) if pd.notnull(x) else ""
                        )
                    else:
                        context.log.warning(f"No job_description column found in {platform} data, creating empty job_description_clean")
                        cleaned_df['job_description_clean'] = ""

                # Apply language detection if enabled
                if config.process_language_detection:
                    context.log.info("Performing language detection...")
                    cleaned_df = language_detector.process_dataframe(cleaned_df, 'job_description_clean')

                    # Filter English jobs if configured
                    if not config.include_non_english:
                        english_jobs = len(cleaned_df[cleaned_df['is_english'] == True])
                        non_english_jobs = len(cleaned_df[cleaned_df['is_english'] == False])

                        stats["english_jobs"] += english_jobs
                        stats["non_english_jobs"] += non_english_jobs

                        cleaned_df = cleaned_df[cleaned_df['is_english'] == True].copy()

                        context.log.info(f"Language filtering: {english_jobs} English, {non_english_jobs} non-English jobs")

                # Apply platform field mapping
                context.log.info("Applying platform field mapping...")
                unified_df = platform_mapper.map_platform_data(cleaned_df, platform)

                # Generate deterministic UIDs for true uniqueness
                context.log.info("Generating deterministic UIDs...")
                unified_df = add_job_uids_to_dataframe(unified_df, uid_column='job_uid')

                # Validate UID uniqueness for this platform
                uid_validation = validate_uid_uniqueness(unified_df, uid_column='job_uid')
                stats["uid_validation"][platform] = uid_validation

                if not uid_validation['is_valid']:
                    context.log.warning(f"UID collisions detected in {platform}: {uid_validation['duplicate_count']} duplicates")
                    # Log sample collision UIDs
                    if uid_validation['duplicate_uids']:
                        sample_collisions = uid_validation['duplicate_uids'][:5]
                        context.log.warning(f"Sample collision UIDs: {sample_collisions}")
                else:
                    context.log.info(f"All {uid_validation['unique_uids']} UIDs are unique for {platform}")

                # Add partition date
                unified_df['partition_date'] = date.today()

                all_unified_jobs.append(unified_df)
                stats["jobs_processed"] += len(unified_df)
                stats["platforms_processed"].append(platform)

                context.log.info(f"Successfully processed {len(unified_df)} jobs from {platform}")

            except Exception as e:
                context.log.error(f"Error processing platform {platform}: {str(e)}")
                stats["failed_jobs"] += 1
                continue

        # Combine all platform data
        if not all_unified_jobs:
            raise ValueError("No jobs were successfully processed from any platform")

        combined_df = pd.concat(all_unified_jobs, ignore_index=True)
        context.log.info(f"Combined {len(combined_df)} jobs from all platforms")

        # Cross-platform UID validation
        context.log.info("Performing cross-platform UID validation...")
        final_uid_validation = validate_uid_uniqueness(combined_df, uid_column='job_uid')
        stats["uid_validation"]["cross_platform"] = final_uid_validation

        if not final_uid_validation['is_valid']:
            context.log.error(f"Cross-platform UID collisions detected: {final_uid_validation['duplicate_count']} duplicates")

            # Add detailed logging to debug the collision
            context.log.error("=== UID COLLISION DEBUGGING ===")

            # Get collision report
            collision_report = get_uid_collision_report(combined_df, uid_column='job_uid')

            if not collision_report.empty:
                context.log.error(f"Collision report shape: {collision_report.shape}")

                # Group by UID to see what records share the same UID
                for uid in final_uid_validation['duplicate_uids'][:5]:  # Show first 5 collision UIDs
                    context.log.error(f"\n--- Collision UID: {uid} ---")

                    # Get all records with this UID
                    collision_records = combined_df[combined_df['job_uid'] == uid]

                    for idx, record in collision_records.iterrows():
                        # Log the composite key components that generated this UID
                        job_id = record.get('job_id', 'NULL')
                        platform = record.get('platform', 'NULL')
                        company_id = record.get('company_id', 'NULL')
                        date_posted = record.get('date_posted', 'NULL')

                        # Recreate the composite key to see what generated this UID
                        job_id_str = str(job_id).strip() if job_id else "NULL_JOB_ID"
                        platform_str = str(platform).lower().strip() if platform else "NULL_PLATFORM"
                        company_id_str = str(company_id).strip() if company_id else "NULL_COMPANY"

                        if date_posted:
                            if isinstance(date_posted, (datetime, date)):
                                date_str = date_posted.strftime("%Y-%m-%d")
                            else:
                                date_str = str(date_posted).strip()
                        else:
                            date_str = "NULL_DATE"

                        composite_key = f"JOB_ID:{job_id_str}|PLATFORM:{platform_str}|COMPANY:{company_id_str}|DATE:{date_str}"

                        context.log.error(f"  Record {idx}:")
                        context.log.error(f"    job_id: '{job_id}' (type: {type(job_id)})")
                        context.log.error(f"    platform: '{platform}' (type: {type(platform)})")
                        context.log.error(f"    company_id: '{company_id}' (type: {type(company_id)})")
                        context.log.error(f"    date_posted: '{date_posted}' (type: {type(date_posted)})")
                        context.log.error(f"    composite_key: '{composite_key}'")

                        # Also show job title for context
                        if 'job_title_clean' in record:
                            context.log.error(f"    job_title: '{record.get('job_title_clean', 'N/A')}'")

            context.log.error("=== END UID COLLISION DEBUGGING ===")

            # This should not happen with deterministic UID generation, so it's an error
            raise ValueError(f"UID collision detected across platforms: {final_uid_validation['duplicate_uids'][:10]}")
        else:
            context.log.info(f"✅ All {final_uid_validation['unique_uids']} cross-platform UIDs are unique")

        # Data quality validation
        context.log.info("Performing data quality validation...")

        # Basic quality checks
        quality_issues = []

        # Check for missing critical fields
        critical_fields = ['job_id', 'job_uid', 'job_title_clean', 'platform', 'company_id']
        for field in critical_fields:
            missing_count = combined_df[field].isna().sum()
            if missing_count > 0:
                quality_issues.append(f"{field}: {missing_count} missing values")

        # Detailed duplicate analysis
        context.log.info("Analyzing duplicates...")

        # Check duplicates by platform (using the same deduplication key we'll use later)
        for platform in config.platforms_to_process:
            platform_data = combined_df[combined_df['platform'] == platform]
            if len(platform_data) > 0:
                # Count duplicates based on actual deduplication logic (job_id + platform + company_id)
                platform_duplicates = platform_data.duplicated(subset=['job_id', 'platform', 'company_id']).sum()
                total_platform_jobs = len(platform_data)
                unique_platform_jobs = len(platform_data.drop_duplicates(subset=['job_id', 'platform', 'company_id']))

                if platform_duplicates > 0:
                    context.log.warning(f"Platform {platform}: {platform_duplicates} duplicate jobs (job_id + platform + company_id) out of {total_platform_jobs} total")
                else:
                    context.log.info(f"Platform {platform}: No duplicates detected ({total_platform_jobs} unique jobs)")

        # Check for actual cross-platform duplicates (same job_id across different platforms)
        job_id_platform_counts = combined_df.groupby('job_id')['platform'].nunique()
        cross_platform_job_ids = job_id_platform_counts[job_id_platform_counts > 1]

        if len(cross_platform_job_ids) > 0:
            context.log.warning(f"True cross-platform duplicates: {len(cross_platform_job_ids)} job_ids appear across multiple platforms")
            # Log sample of actual cross-platform duplicates
            sample_cross_platform = cross_platform_job_ids.head(5)
            for job_id in sample_cross_platform.index:
                platforms_with_job = combined_df[combined_df['job_id'] == job_id]['platform'].unique()
                job_count = len(combined_df[combined_df['job_id'] == job_id])
                if len(platforms_with_job) > 1:  # Only log if truly cross-platform
                    context.log.warning(f"  job_id '{job_id}' appears {job_count} times across platforms: {list(platforms_with_job)}")
        else:
            context.log.info("No cross-platform duplicates detected (good - job_ids are unique across platforms)")

        # Check for actual duplicates that will be removed (using our deduplication logic)
        initial_count = len(combined_df)
        duplicate_mask = combined_df.duplicated(subset=['job_id', 'platform', 'company_id'])
        actual_duplicates_to_remove = duplicate_mask.sum()

        if actual_duplicates_to_remove > 0:
            quality_issues.append(f"Duplicate jobs (job_id + platform + company_id): {actual_duplicates_to_remove}")

            context.log.warning(f"Deduplication summary: {initial_count} total jobs, {actual_duplicates_to_remove} duplicates to remove")

            # Perform the actual deduplication
            combined_df = combined_df.drop_duplicates(subset=['job_id', 'platform', 'company_id'], keep='first')
            final_count = len(combined_df)

            context.log.info(f"After deduplication (job_id + platform + company_id): {final_count} jobs remaining")

            # Verify the math
            expected_final = initial_count - actual_duplicates_to_remove
            if final_count != expected_final:
                context.log.error(f"Deduplication math error: expected {expected_final}, got {final_count}")
        else:
            context.log.info(f"No duplicates to remove - all {initial_count} jobs are unique by (job_id + platform + company_id)")

        # Calculate overall data quality score
        total_fields = len(combined_df.columns)
        combined_df['data_quality_score'] = combined_df.apply(
            lambda row: (total_fields - row.isna().sum()) / total_fields, axis=1
        )

        # Log quality issues
        if quality_issues:
            context.log.warning(f"Data quality issues found: {quality_issues}")

        # Load to Snowflake STAGE table
        context.log.info("Loading unified data to Snowflake...")

        # Clear existing data for today's partition
        delete_sql = f"""
        DELETE FROM {database_name}.{stage_schema}.jobs_unified
        WHERE partition_date = %s
        """
        cursor.execute(delete_sql, (date.today(),))
        conn.commit()

        # Debug: Show final DataFrame columns before loading
        context.log.info(f"Final DataFrame columns before Snowflake load: {list(combined_df.columns)}")
        context.log.info(f"DataFrame shape: {combined_df.shape}")

        # Ensure only expected columns are included
        expected_columns = [
            'job_id', 'job_uid', 'company_id', 'platform', 'job_title_clean', 'job_description_clean',
            'company_name_clean', 'location_standardized', 'job_url', 'date_posted', 'date_retrieved',
            'is_active', 'employment_status', 'department', 'detected_language', 'language_confidence',
            'is_english', 'language_detection_method', 'platform_specific_data', 'data_quality_score',
            'source_raw_table', 'raw_data', 'partition_date'
        ]

        # Filter DataFrame to only include expected columns (if they exist)
        available_columns = [col for col in expected_columns if col in combined_df.columns]
        filtered_df = combined_df[available_columns].copy()

        context.log.info(f"Filtered DataFrame columns: {list(filtered_df.columns)}")

        # Use write_pandas for efficient bulk loading
        # Handle data types for Snowflake - NO DATE/TIMESTAMP CONVERSIONS
        # Let Snowflake handle all date/timestamp conversions natively during write_pandas
        for col in filtered_df.select_dtypes(include=['object']).columns:
            if col not in ['date_posted', 'date_retrieved', 'partition_date']:
                filtered_df[col] = filtered_df[col].fillna('').astype(str)

        # DO NOT CONVERT DATE/TIMESTAMP COLUMNS - preserve as-is from Snowflake
        # Snowflake's write_pandas will handle DATE and TIMESTAMP_NTZ conversions correctly
        # Previous pandas datetime conversions were corrupting the timestamp data

        # Set partition_date for today's partition (simple date object)
        if 'partition_date' in filtered_df.columns:
            filtered_df['partition_date'] = date.today()

        # Debug: Log sample timestamp values to verify they're preserved correctly
        if 'date_retrieved' in filtered_df.columns:
            sample_retrieved = filtered_df['date_retrieved'].dropna().head(3).tolist()
            context.log.info(f"Sample date_retrieved values before write_pandas: {sample_retrieved}")
            context.log.info(f"Sample date_retrieved types: {[type(v) for v in sample_retrieved]}")

            # Fix for UNIX timestamp corruption issue - convert to string first
            # This prevents pandas from misinterpreting the timestamp format
            context.log.info("Converting date_retrieved to string to prevent timestamp corruption")
            filtered_df['date_retrieved'] = filtered_df['date_retrieved'].astype(str)

            # Log sample converted values
            sample_converted = filtered_df['date_retrieved'].dropna().head(3).tolist()
            context.log.info(f"Sample date_retrieved values after string conversion: {sample_converted}")

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

        # Alternative approach: Use explicit SQL INSERT with TRY_CAST for robust timestamp handling
        # This completely avoids pandas timestamp issues and handles UNIX timestamp corruption

        context.log.info("Using explicit SQL INSERT with timestamp casting instead of write_pandas")

        # Create temporary table for bulk insert
        temp_table_name = f"temp_stage_jobs_{int(datetime.now().timestamp())}"

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

        context.log.info(f"Loaded {num_rows} rows to temporary table {temp_table_name}")

        # Now insert from temp table to final table with explicit timestamp casting
        insert_sql = f"""
        INSERT INTO {database_name}.{stage_schema}.jobs_unified
        SELECT
            job_uid,
            job_id,
            company_id,
            platform,
            job_title_clean,
            job_description_clean,
            company_name_clean,
            location_standardized,
            job_url,
            TRY_CAST(date_posted AS DATE) as date_posted,
            TRY_CAST(date_retrieved AS TIMESTAMP_NTZ) as date_retrieved,
            is_active,
            employment_status,
            department,
            detected_language,
            language_confidence,
            is_english,
            language_detection_method,
            TRY_PARSE_JSON(platform_specific_data) as platform_specific_data,
            CURRENT_TIMESTAMP as transformation_timestamp,
            data_quality_score,
            source_raw_table,
            TRY_PARSE_JSON(raw_data) as raw_data,
            TRY_CAST(partition_date AS DATE) as partition_date
        FROM {database_name}.{stage_schema}.{temp_table_name}
        """

        cursor.execute(insert_sql)
        final_count = cursor.rowcount
        conn.commit()

        # Clean up temporary table
        cursor.execute(f"DROP TABLE {database_name}.{stage_schema}.{temp_table_name}")
        conn.commit()

        context.log.info(f"Successfully inserted {final_count} jobs with explicit timestamp casting")
        stats["jobs_loaded"] = final_count

        stats["processing_end"] = datetime.now().isoformat()

        # Final statistics query
        cursor.execute(f"""
        SELECT
            platform,
            COUNT(*) as job_count,
            COUNT(DISTINCT company_id) as company_count,
            COUNT(DISTINCT job_uid) as unique_uids,
            AVG(data_quality_score) as avg_quality_score
        FROM {database_name}.{stage_schema}.jobs_unified
        WHERE partition_date = %s
        GROUP BY platform
        """, (date.today(),))

        platform_stats = cursor.fetchall()
        platform_summary = {}
        for row in platform_stats:
            platform_summary[row[0]] = {
                "job_count": row[1],
                "company_count": row[2],
                "unique_uids": row[3],
                "avg_quality_score": float(row[4]) if row[4] else 0.0
            }

        stats["platform_summary"] = platform_summary

        context.log.info(f"Successfully loaded {stats['jobs_loaded']} unified jobs to STAGE layer")

        # Add Dagster metadata
        context.add_output_metadata({
            "total_raw_jobs": MetadataValue.int(stats["total_raw_jobs"]),
            "jobs_processed": MetadataValue.int(stats["jobs_processed"]),
            "jobs_loaded": MetadataValue.int(stats["jobs_loaded"]),
            "unique_uids_generated": MetadataValue.int(final_uid_validation.get('unique_uids', 0)),
            "uid_collision_free": MetadataValue.bool(final_uid_validation.get('is_valid', False)),
            "english_jobs": MetadataValue.int(stats["english_jobs"]),
            "non_english_jobs": MetadataValue.int(stats["non_english_jobs"]),
            "platforms_processed": MetadataValue.text(", ".join(stats["platforms_processed"])),
            "avg_data_quality": MetadataValue.float(
                sum(p["avg_quality_score"] for p in platform_summary.values()) / len(platform_summary)
                if platform_summary else 0.0
            ),
            "snowflake_table": MetadataValue.text(f"{database_name}.{stage_schema}.jobs_unified"),
            "partition_date": MetadataValue.text(str(date.today()))
        })

    except Exception as e:
        context.log.error(f"Error in stage_jobs_unified processing: {str(e)}")
        stats["error"] = str(e)
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return stats