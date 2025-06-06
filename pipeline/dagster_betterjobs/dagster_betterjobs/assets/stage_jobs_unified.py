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


class StageJobsUnifiedConfig(Config):
    """Configuration for stage jobs unified processing."""
    process_language_detection: bool = True
    language_confidence_threshold: float = 0.8
    include_non_english: bool = False
    batch_size: int = 1000
    max_records: Optional[int] = None
    platforms_to_process: list = ["bamboohr", "greenhouse", "workday", "smartrecruiters"]


@asset(
    group_name="stage_layer",
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
            -- Core identifiers
            job_id STRING PRIMARY KEY,
            company_id STRING,
            platform STRING,

            -- Standardized core fields
            job_title_clean STRING,
            job_description_clean STRING,
            company_name_clean STRING,
            location_standardized STRING,
            job_url STRING,

            -- Date fields
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

        # Data quality validation
        context.log.info("Performing data quality validation...")

        # Basic quality checks
        quality_issues = []

        # Check for missing critical fields
        critical_fields = ['job_id', 'job_title_clean', 'platform', 'company_id']
        for field in critical_fields:
            missing_count = combined_df[field].isna().sum()
            if missing_count > 0:
                quality_issues.append(f"{field}: {missing_count} missing values")

        # Detailed duplicate analysis
        context.log.info("Analyzing duplicates...")

        # Check duplicates by platform
        for platform in config.platforms_to_process:
            platform_data = combined_df[combined_df['platform'] == platform]
            platform_duplicates = platform_data['job_id'].duplicated().sum()
            if platform_duplicates > 0:
                context.log.warning(f"Platform {platform}: {platform_duplicates} duplicate job_ids within platform")

        # Check cross-platform duplicates
        job_id_counts = combined_df['job_id'].value_counts()
        cross_platform_duplicates = job_id_counts[job_id_counts > 1]
        if len(cross_platform_duplicates) > 0:
            context.log.warning(f"Cross-platform duplicates: {len(cross_platform_duplicates)} job_ids appear in multiple platforms")
            # Log sample of cross-platform duplicates
            sample_duplicates = cross_platform_duplicates.head(5)
            for job_id, count in sample_duplicates.items():
                platforms_with_job = combined_df[combined_df['job_id'] == job_id]['platform'].unique()
                context.log.warning(f"  job_id '{job_id}' appears {count} times in platforms: {list(platforms_with_job)}")

        # Check for duplicate job_ids (total)
        duplicate_jobs = combined_df['job_id'].duplicated().sum()
        if duplicate_jobs > 0:
            quality_issues.append(f"Duplicate job_ids: {duplicate_jobs}")

            # Before removing duplicates, let's understand the impact
            unique_jobs_count = combined_df['job_id'].nunique()
            context.log.warning(f"Total jobs: {len(combined_df)}, Unique job_ids: {unique_jobs_count}, Duplicates to remove: {duplicate_jobs}")

            # Use job_id + platform + company_id combination for deduplication to handle overlapping job ID schemas
            # This prevents different companies from having their jobs incorrectly deduplicated
            combined_df = combined_df.drop_duplicates(subset=['job_id', 'platform', 'company_id'], keep='first')
            context.log.info(f"After deduplication (job_id + platform + company_id): {len(combined_df)} jobs remaining")

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
            'job_id', 'company_id', 'platform', 'job_title_clean', 'job_description_clean',
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
        # Handle data types for Snowflake
        for col in filtered_df.select_dtypes(include=['object']).columns:
            if col not in ['date_posted', 'date_retrieved', 'partition_date']:
                filtered_df[col] = filtered_df[col].fillna('').astype(str)

        # Handle date columns (DATE type in Snowflake)
        date_columns = ['date_posted', 'partition_date']
        for col in date_columns:
            if col in filtered_df.columns:
                filtered_df[col] = pd.to_datetime(filtered_df[col], errors='coerce')
                filtered_df[col] = filtered_df[col].dt.date
                filtered_df[col] = filtered_df[col].where(pd.notnull(filtered_df[col]), None)

        # Handle timestamp columns (TIMESTAMP_NTZ type in Snowflake)
        timestamp_columns = ['date_retrieved']
        for col in timestamp_columns:
            if col in filtered_df.columns:
                filtered_df[col] = pd.to_datetime(filtered_df[col], errors='coerce')
                # Keep as datetime for TIMESTAMP_NTZ, don't convert to date
                filtered_df[col] = filtered_df[col].where(pd.notnull(filtered_df[col]), None)

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

        # Use write_pandas for bulk insert
        success, num_chunks, num_rows, output = write_pandas(
            conn,
            filtered_df,
            'jobs_unified',
            database=database_name,
            schema=stage_schema,
            auto_create_table=False,
            overwrite=False,
            quote_identifiers=False
        )

        if success:
            stats["jobs_loaded"] = num_rows
            context.log.info(f"Successfully loaded {num_rows} jobs to STAGE.jobs_unified")
        else:
            raise Exception(f"Failed to load data to Snowflake: {output}")

        stats["processing_end"] = datetime.now().isoformat()

        # Final statistics query
        cursor.execute(f"""
        SELECT
            platform,
            COUNT(*) as job_count,
            COUNT(DISTINCT company_id) as company_count,
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
                "avg_quality_score": float(row[3]) if row[3] else 0.0
            }

        stats["platform_summary"] = platform_summary

        context.log.info(f"Successfully loaded {stats['jobs_loaded']} unified jobs to STAGE layer")

        # Add Dagster metadata
        context.add_output_metadata({
            "total_raw_jobs": MetadataValue.int(stats["total_raw_jobs"]),
            "jobs_processed": MetadataValue.int(stats["jobs_processed"]),
            "jobs_loaded": MetadataValue.int(stats["jobs_loaded"]),
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