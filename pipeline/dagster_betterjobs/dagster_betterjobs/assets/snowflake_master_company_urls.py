import pandas as pd
import hashlib
import boto3
import os
import glob
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from dagster import asset, AssetExecutionContext, Config, MetadataValue
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


def generate_company_id(company_name: str) -> str:
    """
    Generate a stable, unique company ID from company name.
    Uses first 8 characters of SHA-256 hash to create a compact ID.
    """
    # Normalize company name (lowercase, remove extra spaces)
    normalized_name = " ".join(company_name.lower().split())
    # Generate hash
    hash_object = hashlib.sha256(normalized_name.encode())
    # Take first 8 characters for a compact but still unique ID
    return hash_object.hexdigest()[:8]


def get_file_metadata(file_path: str) -> Dict:
    """Get file metadata including size and last modified time."""
    if not os.path.exists(file_path):
        return {}

    stat = os.stat(file_path)
    return {
        'size': stat.st_size,
        'modified_time': datetime.fromtimestamp(stat.st_mtime),
        'path': file_path
    }


def setup_snowflake_stage_and_table(conn, stage_name: str, s3_uri: str, table_name: str):
    """Set up Snowflake stage for S3 and create table if not exists."""
    cursor = conn.cursor()

    try:
        # Ensure we're using the correct database and schema
        cursor.execute("USE DATABASE BETTERJOBS_DB")
        cursor.execute("USE SCHEMA RAW")

        # Create S3 stage if it doesn't exist
        if s3_uri:
            # Ensure S3 URI ends with / for proper path handling
            s3_uri_clean = s3_uri.rstrip('/') + '/'

            stage_sql = f"""
            CREATE STAGE IF NOT EXISTS {stage_name}
            URL = '{s3_uri_clean}'
            STORAGE_INTEGRATION = betterjobs_s3_integration
            FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
            """
            cursor.execute(stage_sql)

        # Create table if it doesn't exist (always create regardless of S3)
        table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            company_id STRING,
            company_name STRING NOT NULL,
            company_industry STRING,
            platform STRING,
            ats_url STRING,
            career_url STRING,
            url_verified BOOLEAN DEFAULT FALSE,
            date_added TIMESTAMP_NTZ,
            last_updated TIMESTAMP_NTZ,
            source_file STRING,
            file_hash STRING,
            ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
        cursor.execute(table_sql)

        # Create processing log table to track files
        log_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name}_processing_log (
            file_path STRING,
            file_hash STRING,
            file_size INTEGER,
            file_modified_time TIMESTAMP_NTZ,
            processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            record_count INTEGER,
            source_type STRING
        )
        """
        cursor.execute(log_table_sql)

        conn.commit()
    finally:
        cursor.close()


def setup_snowflake_tables_only(conn, table_name: str):
    """Set up Snowflake tables without S3 stage."""
    cursor = conn.cursor()

    try:
        # Ensure we're using the correct database and schema
        cursor.execute("USE DATABASE BETTERJOBS_DB")
        cursor.execute("USE SCHEMA RAW")

        # Create table if it doesn't exist
        table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            company_id STRING,
            company_name STRING NOT NULL,
            company_industry STRING,
            platform STRING,
            ats_url STRING,
            career_url STRING,
            url_verified BOOLEAN DEFAULT FALSE,
            date_added TIMESTAMP_NTZ,
            last_updated TIMESTAMP_NTZ,
            source_file STRING,
            file_hash STRING,
            ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
        cursor.execute(table_sql)

        # Create processing log table to track files
        log_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name}_processing_log (
            file_path STRING,
            file_hash STRING,
            file_size INTEGER,
            file_modified_time TIMESTAMP_NTZ,
            processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            record_count INTEGER,
            source_type STRING
        )
        """
        cursor.execute(log_table_sql)

        conn.commit()

        # Verify table was created
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        tables = cursor.fetchall()
        if tables:
            print(f"✓ Table {table_name} created successfully in BETTERJOBS_DB.RAW")
        else:
            print(f"✗ Table {table_name} was not created")

    finally:
        cursor.close()


def get_processed_files(conn, log_table: str, context: AssetExecutionContext = None) -> Dict[str, Dict]:
    """Get list of already processed files with their metadata."""
    cursor = conn.cursor()
    try:
        # Ensure we're in the correct schema context
        cursor.execute("USE DATABASE BETTERJOBS_DB")
        cursor.execute("USE SCHEMA RAW")

        # Check if log table exists first
        cursor.execute(f"SHOW TABLES LIKE '{log_table}'")
        if not cursor.fetchall():
            if context:
                context.log.info(f"Processing log table {log_table} does not exist yet - treating all files as new")
            return {}

        cursor.execute(f"SELECT file_path, file_hash, file_size, file_modified_time FROM {log_table}")
        results = cursor.fetchall()
        return {
            row[0]: {
                'hash': row[1],
                'size': row[2],
                'modified_time': row[3]
            } for row in results
        }
    except Exception as e:
        # If there's any error accessing the log table, assume no files have been processed
        if context:
            context.log.warning(f"Could not access processing log table {log_table}: {str(e)} - treating all files as new")
        return {}
    finally:
        cursor.close()


def calculate_file_hash(file_path: str) -> str:
    """Calculate SHA-256 hash of a file."""
    hash_sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()


def process_local_csv_files(
    conn,
    local_folder: str,
    table_name: str,
    context: AssetExecutionContext
) -> Tuple[List[pd.DataFrame], List[Dict]]:
    """Process CSV files from local folder."""
    processed_dfs = []
    file_metadata_list = []

    if not os.path.exists(local_folder):
        context.log.warning(f"Local folder does not exist: {local_folder}")
        return processed_dfs, file_metadata_list

    # Get list of already processed files
    processed_files = get_processed_files(conn, f"{table_name}_processing_log", context)

    # Find CSV files in the folder
    csv_files = glob.glob(os.path.join(local_folder, "*.csv"))
    context.log.info(f"Found {len(csv_files)} CSV files in local folder")

    for csv_file in csv_files:
        file_path = os.path.abspath(csv_file)
        file_metadata = get_file_metadata(file_path)

        if not file_metadata:
            continue

        # Calculate file hash
        file_hash = calculate_file_hash(file_path)

        # Check if file needs processing
        needs_processing = True
        if file_path in processed_files:
            existing = processed_files[file_path]
            if (existing['hash'] == file_hash and
                existing['size'] == file_metadata['size']):
                needs_processing = False
                context.log.info(f"Skipping already processed file: {file_path}")

        if needs_processing:
            try:
                # Read CSV file
                df = pd.read_csv(file_path)
                if not df.empty:
                    # Convert timestamp columns if they exist in CSV
                    timestamp_columns = ['date_added', 'last_updated']
                    for col in timestamp_columns:
                        if col in df.columns:
                            # Convert timezone-aware timestamps to timezone-naive for Snowflake
                            df[col] = pd.to_datetime(df[col], errors='coerce').dt.tz_localize(None)

                    # Add source metadata
                    df['source_file'] = os.path.basename(file_path)
                    df['file_hash'] = file_hash
                    processed_dfs.append(df)

                    # Track file metadata
                    file_metadata_list.append({
                        'file_path': file_path,
                        'file_hash': file_hash,
                        'file_size': file_metadata['size'],
                        'file_modified_time': file_metadata['modified_time'],
                        'record_count': len(df),
                        'source_type': 'local'
                    })

                    context.log.info(f"Processed local file: {file_path} ({len(df)} records)")

            except Exception as e:
                context.log.error(f"Error processing file {file_path}: {str(e)}")

    return processed_dfs, file_metadata_list


def process_s3_csv_files(
    conn,
    s3_uri: str,
    stage_name: str,
    table_name: str,
    context: AssetExecutionContext
) -> List[Dict]:
    """Process CSV files from S3 using Snowflake stages."""
    file_metadata_list = []

    if not s3_uri:
        context.log.warning("S3_URI not provided, skipping S3 processing")
        return file_metadata_list

    cursor = conn.cursor()
    try:
        # Ensure we're in the correct schema context
        cursor.execute("USE DATABASE BETTERJOBS_DB")
        cursor.execute("USE SCHEMA RAW")

        # List files in the S3 stage
        cursor.execute(f"LIST @{stage_name}")
        s3_files = cursor.fetchall()

        context.log.info(f"Found {len(s3_files)} files in S3 stage")

        # Get list of already processed files
        processed_files = get_processed_files(conn, f"{table_name}_processing_log", context)
        context.log.info(f"Found {len(processed_files)} files in processing log")

        for file_info in s3_files:
            file_name = file_info[0]  # File name from LIST command
            file_size = file_info[1]  # File size
            file_modified = file_info[2]  # Last modified

            if not file_name.lower().endswith('.csv'):
                context.log.info(f"Skipping non-CSV file: {file_name}")
                continue

            # Handle file path properly for stage reference
            # The stage path should be relative to the stage URL
            if '/' in file_name:
                # If the file name contains path separators, use just the filename
                actual_filename = file_name.split('/')[-1]
                stage_file_path = actual_filename
            else:
                # If it's just a filename, use as-is
                actual_filename = file_name
                stage_file_path = file_name

            context.log.info(f"Will use stage file path: '{stage_file_path}' for file: '{actual_filename}'")

            # Generate a hash based on file attributes
            file_hash = hashlib.sha256(f"{file_name}_{file_size}_{file_modified}".encode()).hexdigest()

            # Check if file needs processing
            needs_processing = True
            s3_file_path = f"s3://{file_name}" if not file_name.startswith('s3://') else file_name
            if s3_file_path in processed_files:
                existing = processed_files[s3_file_path]
                if existing['hash'] == file_hash:
                    needs_processing = False
                    context.log.info(f"Skipping already processed S3 file: {file_name}")
                else:
                    context.log.info(f"S3 file {file_name} hash changed, reprocessing")
            else:
                context.log.info(f"Processing new S3 file: {file_name}")

            if needs_processing:
                try:
                    # First, let's examine the file structure to understand the columns
                    try:
                        cursor.execute(f"SELECT $1, $2, $3 FROM @{stage_name}/{stage_file_path} LIMIT 2")
                        sample_rows = cursor.fetchall()
                    except Exception as examine_error:
                        context.log.warning(f"Could not examine file structure for {file_name}: {str(examine_error)}")
                        # Try without subdirectory
                        try:
                            cursor.execute(f"SELECT $1, $2, $3 FROM @{stage_name}/{actual_filename} LIMIT 2")
                            sample_rows = cursor.fetchall()
                            stage_file_path = actual_filename  # Update to working path
                        except Exception as examine_error2:
                            context.log.error(f"Failed to examine file {file_name} with both paths: {str(examine_error2)}")
                            continue

                    # Skip processing if the file looks problematic
                    if not sample_rows:
                        context.log.warning(f"No data found in S3 file {file_name}, skipping")
                        continue

                    # Create a temporary table that matches the actual CSV structure
                    # CSV format: company_id,company_name,company_industry,platform,ats_url,career_url,url_verified,date_added,last_updated
                    temp_table = f"{table_name}_temp_s3"
                    cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")

                    # Create temp table with proper column structure matching CSV
                    create_temp_sql = f"""
                    CREATE OR REPLACE TABLE {temp_table} (
                        company_id STRING,
                        company_name STRING,
                        company_industry STRING,
                        platform STRING,
                        ats_url STRING,
                        career_url STRING,
                        url_verified STRING,
                        date_added STRING,
                        last_updated STRING,
                        source_file STRING,
                        file_hash STRING
                    )
                    """
                    cursor.execute(create_temp_sql)
                    context.log.info(f"Created temp table: {temp_table}")

                    # Use COPY command that properly maps CSV columns to temp table columns
                    # CSV columns: company_id,company_name,company_industry,platform,ats_url,career_url,url_verified,date_added,last_updated
                    copy_sql = f"""
                    COPY INTO {temp_table} (
                        company_id, company_name, company_industry, platform,
                        ats_url, career_url, url_verified, date_added, last_updated,
                        source_file, file_hash
                    )
                    FROM (
                        SELECT
                            $1 as company_id,
                            $2 as company_name,
                            $3 as company_industry,
                            $4 as platform,
                            $5 as ats_url,
                            $6 as career_url,
                            $7 as url_verified,
                            $8 as date_added,
                            $9 as last_updated,
                            '{actual_filename}' as source_file,
                            '{file_hash}' as file_hash
                        FROM @{stage_name}/{stage_file_path}
                    )
                    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
                    ON_ERROR = 'CONTINUE'
                    """

                    cursor.execute(copy_sql)

                    # Check for COPY errors
                    cursor.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))")
                    copy_results = cursor.fetchall()

                    # Get record count and validate data
                    cursor.execute(f"SELECT COUNT(*) FROM {temp_table}")
                    record_count = cursor.fetchone()[0]

                    if record_count > 0:
                        # Validate the data structure before proceeding
                        cursor.execute(f"SELECT company_id, company_name, company_industry, platform FROM {temp_table} LIMIT 5")
                        validation_rows = cursor.fetchall()

                        # Check if company_name looks reasonable (not empty, not just numbers)
                        cursor.execute(f"SELECT COUNT(*) FROM {temp_table} WHERE company_name IS NOT NULL AND LENGTH(TRIM(company_name)) > 0")
                        valid_company_names = cursor.fetchone()[0]

                        if valid_company_names == 0:
                            context.log.error(f"No valid company names found in {file_name}, skipping insertion")
                            cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
                            continue

                        context.log.info(f"Inserting {record_count} records from {file_name} into main table")

                        # Insert into main table, using existing company_id from CSV
                        insert_sql = f"""
                        INSERT INTO {table_name} (
                            company_id, company_name, company_industry, platform,
                            ats_url, career_url, url_verified,
                            date_added, last_updated, source_file, file_hash
                        )
                        SELECT
                            TRIM(company_id) as company_id,
                            TRIM(company_name) as company_name,
                            TRIM(company_industry) as company_industry,
                            TRIM(platform) as platform,
                            NULLIF(TRIM(ats_url), '') as ats_url,
                            NULLIF(TRIM(career_url), '') as career_url,
                            CASE
                                WHEN UPPER(TRIM(url_verified)) IN ('TRUE', '1', 'YES', 'Y') THEN TRUE
                                ELSE FALSE
                            END as url_verified,
                            TRY_TO_TIMESTAMP(date_added) as date_added,
                            TRY_TO_TIMESTAMP(last_updated) as last_updated,
                            source_file,
                            file_hash
                        FROM {temp_table}
                        WHERE company_name IS NOT NULL
                        AND LENGTH(TRIM(company_name)) > 0
                        AND company_name NOT LIKE '%company_name%'  -- Skip header rows that might have slipped through
                        AND company_id IS NOT NULL
                        AND LENGTH(TRIM(company_id)) > 0
                        """
                        cursor.execute(insert_sql)
                        context.log.info(f"Successfully inserted records from {file_name}")

                        # Track file metadata
                        file_metadata_list.append({
                            'file_path': s3_file_path,
                            'file_hash': file_hash,
                            'file_size': file_size,
                            'file_modified_time': datetime.now(),
                            'record_count': record_count,
                            'source_type': 's3'
                        })

                        context.log.info(f"Processed S3 file: {file_name} ({record_count} records)")
                    else:
                        context.log.warning(f"S3 file {file_name} resulted in 0 records")

                    # Clean up temp table
                    cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")

                except Exception as e:
                    context.log.error(f"Error processing S3 file {file_name}: {str(e)}")
                    # Clean up on error
                    try:
                        cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
                    except:
                        pass

        conn.commit()

    finally:
        cursor.close()

    return file_metadata_list


class SnowflakeMasterCompanyUrlsConfig(Config):
    """Configuration for Snowflake master company URLs asset."""
    batch_size: int = 1000
    enable_s3_processing: bool = True
    enable_local_processing: bool = True
    deduplicate_on_load: bool = True


@asset(
    group_name="company_urls_snowflake",
    kinds={"python", "sql", "snowflake"},
    required_resource_keys={"snowflake"}
)
def snowflake_master_company_urls(
    context: AssetExecutionContext,
    config: SnowflakeMasterCompanyUrlsConfig
) -> pd.DataFrame:
    """
    Creates and maintains a master table of company URLs in Snowflake RAW schema.

    This asset ingests CSV files from both S3 bucket (via S3_URI) and local folder
    (via MAIN_INPUT_FOLDER) with incremental processing to detect new/changed files.

    Features:
    - Uses Snowflake stages for efficient S3 data loading
    - Tracks processed files to avoid reprocessing
    - Generates stable company IDs using SHA-256 hash
    - Supports both S3 and local CSV sources
    - Incremental processing based on file hashes and metadata
    - Stores data in RAW schema for further processing
    """
    # Get environment variables
    s3_uri = os.getenv("S3_URI")
    local_folder = os.getenv("MAIN_INPUT_FOLDER")

    # Validate that at least one data source is configured
    if not s3_uri and not local_folder:
        raise ValueError("At least one data source must be configured: S3_URI or MAIN_INPUT_FOLDER")

    # Get Snowflake connection
    conn = context.resources.snowflake.get_connection()

    # Configuration
    stage_name = "COMPANY_URLS_STAGE"
    table_name = "master_company_urls"

    context.log.info(f"Processing company URLs from S3: {s3_uri or 'Not configured'}, Local: {local_folder or 'Not configured'}")

    # Ensure RAW schema exists
    cursor = conn.cursor()
    try:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS RAW")
        conn.commit()
    finally:
        cursor.close()

    # Always set up tables first
    setup_snowflake_tables_only(conn, table_name)
    context.log.info("Created Snowflake tables")

    # Set up S3 stage if enabled and configured
    s3_stage_ready = False
    if config.enable_s3_processing and s3_uri:
        try:
            setup_snowflake_stage_and_table(conn, stage_name, s3_uri, table_name)
            s3_stage_ready = True
            context.log.info("S3 stage setup completed successfully")
        except Exception as e:
            context.log.error(f"S3 stage setup failed: {str(e)}")
            if config.enable_local_processing:
                context.log.info("Will continue with local processing only")
                s3_stage_ready = False
            else:
                raise ValueError(f"S3 processing failed and local processing is disabled: {str(e)}")

    # Track statistics
    stats = {
        "files_processed": 0,
        "records_added": 0,
        "s3_files": 0,
        "local_files": 0,
        "skipped_files": 0,
        "errors": 0
    }

    all_file_metadata = []

    # STEP 1: Process S3 files FIRST (if enabled and configured)
    if config.enable_s3_processing and s3_uri and s3_stage_ready:
        context.log.info("=== STEP 1: Processing S3 files ===")
        try:
            s3_metadata = process_s3_csv_files(conn, s3_uri, stage_name, table_name, context)
            all_file_metadata.extend(s3_metadata)
            stats["s3_files"] = len(s3_metadata)
            context.log.info(f"✓ Successfully processed {len(s3_metadata)} S3 files")
        except Exception as e:
            context.log.error(f"✗ Error processing S3 files: {str(e)}")
            stats["errors"] += 1
    elif config.enable_s3_processing and s3_uri and not s3_stage_ready:
        context.log.warning("S3 processing was enabled but stage setup failed - skipping S3 processing")
    elif config.enable_s3_processing and not s3_uri:
        context.log.info("S3 processing enabled but S3_URI not configured - skipping S3 processing")
    else:
        context.log.info("S3 processing disabled")

    # STEP 2: Process local files SECOND (if enabled and configured)
    local_dfs = []
    if config.enable_local_processing and local_folder:
        context.log.info("=== STEP 2: Processing local files ===")
        try:
            local_dfs, local_metadata = process_local_csv_files(conn, local_folder, table_name, context)
            all_file_metadata.extend(local_metadata)
            stats["local_files"] = len(local_metadata)
            context.log.info(f"✓ Successfully processed {len(local_metadata)} local files")
        except Exception as e:
            context.log.error(f"✗ Error processing local files: {str(e)}")
            stats["errors"] += 1
    elif config.enable_local_processing and not local_folder:
        context.log.info("Local processing enabled but MAIN_INPUT_FOLDER not configured - skipping local processing")
    else:
        context.log.info("Local processing disabled")

    # STEP 3: Process local dataframes if any were collected
    if local_dfs:
        context.log.info("=== STEP 3: Loading local data to Snowflake ===")
        # Combine all local dataframes
        combined_df = pd.concat(local_dfs, ignore_index=True)

        # Ensure required columns exist
        required_columns = ["company_name", "company_industry", "platform", "ats_url", "career_url", "url_verified"]
        for col in required_columns:
            if col not in combined_df.columns:
                combined_df[col] = None

        # Generate company IDs if not present in local files
        if 'company_id' not in combined_df.columns:
            combined_df["company_id"] = combined_df["company_name"].apply(generate_company_id)

        # Add timestamps - only if not already present from CSV
        current_time = datetime.now()
        if 'date_added' not in combined_df.columns:
            combined_df["date_added"] = current_time
        if 'last_updated' not in combined_df.columns:
            combined_df["last_updated"] = current_time
        if 'ingested_at' not in combined_df.columns:
            combined_df["ingested_at"] = current_time

        # Handle data types
        combined_df["url_verified"] = combined_df["url_verified"].fillna(False)
        for col in combined_df.select_dtypes(include=['object']).columns:
            if col not in ['date_added', 'last_updated', 'ingested_at']:
                combined_df[col] = combined_df[col].fillna('').astype(str)

        # Convert timestamp columns to strings for Snowflake
        timestamp_columns = ['date_added', 'last_updated', 'ingested_at']
        for col in timestamp_columns:
            if col in combined_df.columns:
                combined_df[col] = combined_df[col].apply(
                    lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f') if pd.notna(x) else None
                )

        # Use bulk insert with write_pandas for much better performance
        try:
            # Ensure column order matches table schema
            column_order = [
                'company_id', 'company_name', 'company_industry', 'platform',
                'ats_url', 'career_url', 'url_verified', 'date_added', 'last_updated',
                'source_file', 'file_hash', 'ingested_at'
            ]
            combined_df = combined_df[column_order]

            # Use write_pandas for bulk insert
            success, num_chunks, num_rows, output = write_pandas(
                conn,
                combined_df,
                table_name,
                database='BETTERJOBS_DB',
                schema='RAW',
                auto_create_table=False,
                overwrite=False,
                quote_identifiers=False
            )

            if success:
                stats["records_added"] += len(combined_df)
                context.log.info(f"✓ Successfully bulk-inserted {len(combined_df)} records from local files using write_pandas")
            else:
                context.log.error(f"✗ Failed to bulk-insert records: {output}")
                stats["errors"] += 1

        except Exception as e:
            context.log.error(f"✗ Error writing local data to Snowflake: {str(e)}")
            stats["errors"] += 1
    else:
        context.log.info("No local files to process")

    # STEP 4: Update processing log for all processed files
    if all_file_metadata:
        context.log.info("=== STEP 4: Updating processing log ===")
        cursor = conn.cursor()
        try:
            for file_meta in all_file_metadata:
                # Format timestamp properly
                if isinstance(file_meta['file_modified_time'], datetime):
                    modified_time_str = file_meta['file_modified_time'].strftime('%Y-%m-%d %H:%M:%S.%f')
                else:
                    # If it's not a datetime, use current timestamp
                    modified_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

                cursor.execute(f"""
                    MERGE INTO RAW.{table_name}_processing_log AS target
                    USING (SELECT
                        %s as file_path,
                        %s as file_hash,
                        %s as file_size,
                        %s as file_modified_time,
                        %s as record_count,
                        %s as source_type
                    ) AS source
                    ON target.file_path = source.file_path
                    WHEN MATCHED THEN UPDATE SET
                        file_hash = source.file_hash,
                        file_size = source.file_size,
                        file_modified_time = source.file_modified_time,
                        processed_at = CURRENT_TIMESTAMP(),
                        record_count = source.record_count,
                        source_type = source.source_type
                    WHEN NOT MATCHED THEN INSERT (
                        file_path, file_hash, file_size, file_modified_time,
                        record_count, source_type
                    ) VALUES (
                        source.file_path, source.file_hash, source.file_size,
                        source.file_modified_time, source.record_count, source.source_type
                    )
                """, (
                    file_meta['file_path'],
                    file_meta['file_hash'],
                    file_meta['file_size'],
                    modified_time_str,
                    file_meta['record_count'],
                    file_meta['source_type']
                ))
            conn.commit()
            stats["files_processed"] = len(all_file_metadata)
            context.log.info(f"✓ Updated processing log for {len(all_file_metadata)} files")
        except Exception as e:
            context.log.error(f"✗ Error updating processing log: {str(e)}")
            stats["errors"] += 1
        finally:
            cursor.close()

    # STEP 5: Deduplicate data if enabled
    if config.deduplicate_on_load:
        context.log.info("=== STEP 5: Deduplicating data ===")
        cursor = conn.cursor()
        try:
            cursor.execute("USE DATABASE BETTERJOBS_DB")
            cursor.execute("USE SCHEMA RAW")

            # Create a deduplicated temp table
            dedup_sql = f"""
            CREATE OR REPLACE TABLE {table_name}_deduped AS
            SELECT * FROM (
                SELECT
                    company_id,
                    company_name,
                    company_industry,
                    platform,
                    ats_url,
                    career_url,
                    url_verified,
                    date_added,
                    last_updated,
                    source_file,
                    file_hash,
                    ingested_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY company_id
                        ORDER BY url_verified DESC, last_updated DESC
                    ) as rn
                FROM {table_name}
            )
            WHERE rn = 1
            """
            cursor.execute(dedup_sql)

            # Replace main table with deduplicated data
            cursor.execute(f"DROP TABLE {table_name}")
            cursor.execute(f"ALTER TABLE {table_name}_deduped RENAME TO {table_name}")

            conn.commit()
            context.log.info("✓ Deduplication completed successfully")
        except Exception as e:
            context.log.error(f"✗ Error during deduplication: {str(e)}")
            stats["errors"] += 1
        finally:
            cursor.close()

    # Get final stats
    cursor = conn.cursor()
    try:
        cursor.execute("USE DATABASE BETTERJOBS_DB")
        cursor.execute("USE SCHEMA RAW")

        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_records = cursor.fetchone()[0]

        cursor.execute(f"SELECT COUNT(DISTINCT company_id) FROM {table_name}")
        unique_companies = cursor.fetchone()[0]

        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE url_verified = TRUE")
        verified_urls = cursor.fetchone()[0]

    except Exception as e:
        context.log.error(f"Error getting final stats: {str(e)}")
        total_records = 0
        unique_companies = 0
        verified_urls = 0
    finally:
        cursor.close()

    # Log comprehensive summary
    context.log.info("=== Processing Summary ===")
    context.log.info(f"Files processed: {stats['files_processed']}")
    context.log.info(f"S3 files: {stats['s3_files']}")
    context.log.info(f"Local files: {stats['local_files']}")
    context.log.info(f"Records added this run: {stats['records_added']}")
    context.log.info(f"Total records in table: {total_records}")
    context.log.info(f"Unique companies: {unique_companies}")
    context.log.info(f"Verified URLs: {verified_urls}")
    context.log.info(f"Errors encountered: {stats['errors']}")
    context.log.info("=========================")

    # Add metadata for Dagster UI
    context.add_output_metadata({
        "files_processed": MetadataValue.int(stats["files_processed"]),
        "s3_files_processed": MetadataValue.int(stats["s3_files"]),
        "local_files_processed": MetadataValue.int(stats["local_files"]),
        "records_added": MetadataValue.int(stats["records_added"]),
        "total_records": MetadataValue.int(total_records),
        "unique_companies": MetadataValue.int(unique_companies),
        "verified_urls": MetadataValue.int(verified_urls),
        "errors": MetadataValue.int(stats["errors"]),
        "snowflake_table": MetadataValue.text(f"BETTERJOBS_DB.RAW.{table_name}"),
        "s3_enabled": MetadataValue.bool(config.enable_s3_processing and bool(s3_uri)),
        "local_enabled": MetadataValue.bool(config.enable_local_processing and bool(local_folder))
    })

    # Return summary as DataFrame for downstream assets
    summary_df = pd.DataFrame([{
        "files_processed": stats["files_processed"],
        "s3_files": stats["s3_files"],
        "local_files": stats["local_files"],
        "records_added": stats["records_added"],
        "total_records": total_records,
        "unique_companies": unique_companies,
        "verified_urls": verified_urls,
        "errors": stats["errors"],
        "processed_at": datetime.now()
    }])

    conn.close()
    return summary_df