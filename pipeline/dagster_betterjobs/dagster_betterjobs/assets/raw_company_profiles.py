import pandas as pd
import hashlib
import boto3
import os
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from dagster import asset, AssetExecutionContext, Config, MetadataValue
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


def generate_profile_id(company_name: str) -> str:
    """
    Generate a stable, unique profile ID from company name.
    Uses first 8 characters of SHA-256 hash to create a compact ID.
    """
    # Normalize company name (lowercase, remove extra spaces)
    normalized_name = " ".join(company_name.lower().split())
    # Generate hash
    hash_object = hashlib.sha256(normalized_name.encode())
    # Take first 8 characters for a compact but still unique ID
    return hash_object.hexdigest()[:8]


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
            STORAGE_INTEGRATION = betterjobs_s3_integration
            URL = '{s3_uri_clean}'
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_DELIMITER = ','
                RECORD_DELIMITER = '\\n'
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                ESCAPE_UNENCLOSED_FIELD = '\\\\'
            );
            """
            cursor.execute(stage_sql)

        # Create the company profiles table
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            profile_id STRING PRIMARY KEY,
            company_name STRING NOT NULL,
            company_industry STRING,
            employee_count_range STRING,
            city STRING,  -- US headquarters city

            -- File metadata
            source_file STRING,
            file_hash STRING,
            ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,

            -- Raw data preservation
            raw_data STRING
        );
        """
        cursor.execute(create_table_sql)

        # Create file processing log table for tracking
        log_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name}_file_log (
            file_path STRING PRIMARY KEY,
            file_hash STRING,
            file_size NUMBER,
            last_modified TIMESTAMP_NTZ,
            processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
            records_processed NUMBER,
            status STRING
        );
        """
        cursor.execute(log_table_sql)

        conn.commit()

    except Exception as e:
        raise Exception(f"Failed to set up Snowflake stage and table: {str(e)}")
    finally:
        cursor.close()


def get_processed_files(conn, log_table: str, context: AssetExecutionContext = None) -> Dict[str, Dict]:
    """Get list of already processed files with their metadata."""
    cursor = conn.cursor()
    processed_files = {}

    try:
        cursor.execute("USE DATABASE BETTERJOBS_DB")
        cursor.execute("USE SCHEMA RAW")

        cursor.execute(f"SELECT file_path, file_hash, processed_at, records_processed, status FROM {log_table}")
        results = cursor.fetchall()

        for file_path, file_hash, processed_at, records_processed, status in results:
            processed_files[file_path] = {
                'hash': file_hash,
                'processed_at': processed_at,
                'records_processed': records_processed,
                'status': status
            }

        if context:
            context.log.info(f"Found {len(processed_files)} previously processed files")

    except Exception as e:
        if context:
            context.log.warning(f"Could not get processed files (table may not exist yet): {str(e)}")
    finally:
        cursor.close()

    return processed_files


def calculate_file_hash(file_path: str) -> str:
    """Calculate SHA-256 hash of file contents."""
    hash_sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()


def should_process_file(file_info: Dict, processed_files: Dict, context: AssetExecutionContext, conn=None, table_name: str = None) -> Tuple[bool, str]:
    """
    Determine if a file should be processed based on various criteria.
    Returns (should_process, reason)
    """
    file_path = file_info['path']
    file_size = file_info['size']
    file_hash = file_info.get('hash', 'unknown')

    # Check if file was previously processed
    if file_path not in processed_files:
        return True, "New file - never processed"

    prev_info = processed_files[file_path]
    prev_status = prev_info.get('status')
    prev_records = prev_info.get('records_processed')

    # Check if previous processing failed
    if prev_status == 'failed':
        return True, "Previous processing failed - retrying"

    # Check if previous processing resulted in no data
    if prev_status == 'no_data':
        return True, "Previous processing resulted in no data - retrying"

    # Check if previous processing resulted in 0 records (handle None values)
    if prev_records is None or prev_records == 0:
        return True, f"Previous processing resulted in {prev_records} records - retrying"

    # Check if file is marked as successful but no data actually exists in table
    if prev_status == 'success' and prev_records and prev_records > 0:
        if conn and table_name:
            try:
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE source_file = %s", (file_path,))
                actual_count = cursor.fetchone()[0]
                cursor.close()

                if actual_count == 0:
                    return True, f"Marked as successful with {prev_records} records, but no data exists in table - retrying"
                elif actual_count != prev_records:
                    return True, f"Record count mismatch - logged: {prev_records}, actual: {actual_count} - retrying"
            except Exception as e:
                context.log.warning(f"Could not verify data existence for {file_path}: {str(e)}")

    # Check if file content has changed (using hash comparison)
    if file_hash != 'unknown' and prev_info.get('hash') != file_hash:
        return True, f"File content changed - hash mismatch (old: {prev_info.get('hash')[:8]}..., new: {file_hash[:8]}...)"

    # Check if file size changed significantly (fallback if no hash)
    if file_hash == 'unknown' and abs(file_size - prev_info.get('file_size', 0)) > 100:  # 100 byte threshold
        return True, f"File size changed significantly (old: {prev_info.get('file_size', 0)}, new: {file_size})"

    # Only skip if status is 'success' and we have actual records
    if prev_status == 'success' and prev_records and prev_records > 0:
        return False, "File already processed successfully"

    # For any other case, err on the side of reprocessing
    return True, f"Uncertain status (status: {prev_status}, records: {prev_records}) - retrying"


def get_s3_file_info(s3_files: List, context: AssetExecutionContext) -> List[Dict]:
    """
    Extract and normalize S3 file information.
    """
    file_info_list = []

    for file_info in s3_files:
        file_path = file_info[0]  # File name/path
        file_size = file_info[1] if len(file_info) > 1 else 0  # File size
        file_hash_s3 = file_info[2] if len(file_info) > 2 else 'unknown'  # MD5 from S3
        last_modified_raw = file_info[3] if len(file_info) > 3 else None

        # Skip non-CSV files
        if not file_path.lower().endswith('.csv'):
            context.log.info(f"Skipping non-CSV file: {file_path}")
            continue

        # Extract just the filename from the full path for stage operations
        filename_only = file_path.split('/')[-1] if '/' in file_path else file_path

        # Convert last_modified to a format Snowflake can parse
        last_modified_formatted = None
        if last_modified_raw:
            try:
                # Parse the S3 timestamp format: 'Thu, 5 Jun 2025 17:00:03 GMT'
                parsed_dt = datetime.strptime(last_modified_raw, '%a, %d %b %Y %H:%M:%S %Z')
                # Format as ISO timestamp that Snowflake can parse
                last_modified_formatted = parsed_dt.strftime('%Y-%m-%d %H:%M:%S')
            except Exception as e:
                context.log.warning(f"Could not parse timestamp '{last_modified_raw}': {str(e)}")
                last_modified_formatted = None

        file_info_list.append({
            'path': file_path,  # Keep full path for logging/identification
            'filename': filename_only,  # Just filename for stage operations
            'size': file_size,
            'hash': file_hash_s3,
            'last_modified': last_modified_formatted
        })

    return file_info_list


def process_s3_csv_files(
    conn,
    s3_uri: str,
    stage_name: str,
    table_name: str,
    context: AssetExecutionContext
) -> List[Dict]:
    """Process CSV files from S3 using Snowflake COPY command."""
    cursor = conn.cursor()
    processing_results = []

    try:
        cursor.execute("USE DATABASE BETTERJOBS_DB")
        cursor.execute("USE SCHEMA RAW")

        # List files in the S3 stage
        cursor.execute(f"LIST @{stage_name}")
        s3_files = cursor.fetchall()

        if not s3_files:
            context.log.warning(f"No files found in S3 stage {stage_name}")
            return processing_results

        context.log.info(f"Found {len(s3_files)} files in S3 stage")

        # Get file information
        file_info_list = get_s3_file_info(s3_files, context)

        if not file_info_list:
            context.log.warning("No CSV files found to process")
            return processing_results

        # Get already processed files
        processed_files = get_processed_files(conn, f"{table_name}_file_log", context)

        for file_info in file_info_list:
            file_path = file_info['path']
            filename = file_info['filename']
            file_size = file_info['size']
            file_hash = file_info['hash']
            last_modified = file_info['last_modified']

            # Determine if file should be processed
            should_process, reason = should_process_file(file_info, processed_files, context, conn, table_name)

            if not should_process:
                context.log.info(f"Skipping {file_path}: {reason}")
                continue

            context.log.info(f"Processing S3 file: {file_path} - {reason}")

            try:
                # Create temporary staging table for this file
                temp_table = f"{table_name}_temp_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

                # Create temporary table with flexible schema
                cursor.execute(f"""
                CREATE TEMPORARY TABLE {temp_table} (
                    company_name STRING,
                    company_industry STRING,
                    employee_count_range STRING,
                    city STRING,
                    raw_row STRING
                );
                """)

                # Copy data from S3 to temporary table
                copy_sql = f"""
                COPY INTO {temp_table} (company_name, company_industry, employee_count_range, city, raw_row)
                FROM (
                    SELECT
                        $1::STRING,
                        $2::STRING,
                        $3::STRING,
                        $4::STRING,
                        CONCAT($1, '|', $2, '|', $3, '|', $4)
                    FROM @{stage_name}/{filename}
                )
                FILE_FORMAT = (
                    TYPE = 'CSV'
                    FIELD_DELIMITER = ','
                    RECORD_DELIMITER = '\\n'
                    SKIP_HEADER = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                    ESCAPE_UNENCLOSED_FIELD = '\\\\'
                    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
                );
                """

                context.log.info(f"Executing COPY command for {file_path} (filename: {filename})")

                cursor.execute(copy_sql)
                copy_result = cursor.fetchone()

                # Parse Snowflake COPY result correctly
                # Format: (file, status, rows_parsed, rows_loaded, error_limit, errors_seen, first_error, first_error_line, first_error_character, first_error_column_name)
                if copy_result:
                    file_name = copy_result[0] if len(copy_result) > 0 else "unknown"
                    status = copy_result[1] if len(copy_result) > 1 else "unknown"
                    rows_parsed = copy_result[2] if len(copy_result) > 2 else 0
                    rows_loaded = copy_result[3] if len(copy_result) > 3 else 0
                    error_limit = copy_result[4] if len(copy_result) > 4 else 0
                    errors_seen = copy_result[5] if len(copy_result) > 5 else 0
                    first_error = copy_result[6] if len(copy_result) > 6 else None

                    context.log.info(f"COPY operation - Status: {status}, Rows parsed: {rows_parsed}, Rows loaded: {rows_loaded}, Errors: {errors_seen}")

                    if first_error:
                        context.log.warning(f"First error encountered: {first_error}")
                else:
                    rows_loaded = 0
                    context.log.warning("No result returned from COPY command")

                # If no rows loaded, log warning but don't run extensive diagnostics
                if rows_loaded == 0:
                    context.log.warning(f"No rows loaded from {file_path}")

                if rows_loaded > 0:
                    # If this is a reprocessing, clean up old records first
                    if file_path in processed_files:
                        context.log.info(f"Removing old records for {file_path} before reprocessing")
                        cursor.execute(f"DELETE FROM {table_name} WHERE source_file = %s", (file_path,))
                        deleted_rows = cursor.rowcount
                        context.log.info(f"Deleted {deleted_rows} old records")

                    # Insert into main table with transformations
                    insert_sql = f"""
                    INSERT INTO {table_name} (
                        profile_id,
                        company_name,
                        company_industry,
                        employee_count_range,
                        city,
                        source_file,
                        file_hash,
                        raw_data
                    )
                    SELECT DISTINCT
                        -- Generate profile_id from company name
                        SUBSTR(SHA2(LOWER(TRIM(company_name)), 256), 1, 8) as profile_id,
                        TRIM(company_name) as company_name,
                        NULLIF(TRIM(company_industry), '') as company_industry,
                        NULLIF(TRIM(employee_count_range), '') as employee_count_range,
                        NULLIF(TRIM(city), '') as city,
                        '{file_path}' as source_file,
                        '{file_hash}' as file_hash,
                        raw_row as raw_data
                    FROM {temp_table}
                    WHERE company_name IS NOT NULL
                    AND TRIM(company_name) != ''
                    """

                    cursor.execute(insert_sql)
                    inserted_rows = cursor.rowcount

                    # Update or insert processing log
                    upsert_log_sql = f"""
                    MERGE INTO {table_name}_file_log AS target
                    USING (
                        SELECT %s as file_path,
                               %s as file_hash,
                               %s as file_size,
                               %s as last_modified,
                               %s as records_processed,
                               %s as status,
                               CURRENT_TIMESTAMP as processed_at
                    ) AS source
                    ON target.file_path = source.file_path
                    WHEN MATCHED THEN
                        UPDATE SET
                            file_hash = source.file_hash,
                            file_size = source.file_size,
                            last_modified = source.last_modified,
                            records_processed = source.records_processed,
                            status = source.status,
                            processed_at = source.processed_at
                    WHEN NOT MATCHED THEN
                        INSERT (file_path, file_hash, file_size, last_modified, records_processed, status, processed_at)
                        VALUES (source.file_path, source.file_hash, source.file_size, source.last_modified,
                               source.records_processed, source.status, source.processed_at);
                    """

                    cursor.execute(upsert_log_sql, (
                        file_path,
                        file_hash,
                        file_size,
                        last_modified,
                        inserted_rows,
                        'success'
                    ))

                    processing_results.append({
                        'file_path': file_path,
                        'rows_loaded': rows_loaded,
                        'rows_inserted': inserted_rows,
                        'status': 'success'
                    })

                    context.log.info(f"Successfully processed {file_path}: {inserted_rows} records inserted")

                else:
                    context.log.warning(f"No data loaded from {file_path}")

                    # Log as failed due to no data
                    upsert_log_sql = f"""
                    MERGE INTO {table_name}_file_log AS target
                    USING (
                        SELECT %s as file_path,
                               %s as file_hash,
                               %s as file_size,
                               %s as status,
                               CURRENT_TIMESTAMP as processed_at
                    ) AS source
                    ON target.file_path = source.file_path
                    WHEN MATCHED THEN
                        UPDATE SET
                            file_hash = source.file_hash,
                            file_size = source.file_size,
                            status = source.status,
                            processed_at = source.processed_at
                    WHEN NOT MATCHED THEN
                        INSERT (file_path, file_hash, file_size, status, processed_at)
                        VALUES (source.file_path, source.file_hash, source.file_size, source.status, source.processed_at);
                    """

                    cursor.execute(upsert_log_sql, (
                        file_path,
                        file_hash,
                        file_size,
                        'no_data'
                    ))

                # Clean up temporary table
                cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
                conn.commit()

            except Exception as file_error:
                error_msg = str(file_error)
                context.log.error(f"Failed to process {file_path}: {error_msg}")

                # Log failed processing
                try:
                    upsert_log_sql = f"""
                    MERGE INTO {table_name}_file_log AS target
                    USING (
                        SELECT %s as file_path,
                               %s as file_hash,
                               %s as file_size,
                               %s as status,
                               CURRENT_TIMESTAMP as processed_at
                    ) AS source
                    ON target.file_path = source.file_path
                    WHEN MATCHED THEN
                        UPDATE SET
                            file_hash = source.file_hash,
                            file_size = source.file_size,
                            status = source.status,
                            processed_at = source.processed_at
                    WHEN NOT MATCHED THEN
                        INSERT (file_path, file_hash, file_size, status, processed_at)
                        VALUES (source.file_path, source.file_hash, source.file_size, source.status, source.processed_at);
                    """
                    cursor.execute(upsert_log_sql, (
                        file_path,
                        file_hash,
                        file_size,
                        'failed'
                    ))
                    conn.commit()
                except:
                    pass  # Don't fail the whole process if logging fails

                processing_results.append({
                    'file_path': file_path,
                    'rows_loaded': 0,
                    'rows_inserted': 0,
                    'status': 'failed',
                    'error': error_msg
                })

    except Exception as e:
        context.log.error(f"Error processing S3 files: {str(e)}")
        raise
    finally:
        cursor.close()

    return processing_results


def handle_profile_duplicates(conn, table_name: str, context: AssetExecutionContext) -> Dict:
    """Handle potential profile_id duplicates by deduplication."""
    cursor = conn.cursor()
    stats = {"duplicates_found": 0, "duplicates_resolved": 0}

    try:
        cursor.execute("USE DATABASE BETTERJOBS_DB")
        cursor.execute("USE SCHEMA RAW")

        # Check for profile_id duplicates
        cursor.execute(f"""
        SELECT profile_id, COUNT(*) as count,
               LISTAGG(DISTINCT company_name, ', ') as company_names
        FROM {table_name}
        GROUP BY profile_id
        HAVING COUNT(*) > 1
        """)

        duplicates = cursor.fetchall()
        stats["duplicates_found"] = len(duplicates)

        if duplicates:
            context.log.warning(f"Found {len(duplicates)} profile_id duplicates")

            for dup in duplicates:
                profile_id, count, company_names = dup
                context.log.info(f"Resolving duplicate profile_id {profile_id} for companies: {company_names}")

                # Keep the most recent record (by ingested_at)
                cursor.execute(f"""
                DELETE FROM {table_name}
                WHERE profile_id = %s
                AND ingested_at NOT IN (
                    SELECT MAX(ingested_at)
                    FROM {table_name}
                    WHERE profile_id = %s
                )
                """, (profile_id, profile_id))

                removed_count = cursor.rowcount
                stats["duplicates_resolved"] += removed_count
                context.log.info(f"Removed {removed_count} duplicate records for profile_id {profile_id}")

            conn.commit()
            context.log.info(f"Resolved {stats['duplicates_resolved']} duplicate records")

    except Exception as e:
        context.log.error(f"Error handling duplicates: {str(e)}")
    finally:
        cursor.close()

    return stats


class RawCompanyProfilesConfig(Config):
    """Configuration for raw company profiles asset."""
    s3_uri: Optional[str] = os.getenv("S3_URI_COMPANIES_PROFILE")
    stage_name: str = "raw_company_profiles_stage"
    table_name: str = "raw_company_profiles"


@asset(
    group_name="raw_ingestion_extraction",
    kinds={"python", "sql", "snowflake"},
    required_resource_keys={"snowflake"}
)
def raw_company_profiles(
    context: AssetExecutionContext,
    config: RawCompanyProfilesConfig
) -> pd.DataFrame:
    """
    Ingest master company profiles CSV from S3 into RAW layer.

    This asset processes the master_company_profiles.csv file from S3 containing:
    - company_name: Name of the company
    - company_industry: Industry classification
    - employee_count_range: Employee count range (e.g., "1001-5000")
    - city: US headquarters city location

    The data is loaded into raw_company_profiles table with:
    - Automatic profile_id generation from company name
    - File tracking to avoid reprocessing
    - Duplicate handling
    - Raw data preservation for lineage
    """

    # Get Snowflake connection
    conn = context.resources.snowflake.get_connection()

    try:
        # Setup stage and table
        setup_snowflake_stage_and_table(
            conn=conn,
            stage_name=config.stage_name,
            s3_uri=config.s3_uri,
            table_name=config.table_name
        )

        # Process S3 files
        s3_results = []
        if config.s3_uri:
            context.log.info(f"Processing S3 files from: {config.s3_uri}")
            s3_results = process_s3_csv_files(
                conn=conn,
                s3_uri=config.s3_uri,
                stage_name=config.stage_name,
                table_name=config.table_name,
                context=context
            )

        # Handle duplicates
        duplicate_stats = handle_profile_duplicates(conn, config.table_name, context)

        # Get final data for return
        cursor = conn.cursor()
        cursor.execute("USE DATABASE BETTERJOBS_DB")
        cursor.execute("USE SCHEMA RAW")

        cursor.execute(f"""
        SELECT profile_id, company_name, company_industry, employee_count_range, city,
               source_file, ingested_at
        FROM {config.table_name}
        ORDER BY ingested_at DESC
        LIMIT 1000
        """)

        # Convert to DataFrame
        columns = ['profile_id', 'company_name', 'company_industry', 'employee_count_range',
                  'city', 'source_file', 'ingested_at']
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)

        # Get summary statistics
        cursor.execute(f"SELECT COUNT(*) FROM {config.table_name}")
        total_records = cursor.fetchone()[0]

        cursor.execute(f"""
        SELECT COUNT(DISTINCT company_name) as unique_companies,
               COUNT(DISTINCT company_industry) as unique_industries,
               COUNT(CASE WHEN city IS NOT NULL THEN 1 END) as records_with_city
        FROM {config.table_name}
        """)
        stats = cursor.fetchone()

        cursor.close()

        # Prepare metadata
        metadata = {
            "total_records": MetadataValue.int(total_records),
            "unique_companies": MetadataValue.int(stats[0]),
            "unique_industries": MetadataValue.int(stats[1]),
            "records_with_city": MetadataValue.int(stats[2]),
            "s3_files_processed": MetadataValue.int(len([r for r in s3_results if r['status'] == 'success'])),
            "duplicates_found": MetadataValue.int(duplicate_stats['duplicates_found']),
            "duplicates_resolved": MetadataValue.int(duplicate_stats['duplicates_resolved']),
            "sample_data": MetadataValue.md(df.head(10).to_markdown() if not df.empty else "No data")
        }

        context.add_output_metadata(metadata)

        context.log.info(f"Successfully processed company profiles: {total_records} total records")
        context.log.info(f"Unique companies: {stats[0]}, Industries: {stats[1]}, With city: {stats[2]}")

        return df

    except Exception as e:
        context.log.error(f"Failed to process company profiles: {str(e)}")
        raise
    finally:
        conn.close()