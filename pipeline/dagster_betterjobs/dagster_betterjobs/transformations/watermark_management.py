"""
Watermark Management for Incremental Processing

Simple but efficient watermark system to enable incremental processing
in the stage processing layer. Tracks last successful processing timestamp
per platform to avoid full table reprocessing.

Key features:
- Per-platform watermark tracking
- Automatic cushion/overlap for late-arriving data
- Fallback to full refresh when needed
- Partition-aware validation to prevent data gaps
- Simple SQL-based implementation
"""

from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import pandas as pd

from dagster import AssetExecutionContext


class WatermarkConfig:
    """Configuration for watermark-based incremental processing."""

    def __init__(
        self,
        enable_incremental: bool = True,
        overlap_hours: int = 2,
        force_full_refresh: bool = False,
        max_incremental_days: int = 7,
        min_expected_prefixes: int = 15  # Minimum distinct company name prefixes expected
    ):
        self.enable_incremental = enable_incremental
        self.overlap_hours = overlap_hours  # Safety cushion for late-arriving data
        self.force_full_refresh = force_full_refresh
        self.max_incremental_days = max_incremental_days  # Max days to look back incrementally
        self.min_expected_prefixes = min_expected_prefixes  # Partition failure detection threshold


def validate_partition_completeness(
    platform: str,
    watermark_candidate: datetime,
    conn,
    context: AssetExecutionContext,
    config: WatermarkConfig,
    database_name: str = "BETTERJOBS_DB",
    stage_schema: str = "STAGE"
) -> bool:
    """
    Validate that the upstream discovery asset completed successfully for ALL partitions.

    This prevents using incremental processing when some company partitions failed,
    which would create data gaps.

    Args:
        platform: Platform name (e.g., 'bamboohr')
        watermark_candidate: Potential watermark timestamp to validate
        conn: Snowflake connection
        context: Dagster execution context
        config: Watermark configuration

    Returns:
        bool: True if all partitions appear to have completed successfully
    """

    cursor = conn.cursor()
    try:
        # Check data distribution in the recent processing window
        window_start = watermark_candidate - timedelta(hours=2)
        window_end = watermark_candidate + timedelta(hours=1)

        cursor.execute(f"""
        SELECT
            UPPER(SUBSTRING(company_name_clean, 1, 1)) as first_letter,
            COUNT(*) as job_count,
            COUNT(DISTINCT company_id) as company_count,
            MIN(transformation_timestamp) as earliest_transform,
            MAX(transformation_timestamp) as latest_transform
        FROM {database_name}.{stage_schema}.jobs_unified
        WHERE platform = %s
        AND transformation_timestamp >= %s
        AND transformation_timestamp <= %s
        AND company_name_clean IS NOT NULL
        GROUP BY UPPER(SUBSTRING(company_name_clean, 1, 1))
        ORDER BY first_letter
        """, (platform, window_start, window_end))

        partition_stats = cursor.fetchall()

        if not partition_stats:
            context.log.warning(f"[{platform}] No data found in processing window {window_start} to {window_end}")
            return False

        # Analyze partition distribution
        represented_prefixes = set()
        total_jobs = 0

        for prefix, job_count, company_count, earliest, latest in partition_stats:
            represented_prefixes.add(prefix)
            total_jobs += job_count

        # Check if we have reasonable distribution across company name prefixes
        if len(represented_prefixes) < config.min_expected_prefixes:
            context.log.warning(f"[{platform}] Only {len(represented_prefixes)} distinct company prefixes found, expected at least {config.min_expected_prefixes}")
            context.log.warning(f"[{platform}] Represented prefixes: {sorted(represented_prefixes)}")
            context.log.warning(f"[{platform}] This suggests some discovery partitions may have failed")
            return False

        # Additional check: ensure we have some variety in company names
        # This catches cases where all data comes from just a few prefixes
        prefix_distribution = {prefix: count for prefix, count, _, _, _ in partition_stats}
        max_prefix_percentage = max(prefix_distribution.values()) / total_jobs if total_jobs > 0 else 0

        if max_prefix_percentage > 0.7:  # Single prefix dominates >70% of data
            dominant_prefix = max(prefix_distribution, key=prefix_distribution.get)
            context.log.warning(f"[{platform}] Prefix '{dominant_prefix}' dominates {max_prefix_percentage:.1%} of data")
            context.log.warning(f"[{platform}] This suggests other discovery partitions may have failed")
            return False

        context.log.info(f"[{platform}] Partition validation passed: {len(represented_prefixes)} prefixes, {total_jobs} total jobs")
        return True

    except Exception as e:
        context.log.error(f"[{platform}] Error validating partition completeness: {str(e)}")
        return False
    finally:
        cursor.close()


def get_watermark(
    platform: str,
    conn,
    context: AssetExecutionContext,
    config: WatermarkConfig,
    database_name: str = "BETTERJOBS_DB",
    stage_schema: str = "STAGE"
) -> Optional[datetime]:
    """
    Get the last successful processing watermark for a platform.

    This function checks if ALL upstream discovery partitions completed successfully
    before returning a watermark. If any partition failed, forces full refresh.

    Returns:
        datetime: Last processed watermark with overlap cushion applied
        None: If no watermark exists, upstream partitions failed, or full refresh is needed
    """

    if not config.enable_incremental or config.force_full_refresh:
        context.log.info(f"[{platform}] Incremental processing disabled, using full refresh")
        return None

    cursor = conn.cursor()
    try:
        # Get the latest successful processing timestamp from stage table
        cursor.execute(f"""
        SELECT MAX(transformation_timestamp) as last_processed
        FROM {database_name}.{stage_schema}.jobs_unified
        WHERE platform = %s
        AND transformation_timestamp >= CURRENT_TIMESTAMP - INTERVAL '{config.max_incremental_days} days'
        """, (platform,))

        result = cursor.fetchone()

        if result and result[0]:
            last_processed = result[0]

            # Validate that all discovery partitions completed successfully
            context.log.info(f"[{platform}] Validating partition completeness for watermark: {last_processed}")

            if validate_partition_completeness(platform, last_processed, conn, context, config, database_name, stage_schema):
                # All partitions appear complete, safe to use incremental processing
                watermark = last_processed - timedelta(hours=config.overlap_hours)

                context.log.info(f"[{platform}] Partition validation passed - using incremental processing")
                context.log.info(f"[{platform}] Watermark: {last_processed}, with {config.overlap_hours}h overlap: {watermark}")
                return watermark
            else:
                # Partition validation failed, force full refresh
                context.log.warning(f"[{platform}] Partition validation failed - forcing full refresh to ensure data completeness")
                return None
        else:
            context.log.info(f"[{platform}] No watermark found, performing full refresh")
            return None

    except Exception as e:
        context.log.warning(f"[{platform}] Error retrieving watermark: {str(e)}, falling back to full refresh")
        return None
    finally:
        cursor.close()


def load_incremental_raw_data(
    platform: str,
    watermark: Optional[datetime],
    conn,
    context: AssetExecutionContext,
    max_records: Optional[int] = None,
    database_name: str = "BETTERJOBS_DB",
    raw_schema: str = "RAW"
) -> pd.DataFrame:
    """
    Load raw data incrementally based on watermark.

    Args:
        platform: Platform name
        watermark: Last processed timestamp (None for full refresh)
        conn: Snowflake connection
        context: Dagster execution context
        max_records: Optional limit on records

    Returns:
        DataFrame with raw data since watermark
    """

    raw_table_name = f"{platform}_jobs"

    # Base query
    query_sql = f"""
    SELECT
        j.*,
        m.company_name
    FROM {database_name}.{raw_schema}.{raw_table_name} j
    LEFT JOIN {database_name}.{raw_schema}.master_company_urls m
        ON j.company_id = m.company_id
    WHERE j.is_active = TRUE
    """

    # Add incremental filter if watermark exists
    params = []
    if watermark:
        query_sql += " AND j.date_retrieved > %s"
        params.append(watermark)
        context.log.info(f"[{platform}] Loading incremental data since {watermark}")
    else:
        context.log.info(f"[{platform}] Loading full data (no watermark)")

    # Add record limit if specified
    if max_records:
        query_sql += f" LIMIT {max_records}"

    cursor = conn.cursor()
    try:
        cursor.execute(query_sql, params)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        if not results:
            context.log.info(f"[{platform}] No {'incremental' if watermark else 'raw'} data found")
            return pd.DataFrame()

        # Convert to DataFrame and normalize column names
        raw_df = pd.DataFrame(results, columns=columns)
        raw_df.columns = raw_df.columns.str.lower()

        context.log.info(f"[{platform}] Loaded {len(raw_df)} {'incremental' if watermark else 'raw'} jobs")
        return raw_df

    finally:
        cursor.close()


def upsert_platform_data_to_snowflake(
    platform_df: pd.DataFrame,
    platform: str,
    conn,
    context: AssetExecutionContext,
    database_name: str = "BETTERJOBS_DB",
    stage_schema: str = "STAGE"
) -> int:
    """
    Load platform data using MERGE (upsert) instead of DELETE + INSERT.

    This enables incremental processing by updating existing records
    and inserting new ones, rather than clearing the entire partition.

    Returns:
        Number of records processed (inserted + updated)
    """

    if platform_df.empty:
        context.log.info(f"[{platform}] No data to upsert")
        return 0

    from snowflake.connector.pandas_tools import write_pandas

    cursor = conn.cursor()
    try:
        # Ensure proper data types before upload
        platform_df_fixed = platform_df.copy()

        # Fix date_retrieved to ensure it stays as TIMESTAMP_NTZ
        if 'date_retrieved' in platform_df_fixed.columns:
            import pandas as pd
            platform_df_fixed['date_retrieved'] = pd.to_datetime(platform_df_fixed['date_retrieved'], utc=True).dt.tz_localize(None)

        # Fix date_posted to ensure it stays as DATE
        if 'date_posted' in platform_df_fixed.columns:
            import pandas as pd
            platform_df_fixed['date_posted'] = pd.to_datetime(platform_df_fixed['date_posted']).dt.date

        # Fix partition_date to ensure it stays as DATE
        if 'partition_date' in platform_df_fixed.columns:
            import pandas as pd
            platform_df_fixed['partition_date'] = pd.to_datetime(platform_df_fixed['partition_date']).dt.date

        # Create temporary table for staging data with unique identifier
        import uuid
        unique_id = str(uuid.uuid4()).replace('-', '')[:8]  # 8 chars for readability
        temp_table_name = f"temp_{platform}_jobs_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{unique_id}"

        context.log.info(f"[{platform}] Uploading {len(platform_df)} jobs via temporary table")

        # Write data to temporary table
        success, nchunks, nrows, _ = write_pandas(
            conn,
            platform_df_fixed,
            table_name=temp_table_name,
            database=database_name,
            schema=stage_schema,
            auto_create_table=True,
            overwrite=True
        )

        if not success:
            raise Exception(f"Failed to write data to temporary table {temp_table_name}")

        conn.commit()

        # Get the actual table name as created in Snowflake
        cursor.execute(f"""
        SELECT table_name
        FROM {database_name}.information_schema.tables
        WHERE table_schema = '{stage_schema}'
        AND LOWER(table_name) = LOWER('{temp_table_name}')
        """)

        result = cursor.fetchone()
        if not result:
            raise Exception(f"Temporary table {temp_table_name} was not found after creation")

        actual_temp_table_name = result[0]

        # Get available columns from the temporary table
        cursor.execute(f'DESCRIBE TABLE {database_name}.{stage_schema}."{actual_temp_table_name}"')
        temp_columns = [row[0].lower() for row in cursor.fetchall()]

        # Define expected columns for MERGE operation
        merge_columns = [
            'job_uid', 'job_id', 'company_id', 'platform',
            'job_title_clean', 'job_description_clean', 'company_name_clean',
            'location_standardized', 'job_url', 'date_posted', 'date_retrieved',
            'is_active', 'employment_status', 'department', 'detected_language',
            'language_confidence', 'is_english', 'language_detection_method',
            'platform_specific_data', 'data_quality_score', 'source_raw_table',
            'raw_data', 'partition_date'
        ]

        # Build column lists for MERGE
        available_merge_columns = [col for col in merge_columns if col in temp_columns]

        # Check if job_uid exists - this is critical for MERGE
        if 'job_uid' not in temp_columns:
            raise Exception(f"Critical error: job_uid column not found in temporary table")

        if 'job_uid' not in available_merge_columns:
            raise Exception(f"Critical error: job_uid not in available merge columns")

        # Add transformation_timestamp to update columns
        update_set_clauses = []
        insert_columns = []
        insert_values = []

        for col in available_merge_columns:
            if col != 'job_uid':  # job_uid is the match key, don't update it
                if col == 'date_posted' or col == 'partition_date':
                    # Handle DATE columns
                    update_set_clauses.append(f"{col} = TRY_CAST(source.\"{col}\" AS DATE)")
                    insert_values.append(f"TRY_CAST(source.\"{col}\" AS DATE)")
                elif col == 'date_retrieved':
                    # Handle date_retrieved: write_pandas may store as NUMBER (nanoseconds since epoch)
                    # Convert nanoseconds to timestamp using TO_TIMESTAMP with nanosecond precision
                    update_set_clauses.append(f"{col} = TO_TIMESTAMP(source.\"{col}\" / 1000000000)")
                    insert_values.append(f"TO_TIMESTAMP(source.\"{col}\" / 1000000000)")
                elif col in ['platform_specific_data', 'raw_data']:
                    update_set_clauses.append(f"{col} = TRY_PARSE_JSON(source.\"{col}\")")
                    insert_values.append(f"TRY_PARSE_JSON(source.\"{col}\")")
                else:
                    update_set_clauses.append(f"{col} = source.\"{col}\"")
                    insert_values.append(f"source.\"{col}\"")
            else:
                insert_values.append(f"source.\"{col}\"")

            insert_columns.append(col)

        # Add transformation_timestamp for both INSERT and UPDATE
        update_set_clauses.append("transformation_timestamp = CURRENT_TIMESTAMP")
        insert_columns.append("transformation_timestamp")
        insert_values.append("CURRENT_TIMESTAMP")

        # Handle optional department column
        if 'department' not in temp_columns:
            department_idx = next((i for i, col in enumerate(insert_columns) if col == 'department'), None)
            if department_idx is not None:
                insert_values[department_idx] = "NULL"

        # Build MERGE statement
        merge_sql = f"""
        MERGE INTO {database_name}.{stage_schema}.jobs_unified AS target
        USING {database_name}.{stage_schema}."{actual_temp_table_name}" AS source
        ON target.job_uid = source."job_uid"
        WHEN MATCHED THEN
            UPDATE SET {', '.join(update_set_clauses)}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(insert_columns)})
            VALUES ({', '.join(insert_values)})
        """

        cursor.execute(merge_sql)
        merge_count = cursor.rowcount
        conn.commit()

        # Clean up temporary table
        cursor.execute(f'DROP TABLE {database_name}.{stage_schema}."{actual_temp_table_name}"')
        conn.commit()

        context.log.info(f"[{platform}] MERGE completed: {merge_count} records processed")
        return merge_count

    finally:
        cursor.close()


def get_processing_stats(
    platform: str,
    conn,
    context: AssetExecutionContext,
    watermark: Optional[datetime] = None,
    database_name: str = "BETTERJOBS_DB",
    stage_schema: str = "STAGE"
) -> Dict[str, Any]:
    """
    Get processing statistics for incremental vs full refresh runs.

    Returns:
        Dictionary with processing statistics and metadata
    """

    cursor = conn.cursor()
    try:
        # Get current platform statistics
        cursor.execute(f"""
        SELECT
            COUNT(*) as total_jobs,
            COUNT(DISTINCT company_id) as company_count,
            MIN(transformation_timestamp) as earliest_transform,
            MAX(transformation_timestamp) as latest_transform,
            AVG(data_quality_score) as avg_quality_score
        FROM {database_name}.{stage_schema}.jobs_unified
        WHERE platform = %s
        AND partition_date = CURRENT_DATE
        """, (platform,))

        result = cursor.fetchone()

        stats = {
            "platform": platform,
            "processing_mode": "incremental" if watermark else "full_refresh",
            "watermark_used": watermark.isoformat() if watermark else None,
            "total_jobs": result[0] if result else 0,
            "company_count": result[1] if result else 0,
            "earliest_transform": result[2].isoformat() if result and result[2] else None,
            "latest_transform": result[3].isoformat() if result and result[3] else None,
            "avg_quality_score": float(result[4]) if result and result[4] else 0.0
        }

        # Get incremental statistics if watermark was used
        if watermark:
            cursor.execute(f"""
            SELECT COUNT(*) as incremental_count
            FROM {database_name}.{stage_schema}.jobs_unified
            WHERE platform = %s
            AND transformation_timestamp >= %s
            AND partition_date = CURRENT_DATE
            """, (platform, watermark))

            inc_result = cursor.fetchone()
            stats["incremental_records"] = inc_result[0] if inc_result else 0

        return stats

    finally:
        cursor.close()