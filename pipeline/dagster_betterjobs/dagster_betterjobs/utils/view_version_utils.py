"""
View Version Management Utilities for Hash-Based Change Detection

This module provides utilities for managing view versions using content hashing
to detect changes in view definitions and apply updates only when necessary.

Part of ENHANCEMENT-029: Hash-Based View Update Management
"""

import hashlib
from pathlib import Path
from typing import Optional, Dict, Any
from dagster import AssetExecutionContext
from dagster_snowflake import SnowflakeResource


def calculate_view_content_hash(sql_content: str) -> str:
    """
    Calculate MD5 hash of view SQL content

    Args:
        sql_content: Raw SQL content from view file

    Returns:
        MD5 hash string of normalized content
    """
    # Normalize content: remove comments, extra whitespace
    cleaned_content = normalize_sql_content(sql_content)
    return hashlib.md5(cleaned_content.encode()).hexdigest()


def normalize_sql_content(sql_content: str) -> str:
    """
    Normalize SQL content for consistent hashing

    Args:
        sql_content: Raw SQL content

    Returns:
        Normalized SQL content
    """
    lines = []
    for line in sql_content.split('\n'):
        line = line.strip()
        # Skip empty lines and comments
        if line and not line.startswith('--') and not line.startswith('/*'):
            lines.append(line)
    return '\n'.join(lines)


def get_stored_view_hash(view_name: str, snowflake: SnowflakeResource) -> Optional[str]:
    """
    Retrieve stored hash for view from tracking table

    Args:
        view_name: Fully qualified view name (e.g., 'ANALYTICS.JOB_METRICS')
        snowflake: Snowflake resource connection

    Returns:
        Stored content hash or None if not found
    """
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT CONTENT_HASH FROM BETTERJOBS_DB.STAGE.VIEW_VERSION_TRACKING WHERE VIEW_NAME = %s",
                (view_name,)
            )
            result = cursor.fetchone()
            return result[0] if result else None
        finally:
            cursor.close()


def update_view_hash(view_name: str, content_hash: str, file_path: str,
                    view_schema: str, snowflake: SnowflakeResource,
                    context: AssetExecutionContext) -> None:
    """
    Update stored hash for view in tracking table

    Args:
        view_name: Fully qualified view name
        content_hash: New content hash
        file_path: Path to view SQL file
        view_schema: Schema name (RAW, STAGE, ANALYTICS)
        snowflake: Snowflake resource connection
        context: Dagster execution context for logging
    """
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        try:
            # Get previous hash for audit trail
            previous_hash = get_stored_view_hash(view_name, snowflake)

            cursor.execute("""
                MERGE INTO BETTERJOBS_DB.STAGE.VIEW_VERSION_TRACKING t
                USING (SELECT %s as view_name, %s as content_hash, %s as file_path,
                              %s as view_schema, %s as previous_hash) s
                ON t.VIEW_NAME = s.view_name
                WHEN MATCHED THEN
                    UPDATE SET
                        CONTENT_HASH = s.content_hash,
                        LAST_UPDATED = CURRENT_TIMESTAMP,
                        FILE_PATH = s.file_path,
                        PREVIOUS_HASH = s.previous_hash,
                        UPDATE_REASON = 'content_changed'
                WHEN NOT MATCHED THEN
                    INSERT (VIEW_NAME, CONTENT_HASH, FILE_PATH, VIEW_SCHEMA, PREVIOUS_HASH, UPDATE_REASON)
                    VALUES (s.view_name, s.content_hash, s.file_path, s.view_schema, s.previous_hash, 'initial_creation')
            """, (view_name, content_hash, file_path, view_schema, previous_hash))

            context.log.info(f"📊 Updated hash tracking for {view_name}: {content_hash[:8]}...")
        finally:
            cursor.close()


def view_needs_update(view_name: str, current_hash: str, snowflake: SnowflakeResource) -> bool:
    """
    Check if view needs updating based on hash comparison

    Args:
        view_name: Fully qualified view name
        current_hash: Current content hash
        snowflake: Snowflake resource connection

    Returns:
        True if view needs updating, False otherwise
    """
    stored_hash = get_stored_view_hash(view_name, snowflake)
    return stored_hash != current_hash


def extract_view_name_from_file(view_file: str) -> str:
    """
    Extract view name from SQL file following project naming conventions

    Args:
        view_file: View SQL filename (e.g., 'analytics_job_metrics.sql')

    Returns:
        Fully qualified view name (e.g., 'ANALYTICS.JOB_METRICS')
    """
    # Remove .sql extension and convert to schema.view format
    base_name = Path(view_file).stem

    # Determine schema based on prefix
    if base_name.startswith('analytics_'):
        schema = 'ANALYTICS'
        view_name = base_name.replace('analytics_', '').upper()
    elif base_name.startswith('stage_'):
        schema = 'STAGE'
        view_name = base_name.replace('stage_', '').upper()
    elif base_name.startswith('raw_'):
        schema = 'RAW'
        view_name = base_name.replace('raw_', '').upper()
    else:
        schema = 'ANALYTICS'  # Default
        view_name = base_name.upper()

    return f"{schema}.{view_name}"


def get_objects_directory() -> Path:
    """
    Get the path to the SQL objects directory

    Returns:
        Path to pipeline/sql/objects directory
    """
    current_dir = Path(__file__).parent
    # Navigate to project root and then to SQL objects directory
    project_root = current_dir.parent.parent.parent.parent
    return project_root / "pipeline" / "sql" / "objects"


def ensure_tracking_table_exists(snowflake: SnowflakeResource, context: AssetExecutionContext) -> None:
    """
    Ensure the VIEW_VERSION_TRACKING table exists before using it

    Args:
        snowflake: Snowflake resource connection
        context: Dagster execution context for logging
    """
    from ..utils.schema_utils import ensure_object_exists

    try:
        ensure_object_exists("tables/stage_view_version_tracking.sql", snowflake, context)
        context.log.info("✅ View version tracking table ready")
    except Exception as e:
        context.log.error(f"❌ Failed to ensure tracking table exists: {str(e)}")
        raise