"""
Schema-as-Code Utility Functions

This module contains utility functions for the schema-as-code approach where each database
object has exactly one canonical SQL definition file. Assets use these utilities to
dynamically ensure required objects exist by creating them on-demand.

Usage:
    from dagster_betterjobs.utils.schema_utils import ensure_object_exists

    @asset
    def my_asset(context, snowflake):
        table_name = ensure_object_exists("tables/raw_bamboohr_jobs.sql", snowflake, context)
        # table_name = "BETTERJOBS_DB.RAW.bamboohr_jobs"
"""

import os
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Any
from dagster import AssetExecutionContext, get_dagster_logger
from dagster_snowflake import SnowflakeResource


logger = get_dagster_logger()


def ensure_object_exists(sql_file_path: str, snowflake: SnowflakeResource, context: AssetExecutionContext) -> str:
    """
    Ensure a database object exists using its canonical SQL file.

    This function implements the core self-healing behavior: if an object doesn't exist,
    it automatically creates it using the canonical SQL file definition.

    Args:
        sql_file_path: Path to SQL file relative to pipeline/sql/objects/
                      (e.g., "tables/raw_bamboohr_jobs.sql")
        snowflake: SnowflakeResource instance for database operations
        context: Dagster execution context for logging

    Returns:
        Fully qualified object name (e.g., "BETTERJOBS_DB.RAW.bamboohr_jobs")

    Raises:
        Exception: If object creation fails with helpful error message

    Example:
        >>> raw_table = ensure_object_exists("tables/raw_bamboohr_jobs.sql", snowflake, context)
        >>> # Returns: "BETTERJOBS_DB.RAW.bamboohr_jobs"
        >>> # Creates the table if it doesn't exist
    """

    # Extract object name from file path using naming convention
    object_name = extract_object_name_from_file(sql_file_path)

    # Check if object already exists
    if not object_exists(object_name, snowflake):
        context.log.info(f"🔧 SELF-HEALING: Creating missing object {object_name} from {sql_file_path}")

        # Build full path to SQL file
        project_root = Path(__file__).parent.parent.parent.parent.parent
        sql_file_full_path = project_root / "pipeline" / "sql" / "objects" / sql_file_path

        if not sql_file_full_path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file_full_path}")

        # Execute the canonical SQL file to create the object
        result = execute_sql_file(snowflake, str(sql_file_full_path), context)

        if result["status"] == "error":
            # Provide helpful error message indicating which infrastructure asset to run
            schema_layer = object_name.split('.')[1].lower()  # Extract schema from BETTERJOBS_DB.SCHEMA.table
            raise Exception(
                f"Failed to create {object_name} from {sql_file_path}. "
                f"Consider running '{schema_layer}_schema_setup' infrastructure asset first. "
                f"Error: {result['error']}"
            )

        context.log.info(f"✅ HEALED: Successfully created {object_name}")
    else:
        context.log.info(f"🔍 SKIPPED: Object {object_name} already exists")

    return object_name


def object_exists(object_name: str, snowflake: SnowflakeResource) -> bool:
    """
    Check if a database object exists using a simple SELECT query.

    This is a lightweight way to test object existence without requiring
    metadata queries or complex permissions.

    Args:
        object_name: Fully qualified object name (e.g., "BETTERJOBS_DB.RAW.bamboohr_jobs")
        snowflake: SnowflakeResource instance

    Returns:
        True if object exists and is accessible, False otherwise

    Example:
        >>> if object_exists("BETTERJOBS_DB.RAW.bamboohr_jobs", snowflake):
        ...     print("Table exists!")
    """
    try:
        with snowflake.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # Simple existence check using SELECT with LIMIT 1 for efficiency
                cursor.execute(f"SELECT 1 FROM {object_name} LIMIT 1")
                return True
            finally:
                cursor.close()
    except Exception:
        # Any exception (table doesn't exist, permission denied, etc.) means we can't use it
        return False


def extract_object_name_from_file(sql_file_path: str) -> str:
    """
    Convert SQL file path to fully qualified database object name using naming convention.

    Naming Convention:
    - File: "tables/raw_bamboohr_jobs.sql" → Object: "BETTERJOBS_DB.RAW.bamboohr_jobs"
    - File: "views/analytics_company_summary_view.sql" → Object: "BETTERJOBS_DB.ANALYTICS.company_summary_view"
    - File: "tables/stage_jobs_unified.sql" → Object: "BETTERJOBS_DB.STAGE.jobs_unified"

    Args:
        sql_file_path: Path to SQL file (e.g., "tables/raw_bamboohr_jobs.sql")

    Returns:
        Fully qualified object name with database prefix

    Raises:
        ValueError: If file path doesn't follow expected naming convention

    Example:
        >>> extract_object_name_from_file("tables/raw_bamboohr_jobs.sql")
        'BETTERJOBS_DB.RAW.bamboohr_jobs'
    """

    # Extract filename without extension
    # "tables/raw_bamboohr_jobs.sql" → "raw_bamboohr_jobs"
    file_name = Path(sql_file_path).stem

    # Split on first underscore to separate schema from object name
    # "raw_bamboohr_jobs" → ["raw", "bamboohr_jobs"]
    try:
        schema, object_name = file_name.split('_', 1)
    except ValueError:
        raise ValueError(
            f"Invalid file naming convention: {sql_file_path}. "
            f"Expected format: {{schema}}_{{object_name}}.sql (e.g., raw_bamboohr_jobs.sql)"
        )

    # Build fully qualified name: database.schema.object
    # "raw" + "bamboohr_jobs" → "BETTERJOBS_DB.RAW.bamboohr_jobs"
    database_name = os.environ.get('SNOWFLAKE_DATABASE', 'BETTERJOBS_DB')
    return f"{database_name}.{schema.upper()}.{object_name}"


def execute_sql_file(snowflake: SnowflakeResource, file_path: str, context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Execute a SQL file with proper error handling and logging.

    This utility handles multi-statement SQL files, filters out comments,
    provides detailed execution results for debugging, and supports environment variable substitution.

    Environment Variable Support:
    - Use ${ENV_VAR_NAME} syntax in SQL files
    - Variables are substituted from os.environ before execution
    - Missing environment variables will cause execution to fail with helpful error message

    Args:
        snowflake: SnowflakeResource instance
        file_path: Absolute path to SQL file
        context: Dagster execution context for logging

    Returns:
        Dictionary with execution results:
        {
            "status": "success" | "error",
            "file": file_path,
            "statements_executed": int,
            "statements_failed": int,
            "results": [{"statement_num": int, "statement": str, "status": str}],
            "error": str (only if status == "error")
        }
    """
    try:
        # Read SQL file content
        with open(file_path, 'r') as f:
            sql_content = f.read()

        # Substitute environment variables using ${VAR_NAME} syntax
        import re
        def replace_env_var(match):
            var_name = match.group(1)
            var_value = os.environ.get(var_name)
            if var_value is None:
                raise ValueError(f"Environment variable '{var_name}' not found. Required for SQL file: {file_path}")
            context.log.debug(f"Substituting ${{{var_name}}} with environment variable value")
            return var_value

        # Replace ${VAR_NAME} patterns with environment variable values
        sql_content = re.sub(r'\$\{([A-Za-z_][A-Za-z0-9_]*)\}', replace_env_var, sql_content)

        # Parse SQL into individual statements
        # Remove comments and empty lines for better parsing
        lines = sql_content.split('\n')
        cleaned_lines = []

        for line in lines:
            line = line.strip()
            # Skip empty lines and comments
            if line and not line.startswith('--') and not line.startswith('/*'):
                cleaned_lines.append(line)

        cleaned_sql = '\n'.join(cleaned_lines)
        statements = [stmt.strip() for stmt in cleaned_sql.split(';') if stmt.strip()]

        # Execute each statement
        results = []
        with snowflake.get_connection() as conn:
            cursor = conn.cursor()
            try:
                for i, statement in enumerate(statements):
                    statement_num = i + 1
                    context.log.debug(f"Executing statement {statement_num}/{len(statements)}: {statement[:100]}...")

                    try:
                        cursor.execute(statement)
                        results.append({
                            "statement_num": statement_num,
                            "statement": statement[:200] + "..." if len(statement) > 200 else statement,
                            "status": "success"
                        })
                    except Exception as e:
                        context.log.warning(f"Statement {statement_num} failed: {str(e)}")
                        results.append({
                            "statement_num": statement_num,
                            "statement": statement[:200] + "..." if len(statement) > 200 else statement,
                            "status": "error",
                            "error": str(e)
                        })
                        # Continue with remaining statements for non-critical errors
            finally:
                cursor.close()

        # Calculate summary statistics
        successful_statements = len([r for r in results if r["status"] == "success"])
        failed_statements = len([r for r in results if r["status"] == "error"])

        return {
            "status": "success",
            "file": file_path,
            "statements_executed": successful_statements,
            "statements_failed": failed_statements,
            "results": results
        }

    except Exception as e:
        context.log.error(f"Error executing SQL file {file_path}: {str(e)}")
        return {
            "status": "error",
            "file": file_path,
            "error": str(e)
        }


def get_schema_layer_from_object_name(object_name: str) -> str:
    """
    Extract schema layer from fully qualified object name for error messaging.

    Args:
        object_name: Fully qualified name (e.g., "BETTERJOBS_DB.RAW.bamboohr_jobs")

    Returns:
        Schema layer in lowercase (e.g., "raw", "stage", "analytics")

    Example:
        >>> get_schema_layer_from_object_name("BETTERJOBS_DB.STAGE.jobs_unified")
        'stage'
    """
    try:
        parts = object_name.split('.')
        if len(parts) >= 3:
            return parts[1].lower()  # BETTERJOBS_DB.SCHEMA.table → schema
        return "unknown"
    except:
        return "unknown"


def ensure_temp_object_exists(
    base_name: str,
    sql_file_path: str,
    snowflake: SnowflakeResource,
    context: AssetExecutionContext,
    session_specific: bool = True,
    existing_connection = None
) -> str:
    """
    Create a uniquely named temporary object based on a SQL template file.

    This function addresses race conditions where multiple concurrent runs would
    share the same temporary table name, causing failures when one run drops
    the table while another is still using it.

    Args:
        base_name: Base name for the temporary object (e.g., "temp_company_urls")
        sql_file_path: Path to SQL template file relative to pipeline/sql/objects/
        snowflake: SnowflakeResource instance
        context: Dagster execution context
        session_specific: If True, creates session-temporary table; if False, creates regular table
        existing_connection: Optional existing Snowflake connection to use (required for TEMPORARY tables)

    Returns:
        Fully qualified unique temporary table name

    Example:
        >>> temp_table = ensure_temp_object_exists(
        ...     "temp_company_urls",
        ...     "tables/raw_master_company_urls_temp_s3.sql",
        ...     snowflake,
        ...     context,
        ...     existing_connection=conn
        ... )
        >>> print(temp_table)
        "BETTERJOBS_DB.RAW.temp_company_urls_20250706_142230_a1b2c3d4"
    """

    # Generate unique suffix using timestamp + UUID (similar to BUG-021 fix)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    unique_id = str(uuid.uuid4()).replace('-', '')[:8]  # 8 chars for readability
    unique_table_name = f"{base_name}_{timestamp}_{unique_id}"

    context.log.info(f"🔧 TEMP-OBJECT: Creating unique temporary table: {unique_table_name}")

    # Read the SQL template file using absolute path (same approach as ensure_object_exists)
    project_root = Path(__file__).parent.parent.parent.parent.parent
    sql_file_full_path = project_root / "pipeline" / "sql" / "objects" / sql_file_path

    if not sql_file_full_path.exists():
        raise FileNotFoundError(f"SQL template file not found: {sql_file_full_path}")

    with open(sql_file_full_path, 'r') as f:
        sql_template = f.read()

    # Parse the original table name from the SQL template
    # Extract: CREATE TABLE IF NOT EXISTS BETTERJOBS_DB.RAW.MASTER_COMPANY_URLS_TEMP_S3
    table_name_pattern = r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)"
    match = re.search(table_name_pattern, sql_template, re.IGNORECASE)

    if not match:
        raise ValueError(f"Could not extract table name from SQL template: {sql_file_full_path}")

    original_fqn = match.group(1)

    # Extract database and schema from original FQN
    parts = original_fqn.split('.')
    if len(parts) != 3:
        raise ValueError(f"Invalid fully qualified table name in template: {original_fqn}")

    database, schema, _ = parts
    unique_fqn = f"{database}.{schema}.{unique_table_name}"

    # Create modified SQL with unique table name
    if session_specific:
        # Create as TEMPORARY table (automatically dropped when session ends)
        modified_sql = re.sub(
            r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[^\s(]+",
            f"CREATE OR REPLACE TEMPORARY TABLE {unique_fqn}",
            sql_template,
            flags=re.IGNORECASE
        )
    else:
        # Create as regular table with unique name
        modified_sql = re.sub(
            r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[^\s(]+",
            f"CREATE OR REPLACE TABLE {unique_fqn}",
            sql_template,
            flags=re.IGNORECASE
        )

    # Execute the modified SQL to create the temporary table
    # For TEMPORARY tables, we must use the existing connection to ensure session consistency
    if session_specific and not existing_connection:
        raise ValueError("existing_connection is required when session_specific=True for TEMPORARY tables")

    try:
        if existing_connection:
            # Use the provided connection (required for TEMPORARY tables)
            cursor = existing_connection.cursor()
            cursor.execute(modified_sql)
            cursor.close()
        else:
            # Create new connection (only for regular tables)
            with snowflake.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(modified_sql)

        context.log.info(f"🔧 TEMP-OBJECT: Successfully created {unique_fqn}")

    except Exception as e:
        context.log.error(f"Failed to create temporary table {unique_fqn}: {str(e)}")
        raise Exception(f"Temporary table creation failed: {str(e)}")

    return unique_fqn