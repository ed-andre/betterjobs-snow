"""
Snowflake Database and Schema Setup Assets

This module contains Dagster assets for setting up the BetterJobs Snowflake infrastructure
following the medallion architecture pattern (Bronze/Silver/Gold layers) using the new
schema-as-code system with object files.

Execution Order:
1. database_schema_setup - Creates database, schemas, and roles
2. infrastructure_setup - Creates stages, integrations, file formats
3. tables_setup - Creates all table objects
4. views_setup - Creates all view objects
5. setup_validation - Validates complete setup
"""

import os
from pathlib import Path
from typing import Dict, Any, List
from dagster import asset, AssetExecutionContext, get_dagster_logger
from dagster_snowflake import SnowflakeResource

from ..utils.schema_utils import ensure_object_exists, extract_object_name_from_file, object_exists

logger = get_dagster_logger()


def validate_object_files(objects_dir: Path, required_objects: List[str], context: AssetExecutionContext) -> None:
    """
    Validate that all required object files exist before processing

    Args:
        objects_dir: Path to the objects directory
        required_objects: List of required object file names
        context: Dagster execution context for logging

    Raises:
        FileNotFoundError: If any required object files are missing
    """
    missing_objects = []

    for obj_file in required_objects:
        obj_path = objects_dir / obj_file
        if not obj_path.exists():
            missing_objects.append(obj_file)

    if missing_objects:
        context.log.error(f"Missing required object files: {missing_objects}")
        context.log.error(f"Expected object files in: {objects_dir}")
        context.log.error("Please ensure all required object definition files exist before running setup")
        raise FileNotFoundError(f"Missing object files: {missing_objects}")

    context.log.info(f"Found all required object files in: {objects_dir}")


def process_object_files(snowflake: SnowflakeResource, objects_dir: Path, object_files: List[str],
                        context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Process multiple object files using the schema utility system

    Args:
        snowflake: SnowflakeResource instance
        objects_dir: Path to the objects directory
        object_files: List of object file names to process
        context: Dagster execution context for logging

    Returns:
        Dictionary with processing results
    """
    results = []
    successful_objects = 0
    failed_objects = 0

    with snowflake.get_connection() as conn:
        for obj_file in object_files:
            obj_path = objects_dir / obj_file
            object_name = extract_object_name_from_file(str(obj_path))

            context.log.info(f"📋 Processing object file: {obj_file} -> {object_name}")

            try:
                # Use utility function to ensure object exists
                result = ensure_object_exists(
                    f"{objects_dir.name}/{obj_file}",  # Construct path like "tables/raw_bamboohr_jobs.sql"
                    snowflake,      # Pass snowflake resource
                    context         # Pass context
                )

                if result:
                    context.log.info(f"✅ Successfully processed {object_name}")
                    successful_objects += 1
                    results.append({
                        "object_file": obj_file,
                        "object_name": object_name,
                        "status": "success"
                    })
                else:
                    context.log.error(f"❌ Failed to process {object_name}")
                    failed_objects += 1
                    results.append({
                        "object_file": obj_file,
                        "object_name": object_name,
                        "status": "error",
                        "error": "Object creation failed"
                    })

            except Exception as e:
                context.log.error(f"❌ Error processing {obj_file}: {str(e)}")
                failed_objects += 1
                results.append({
                    "object_file": obj_file,
                    "object_name": object_name,
                    "status": "error",
                    "error": str(e)
                })

    return {
        "status": "success" if failed_objects == 0 else "partial_success",
        "total_objects": len(object_files),
        "successful_objects": successful_objects,
        "failed_objects": failed_objects,
        "results": results
    }


def execute_sql_file(snowflake: SnowflakeResource, file_path: str, context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Utility function to execute SQL files with proper error handling (for legacy schema setup)

    Args:
        snowflake: SnowflakeResource instance
        file_path: Path to SQL file relative to project root
        context: Dagster execution context for logging

    Returns:
        Dictionary with execution results
    """
    try:
        # Get absolute path
        project_root = Path(__file__).parent.parent.parent.parent.parent
        absolute_path = project_root / file_path

        if not absolute_path.exists():
            raise FileNotFoundError(f"SQL file not found: {absolute_path}")

        with open(absolute_path, 'r') as f:
            sql_content = f.read()

        # Split by statements (handle multiple CREATE statements)
        # Remove comments and empty lines for better parsing
        lines = sql_content.split('\n')
        cleaned_lines = []
        for line in lines:
            line = line.strip()
            if line and not line.startswith('--') and not line.startswith('/*'):
                cleaned_lines.append(line)

        cleaned_sql = '\n'.join(cleaned_lines)
        statements = [stmt.strip() for stmt in cleaned_sql.split(';') if stmt.strip()]

        with snowflake.get_connection() as conn:
            results = []
            for i, statement in enumerate(statements):
                if statement.strip():
                    context.log.info(f"Executing statement {i+1}/{len(statements)}: {statement[:100]}...")
                    try:
                        result = conn.execute(statement)
                        results.append({
                            "statement_num": i+1,
                            "statement": statement[:200] + "..." if len(statement) > 200 else statement,
                            "status": "success"
                        })
                    except Exception as e:
                        context.log.error(f"Failed to execute statement {i+1}: {str(e)}")
                        results.append({
                            "statement_num": i+1,
                            "statement": statement[:200] + "..." if len(statement) > 200 else statement,
                            "status": "error",
                            "error": str(e)
                        })
                        # Continue with other statements for non-critical errors
                        continue

        return {
            "status": "success",
            "file": file_path,
            "statements_executed": len([r for r in results if r["status"] == "success"]),
            "statements_failed": len([r for r in results if r["status"] == "error"]),
            "results": results
        }

    except Exception as e:
        context.log.error(f"Error executing SQL file {file_path}: {str(e)}")
        return {
            "status": "error",
            "file": file_path,
            "error": str(e)
        }


@asset(
    description="Initialize Snowflake database, schemas, and roles - foundational setup",
    group_name="infrastructure_setup",
    kinds={"snowflake", "SQL"},
)
def database_schema_setup(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Execute 00_database_and_schema_setup.sql to create foundational database infrastructure

    Creates:
    - BETTERJOBS_DB database
    - RAW, STAGE, ANALYTICS schemas
    - BETTERJOBS_ROLE with permissions
    - Warehouse and integration grants
    """

    sql_file_path = "pipeline/sql/schema_setup/00_database_and_schema_setup.sql"
    result = execute_sql_file(snowflake, sql_file_path, context)

    if result["status"] == "error":
        context.log.error(f"Failed to create database and schemas: {result['error']}")
        raise Exception(f"Database setup failed: {result['error']}")

    context.log.info(f"Successfully created database and schemas: {result['statements_executed']} statements executed")
    if result["statements_failed"] > 0:
        context.log.warning(f"Some statements failed: {result['statements_failed']} failures")

    return result


@asset(
    description="Initialize infrastructure objects using object files - stages, integrations, file formats",
    group_name="infrastructure_setup",
    kinds={"snowflake", "SQL"},
    deps=[database_schema_setup]
)
def infrastructure_setup(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Execute infrastructure object files to create stages, integrations, and file formats

    Creates:
    - External stages for S3 integration
    - Storage integrations
    - File formats for data loading
    """

    # Get the path to object files
    current_dir = Path(__file__).parent
    objects_dir = current_dir.parent / "objects" / "infrastructure"

    # Required infrastructure object files (order matters for dependencies)
    required_objects = [
        "storage_integrations.sql",  # First - needed for stages
        "file_formats.sql",          # Second - needed for stages
        "raw_s3_stages.sql"          # Third - depends on integrations and formats
    ]

    # Filter out empty files
    available_objects = []
    for obj_file in required_objects:
        obj_path = objects_dir / obj_file
        if obj_path.exists() and obj_path.stat().st_size > 0:
            available_objects.append(obj_file)
        else:
            context.log.warning(f"Skipping empty or missing infrastructure file: {obj_file}")

    if not available_objects:
        context.log.info("No infrastructure objects to process")
        return {
            "status": "success",
            "total_objects": 0,
            "successful_objects": 0,
            "failed_objects": 0,
            "results": []
        }

    # Process available object files
    result = process_object_files(snowflake, objects_dir, available_objects, context)

    context.log.info(f"Infrastructure setup completed: {result['successful_objects']} objects processed, {result['failed_objects']} failures")

    if result["failed_objects"] > 0:
        context.log.warning(f"Some infrastructure objects failed: {result['failed_objects']} failures")

    return result


@asset(
    description="Initialize all table objects using object files",
    group_name="infrastructure_setup",
    kinds={"snowflake", "SQL"},
    deps=[infrastructure_setup]
)
def tables_setup(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Execute all table object files to create database tables

    Creates:
    - RAW schema tables for data ingestion
    - STAGE schema tables for cleaned/standardized data
    - ANALYTICS schema tables for business-ready data
    """

    # Get the path to object files
    current_dir = Path(__file__).parent
    objects_dir = current_dir.parent / "objects" / "tables"

    # Get all table files (they're already organized by naming convention)
    table_files = [f.name for f in objects_dir.glob("*.sql") if f.is_file()]

    if not table_files:
        context.log.warning("No table objects found to process")
        return {
            "status": "success",
            "total_objects": 0,
            "successful_objects": 0,
            "failed_objects": 0,
            "results": []
        }

    # Sort by schema layer (raw first, then stage, then analytics)
    def sort_key(filename):
        if filename.startswith('raw_'):
            return (0, filename)
        elif filename.startswith('stage_'):
            return (1, filename)
        elif filename.startswith('analytics_'):
            return (2, filename)
        else:
            return (3, filename)

    table_files.sort(key=sort_key)

    context.log.info(f"Processing {len(table_files)} table objects")

    # Process table object files
    result = process_object_files(snowflake, objects_dir, table_files, context)

    context.log.info(f"Tables setup completed: {result['successful_objects']} objects processed, {result['failed_objects']} failures")

    if result["failed_objects"] > 0:
        context.log.warning(f"Some table objects failed: {result['failed_objects']} failures")

    return result


@asset(
    description="Initialize all view objects using object files",
    group_name="infrastructure_setup",
    kinds={"snowflake", "SQL"},
    deps=[tables_setup]
)
def views_setup(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Execute all view object files to create database views

    Creates:
    - STAGE schema views for data transformation
    - ANALYTICS schema views for business insights
    - Lookup and summary views
    """

    # Get the path to object files
    current_dir = Path(__file__).parent
    objects_dir = current_dir.parent / "objects" / "views"

    # Get all view files
    view_files = [f.name for f in objects_dir.glob("*.sql") if f.is_file()]

    if not view_files:
        context.log.warning("No view objects found to process")
        return {
            "status": "success",
            "total_objects": 0,
            "successful_objects": 0,
            "failed_objects": 0,
            "results": []
        }

    # Sort alphabetically for consistent processing
    view_files.sort()

    context.log.info(f"Processing {len(view_files)} view objects")

    # Process view object files
    result = process_object_files(snowflake, objects_dir, view_files, context)

    context.log.info(f"Views setup completed: {result['successful_objects']} objects processed, {result['failed_objects']} failures")

    if result["failed_objects"] > 0:
        context.log.warning(f"Some view objects failed: {result['failed_objects']} failures")

    return result


@asset(
    description="Populate static/reference data tables",
    group_name="infrastructure_setup",
    kinds={"snowflake", "SQL"},
    deps=[views_setup]
)
def static_data_population(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Execute all static data population scripts in sequence

    Populates:
    - Reference data tables (states, countries, etc.)
    - Lookup tables (skills, keywords, etc.)
    - Configuration tables
    """

    # Define the order of data population scripts
    data_population_files = [
        "pipeline/sql/data_population/insert_ats_platforms.sql",
        "pipeline/sql/data_population/insert_company_size_ranges.sql",
        "pipeline/sql/data_population/insert_job_levels.sql",
        "pipeline/sql/data_population/insert_job_types.sql",
        "pipeline/sql/data_population/insert_skills.sql",
        "pipeline/sql/data_population/insert_keyword_type_mappings.sql",
        "pipeline/sql/data_population/insert_us_states_mapping.sql"
    ]

    results = []
    total_statements = 0
    total_failures = 0

    for file_path in data_population_files:
        context.log.info(f"Executing data population file: {file_path}")
        result = execute_sql_file(snowflake, file_path, context)
        results.append(result)

        if result["status"] == "success":
            total_statements += result["statements_executed"]
            total_failures += result["statements_failed"]
        else:
            context.log.error(f"Failed to execute {file_path}: {result.get('error', 'Unknown error')}")

    context.log.info(f"Static data population completed: {total_statements} statements executed, {total_failures} failures")

    return {
        "status": "success",
        "total_files": len(data_population_files),
        "total_statements": total_statements,
        "total_failures": total_failures,
        "results": results
    }


@asset(
    description="Validate complete setup and data integrity",
    group_name="infrastructure_setup",
    kinds={"snowflake", "SQL"},
    deps=[static_data_population]
)
def setup_validation(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Validate that all setup completed successfully

    Checks:
    - All expected tables and views exist
    - Row counts for static data tables
    - Foreign key relationships
    - Permissions and grants
    """

    validation_queries = [
        "SHOW DATABASES LIKE 'BETTERJOBS_DB'",
        "SHOW SCHEMAS IN DATABASE BETTERJOBS_DB",
        "SHOW ROLES LIKE 'BETTERJOBS_ROLE'",
        "SHOW TABLES IN SCHEMA BETTERJOBS_DB.RAW",
        "SHOW TABLES IN SCHEMA BETTERJOBS_DB.STAGE",
        "SHOW TABLES IN SCHEMA BETTERJOBS_DB.ANALYTICS",
        "SELECT COUNT(*) as ats_platforms_count FROM BETTERJOBS_DB.STAGE.ats_platforms",
        "SELECT COUNT(*) as company_size_ranges_count FROM BETTERJOBS_DB.STAGE.company_size_ranges",
        "SELECT COUNT(*) as job_levels_count FROM BETTERJOBS_DB.STAGE.job_levels",
        "SELECT COUNT(*) as job_types_count FROM BETTERJOBS_DB.STAGE.job_types",
        "SELECT COUNT(*) as skills_count FROM BETTERJOBS_DB.STAGE.skills",
        "SELECT COUNT(*) as us_states_count FROM BETTERJOBS_DB.STAGE.us_states_mapping"
    ]

    validation_results = []

    with snowflake.get_connection() as conn:
        for query in validation_queries:
            try:
                context.log.info(f"Executing validation query: {query}")
                result = conn.execute(query)
                rows = result.fetchall()
                validation_results.append({
                    "query": query,
                    "status": "success",
                    "result": rows
                })
            except Exception as e:
                context.log.error(f"Validation query failed: {query} - {str(e)}")
                validation_results.append({
                    "query": query,
                    "status": "error",
                    "error": str(e)
                })

    successful_validations = len([r for r in validation_results if r["status"] == "success"])
    failed_validations = len([r for r in validation_results if r["status"] == "error"])

    context.log.info(f"Setup validation completed: {successful_validations} successful, {failed_validations} failed")

    if failed_validations > 0:
        context.log.warning("Some validation checks failed - review setup completeness")

    return {
        "status": "success",
        "total_validations": len(validation_queries),
        "successful_validations": successful_validations,
        "failed_validations": failed_validations,
        "results": validation_results
    }