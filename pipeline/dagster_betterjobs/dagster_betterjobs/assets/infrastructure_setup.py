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
import re
import yaml
from pathlib import Path
from typing import Dict, Any, List, Optional
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


def load_table_creation_order(objects_dir: Path, context: AssetExecutionContext) -> Optional[List[str]]:
    """
    Load table creation order from YAML configuration file.

    Args:
        objects_dir: Path to the objects/tables directory
        context: Dagster execution context for logging

    Returns:
        List of table filenames in dependency order, or None if config not found
    """
    config_file = objects_dir / "table_creation_order.yaml"

    if not config_file.exists():
        context.log.warning("Table creation order config not found, falling back to alphabetical ordering")
        return None

    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)

        # Combine all layers in dependency order
        ordered_files = []
        for layer in ['raw_layer', 'stage_layer', 'analytics_layer', 'serve_layer']:
            if layer in config:
                ordered_files.extend(config[layer])
                context.log.info(f"Loaded {len(config[layer])} tables from {layer}")

        context.log.info(f"Successfully loaded table creation order: {len(ordered_files)} tables total")
        return ordered_files

    except Exception as e:
        context.log.error(f"Failed to load table creation order config: {str(e)}")
        context.log.warning("Falling back to alphabetical ordering")
        return None


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
            cursor = conn.cursor()
            results = []
            try:
                for i, statement in enumerate(statements):
                    if statement.strip():
                        context.log.info(f"Executing statement {i+1}/{len(statements)}: {statement[:100]}...")
                        try:
                            cursor.execute(statement)
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
            finally:
                cursor.close()

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
    group_name="0_infrastructure_setup",
    kinds={"snowflake", "SQL", "python"},
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
        raise RuntimeError(f"Database schema setup failed: {result['error']}")

    # Check for any failed statements
    if result["statements_failed"] > 0:
        context.log.error(f"Database schema setup failed: {result['statements_failed']} statements failed")
        raise RuntimeError(f"Database schema setup failed: {result['statements_failed']}/{result['statements_executed']} statements failed. Check logs for details.")

    context.log.info(f"✅ Successfully created database and schemas: {result['statements_executed']} statements executed")
    return result


@asset(
    description="Initialize infrastructure objects using object files - stages, integrations, file formats",
    group_name="0_infrastructure_setup",
    kinds={"snowflake", "SQL", "python"},
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
    # Navigate to project root and then to SQL objects directory
    project_root = current_dir.parent.parent.parent.parent
    objects_dir = project_root / "pipeline" / "sql" / "objects" / "infrastructure"

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

    # Fail asset if any objects failed
    if result["failed_objects"] > 0:
        failed_objects = [r["object_name"] for r in result["results"] if r["status"] == "error"]
        context.log.error(f"Infrastructure setup failed for {result['failed_objects']} objects: {failed_objects}")
        raise RuntimeError(f"Infrastructure setup failed: {result['failed_objects']}/{result['total_objects']} objects failed processing. Failed objects: {', '.join(failed_objects[:5])}{'...' if len(failed_objects) > 5 else ''}. Check logs for details.")

    context.log.info(f"✅ Infrastructure setup completed successfully: {result['successful_objects']} objects processed")
    return result


@asset(
    description="Initialize all table objects using object files",
    group_name="0_infrastructure_setup",
    kinds={"snowflake", "SQL", "python"},
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
    # Navigate to project root and then to SQL objects directory
    project_root = current_dir.parent.parent.parent.parent
    objects_dir = project_root / "pipeline" / "sql" / "objects" / "tables"

    # Try to load dependency-ordered table list from configuration
    ordered_files = load_table_creation_order(objects_dir, context)

    if ordered_files:
        # Use dependency-based ordering from configuration
        table_files = []
        for file_name in ordered_files:
            file_path = objects_dir / file_name
            if file_path.exists():
                table_files.append(file_name)
            else:
                context.log.warning(f"Configured table file not found: {file_name}")

        # Add any SQL files not in configuration (for safety and completeness)
        all_sql_files = [f.name for f in objects_dir.glob("*.sql") if f.is_file()]
        missing_files = set(all_sql_files) - set(table_files)
        if missing_files:
            context.log.warning(f"Files not in configuration, adding at end: {sorted(missing_files)}")
            table_files.extend(sorted(missing_files))

        context.log.info(f"🔧 Using dependency-ordered table creation: {len(table_files)} files")

    else:
        # Fallback to existing alphabetical ordering by schema layer
        table_files = [f.name for f in objects_dir.glob("*.sql") if f.is_file()]

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
        context.log.info(f"📋 Using alphabetical table creation (fallback): {len(table_files)} files")

    if not table_files:
        context.log.warning("No table objects found to process")
        return {
            "status": "success",
            "total_objects": 0,
            "successful_objects": 0,
            "failed_objects": 0,
            "results": []
        }

    context.log.info(f"Processing {len(table_files)} table objects")

    # Process table object files
    result = process_object_files(snowflake, objects_dir, table_files, context)

    context.log.info(f"Tables setup completed: {result['successful_objects']} objects processed, {result['failed_objects']} failures")

    # Fail asset if any objects failed
    if result["failed_objects"] > 0:
        failed_tables = [r["object_name"] for r in result["results"] if r["status"] == "error"]
        context.log.error(f"Tables setup failed for {result['failed_objects']} tables: {failed_tables}")
        raise RuntimeError(f"Tables setup failed: {result['failed_objects']}/{result['total_objects']} tables failed processing. Failed tables: {', '.join(failed_tables[:5])}{'...' if len(failed_tables) > 5 else ''}. Check logs for details.")

    context.log.info(f"✅ Tables setup completed successfully: {result['successful_objects']} tables processed")
    return result


def process_view_files_with_change_detection(snowflake: SnowflakeResource, objects_dir: Path,
                                           view_files: List[str], context: AssetExecutionContext,
                                           update_strategy: str) -> Dict[str, Any]:
    """
    Process view files with hash-based change detection

    Args:
        snowflake: Snowflake resource connection
        objects_dir: Path to views directory
        view_files: List of view SQL files
        context: Dagster execution context
        update_strategy: Update strategy ('hash_based', 'always_replace', 'create_if_not_exists')

    Returns:
        Dictionary with processing results
    """
    from ..utils.view_version_utils import (
        calculate_view_content_hash, view_needs_update, update_view_hash,
        extract_view_name_from_file
    )

    results = []
    successful_views = 0
    failed_views = 0
    updated_views = 0
    skipped_views = 0

    with snowflake.get_connection() as conn:
        for view_file in view_files:
            view_path = objects_dir / view_file
            view_name = extract_view_name_from_file(view_file)

            context.log.info(f"📋 Processing view file: {view_file} -> {view_name}")

            try:
                # Read SQL content
                with open(view_path, 'r') as f:
                    sql_content = f.read()

                # Calculate content hash
                content_hash = calculate_view_content_hash(sql_content)

                # Determine if update is needed based on strategy
                should_update = False
                if update_strategy == 'always_replace':
                    should_update = True
                    context.log.info(f"🔄 Force updating view (always_replace): {view_name}")
                elif update_strategy == 'hash_based':
                    should_update = view_needs_update(view_name, content_hash, snowflake)
                    if should_update:
                        context.log.info(f"📝 View definition changed, updating: {view_name}")
                    else:
                        context.log.info(f"✅ View unchanged, skipping: {view_name}")
                else:  # create_if_not_exists
                    should_update = True  # Let database handle CREATE VIEW IF NOT EXISTS
                    context.log.info(f"➕ Creating view if not exists: {view_name}")

                if should_update:
                    # Execute view creation/update
                    cursor = conn.cursor()
                    try:
                        if update_strategy == 'create_if_not_exists':
                            # Use existing CREATE VIEW IF NOT EXISTS pattern
                            cursor.execute(sql_content)
                        else:
                            # Use CREATE OR REPLACE VIEW pattern
                            # Handle both CREATE VIEW IF NOT EXISTS and CREATE VIEW patterns
                            if 'CREATE VIEW IF NOT EXISTS' in sql_content.upper():
                                # Replace CREATE VIEW IF NOT EXISTS with CREATE OR REPLACE VIEW (case-insensitive)
                                updated_sql = re.sub(r'CREATE\s+VIEW\s+IF\s+NOT\s+EXISTS', 'CREATE OR REPLACE VIEW', sql_content, count=1, flags=re.IGNORECASE)
                                context.log.debug(f"🔄 Replaced CREATE VIEW IF NOT EXISTS with CREATE OR REPLACE VIEW for {view_name}")
                                cursor.execute(updated_sql)
                            elif 'CREATE VIEW' in sql_content.upper() and 'CREATE OR REPLACE VIEW' not in sql_content.upper():
                                # Replace CREATE VIEW with CREATE OR REPLACE VIEW (case-insensitive)
                                updated_sql = re.sub(r'CREATE\s+VIEW', 'CREATE OR REPLACE VIEW', sql_content, count=1, flags=re.IGNORECASE)
                                context.log.debug(f"🔄 Replaced CREATE VIEW with CREATE OR REPLACE VIEW for {view_name}")
                                cursor.execute(updated_sql)
                            else:
                                # If it's already CREATE OR REPLACE VIEW, use as is
                                context.log.debug(f"🔄 Using existing CREATE OR REPLACE VIEW for {view_name}")
                                cursor.execute(sql_content)

                        context.log.info(f"✅ Successfully processed view: {view_name}")

                        # Update hash tracking for hash-based strategy
                        if update_strategy == 'hash_based':
                            schema_name = view_name.split('.')[0]
                            update_view_hash(view_name, content_hash, str(view_path),
                                           schema_name, snowflake, context)

                        updated_views += 1
                        successful_views += 1
                    finally:
                        cursor.close()
                else:
                    skipped_views += 1
                    successful_views += 1

                results.append({
                    "view_file": view_file,
                    "view_name": view_name,
                    "status": "updated" if should_update else "skipped",
                    "content_hash": content_hash[:8] + "...",
                    "strategy": update_strategy
                })

            except Exception as e:
                context.log.error(f"❌ Error processing {view_file}: {str(e)}")
                failed_views += 1
                results.append({
                    "view_file": view_file,
                    "view_name": view_name,
                    "status": "error",
                    "error": str(e),
                    "strategy": update_strategy
                })

    return {
        "status": "success" if failed_views == 0 else "partial_success",
        "total_views": len(view_files),
        "successful_views": successful_views,
        "failed_views": failed_views,
        "updated_views": updated_views,
        "skipped_views": skipped_views,
        "update_strategy": update_strategy,
        "results": results
    }


@asset(
    description="Initialize all view objects using hash-based change detection",
    group_name="0_infrastructure_setup",
    kinds={"snowflake", "SQL", "python"},
    deps=[tables_setup]
)
def views_setup(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Execute all view object files with intelligent change detection

    Creates/Updates:
    - STAGE schema views for data transformation
    - ANALYTICS schema views for business insights
    - Lookup and summary views

    Uses hash-based change detection to only update views when definitions change.
    """
    from ..utils.view_version_utils import (
        calculate_view_content_hash, view_needs_update, update_view_hash,
        extract_view_name_from_file, get_objects_directory, ensure_tracking_table_exists
    )
    from ..config.view_update_config import get_view_update_strategy

    # Ensure tracking table exists
    ensure_tracking_table_exists(snowflake, context)

    # Get the path to object files
    objects_dir = get_objects_directory() / "views"

    # Get all view files
    view_files = [f.name for f in objects_dir.glob("*.sql") if f.is_file()]

    if not view_files:
        context.log.warning("No view objects found to process")
        return {
            "status": "success",
            "total_objects": 0,
            "successful_objects": 0,
            "failed_objects": 0,
            "updated_views": 0,
            "skipped_views": 0,
            "results": []
        }

    # Sort alphabetically for consistent processing
    view_files.sort()

    # Get update strategy
    update_strategy = get_view_update_strategy()
    context.log.info(f"🔧 Processing {len(view_files)} view objects with strategy: {update_strategy}")

    # Process views with change detection
    result = process_view_files_with_change_detection(snowflake, objects_dir, view_files, context, update_strategy)

    context.log.info(f"Views setup completed: {result['updated_views']} updated, {result['skipped_views']} skipped, {result['failed_views']} failures")

    # Fail asset if any views failed
    if result["failed_views"] > 0:
        failed_views = [r["view_name"] for r in result["results"] if r["status"] == "error"]
        context.log.error(f"Views setup failed for {result['failed_views']} views: {failed_views}")
        raise RuntimeError(f"Views setup failed: {result['failed_views']}/{result['total_views']} views failed processing. Failed views: {', '.join(failed_views[:5])}{'...' if len(failed_views) > 5 else ''}. Check logs for details.")

    context.log.info(f"✅ Views setup completed successfully: {result['updated_views']} updated, {result['skipped_views']} skipped")
    return result


@asset(
    description="Populate static/reference data tables",
    group_name="0_infrastructure_setup",
    kinds={"snowflake", "SQL", "python"},
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
        "pipeline/sql/data_population/insert_countries_mapping.sql",
        "pipeline/sql/data_population/insert_keyword_standardization_rules.sql",
        "pipeline/sql/data_population/insert_keyword_type_mappings.sql",
        "pipeline/sql/data_population/insert_location_metro_area_mapping.sql",
        "pipeline/sql/data_population/insert_location_region_mapping.sql",
        "pipeline/sql/data_population/insert_location_tech_hub_mapping.sql",
        "pipeline/sql/data_population/insert_location_standardization_rules.sql",
        "pipeline/sql/data_population/insert_skill_family_mappings.sql",
        "pipeline/sql/data_population/insert_skill_standardization_rules.sql",
        "pipeline/sql/data_population/insert_skill_category_patterns.sql",
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

    # Fail asset if any files failed completely or had statement failures
    failed_files = [r["file_path"] if "file_path" in r else "unknown" for r in results if r["status"] == "error"]

    if failed_files or total_failures > 0:
        if failed_files:
            context.log.error(f"Static data population failed for files: {failed_files}")
            raise RuntimeError(f"Static data population failed: {len(failed_files)} files failed completely. Failed files: {', '.join(failed_files[:5])}{'...' if len(failed_files) > 5 else ''}. Check logs for details.")
        else:
            context.log.error(f"Static data population failed: {total_failures} statement failures")
            raise RuntimeError(f"Static data population failed: {total_failures} statements failed across {len(data_population_files)} files. Check logs for details.")

    context.log.info(f"✅ Static data population completed successfully: {total_statements} statements executed across {len(data_population_files)} files")
    return {
        "status": "success",
        "total_files": len(data_population_files),
        "total_statements": total_statements,
        "total_failures": total_failures,
        "results": results
    }


@asset(
    description="Validate complete setup and data integrity",
    group_name="0_infrastructure_setup",
    kinds={"snowflake", "SQL", "python"},
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

    # Load validation queries from SQL file following schema as code pattern
    sql_file_path = Path(__file__).parent.parent.parent.parent / "sql" / "schema_setup" / "01_setup_validation_queries.sql"

    try:
        with open(sql_file_path, 'r') as file:
            sql_content = file.read()
    except FileNotFoundError:
        context.log.error(f"Validation queries file not found: {sql_file_path}")
        raise

    # Parse individual queries from the SQL file
    # Split by semicolon and filter out comments and empty lines
    raw_queries = sql_content.split(';')
    validation_queries = []

    for query in raw_queries:
        # Clean up the query: remove comments and whitespace
        cleaned_query = []
        for line in query.strip().split('\n'):
            line = line.strip()
            # Skip empty lines and comment lines
            if line and not line.startswith('--'):
                cleaned_query.append(line)

        if cleaned_query:
            final_query = ' '.join(cleaned_query).strip()
            if final_query:
                validation_queries.append(final_query)

    context.log.info(f"Loaded {len(validation_queries)} validation queries from {sql_file_path}")

    validation_results = []

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for query in validation_queries:
                try:
                    context.log.info(f"Executing validation query: {query}")
                    cursor.execute(query)
                    rows = cursor.fetchall()
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
        finally:
            cursor.close()

    successful_validations = len([r for r in validation_results if r["status"] == "success"])
    failed_validations = len([r for r in validation_results if r["status"] == "error"])

    context.log.info(f"Setup validation completed: {successful_validations} successful, {failed_validations} failed")

    # Fail asset if any validation checks failed
    if failed_validations > 0:
        failed_queries = [r["query"][:50] + "..." for r in validation_results if r["status"] == "error"]
        context.log.error(f"Setup validation failed: {failed_validations} validation checks failed")
        raise RuntimeError(f"Setup validation failed: {failed_validations}/{len(validation_queries)} validation checks failed. This indicates incomplete or incorrect setup. Check logs for details.")

    context.log.info(f"✅ Setup validation completed successfully: {successful_validations} validation checks passed")
    return {
        "status": "success",
        "total_validations": len(validation_queries),
        "successful_validations": successful_validations,
        "failed_validations": failed_validations,
        "results": validation_results
    }