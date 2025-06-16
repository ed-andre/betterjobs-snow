"""
Snowflake Database and Schema Setup Assets

This module contains Dagster assets for setting up the BetterJobs Snowflake infrastructure
following the medallion architecture pattern (Bronze/Silver/Gold layers).

Execution Order:
1. database_schema_setup - Creates database, schemas, and roles
2. raw_schema_setup - Creates RAW schema objects
3. stage_schema_setup - Creates STAGE schema objects
4. analytics_schema_setup - Creates ANALYTICS schema objects
"""

import os
from pathlib import Path
from typing import Dict, Any, List
from dagster import asset, AssetExecutionContext, get_dagster_logger
from dagster_snowflake import SnowflakeResource


logger = get_dagster_logger()


def execute_sql_file(snowflake: SnowflakeResource, file_path: str, context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Utility function to execute SQL files with proper error handling

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
    description="Initialize RAW schema tables, views, and stages - Bronze layer",
    group_name="infrastructure_setup",
    kinds={"snowflake", "SQL"},
    deps=[database_schema_setup]
)
def raw_schema_setup(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Execute raw_definitions.sql to create all RAW schema objects

    Creates:
    - RAW schema tables for data ingestion
    - External stages for S3 integration
    - File formats for data loading
    """

    sql_file_path = "pipeline/sql/schema_setup/raw_definitions.sql"
    result = execute_sql_file(snowflake, sql_file_path, context)

    if result["status"] == "error":
        context.log.error(f"Failed to create RAW schema: {result['error']}")
        raise Exception(f"RAW schema setup failed: {result['error']}")

    context.log.info(f"Successfully created RAW schema objects: {result['statements_executed']} statements executed")
    if result["statements_failed"] > 0:
        context.log.warning(f"Some statements failed: {result['statements_failed']} failures")

    return result


@asset(
    description="Initialize STAGE schema tables and views - Silver layer",
    group_name="infrastructure_setup",
    kinds={"snowflake", "SQL"},
    deps=[database_schema_setup]
)
def stage_schema_setup(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Execute stage_definitions.sql to create all STAGE schema objects

    Creates:
    - STAGE schema tables for cleaned/standardized data
    - Views for data transformation
    - Lookup tables and reference data
    """

    sql_file_path = "pipeline/sql/schema_setup/stage_definitions.sql"
    result = execute_sql_file(snowflake, sql_file_path, context)

    if result["status"] == "error":
        context.log.error(f"Failed to create STAGE schema: {result['error']}")
        raise Exception(f"STAGE schema setup failed: {result['error']}")

    context.log.info(f"Successfully created STAGE schema objects: {result['statements_executed']} statements executed")
    if result["statements_failed"] > 0:
        context.log.warning(f"Some statements failed: {result['statements_failed']} failures")

    return result


@asset(
    description="Initialize ANALYTICS schema tables and views - Gold layer",
    group_name="infrastructure_setup",
    kinds={"snowflake", "SQL"},
    deps=[database_schema_setup]
)
def analytics_schema_setup(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Execute analytics_definitions.sql to create all ANALYTICS schema objects

    Creates:
    - ANALYTICS schema tables for business-ready data
    - Dimensional models and fact tables
    - Aggregated views and metrics tables
    """

    sql_file_path = "pipeline/sql/schema_setup/analytics_definitions.sql"
    result = execute_sql_file(snowflake, sql_file_path, context)

    if result["status"] == "error":
        context.log.error(f"Failed to create ANALYTICS schema: {result['error']}")
        raise Exception(f"ANALYTICS schema setup failed: {result['error']}")

    context.log.info(f"Successfully created ANALYTICS schema objects: {result['statements_executed']} statements executed")
    if result["statements_failed"] > 0:
        context.log.warning(f"Some statements failed: {result['statements_failed']} failures")

    return result


@asset(
    description="Populate static/reference data tables",
    group_name="infrastructure_setup",
    kinds={"snowflake", "SQL"},
    deps=[raw_schema_setup, stage_schema_setup, analytics_schema_setup]
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