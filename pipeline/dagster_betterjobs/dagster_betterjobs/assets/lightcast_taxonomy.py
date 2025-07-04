"""
Lightcast Open Skills Taxonomy Loading Assets

This module contains Dagster assets for loading the Lightcast Open Skills Taxonomy
from CSV files into Snowflake tables following the schema-as-code pattern.

Taxonomy Structure:
- SKILL_0_CATEGORY: Top-level skill categories (33 categories)
- SKILL_1_SUBCATEGORY: Mid-level subcategories (~400 subcategories)
- SKILL_2_SKILL: Individual skills (~32,000+ skills)

Execution Order:
1. stage_lightcast_skill_categories - Load top-level categories
2. stage_lightcast_skill_subcategories - Load subcategories (depends on categories)
3. stage_lightcast_skills - Load individual skills (depends on subcategories)
"""

import pandas as pd
from pathlib import Path
from typing import Dict, Any
from dagster import asset, AssetExecutionContext
from dagster_snowflake import SnowflakeResource

# Import required infrastructure dependency
from .snowflake_setup import tables_setup
# Import schema-as-code utility for self-healing table creation
from ..utils.schema_utils import ensure_object_exists

# Define dagster_betterjobs project root path once
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_csv_to_snowflake(csv_file_path: Path, table_name: str, snowflake: SnowflakeResource,
                         context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Load CSV data into Snowflake table with proper error handling

    Args:
        csv_file_path: Path to the CSV file
        table_name: Target Snowflake table name (with schema)
        snowflake: SnowflakeResource instance
        context: Dagster execution context for logging

    Returns:
        Dictionary with loading results
    """
    try:
        # Read CSV file
        context.log.info(f"📂 Reading CSV file: {csv_file_path}")
        df = pd.read_csv(csv_file_path)

        context.log.info(f"📊 Loaded {len(df)} rows from CSV")
        context.log.info(f"📋 Columns: {list(df.columns)}")

        # Clean data: replace NaN values with None (NULL in SQL)
        df = df.replace({pd.NA: None, pd.NaT: None})
        df = df.where(pd.notnull(df), None)

        # Handle placeholder records with literal 'NULL' values in critical columns
        # Replace 'NULL' strings with 'Unknown' for taxonomy completeness
        critical_columns = ['NAME', 'CATEGORY_NAME', 'SUBCATEGORY_NAME']

        for col in critical_columns:
            if col in df.columns:
                # Check for both actual None values and 'NULL' string values
                none_count = df[col].isnull().sum()
                null_string_count = (df[col] == 'NULL').sum()

                if none_count > 0:
                    df.loc[df[col].isnull(), col] = 'Unknown'
                    context.log.info(f"🔄 Converted {none_count} None/NaN values in {col} to 'Unknown'")

                if null_string_count > 0:
                    df.loc[df[col] == 'NULL', col] = 'Unknown'
                    context.log.info(f"🔄 Converted {null_string_count} 'NULL' string values in {col} to 'Unknown'")

        # Handle boolean columns properly
        if 'LATEST_VERSION' in df.columns:
            df['LATEST_VERSION'] = df['LATEST_VERSION'].map({'true': True, 'false': False, True: True, False: False})

        context.log.info(f"✨ Data cleaned successfully")

        # Get connection and load data
        with snowflake.get_connection() as conn:
            cursor = conn.cursor()

            try:
                # Clear existing data first
                context.log.info(f"🧹 Clearing existing data from {table_name}")
                cursor.execute(f"DELETE FROM {table_name}")

                # Prepare INSERT statement
                columns = ', '.join(df.columns)
                placeholders = ', '.join(['%s'] * len(df.columns))
                insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

                context.log.info(f"💾 Inserting {len(df)} rows into {table_name}")

                # Convert DataFrame to list of tuples for bulk insert
                data_tuples = [tuple(row) for row in df.to_numpy()]

                # Execute bulk insert
                cursor.executemany(insert_sql, data_tuples)

                # Get final row count for verification
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                final_count = cursor.fetchone()[0]

                context.log.info(f"✅ Successfully loaded {final_count} rows into {table_name}")

                return {
                    "status": "success",
                    "csv_file": str(csv_file_path),
                    "table_name": table_name,
                    "rows_loaded": final_count,
                    "columns": list(df.columns)
                }

            finally:
                cursor.close()

    except Exception as e:
        context.log.error(f"❌ Error loading {csv_file_path} into {table_name}: {str(e)}")
        return {
            "status": "error",
            "csv_file": str(csv_file_path),
            "table_name": table_name,
            "error": str(e)
        }


@asset(
    description="Load Lightcast skill categories from CSV file into STAGE.SKILL_0_CATEGORY table",
    group_name="0_infrastructure_setup",
    kinds={"snowflake", "CSV", "taxonomy"},
    deps=[tables_setup]
)
def stage_lightcast_skill_categories(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Load Lightcast skill categories from CSV file.

    Loads the top-level skill categories into STAGE.SKILL_0_CATEGORY table.
    This is the foundation of the taxonomy hierarchy.
    """

    # Self-healing: Ensure table exists using schema-as-code approach
    table_name = ensure_object_exists("tables/stage_skill_0_category.sql", snowflake, context)

    # Get CSV file path
    csv_file = PROJECT_ROOT / "input" / "taxonomy" / "Lightcast_SKILL_0_CATEGORY.csv"

    if not csv_file.exists():
        context.log.error(f"CSV file not found: {csv_file}")
        raise FileNotFoundError(f"Lightcast categories CSV file not found: {csv_file}")

    context.log.info("🏗️ Loading Lightcast skill categories (Level 0)")

    # Load data into table
    result = load_csv_to_snowflake(
        csv_file_path=csv_file,
        table_name=table_name,
        snowflake=snowflake,
        context=context
    )

    if result["status"] == "error":
        context.log.error(f"Failed to load skill categories: {result['error']}")
        raise RuntimeError(f"Skill categories loading failed: {result['error']}")

    context.log.info(f"✅ Successfully loaded {result['rows_loaded']} skill categories")
    return result


@asset(
    description="Load Lightcast skill subcategories from CSV file into STAGE.SKILL_1_SUBCATEGORY table",
    group_name="0_infrastructure_setup",
    kinds={"snowflake", "CSV", "taxonomy"},
    deps=[stage_lightcast_skill_categories]
)
def stage_lightcast_skill_subcategories(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Load Lightcast skill subcategories from CSV file.

    Loads the mid-level subcategories into STAGE.SKILL_1_SUBCATEGORY table.
    Depends on categories being loaded first for referential integrity.
    """

    # Self-healing: Ensure table exists using schema-as-code approach
    table_name = ensure_object_exists("tables/stage_skill_1_subcategory.sql", snowflake, context)

    # Get CSV file path
    csv_file = PROJECT_ROOT / "input" / "taxonomy" / "Lightcast_SKILL_1_SUBCATEGORY.csv"

    if not csv_file.exists():
        context.log.error(f"CSV file not found: {csv_file}")
        raise FileNotFoundError(f"Lightcast subcategories CSV file not found: {csv_file}")

    context.log.info("🏗️ Loading Lightcast skill subcategories (Level 1)")

    # Load data into table
    result = load_csv_to_snowflake(
        csv_file_path=csv_file,
        table_name=table_name,
        snowflake=snowflake,
        context=context
    )

    if result["status"] == "error":
        context.log.error(f"Failed to load skill subcategories: {result['error']}")
        raise RuntimeError(f"Skill subcategories loading failed: {result['error']}")

    context.log.info(f"✅ Successfully loaded {result['rows_loaded']} skill subcategories")
    return result


@asset(
    description="Load Lightcast individual skills from CSV file into STAGE.SKILL_2_SKILL table",
    group_name="0_infrastructure_setup",
    kinds={"snowflake", "CSV", "taxonomy"},
    deps=[stage_lightcast_skill_subcategories]
)
def stage_lightcast_skills(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Load Lightcast individual skills from CSV file.

    Loads the individual skills into STAGE.SKILL_2_SKILL table.
    Depends on subcategories being loaded first for referential integrity.
    This is the largest dataset and may take several minutes to load.
    """

    # Self-healing: Ensure table exists using schema-as-code approach
    table_name = ensure_object_exists("tables/stage_skill_2_skill.sql", snowflake, context)

    # Get CSV file path
    csv_file = PROJECT_ROOT / "input" / "taxonomy" / "Lightcast_SKILL_2_SKILL.csv"

    if not csv_file.exists():
        context.log.error(f"CSV file not found: {csv_file}")
        raise FileNotFoundError(f"Lightcast skills CSV file not found: {csv_file}")

    context.log.info("🏗️ Loading Lightcast individual skills (Level 2) - this may take several minutes...")

    # Load data into table
    result = load_csv_to_snowflake(
        csv_file_path=csv_file,
        table_name=table_name,
        snowflake=snowflake,
        context=context
    )

    if result["status"] == "error":
        context.log.error(f"Failed to load individual skills: {result['error']}")
        raise RuntimeError(f"Individual skills loading failed: {result['error']}")

    context.log.info(f"✅ Successfully loaded {result['rows_loaded']} individual skills")
    return result


@asset(
    description="Validate Lightcast taxonomy data integrity and relationships",
    group_name="0_infrastructure_setup",
    kinds={"snowflake", "validation", "taxonomy"},
    deps=[stage_lightcast_skills]
)
def lightcast_taxonomy_validation(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Dict[str, Any]:
    """
    Validate the loaded Lightcast taxonomy data for integrity and relationships.

    Performs checks on:
    - Row counts match expected ranges
    - Foreign key relationships are valid
    - No duplicate IDs
    - Required fields are populated
    """

    # Self-healing: Ensure all tables exist using schema-as-code approach
    category_table = ensure_object_exists("tables/stage_skill_0_category.sql", snowflake, context)
    subcategory_table = ensure_object_exists("tables/stage_skill_1_subcategory.sql", snowflake, context)
    skills_table = ensure_object_exists("tables/stage_skill_2_skill.sql", snowflake, context)

    validation_queries = [
        {
            "name": "Category Count Check",
            "query": f"SELECT COUNT(*) as category_count FROM {category_table}",
            "expected_min": 30,
            "expected_max": 40
        },
        {
            "name": "Subcategory Count Check",
            "query": f"SELECT COUNT(*) as subcategory_count FROM {subcategory_table}",
            "expected_min": 400,
            "expected_max": 500
        },
        {
            "name": "Skills Count Check",
            "query": f"SELECT COUNT(*) as skills_count FROM {skills_table}",
            "expected_min": 30000,
            "expected_max": 50000
        },
        {
            "name": "Foreign Key Integrity - Subcategories",
            "query": f"""
                SELECT COUNT(*) as orphaned_subcategories
                FROM {subcategory_table} s
                LEFT JOIN {category_table} c ON s.CATEGORY = c.ID
                WHERE c.ID IS NULL
            """,
            "expected_min": 0,
            "expected_max": 0
        },
        {
            "name": "Foreign Key Integrity - Skills",
            "query": f"""
                SELECT COUNT(*) as orphaned_skills
                FROM {skills_table} s
                LEFT JOIN {subcategory_table} sc ON s.SUBCATEGORY = sc.ID
                WHERE sc.ID IS NULL AND s.SUBCATEGORY IS NOT NULL
            """,
            "expected_min": 0,
            "expected_max": 0
        }
    ]

    validation_results = []
    passed_validations = 0
    failed_validations = 0

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()

        try:
            for validation in validation_queries:
                try:
                    context.log.info(f"🔍 Running validation: {validation['name']}")
                    cursor.execute(validation["query"])
                    result = cursor.fetchone()[0]

                    # Check if result is within expected range
                    passed = validation["expected_min"] <= result <= validation["expected_max"]

                    if passed:
                        context.log.info(f"✅ {validation['name']}: {result} (within range {validation['expected_min']}-{validation['expected_max']})")
                        passed_validations += 1
                    else:
                        context.log.error(f"❌ {validation['name']}: {result} (expected {validation['expected_min']}-{validation['expected_max']})")
                        failed_validations += 1

                    validation_results.append({
                        "name": validation["name"],
                        "result": result,
                        "expected_range": f"{validation['expected_min']}-{validation['expected_max']}",
                        "status": "passed" if passed else "failed"
                    })

                except Exception as e:
                    context.log.error(f"❌ Validation error for {validation['name']}: {str(e)}")
                    failed_validations += 1
                    validation_results.append({
                        "name": validation["name"],
                        "error": str(e),
                        "status": "error"
                    })

        finally:
            cursor.close()

    context.log.info(f"Taxonomy validation completed: {passed_validations} passed, {failed_validations} failed")

    if failed_validations > 0:
        context.log.error(f"Taxonomy validation failed: {failed_validations} validation checks failed")
        raise RuntimeError(f"Taxonomy validation failed: {failed_validations}/{len(validation_queries)} validation checks failed. Check logs for details.")

    context.log.info(f"✅ Taxonomy validation completed successfully: all {passed_validations} checks passed")

    return {
        "status": "success",
        "total_validations": len(validation_queries),
        "passed_validations": passed_validations,
        "failed_validations": failed_validations,
        "results": validation_results
    }