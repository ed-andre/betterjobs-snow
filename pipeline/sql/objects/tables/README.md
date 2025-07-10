# Database Table Definitions

This directory contains SQL table definition files for the BetterJobs data pipeline following the medallion architecture (Raw → Stage → Analytics → Serve layers). Each table has exactly one canonical SQL definition file that serves as the source of truth.

## Purpose

**Table definitions are managed as SQL files to:**
- ✅ Enable version control of database schema changes
- ✅ Support self-healing infrastructure via `ensure_object_exists()`
- ✅ Maintain clear separation between schema and application logic
- ✅ Allow dependency management through YAML configuration
- ✅ Facilitate automated table creation in correct order

## Table Layers

### Raw Layer
- **Purpose**: Raw, unprocessed data from external sources
- **Examples**: `raw_bamboohr_jobs.sql`, `raw_greenhouse_jobs.sql`
- **Naming**: `raw_{source}_{entity}.sql`

### Stage Layer
- **Purpose**: Cleaned, standardized, and normalized data
- **Examples**: `stage_jobs_unified.sql`, `stage_skills_normalized.sql`
- **Naming**: `stage_{entity}_{purpose}.sql`

### Analytics Layer
- **Purpose**: Business-ready dimensional model and fact tables
- **Examples**: `analytics_dim_company.sql`, `analytics_fact_job_postings.sql`
- **Naming**: `analytics_{type}_{entity}.sql`

### Serve Layer
- **Purpose**: Denormalized tables optimized for application queries
- **Examples**: `serve_denorm_job_postings.sql`, `serve_denorm_skills.sql`
- **Naming**: `serve_{purpose}_{entity}.sql`

## Adding New Tables

### 1. Create the SQL File

Create a new SQL file with the table definition:

```sql
-- Example: stage_new_entity.sql
CREATE TABLE IF NOT EXISTS ${SNOWFLAKE_DATABASE}.STAGE.new_entity (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add comments for documentation
COMMENT ON TABLE ${SNOWFLAKE_DATABASE}.STAGE.new_entity IS 'Description of what this table contains';
COMMENT ON COLUMN ${SNOWFLAKE_DATABASE}.STAGE.new_entity.id IS 'Unique identifier';
COMMENT ON COLUMN ${SNOWFLAKE_DATABASE}.STAGE.new_entity.name IS 'Entity name';
```

### 2. Update Dependency Configuration

Add your new table to `table_creation_order.yaml` in the appropriate layer and position:

```yaml
stage_layer:
  # Core foundation tables
  - stage_company_profiles.sql
  - stage_transformation_logs.sql

  # Add your table in the correct dependency order
  - stage_new_entity.sql  # Your new table

  # Tables that depend on your table come after
  - stage_entity_bridge.sql
```

### 3. Use in Dagster Assets

Reference the table in your assets using `ensure_object_exists()`:

```python
from dagster import asset, AssetExecutionContext
from dagster_snowflake import SnowflakeResource
from ..utils.schema_utils import ensure_object_exists

@asset
def process_new_entity(context: AssetExecutionContext, snowflake: SnowflakeResource):
    """Process data for new entity table"""

    # Ensure the table exists (creates if missing)
    table_name = ensure_object_exists("tables/stage_new_entity.sql", snowflake, context)

    # Use the table in your processing
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            INSERT INTO {table_name} (id, name, category)
            SELECT id, name, category
            FROM source_table
            WHERE condition = 'value'
        """)

    return {"table": table_name, "status": "success"}
```

## Schema-as-Code Benefits

### Self-Healing Infrastructure
The `ensure_object_exists()` function automatically creates missing tables:

```python
# If stage_jobs_unified table doesn't exist, it gets created automatically
table_name = ensure_object_exists("tables/stage_jobs_unified.sql", snowflake, context)
```

### Environment Variable Support
SQL files support environment variable substitution:

```sql
-- Use ${SNOWFLAKE_DATABASE} for environment-specific database names
CREATE TABLE IF NOT EXISTS ${SNOWFLAKE_DATABASE}.STAGE.my_table (
    id VARCHAR(50) PRIMARY KEY
);
```

### Dependency Management
Tables are created in the correct order based on `table_creation_order.yaml` configuration, preventing foreign key constraint violations.

## Best Practices

### 1. File Naming Convention
- **Pattern**: `{layer}_{entity}_{purpose}.sql`
- **Layer**: `raw`, `stage`, `analytics`, `serve`
- **Entity**: Primary subject (jobs, companies, skills)
- **Purpose**: Optional suffix (normalized, bridge, summary)

### 2. SQL Structure Guidelines
- Always use `CREATE TABLE IF NOT EXISTS`
- Include `${SNOWFLAKE_DATABASE}` for environment portability
- Add table and column comments for documentation
- Use consistent data types across similar fields
- Include audit fields (`created_date`, `updated_date`)

### 3. Dependency Management
- **Foundation first**: Base tables before derived tables
- **References last**: Bridge/junction tables after referenced tables
- **Layer respect**: Raw → Stage → Analytics → Serve
- **Avoid cycles**: No circular dependencies

### 4. Testing
- Test SQL files individually in Snowflake before committing
- Verify table creation order doesn't break dependencies
- Check that `ensure_object_exists()` works in development environment

## Configuration File Structure

The `table_creation_order.yaml` organizes tables by layer and dependency:

```yaml
raw_layer:
  - raw_source_data.sql     # Independent raw tables first

stage_layer:
  - stage_base_tables.sql   # Foundation tables
  - stage_normalized.sql    # Normalized dimensions
  - stage_bridges.sql       # Junction tables last

analytics_layer:
  - analytics_dims.sql      # Dimensions first
  - analytics_facts.sql     # Facts after dimensions

serve_layer:
  - serve_denorm.sql        # Serving tables last
```

## Integration with Infrastructure

Tables are automatically created during infrastructure setup:

1. **`tables_setup` Asset**: Reads `table_creation_order.yaml` and processes all table files in dependency order
2. **Asset Dependencies**: Individual assets use `ensure_object_exists()` for self-healing
3. **Environment Portability**: `${SNOWFLAKE_DATABASE}` enables deployment across environments

This approach ensures reliable, maintainable database schema management with automatic dependency resolution and self-healing capabilities.