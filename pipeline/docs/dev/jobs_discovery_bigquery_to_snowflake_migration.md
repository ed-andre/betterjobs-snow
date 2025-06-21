# Job Discovery Assets: BigQuery to Snowflake Migration Guide

This document provides a comprehensive guide for migrating job discovery assets from BigQuery to Snowflake, based on the successful migration of the `bamboohr_company_jobs_discovery` asset.

## Overview

The migration involves updating connection management, query syntax, data type handling, and table operations to work with Snowflake instead of BigQuery. This guide covers all the necessary changes to ensure a smooth transition.

## Key Changes Required

### 1. Resource Configuration

**Before (BigQuery):**
```python
@asset(
    required_resource_keys={"bigquery"},
    # ...
)
```

**After (Snowflake):**
```python
@asset(
    required_resource_keys={"snowflake"},
    # ...
)
```

### 2. Connection Management

**Before (BigQuery):**
```python
client = context.resources.bigquery
project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
dataset_name = os.getenv("BIGQUERY_DATASET", "jobs_raw")
```

**After (Snowflake):**
```python
conn = context.resources.snowflake.get_connection()
database_name = os.getenv("SNOWFLAKE_DATABASE", "BETTERJOBS_DB")
schema_name = os.getenv("SNOWFLAKE_RAW_SCHEMA", "RAW")
```

### 3. Table Creation

**Before (BigQuery):**
```python
table_id = f"{project_id}.{dataset_name}.{platform}_jobs"
table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table, exists_ok=True)
```

**After (Snowflake):**
```python
cursor = conn.cursor()
cursor.execute(f"USE DATABASE {database_name}")
cursor.execute(f"USE SCHEMA {schema_name}")

create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {platform}_jobs (
    job_id STRING,
    company_id STRING,
    job_title STRING,
    job_description STRING,
    job_url STRING,
    location STRING,
    department STRING,
    employment_status STRING,
    date_posted DATE,
    date_retrieved TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN,
    raw_data STRING,
    partition_key STRING,
    compensation STRING,
    work_type STRING
)
"""
cursor.execute(create_table_sql)
conn.commit()
cursor.close()
```

### 4. Data Type Mapping

| BigQuery Type | Snowflake Type | Notes |
|---------------|----------------|-------|
| `STRING` | `STRING` | No change |
| `TIMESTAMP` | `TIMESTAMP_NTZ` | Use NTZ (no timezone) for consistency |
| `DATE` | `DATE` | No change |
| `BOOLEAN` | `BOOLEAN` | No change |
| `INTEGER` | `NUMBER` | Or `INTEGER` |
| `FLOAT` | `FLOAT` | No change |
| `JSON` | `VARIANT` | For structured data |

### 5. Query Syntax Changes

**Regex Pattern Matching:**

**Before (BigQuery):**
```sql
WHERE REGEXP_CONTAINS(company_name, r'^[0-9]')
```

**After (Snowflake):**
```sql
WHERE SUBSTRING(company_name, 1, 1) BETWEEN '0' AND '9'
```

**String Pattern Matching:**

**Before (BigQuery):**
```sql
WHERE company_name LIKE 'A%'
```

**After (Snowflake):**
```sql
WHERE (company_name LIKE 'A%' OR company_name LIKE 'a%')
```

### 6. Data Loading

**Before (BigQuery):**
```python
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.PARQUET
job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

job = client.load_table_from_dataframe(
    jobs_df, table, job_config=job_config
)
job.result()
```

**After (Snowflake):**
```python
from snowflake.connector.pandas_tools import write_pandas

# Prepare data types
for col in jobs_df.select_dtypes(include=['object']).columns:
    if col not in ['date_posted']:
        jobs_df[col] = jobs_df[col].fillna('').astype(str)

# Handle date columns
if 'date_posted' in jobs_df.columns:
    jobs_df['date_posted'] = pd.to_datetime(jobs_df['date_posted'], errors='coerce')
    jobs_df['date_posted'] = jobs_df['date_posted'].dt.date
    jobs_df['date_posted'] = jobs_df['date_posted'].where(pd.notnull(jobs_df['date_posted']), None)

# Handle boolean columns
if 'is_active' in jobs_df.columns:
    jobs_df['is_active'] = jobs_df['is_active'].astype(bool)

# Load data
success, num_chunks, num_rows, output = write_pandas(
    conn,
    jobs_df,
    f'{platform}_jobs',
    database=database_name,
    schema=schema_name,
    auto_create_table=False,
    overwrite=False,
    quote_identifiers=False
)
```

### 7. Connection Cleanup

**Before (BigQuery):**
```python
# BigQuery client handles connection cleanup automatically
```

**After (Snowflake):**
```python
# Always close Snowflake connections
conn.close()
```

## Step-by-Step Migration Process

### Step 1: Update Imports

Add Snowflake-specific imports:
```python
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
```

Remove BigQuery-specific imports:
```python
# Remove these lines
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
```

### Step 2: Update Asset Decorator

Change the required resource from `bigquery` to `snowflake`:
```python
@asset(
    group_name="1_raw_ingestion_extraction",
    kinds={"API", "snowflake", "python"},  # Change from "bigquery" to "snowflake"
    required_resource_keys={"snowflake"},  # Change from {"bigquery"}
    # ... other parameters
)
```

### Step 3: Update Connection Setup

Replace BigQuery client initialization with Snowflake connection:
```python
# Replace BigQuery setup
conn = context.resources.snowflake.get_connection()
database_name = os.getenv("SNOWFLAKE_DATABASE", "BETTERJOBS_DB")
schema_name = os.getenv("SNOWFLAKE_RAW_SCHEMA", "RAW")
```

### Step 4: Update Table Creation Logic

Replace BigQuery table creation with Snowflake DDL:
```python
cursor = conn.cursor()
try:
    cursor.execute(f"USE DATABASE {database_name}")
    cursor.execute(f"USE SCHEMA {schema_name}")

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {platform}_jobs (
        -- Define your table schema here
    )
    """
    cursor.execute(create_table_sql)
    conn.commit()
except Exception as e:
    context.log.error(f"Error creating jobs table: {str(e)}")
    return {"error": str(e), "status": "failed"}
finally:
    cursor.close()
```

### Step 5: Update Query Logic

Update any SQL queries to use Snowflake syntax:
- Replace `REGEXP_CONTAINS` with `SUBSTRING` and `BETWEEN` operations
- Update table references to use three-part naming: `database.schema.table`
- Handle case sensitivity for string comparisons

### Step 6: Update Data Loading

Replace BigQuery loading with `write_pandas`:
```python
# Prepare DataFrame for Snowflake
# ... data type handling code from example above ...

# Load data using write_pandas
success, num_chunks, num_rows, output = write_pandas(
    conn,
    jobs_df,
    f'{platform}_jobs',
    database=database_name,
    schema=schema_name,
    auto_create_table=False,
    overwrite=False,
    quote_identifiers=False
)

if success:
    context.log.info(f"Successfully loaded {len(jobs_df)} jobs")
else:
    context.log.error(f"Failed to load jobs: {output}")
```

### Step 7: Add Connection Cleanup

Ensure Snowflake connections are properly closed:
```python
# At the end of your asset function
conn.close()
```

## Common Pitfalls and Solutions

### 1. Case Sensitivity
**Issue:** Snowflake is case-sensitive by default.
**Solution:** Use both uppercase and lowercase in LIKE patterns:
```sql
WHERE (company_name LIKE 'A%' OR company_name LIKE 'a%')
```

### 2. Date Handling
**Issue:** Date format differences between BigQuery and Snowflake.
**Solution:** Explicitly convert pandas datetime to date objects:
```python
jobs_df['date_posted'] = pd.to_datetime(jobs_df['date_posted'], errors='coerce')
jobs_df['date_posted'] = jobs_df['date_posted'].dt.date
```

### 3. Boolean Columns
**Issue:** Boolean handling differs between platforms.
**Solution:** Explicitly cast boolean columns:
```python
jobs_df['is_active'] = jobs_df['is_active'].astype(bool)
```

### 4. NULL Handling
**Issue:** Different NULL behavior in pandas and Snowflake.
**Solution:** Use `where()` to handle NaN values:
```python
jobs_df['date_posted'] = jobs_df['date_posted'].where(pd.notnull(jobs_df['date_posted']), None)
```

### 5. Connection Management
**Issue:** Forgetting to close Snowflake connections.
**Solution:** Always use try/finally blocks or context managers.

## Testing the Migration

### 1. Unit Tests
Update unit tests to mock Snowflake resources instead of BigQuery:
```python
@mock.patch('dagster_betterjobs.resources.snowflake')
def test_job_discovery_asset(mock_snowflake):
    # Test implementation
```

### 2. Integration Tests
Test with a Snowflake development environment:
- Verify table creation
- Test data loading
- Validate query results
- Check partition filtering

### 3. Data Validation
Compare results between BigQuery and Snowflake versions:
- Row counts should match
- Data types should be preserved
- Query performance should be acceptable

## Environment Variables

Ensure these environment variables are configured:
```bash
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=BETTERJOBS_DB
SNOWFLAKE_RAW_SCHEMA=RAW
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_ROLE=your_role
```

## Assets to Migrate

The following job discovery assets need to be migrated using this guide:
- `greenhouse_company_jobs_discovery`
- `lever_company_jobs_discovery`
- `workable_company_jobs_discovery`
- `smartrecruiters_company_jobs_discovery`
- Any other `*_jobs_discovery` assets

## Performance Considerations

1. **Batch Size:** Snowflake handles larger batches well, consider increasing batch sizes
2. **Connection Pooling:** Use connection pooling for high-volume processing
3. **Warehouse Size:** Choose appropriate warehouse size for your data volume
4. **Query Optimization:** Use Snowflake-specific query optimizations

## Rollback Plan

Keep BigQuery versions available during migration:
1. Create new Snowflake assets with different names initially
2. Test thoroughly in development
3. Run both versions in parallel during transition
4. Deprecate BigQuery versions only after Snowflake versions are proven stable

## Support and Troubleshooting

For issues during migration:
1. Check Snowflake connection parameters
2. Verify table schemas match expectations
3. Test queries in Snowflake console first
4. Review Dagster logs for detailed error messages
5. Validate data types and NULL handling