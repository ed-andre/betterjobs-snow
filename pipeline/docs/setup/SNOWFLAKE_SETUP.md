# Snowflake Master Company URLs Asset Setup

This document provides instructions for setting up and using the new Snowflake-based master company URLs asset that replaces the BigQuery implementation.

## Overview

The `snowflake_master_company_urls` asset provides:
- CSV ingestion from both S3 buckets and local folders
- Incremental processing to detect new/changed files
- Snowflake stages for efficient S3 data loading
- File tracking to avoid reprocessing
- Data storage in RAW schema for further processing
- Automatic deduplication based on company IDs
- Robust timestamp handling for CSV imports
- Error-resilient processing with detailed logging

## Prerequisites

### 1. Snowflake Setup
Ensure your Snowflake environment is configured with the required warehouse, database, and permissions:

#### Basic Setup (Required)
```sql
-- Create warehouse (if not already created)
CREATE WAREHOUSE IF NOT EXISTS BETTERJOBS_WH
    WITH WAREHOUSE_SIZE = 'Small'
    AUTO_SUSPEND = 60
    AUTO_RESUME = true;

-- Create database (if not already created)
CREATE DATABASE IF NOT EXISTS BETTERJOBS_DB;

-- Create RAW schema (will be created automatically by the asset)
CREATE SCHEMA IF NOT EXISTS BETTERJOBS_DB.RAW;
```

#### Advanced Setup (Recommended for Production)
For production environments or when using a dedicated service account, create a specific role with minimal required permissions:

```sql
-- Create role specific to the project
CREATE ROLE IF NOT EXISTS BETTERJOBS_ROLE;

-- Grant necessary permissions to the role
GRANT USAGE ON WAREHOUSE BETTERJOBS_WH TO ROLE BETTERJOBS_ROLE;
GRANT USAGE ON DATABASE BETTERJOBS_DB TO ROLE BETTERJOBS_ROLE;
GRANT CREATE SCHEMA ON DATABASE BETTERJOBS_DB TO ROLE BETTERJOBS_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE BETTERJOBS_DB TO ROLE BETTERJOBS_ROLE;
GRANT CREATE TABLE ON ALL SCHEMAS IN DATABASE BETTERJOBS_DB TO ROLE BETTERJOBS_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE BETTERJOBS_DB TO ROLE BETTERJOBS_ROLE;

-- Grant the BETTERJOBS_ROLE to your user (replace with actual username)
GRANT ROLE BETTERJOBS_ROLE TO USER your_username_here;

-- Set default role and warehouse for user (optional but recommended)
ALTER USER your_username_here SET DEFAULT_ROLE = BETTERJOBS_ROLE;
ALTER USER your_username_here SET DEFAULT_WAREHOUSE = BETTERJOBS_WH;
```

**Note:**
- Replace `your_username_here` with your actual Snowflake username
- For production, use `BETTERJOBS_ROLE` instead of `ACCOUNTADMIN` for the `SNOWFLAKE_ROLE` environment variable
- The basic setup can use `ACCOUNTADMIN` role for development/testing

### 2. Required Dependencies
These dependencies are automatically included in setup.py:
```
snowflake-connector-python>=3.0.0
boto3>=1.26.0  # For S3 integration
pyarrow>=10.0.0  # Required for pandas integration with Snowflake
```

### 3. Environment Variables
Set the following environment variables:

#### Snowflake Connection
```bash
export SNOWFLAKE_ACCOUNT="your_account.region"
export SNOWFLAKE_USER="your_username"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_WAREHOUSE="BETTERJOBS_WH"
export SNOWFLAKE_DATABASE="BETTERJOBS_DB"
export SNOWFLAKE_RAW_SCHEMA="RAW"
export SNOWFLAKE_ROLE="BETTERJOBS_ROLE"  # Use BETTERJOBS_ROLE for production, ACCOUNTADMIN for development
```

#### Data Sources
```bash
# S3 bucket URI (optional)
export S3_URI="s3://your-bucket-name/path/to/csv/files/"

# Local input folder (optional)
export MAIN_INPUT_FOLDER="/path/to/local/csv/files"
```

**Note:** At least one of S3_URI or MAIN_INPUT_FOLDER must be configured.

## CSV File Format

The asset expects CSV files with the following columns:
- `company_name` (required): Name of the company
- `company_industry` (optional): Industry classification
- `platform` (optional): ATS platform (workday, greenhouse, bamboohr, etc.)
- `ats_url` (optional): URL to the ATS job board
- `career_url` (optional): URL to the company's career page
- `url_verified` (optional): Boolean indicating if URLs are verified
- `date_added` (optional): Timestamp when record was first added
- `last_updated` (optional): Timestamp when record was last updated

Example CSV content:
```csv
company_name,company_industry,platform,ats_url,career_url,url_verified,date_added,last_updated
"Acme Corporation","Technology","workday","https://acme.workday.com","https://acme.com/careers",true,"2024-01-15 10:30:00","2024-01-15 10:30:00"
"Tech Solutions Inc","Software","greenhouse","https://greenhouse.io/acme","https://techsolutions.com/jobs",true,"2024-01-16 09:15:00","2024-01-16 09:15:00"
```

**Note:** Timestamp columns support multiple formats and are automatically converted to Snowflake-compatible format.

## S3 Setup (Optional)

If using S3 as a data source:

### 1. Create S3 Bucket and Upload Files
```bash
# Create bucket (if needed)
aws s3 mb s3://your-bucket-name

# Upload CSV files
aws s3 cp company_data.csv s3://your-bucket-name/path/to/csv/files/
```

### 2. Snowflake S3 Integration
The asset requires a Snowflake storage integration named `betterjobs_s3_integration`. Set this up once:

```sql
-- Create storage integration
CREATE STORAGE INTEGRATION betterjobs_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::your-account:role/snowflake-s3-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://your-bucket-name/');

-- Grant usage to your role
GRANT USAGE ON INTEGRATION betterjobs_s3_integration TO ROLE your_role;
```

Refer to [Snowflake's S3 documentation](https://docs.snowflake.com/en/user-guide/data-load-s3) for detailed setup.

## Asset Configuration

The asset accepts the following configuration parameters:

```python
@asset
def snowflake_master_company_urls(
    context: AssetExecutionContext,
    config: SnowflakeMasterCompanyUrlsConfig  # Optional configuration
) -> pd.DataFrame:
```

### Configuration Options
- `batch_size`: Number of records to process in batches (default: 1000)
- `enable_s3_processing`: Whether to process S3 files (default: True)
- `enable_local_processing`: Whether to process local files (default: True)
- `deduplicate_on_load`: Whether to deduplicate data after loading (default: True)

## Usage Examples

### 1. Basic Usage (Process all sources)
```python
# Run the asset with default configuration
from dagster import materialize
from dagster_betterjobs.assets.snowflake_master_company_urls import snowflake_master_company_urls

result = materialize([snowflake_master_company_urls])
```

### 2. S3 Only Processing
```python
from dagster_betterjobs.assets.snowflake_master_company_urls import (
    snowflake_master_company_urls,
    SnowflakeMasterCompanyUrlsConfig
)

config = SnowflakeMasterCompanyUrlsConfig(
    enable_s3_processing=True,
    enable_local_processing=False,
    batch_size=500
)

result = materialize([snowflake_master_company_urls], config=config)
```

### 3. Local Files Only
```python
config = SnowflakeMasterCompanyUrlsConfig(
    enable_s3_processing=False,
    enable_local_processing=True,
    deduplicate_on_load=True
)

result = materialize([snowflake_master_company_urls], config=config)
```

## Database Schema

The asset creates the following tables in the RAW schema:

### 1. master_company_urls
Main table storing company URL data:
```sql
CREATE TABLE RAW.master_company_urls (
    company_id STRING,                    -- Generated 8-char hash
    company_name STRING NOT NULL,        -- Company name
    company_industry STRING,             -- Industry classification
    platform STRING,                     -- ATS platform
    ats_url STRING,                      -- ATS job board URL
    career_url STRING,                   -- Company career page URL
    url_verified BOOLEAN DEFAULT FALSE, -- URL verification status
    date_added TIMESTAMP_NTZ,           -- First ingestion timestamp
    last_updated TIMESTAMP_NTZ,         -- Last update timestamp
    source_file STRING,                 -- Source file name
    file_hash STRING,                   -- File hash for tracking
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### 2. master_company_urls_processing_log
Tracks processed files for incremental processing:
```sql
CREATE TABLE RAW.master_company_urls_processing_log (
    file_path STRING,                   -- Full file path/URI
    file_hash STRING,                  -- File content hash
    file_size INTEGER,                 -- File size in bytes
    file_modified_time TIMESTAMP_NTZ,  -- File modification time
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    record_count INTEGER,              -- Number of records processed
    source_type STRING                 -- 's3' or 'local'
);
```

## Features

### Timestamp Handling
The asset includes robust timestamp processing:
- Automatic detection and conversion of timezone-aware timestamps
- Support for multiple timestamp formats in CSV files
- Proper handling of both string and datetime objects
- Conversion to Snowflake-compatible format (`YYYY-MM-DD HH:MM:SS.ffffff`)

### Error Resilience
- Graceful handling of missing or malformed files
- Detailed error logging with context
- Continuation of processing when individual files fail
- Comprehensive validation of file formats and data

### Performance Optimizations
- Uses `write_pandas` for bulk data insertion
- Efficient file hashing for change detection
- Batch processing for large datasets
- Optimized SQL for deduplication

## Incremental Processing

The asset implements incremental processing by:

1. **File Tracking**: Maintains a log of processed files with their hashes
2. **Hash Comparison**: Compares file hashes to detect changes
3. **Skip Logic**: Skips files that haven't changed since last processing
4. **Metadata Storage**: Stores file metadata for future comparisons

### For Local Files:
- Uses file modification time and content hash
- Tracks full file path
- Detects file changes and reprocesses automatically

### For S3 Files:
- Uses S3 metadata (size, last modified) to generate hash
- Tracks S3 URI
- Handles file path variations automatically

## Monitoring and Troubleshooting

### 1. Check Asset Metadata
The asset provides comprehensive metadata including:
- Files processed counts
- Record counts
- Error counts
- Processing statistics
- Performance metrics

### 2. Query Processing Log
```sql
-- Check what files have been processed
SELECT file_path, processed_at, record_count, source_type
FROM RAW.master_company_urls_processing_log
ORDER BY processed_at DESC;

-- Check for errors (files with 0 records)
SELECT * FROM RAW.master_company_urls_processing_log
WHERE record_count = 0;

-- Check processing summary by source type
SELECT
    source_type,
    COUNT(*) as file_count,
    SUM(record_count) as total_records,
    AVG(record_count) as avg_records_per_file
FROM RAW.master_company_urls_processing_log
GROUP BY source_type;
```

### 3. Verify Data Quality
```sql
-- Check total records and unique companies
SELECT
    COUNT(*) as total_records,
    COUNT(DISTINCT company_id) as unique_companies,
    COUNT(CASE WHEN url_verified THEN 1 END) as verified_urls,
    COUNT(CASE WHEN ats_url IS NOT NULL AND ats_url != '' THEN 1 END) as records_with_ats_url,
    COUNT(CASE WHEN career_url IS NOT NULL AND career_url != '' THEN 1 END) as records_with_career_url
FROM RAW.master_company_urls;

-- Check for potential duplicates
SELECT company_id, COUNT(*), STRING_AGG(company_name, ', ') as names
FROM RAW.master_company_urls
GROUP BY company_id
HAVING COUNT(*) > 1;

-- Check platform distribution
SELECT platform, COUNT(*) as company_count
FROM RAW.master_company_urls
GROUP BY platform
ORDER BY company_count DESC;
```

### 4. Common Troubleshooting

#### S3 Access Issues
```sql
-- Test storage integration
SELECT SYSTEM$VALIDATE_STORAGE_INTEGRATION('betterjobs_s3_integration');

-- List files in stage
LIST @COMPANY_URLS_STAGE;
```

#### Timestamp Format Issues
The asset automatically handles various timestamp formats. Check logs for conversion warnings.

#### Performance Issues
- Increase `batch_size` for larger files
- Scale up Snowflake warehouse if needed
- Monitor processing log for slow files

## Migration from BigQuery

To migrate from the existing BigQuery implementation:

1. **Parallel Testing**: Run both assets in parallel to compare results
2. **Data Validation**: Compare record counts and data quality
3. **Switch Dependencies**: Update downstream assets to depend on the Snowflake asset
4. **Decommission**: Remove BigQuery asset after successful validation

Example dependency update:
```python
# Before (BigQuery dependency)
@asset(deps=["master_company_urls"])
def downstream_asset():
    pass

# After (Snowflake dependency)
@asset(deps=["snowflake_master_company_urls"])
def downstream_asset():
    pass
```

## Performance Considerations

1. **Batch Size**: Default 1000 works well for most cases. Increase for larger files.
2. **Warehouse Size**: Consider 'Medium' or 'Large' for processing many large files
3. **File Organization**: Organize S3 files to minimize LIST operations
4. **Deduplication**: Can be disabled for better performance if not needed
5. **Incremental Processing**: Significantly reduces processing time for unchanged files

## Security Best Practices

1. **Environment Variables**: Store sensitive credentials in environment variables
2. **IAM Roles**: Use IAM roles for S3 access instead of access keys
3. **Snowflake Roles**: Use least-privilege roles for Snowflake access
4. **Network Security**: Consider VPC/private networks for S3-Snowflake communication
5. **Data Encryption**: Ensure S3 buckets and Snowflake have encryption enabled