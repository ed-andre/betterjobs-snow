# BetterJobs Snowflake Infrastructure Setup

This document provides instructions for setting up the BetterJobs Snowflake infrastructure using automated setup assets.

## Prerequisites

Configure your environment variables using the provided `.env.example` under `pipeline/dagster_betterjobs` if you haven't already:

1. Rename it to `.env`
2. **Edit the .env file** with your actual Snowflake credentials and AWS S3 configuration

**Note:** ACCOUNTADMIN role is required for the initial infrastructure setup.

## Infrastructure Setup Procedure

![Infrastructure Setup Pipeline](../../media/0-infra-setup_dagsterpipeline.png)

Start the Dagster development environment if you haven't already:
```bash
cd pipeline/dagster_betterjobs
dagster dev
```

Follow these steps in order in the Dagster UI:

### Step 1: Run database_schema_setup Asset
Materialize the `database_schema_setup` asset to create:
- BETTERJOBS_WH warehouse
- BETTERJOBS_DB database
- BETTERJOBS_ROLE with appropriate permissions
- RAW, STAGE, and ANALYTICS schemas

### Step 2: Set Up AWS IAM Role
Before running the infrastructure setup, you need to create the AWS IAM role that Snowflake will use to access S3.

**For detailed instructions, see: [S3_SNOWFLAKE_SETUP.md](S3_SNOWFLAKE_SETUP.md)**

Quick summary:
1. Create an IAM policy with S3 permissions for the `betterjobs-dagster` bucket
2. Create an IAM role (e.g., `SnowflakeBetterJobsRole`) and attach the policy
3. Set the `SNOWFLAKE_S3_ROLE_ARN` in your `.env` file to the ARN of the IAM role

### Step 3: Run infrastructure_setup Asset
Materialize the `infrastructure_setup` asset to create:
- S3 storage integration (`betterjobs_s3_integration`)
- File formats for CSV and JSON data loading
- External stages for S3 data access

### Step 4: Configure S3 Trust Relationship
Go to Snowflake and run:
```sql
DESC STORAGE INTEGRATION betterjobs_s3_integration;
```

Copy the values for `STORAGE_AWS_EXTERNAL_ID` and `STORAGE_AWS_IAM_USER_ARN`, then go to your AWS IAM role and edit the Trust Relationship to add these values.

**For detailed S3 integration setup instructions, see: [S3_SNOWFLAKE_SETUP.md](S3_SNOWFLAKE_SETUP.md)**

### Step 5: Test S3 Storage Integration
Verify the S3 integration is working:
```sql
SHOW STORAGE INTEGRATIONS;
SHOW STAGES;
LIST @BETTERJOBS_DB.RAW.COMPANY_URLS_STAGE;
```

### Step 6: Run tables_setup Asset
Materialize the `tables_setup` asset to create all database tables across RAW, STAGE, and ANALYTICS schemas.

### Step 7: Run views_setup Asset
Materialize the `views_setup` asset to create all database views for data transformation and analytics.

### Step 8: Run static_data_population Asset
Materialize the `static_data_population` asset to populate lookup tables and standardization rules required for data processing.

## Verification

After completing the setup, verify your infrastructure by materializing setup_validation asset or manually running the following queries:

```sql
-- Check database and schemas
SHOW DATABASES LIKE 'BETTERJOBS_DB';
SHOW SCHEMAS IN DATABASE BETTERJOBS_DB;

-- Check warehouse and role
SHOW WAREHOUSES LIKE 'BETTERJOBS_WH';
SHOW ROLES LIKE 'BETTERJOBS_ROLE';

-- Verify storage integration
SHOW STORAGE INTEGRATIONS;
LIST @BETTERJOBS_DB.RAW.COMPANY_URLS_STAGE;
```

Your Snowflake infrastructure is now ready for data processing!