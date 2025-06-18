-- =============================================================================
-- BetterJobs Database and Schema Setup
-- =============================================================================
-- This file creates the foundational database and schemas for the BetterJobs
-- data pipeline following the medallion architecture pattern.
--
-- Execution Order: Run this FIRST before raw_definitions.sql and stage_definitions.sql
-- Dependencies: None (foundational setup)
-- =============================================================================

-- Use appropriate role and warehouse
USE ROLE ACCOUNTADMIN;  -- Use BETTERJOBS_ROLE in production
USE WAREHOUSE BETTERJOBS_WH;

/*********** DATABASE ***********/

-- Create main database if it doesn't exist
CREATE DATABASE IF NOT EXISTS BETTERJOBS_DB
    COMMENT = 'Main database for BetterJobs data pipeline - Medallion Architecture (Bronze/Silver/Gold)';

-- Use the database for subsequent operations
USE DATABASE BETTERJOBS_DB;

/*********** SCHEMAS ***********/

-- RAW Schema (Bronze Layer) - Landing zone for raw data from all sources
CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Bronze layer: Raw data ingestion from ATS platforms, S3, and external sources';

-- STAGE Schema (Silver Layer) - Cleaned, standardized, and enriched data
CREATE SCHEMA IF NOT EXISTS STAGE
    COMMENT = 'Silver layer: Cleaned, standardized data with LLM enrichment and normalization';

-- ANALYTICS Schema (Gold Layer) - Business-ready dimensional models and aggregates
CREATE SCHEMA IF NOT EXISTS ANALYTICS
    COMMENT = 'Gold layer: Business-ready dimensional models, aggregates, and analytics tables';

/*********** ROLES AND PERMISSIONS ***********/

-- Create application-specific role if it doesn't exist
CREATE ROLE IF NOT EXISTS BETTERJOBS_ROLE
    COMMENT = 'Application role for BetterJobs pipeline with minimal required permissions';

-- Grant warehouse usage
GRANT USAGE ON WAREHOUSE BETTERJOBS_WH TO ROLE BETTERJOBS_ROLE;

-- Grant database permissions
GRANT USAGE ON DATABASE BETTERJOBS_DB TO ROLE BETTERJOBS_ROLE;
GRANT CREATE SCHEMA ON DATABASE BETTERJOBS_DB TO ROLE BETTERJOBS_ROLE;

-- Grant schema permissions
GRANT USAGE ON SCHEMA BETTERJOBS_DB.RAW TO ROLE BETTERJOBS_ROLE;
GRANT USAGE ON SCHEMA BETTERJOBS_DB.STAGE TO ROLE BETTERJOBS_ROLE;
GRANT USAGE ON SCHEMA BETTERJOBS_DB.ANALYTICS TO ROLE BETTERJOBS_ROLE;

-- Grant table permissions (current and future)
GRANT CREATE TABLE ON SCHEMA BETTERJOBS_DB.RAW TO ROLE BETTERJOBS_ROLE;
GRANT CREATE TABLE ON SCHEMA BETTERJOBS_DB.STAGE TO ROLE BETTERJOBS_ROLE;
GRANT CREATE TABLE ON SCHEMA BETTERJOBS_DB.ANALYTICS TO ROLE BETTERJOBS_ROLE;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA BETTERJOBS_DB.RAW TO ROLE BETTERJOBS_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA BETTERJOBS_DB.STAGE TO ROLE BETTERJOBS_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA BETTERJOBS_DB.ANALYTICS TO ROLE BETTERJOBS_ROLE;

GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA BETTERJOBS_DB.RAW TO ROLE BETTERJOBS_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA BETTERJOBS_DB.STAGE TO ROLE BETTERJOBS_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA BETTERJOBS_DB.ANALYTICS TO ROLE BETTERJOBS_ROLE;

-- Grant view permissions (current and future)
GRANT CREATE VIEW ON SCHEMA BETTERJOBS_DB.RAW TO ROLE BETTERJOBS_ROLE;
GRANT CREATE VIEW ON SCHEMA BETTERJOBS_DB.STAGE TO ROLE BETTERJOBS_ROLE;
GRANT CREATE VIEW ON SCHEMA BETTERJOBS_DB.ANALYTICS TO ROLE BETTERJOBS_ROLE;

GRANT SELECT ON ALL VIEWS IN SCHEMA BETTERJOBS_DB.RAW TO ROLE BETTERJOBS_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA BETTERJOBS_DB.STAGE TO ROLE BETTERJOBS_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA BETTERJOBS_DB.ANALYTICS TO ROLE BETTERJOBS_ROLE;

GRANT SELECT ON FUTURE VIEWS IN SCHEMA BETTERJOBS_DB.RAW TO ROLE BETTERJOBS_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA BETTERJOBS_DB.STAGE TO ROLE BETTERJOBS_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA BETTERJOBS_DB.ANALYTICS TO ROLE BETTERJOBS_ROLE;

-- Grant stage permissions for S3 integration
GRANT CREATE STAGE ON SCHEMA BETTERJOBS_DB.RAW TO ROLE BETTERJOBS_ROLE;
GRANT USAGE ON ALL STAGES IN SCHEMA BETTERJOBS_DB.RAW TO ROLE BETTERJOBS_ROLE;
GRANT USAGE ON FUTURE STAGES IN SCHEMA BETTERJOBS_DB.RAW TO ROLE BETTERJOBS_ROLE;

/*********** STORAGE INTEGRATION ***********/

-- Create storage integration for S3 (if not already exists)
-- Note: This requires ACCOUNTADMIN role and proper AWS IAM setup
-- Uncomment and configure the following if S3 integration is needed:


-- CREATE STORAGE INTEGRATION IF NOT EXISTS betterjobs_s3_integration
--    TYPE = EXTERNAL_STAGE
--    STORAGE_PROVIDER = 'S3'
--    ENABLED = TRUE
--    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR_ACCOUNT_ID:role/snowflake-s3-role' -- TODO: Update to use env variable
--    STORAGE_ALLOWED_LOCATIONS = ('s3://betterjobs-dagster/'); -- TODO: Update to use env variable

-- Grant usage to the application role
-- GRANT USAGE ON INTEGRATION betterjobs_s3_integration TO ROLE BETTERJOBS_ROLE;


/*********** VERIFICATION ***********/

-- Show created objects for verification
SHOW DATABASES LIKE 'BETTERJOBS_DB';
SHOW SCHEMAS IN DATABASE BETTERJOBS_DB;
SHOW ROLES LIKE 'BETTERJOBS_ROLE';

-- Display success message
SELECT 'Database and schema setup completed successfully!' AS STATUS;