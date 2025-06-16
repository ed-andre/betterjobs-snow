/*********** STORAGE INTEGRATION ***********/

-- Create storage integration for S3 (if not already exists)
-- Note: This requires ACCOUNTADMIN role and proper AWS IAM setup
-- Uncomment and configure the following if S3 integration is needed:

/*
CREATE STORAGE INTEGRATION IF NOT EXISTS betterjobs_s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR_ACCOUNT_ID:role/snowflake-s3-role' -- TODO: Set as env variable
    STORAGE_ALLOWED_LOCATIONS = ('s3://betterjobs-dagster/'); -- TODO: Set as env variable