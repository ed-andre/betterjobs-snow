/*********** STORAGE INTEGRATION ***********/

-- Create storage integration for S3 (if not already exists)
-- Note: This requires ACCOUNTADMIN role and proper AWS IAM setup
-- Uncomment and configure the following if S3 integration is needed:

CREATE STORAGE INTEGRATION IF NOT EXISTS betterjobs_s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = '${SNOWFLAKE_S3_ROLE_ARN}'
    STORAGE_ALLOWED_LOCATIONS = ('${SNOWFLAKE_S3_BUCKET_URL}');

-- Grant usage to the application role
GRANT USAGE ON INTEGRATION betterjobs_s3_integration TO ROLE BETTERJOBS_ROLE;