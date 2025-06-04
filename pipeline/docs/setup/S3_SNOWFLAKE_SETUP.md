# S3-Snowflake Integration Setup Guide

This guide provides step-by-step instructions for setting up S3 integration with Snowflake to resolve the "Access Denied" error.

## Problem
Even with ACCOUNTADMIN user, Snowflake cannot access S3 buckets without proper permissions setup. The error `Access Denied (Status Code: 403; Error Code: AccessDenied)` occurs because Snowflake needs explicit S3 access configuration.

## Official Snowflake Documentation

For detailed and authoritative instructions on configuring S3-Snowflake integration, refer to the official Snowflake documentation:

**[Configuring a Snowflake Storage Integration to Access Amazon S3](https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration)**

This documentation provides comprehensive steps for:
- Creating IAM roles and policies in AWS
- Setting up Snowflake storage integrations
- Configuring trust relationships
- Creating external stages
- Troubleshooting common issues

## Quick Setup Summary for BetterJobs Project

### Prerequisites
- AWS account with permissions to create IAM roles and policies
- Snowflake account with ACCOUNTADMIN privileges
- S3 bucket: `betterjobs-dagster`

### Step 1: Create IAM Policy in AWS

Create an IAM policy with the following permissions for your S3 bucket:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": [
                "arn:aws:s3:::betterjobs-dagster",
                "arn:aws:s3:::betterjobs-dagster/*"
            ]
        }
    ]
}
```

### Step 2: Create IAM Role

Follow the [official documentation](https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration) to:
1. Create an IAM role (e.g., `SnowflakeBetterJobsRole`)
2. Attach the IAM policy from Step 1
3. Configure the trust relationship (you'll update this after creating the storage integration)

### Step 3: Create Storage Integration in Snowflake

```sql
-- Create storage integration
CREATE OR REPLACE STORAGE INTEGRATION betterjobs_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR_ACCOUNT_ID:role/SnowflakeBetterJobsRole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://betterjobs-dagster/');

-- Get the external ID and user ARN for the trust relationship
DESC STORAGE INTEGRATION betterjobs_s3_integration;
```

Record the values for:
- `STORAGE_AWS_IAM_USER_ARN`
- `STORAGE_AWS_EXTERNAL_ID`

### Step 4: Update IAM Role Trust Relationship

Using the values from Step 3, update your IAM role's trust relationship as detailed in the [official documentation](https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration):

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "<STORAGE_AWS_IAM_USER_ARN from Step 3>"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID from Step 3>"
                }
            }
        }
    ]
}
```

### Step 5: Verify Integration

Test your storage integration:

```sql
-- Test the storage integration
LIST @COMPANY_URLS_STAGE;

-- Validate the integration
SELECT SYSTEM$VALIDATE_STORAGE_INTEGRATION('betterjobs_s3_integration');
```

## Alternative Setup Options

### Option 2: AWS Credentials in Stage (Less Secure)

For development/testing only, you can use AWS credentials directly:

```python
def setup_snowflake_stage_and_table(conn, stage_name: str, s3_uri: str, table_name: str):
    """Set up Snowflake stage for S3 and create table if not exists."""
    cursor = conn.cursor()

    try:
        if s3_uri:
            # Get AWS credentials from environment
            aws_key_id = os.getenv("AWS_ACCESS_KEY_ID")
            aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

            if not aws_key_id or not aws_secret_key:
                raise ValueError("AWS credentials not found in environment variables")

            stage_sql = f"""
            CREATE STAGE IF NOT EXISTS {stage_name}
            URL = '{s3_uri}'
            CREDENTIALS = (AWS_KEY_ID = '{aws_key_id}' AWS_SECRET_KEY = '{aws_secret_key}')
            FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
            """
            cursor.execute(stage_sql)
        # ... rest of the function
```

Set environment variables:
```bash
export AWS_ACCESS_KEY_ID="your_access_key_id"
export AWS_SECRET_ACCESS_KEY="your_secret_access_key"
```

### Option 3: Disable S3 Processing (Temporary)

For immediate testing with local files only:

```python
config = SnowflakeMasterCompanyUrlsConfig(
    enable_s3_processing=False,  # Disable S3
    enable_local_processing=True,
    deduplicate_on_load=True
)
```

## Testing and Troubleshooting

### Test Commands

```sql
-- Check storage integration status
SHOW STORAGE INTEGRATIONS;

-- Check stage status
SHOW STAGES;

-- List files in stage (should work if setup is correct)
LIST @COMPANY_URLS_STAGE;

-- Validate storage integration
SELECT SYSTEM$VALIDATE_STORAGE_INTEGRATION('betterjobs_s3_integration');
```

### Common Issues

Refer to the [official troubleshooting section](https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration) for detailed solutions to common issues such as:

1. **External ID mismatch**: Trust relationship configuration
2. **Role ARN not found**: Storage integration role configuration
3. **Insufficient permissions**: IAM policy permissions
4. **Access denied**: Bucket permissions and policies

### Asset Configuration for Testing

Test your setup incrementally:

```python
# Test 1: Local only
config = SnowflakeMasterCompanyUrlsConfig(
    enable_s3_processing=False,
    enable_local_processing=True,
    deduplicate_on_load=True
)

# Test 2: S3 only (after fixing S3 setup)
config = SnowflakeMasterCompanyUrlsConfig(
    enable_s3_processing=True,
    enable_local_processing=False,
    deduplicate_on_load=True
)

# Test 3: Both sources
config = SnowflakeMasterCompanyUrlsConfig(
    enable_s3_processing=True,
    enable_local_processing=True,
    deduplicate_on_load=True
)
```

## Additional Resources

- [Snowflake Storage Integration Documentation](https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration)
- [AWS IAM Best Practices](https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html)
- [Snowflake External Stages](https://docs.snowflake.com/en/user-guide/data-load-s3-create-stage)

## Security Best Practices

1. **Use Storage Integration**: Preferred over embedding credentials
2. **Least Privilege**: Grant minimal required S3 permissions
3. **Regular Rotation**: Rotate credentials regularly if using direct credentials
4. **Environment Variables**: Never hardcode credentials in code
5. **VPC Endpoints**: Consider using VPC endpoints for private network access