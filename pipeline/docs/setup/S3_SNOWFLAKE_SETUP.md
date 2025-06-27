# S3-Snowflake Integration Setup Guide

This guide provides instructions for configuring AWS S3 access for Snowflake after the storage integration has been created.

- AWS account with permissions to create IAM roles and policies
- Snowflake storage integration already created (via `infrastructure_setup` asset)
- S3 bucket: `betterjobs-dagster`

## Setup Steps

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

a. Create an IAM role (e.g., `SnowflakeBetterJobsRole`) and attach the IAM policy from Step 1.
b. Set the SNOWFLAKE_S3_ROLE_ARN in the .env file to the ARN of the IAM role you created.


### Step 3: Get Snowflake Integration Details

In Snowflake, run the following query to get the integration details:

```sql
DESC STORAGE INTEGRATION betterjobs_s3_integration;
```

Record the values for:
- `STORAGE_AWS_IAM_USER_ARN`
- `STORAGE_AWS_EXTERNAL_ID`

### Step 4: Update IAM Role Trust Relationship

Using the values from Step 3, update your IAM role's trust relationship:

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
SHOW STORAGE INTEGRATIONS;
LIST @BETTERJOBS_DB.RAW.COMPANY_URLS_STAGE;
```

Your S3-Snowflake integration is now configured and ready for data ingestion!