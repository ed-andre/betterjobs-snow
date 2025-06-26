/*********** STAGES ***********/

-- COMPANY_URLS_STAGE - External stage for S3 company URLs CSV files
CREATE OR REPLACE STAGE BETTERJOBS_DB.RAW.COMPANY_URLS_STAGE
    STORAGE_INTEGRATION = betterjobs_s3_integration
    URL = '${SNOWFLAKE_S3_COMPANY_URLS_URL}'
    FILE_FORMAT = BETTERJOBS_DB.RAW.CSV_STANDARD_FORMAT
    COMMENT = 'External stage for loading company URLs from S3 CSV files';

-- RAW_COMPANY_PROFILES_STAGE - External stage for S3 company profiles CSV files
CREATE OR REPLACE STAGE BETTERJOBS_DB.RAW.RAW_COMPANY_PROFILES_STAGE
    STORAGE_INTEGRATION = betterjobs_s3_integration
    URL = '${SNOWFLAKE_S3_COMPANY_PROFILES_URL}'
    FILE_FORMAT = BETTERJOBS_DB.RAW.CSV_STANDARD_FORMAT
    COMMENT = 'External stage for loading company profiles from S3 CSV files';