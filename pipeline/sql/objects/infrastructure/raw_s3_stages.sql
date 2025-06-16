/*********** STAGES ***********/

-- COMPANY_URLS_STAGE - External stage for S3 company URLs CSV files
create or replace STAGE BETTERJOBS_DB.RAW.COMPANY_URLS_STAGE
    STORAGE_INTEGRATION = betterjobs_s3_integration
    URL = 's3://betterjobs-dagster/company_urls/'
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        RECORD_DELIMITER = '\n'
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        ESCAPE_UNENCLOSED_FIELD = '\\'
        NULL_IF = ('NULL', 'null', '')
        EMPTY_FIELD_AS_NULL = TRUE
        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    );

-- RAW_COMPANY_PROFILES_STAGE - External stage for S3 company profiles CSV files
create or replace STAGE BETTERJOBS_DB.RAW.RAW_COMPANY_PROFILES_STAGE
    STORAGE_INTEGRATION = betterjobs_s3_integration
    URL = 's3://betterjobs-dagster/company_profiles/'
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        RECORD_DELIMITER = '\n'
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        ESCAPE_UNENCLOSED_FIELD = '\\'
        NULL_IF = ('NULL', 'null', '')
        EMPTY_FIELD_AS_NULL = TRUE
        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    );