-- Standard CSV file format for company data imports
-- Used by: COMPANY_URLS_STAGE, RAW_COMPANY_PROFILES_STAGE, and other CSV ingestion
CREATE OR REPLACE FILE FORMAT BETTERJOBS_DB.RAW.CSV_STANDARD_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ESCAPE_UNENCLOSED_FIELD = '\\'
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    COMMENT = 'Standard CSV format for company data with header row, quoted fields, and error tolerance';

-- Strict CSV file format for data that requires exact column matching
-- Used for: Critical data imports where schema validation is important
CREATE OR REPLACE FILE FORMAT BETTERJOBS_DB.RAW.CSV_STRICT_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ESCAPE_UNENCLOSED_FIELD = '\\'
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
    COMMENT = 'Strict CSV format with column count validation for critical data imports';

-- JSON file format for API responses and structured data
-- Used for: API response caching, structured data imports
CREATE OR REPLACE FILE FORMAT BETTERJOBS_DB.RAW.JSON_FORMAT
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = FALSE
    COMMENT = 'JSON format for API responses and structured data imports';
