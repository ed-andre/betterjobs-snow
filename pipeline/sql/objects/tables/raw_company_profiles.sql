CREATE TABLE IF NOT EXISTS BETTERJOBS_DB.RAW.COMPANY_PROFILES (
    profile_id STRING PRIMARY KEY,
    company_name STRING NOT NULL,
    company_industry STRING,
    employee_count_range STRING,
    city STRING,  -- US headquarters city

    -- File metadata
    source_file STRING,
    file_hash STRING,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,

    -- Raw data preservation
    raw_data STRING
);