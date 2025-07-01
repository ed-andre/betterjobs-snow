CREATE TABLE IF NOT EXISTS BETTERJOBS_DB.RAW.COMPANY_PROFILES_FILE_LOG (
    file_path STRING PRIMARY KEY,
    file_hash STRING,
    file_size NUMBER,
    last_modified TIMESTAMP_NTZ,
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    records_processed NUMBER,
    status STRING
);