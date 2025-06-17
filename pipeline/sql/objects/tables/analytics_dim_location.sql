CREATE TABLE ANALYTICS.dim_location (
    location_key STRING PRIMARY KEY,
    location_id STRING,              -- FK to STAGE.LOCATIONS_NORMALIZED.LOCATION_ID

    -- Location Hierarchy (from STAGE.LOCATIONS_NORMALIZED)
    location_name STRING,
    city STRING,
    state_province STRING,
    country STRING,
    region STRING,
    metro_area STRING,

    -- Location Intelligence from STAGE
    location_type STRING,            -- office, remote, hybrid
    is_remote_friendly BOOLEAN,      -- From STAGE.LOCATIONS_NORMALIZED
    is_major_tech_hub BOOLEAN,       -- From STAGE.LOCATIONS_NORMALIZED
    cost_of_living_index FLOAT,      -- From STAGE.LOCATIONS_NORMALIZED
    average_salary_adjustment FLOAT, -- From STAGE.LOCATIONS_NORMALIZED
    stage_confidence_score FLOAT,    -- From STAGE.LOCATIONS_NORMALIZED.CONFIDENCE_SCORE
    frequency_count INTEGER,         -- From STAGE.LOCATIONS_NORMALIZED.FREQUENCY_COUNT

    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (country, state_province, city);