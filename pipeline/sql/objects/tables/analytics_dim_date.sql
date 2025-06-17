CREATE TABLE ANALYTICS.dim_date (
    date_key STRING PRIMARY KEY,
    full_date DATE,

    -- Essential Date Attributes
    day_name STRING,
    day_of_week INTEGER,
    week_beginning_date DATE,
    week_ending_date DATE,
    month_number INTEGER,
    month_name STRING,
    quarter_number INTEGER,
    year_number INTEGER,

    -- Business Context
    is_business_day BOOLEAN,
    is_weekend BOOLEAN,

    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (full_date);