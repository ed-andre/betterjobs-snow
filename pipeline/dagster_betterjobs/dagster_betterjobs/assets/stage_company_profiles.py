import pandas as pd
from typing import Dict, Optional
from dagster import asset, AssetExecutionContext, Config, MetadataValue
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from ..transformations.text_cleaning import clean_company_name, normalize_whitespace
from ..assets.snowflake_master_company_urls import generate_company_id


def standardize_industry(industry: Optional[str]) -> str:
    """
    Standardize company industry names.

    Args:
        industry: Raw industry string

    Returns:
        Standardized industry name
    """
    if not industry or not isinstance(industry, str):
        return ""

    # Clean basic formatting
    clean_industry = normalize_whitespace(industry.strip())

    # Common industry standardizations
    industry_mappings = {
        # Technology
        r'(?i)\bsoft.*dev.*\b': 'Software Development',
        r'(?i)\btech.*\b': 'Technology',
        r'(?i)\bsoftware\b': 'Software Development',
        r'(?i)\binformation.*tech.*\b': 'Information Technology',
        r'(?i)\bIT\b': 'Information Technology',
        r'(?i)\bcomputer.*soft.*\b': 'Software Development',

        # Healthcare & Life Sciences
        r'(?i)\bpharma.*\b': 'Pharmaceutical Manufacturing',
        r'(?i)\bbiotech.*\b': 'Biotechnology',
        r'(?i)\bhealth.*care\b': 'Healthcare',
        r'(?i)\bmedical.*\b': 'Healthcare',
        r'(?i)\bhospital.*\b': 'Healthcare',

        # Finance
        r'(?i)\bfin.*serv.*\b': 'Financial Services',
        r'(?i)\bbanking\b': 'Financial Services',
        r'(?i)\binvestment\b': 'Financial Services',
        r'(?i)\binsurance\b': 'Insurance',

        # Professional Services
        r'(?i)\bconsult.*\b': 'Consulting',
        r'(?i)\blegal.*serv.*\b': 'Legal Services',
        r'(?i)\baccounting\b': 'Accounting',

        # Manufacturing & Industrial
        r'(?i)\bmanufact.*\b': 'Manufacturing',
        r'(?i)\bindustrial\b': 'Manufacturing',
        r'(?i)\bautomotive\b': 'Automotive',

        # Retail & Consumer
        r'(?i)\bretail\b': 'Retail',
        r'(?i)\be-commerce\b': 'E-commerce',
        r'(?i)\becommerce\b': 'E-commerce',
        r'(?i)\bconsumer.*goods\b': 'Consumer Goods',

        # Other common industries
        r'(?i)\breal.*estate\b': 'Real Estate',
        r'(?i)\beducation\b': 'Education',
        r'(?i)\bmedia\b': 'Media & Communications',
        r'(?i)\bentertain.*\b': 'Entertainment',
        r'(?i)\bhospitality\b': 'Hospitality',
        r'(?i)\brestaurant\b': 'Food Services',
        r'(?i)\btravel\b': 'Travel & Tourism',
        r'(?i)\benergy\b': 'Energy',
        r'(?i)\butilities\b': 'Utilities',
        r'(?i)\bgovernment\b': 'Government',
        r'(?i)\bnon.*profit\b': 'Non-Profit',
        r'(?i)\bsports\b': 'Sports & Recreation',
    }

    # Apply mappings
    import re
    for pattern, standardized in industry_mappings.items():
        if re.search(pattern, clean_industry):
            return standardized

    # If no mapping found, return cleaned original
    return clean_industry


def categorize_company_size(employee_range: Optional[str]) -> str:
    """
    Categorize company size based on employee count range.

    Args:
        employee_range: Employee count range string (e.g., "1001-5000")

    Returns:
        Company size category
    """
    if not employee_range or not isinstance(employee_range, str):
        return "Unknown"

    range_lower = employee_range.lower()

    # Extract numeric ranges using regex
    import re
    numbers = re.findall(r'\d+', range_lower)

    if not numbers:
        return "Unknown"

    # Get the lower bound of the range
    lower_bound = int(numbers[0])

    # Categorize based on lower bound
    if lower_bound <= 50:
        return "Startup"
    elif lower_bound <= 500:
        return "Small"
    elif lower_bound <= 5000:
        return "Medium"
    else:
        return "Large"


def setup_stage_schema_and_table(conn):
    """Set up STAGE schema and company_profiles table if not exists."""
    cursor = conn.cursor()

    try:
        # Ensure we're using the correct database
        cursor.execute("USE DATABASE BETTERJOBS_DB")

        # Create STAGE schema if it doesn't exist
        cursor.execute("CREATE SCHEMA IF NOT EXISTS STAGE")
        cursor.execute("USE SCHEMA STAGE")

        # Create the company profiles table with proper schema
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS COMPANY_PROFILES (
            COMPANY_ID STRING PRIMARY KEY,
            PROFILE_ID STRING,
            COMPANY_NAME_STANDARDIZED STRING,
            COMPANY_INDUSTRY_STANDARDIZED STRING,
            COMPANY_SIZE_CATEGORY STRING,
            EMPLOYEE_COUNT_RANGE STRING,
            FUNDING_STAGE STRING,
            HEADQUARTERS_LOCATION STRING,
            CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
            UPDATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_sql)

        conn.commit()

    except Exception as e:
        raise Exception(f"Failed to set up STAGE schema and table: {str(e)}")
    finally:
        cursor.close()


class StageCompanyProfilesConfig(Config):
    """Configuration for stage company profiles transformation."""
    pass


@asset(
    group_name="stage_cleansing_enrichment_validation_transformation",
    kinds={"snowflake", "python", "transformation"},
    required_resource_keys={"snowflake"},
    deps=["raw_company_profiles", "snowflake_master_company_urls"]
)
def stage_company_profiles(
    context: AssetExecutionContext,
    config: StageCompanyProfilesConfig
) -> pd.DataFrame:
    """
    Transform raw company profiles to STAGE layer format.

    This asset performs transformations from RAW.raw_company_profiles to STAGE.company_profiles:
    - PROFILE_ID preserved for lineage tracking (BUG-014 fix)
    - COMPANY_ID generated using deterministic generate_company_id() function for consistency across pipeline
    - Company name standardization and cleaning
    - Industry standardization with common mappings
    - Company size categorization from employee count ranges
    - Location cleaning for headquarters
    - Sets FUNDING_STAGE to NULL (not available in raw data)

    BUG-014 Resolution: Fixed inconsistent COMPANY_ID generation across pipeline tables by:
    - Joining with MASTER_COMPANY_URLS to get platform information for each company
    - Using generate_company_id(company_name, platform) with actual platform data
    - Fallback to 'COMPANY_PROFILES' platform for companies not in MASTER_COMPANY_URLS
    - Preserving original PROFILE_ID field for data lineage
    - Ensuring complete compatibility with other pipeline tables using same company_name + platform combination
    """

    # Get Snowflake connection
    conn = context.resources.snowflake.get_connection()

    try:
        # Setup STAGE schema and table
        setup_stage_schema_and_table(conn)

        # Load raw company profiles data
        cursor = conn.cursor()
        cursor.execute("USE DATABASE BETTERJOBS_DB")
        cursor.execute("USE SCHEMA RAW")

        context.log.info("Loading raw company profiles data...")

        # BUG-014 FIX: Join with MASTER_COMPANY_URLS to get platform information
        # This ensures consistent company_id generation across pipeline tables
        load_query = """
        SELECT DISTINCT
            rcp.profile_id,
            rcp.company_name,
            rcp.company_industry,
            rcp.employee_count_range,
            rcp.city,
            rcp.ingested_at,
            mcu.platform
        FROM raw_company_profiles rcp
        LEFT JOIN master_company_urls mcu
            ON TRIM(UPPER(rcp.company_name)) = TRIM(UPPER(mcu.company_name))
        WHERE rcp.company_name IS NOT NULL
        AND TRIM(rcp.company_name) != ''
        ORDER BY rcp.ingested_at DESC
        """

        cursor.execute(load_query)
        raw_data = cursor.fetchall()
        cursor.close()

        if not raw_data:
            context.log.warning("No raw company profiles data found")
            return pd.DataFrame()

        # Convert to DataFrame for processing
        df = pd.DataFrame(raw_data, columns=[
            'profile_id', 'company_name', 'company_industry',
            'employee_count_range', 'city', 'ingested_at', 'platform'
        ])

        context.log.info(f"Loaded {len(df)} raw company profiles for transformation")

        # Apply transformations
        context.log.info("Applying text cleaning and standardization...")

        # BUG-014 FIX: Generate deterministic company ID using standardized function with platform from join
        # Preserve profile_id for lineage, generate company_id for consistency across pipeline
        df['profile_id_preserved'] = df['profile_id']  # Preserve original profile ID for lineage

        # Use platform from MASTER_COMPANY_URLS join, fallback to 'COMPANY_PROFILES' if null
        df['platform_for_id'] = df['platform'].fillna('COMPANY_PROFILES')
        df['company_id'] = df.apply(lambda row: generate_company_id(row['company_name'], row['platform_for_id']), axis=1)
        df['company_name_standardized'] = df['company_name'].apply(clean_company_name)
        df['company_industry_standardized'] = df['company_industry'].apply(standardize_industry)
        df['company_size_category'] = df['employee_count_range'].apply(categorize_company_size)
        df['headquarters_location'] = df['city'].apply(lambda x: normalize_whitespace(x) if x else "")
        df['funding_stage'] = None  # Not available in raw data

        # Log platform distribution for debugging
        platform_distribution = df['platform_for_id'].value_counts().to_dict()
        companies_with_platform = int((df['platform'].notna()).sum())
        companies_without_platform = int((df['platform'].isna()).sum())

        context.log.info(f"Generated {len(df)} deterministic company IDs using generate_company_id() function")
        context.log.info(f"Platform distribution: {platform_distribution}")
        context.log.info(f"Companies with platform from MASTER_COMPANY_URLS: {companies_with_platform}")
        context.log.info(f"Companies using fallback 'COMPANY_PROFILES' platform: {companies_without_platform}")

        # Prepare final dataset for loading
        stage_df = df[[
            'profile_id_preserved',
            'company_id',
            'company_name_standardized',
            'company_industry_standardized',
            'company_size_category',
            'employee_count_range',
            'funding_stage',
            'headquarters_location'
        ]].copy()

        # Rename columns to match Snowflake uppercase schema
        stage_df.columns = [
            'PROFILE_ID',
            'COMPANY_ID',
            'COMPANY_NAME_STANDARDIZED',
            'COMPANY_INDUSTRY_STANDARDIZED',
            'COMPANY_SIZE_CATEGORY',
            'EMPLOYEE_COUNT_RANGE',
            'FUNDING_STAGE',
            'HEADQUARTERS_LOCATION'
        ]

        # Data quality validation
        context.log.info("Performing data quality validation...")

        # Check for required fields (convert numpy types immediately)
        missing_company_id = int(stage_df['COMPANY_ID'].isna().sum())
        missing_company_name = int((stage_df['COMPANY_NAME_STANDARDIZED'].isna() |
                                   (stage_df['COMPANY_NAME_STANDARDIZED'] == '')).sum())

        if missing_company_id > 0:
            context.log.error(f"Found {missing_company_id} records with missing company_id")

        if missing_company_name > 0:
            context.log.warning(f"Found {missing_company_name} records with missing company name")
            # Filter out records with missing company names
            stage_df = stage_df[
                stage_df['COMPANY_NAME_STANDARDIZED'].notna() &
                (stage_df['COMPANY_NAME_STANDARDIZED'] != '')
            ]

        # Check for duplicates (convert numpy type immediately)
        duplicates = int(stage_df.duplicated(subset=['COMPANY_ID']).sum())
        if duplicates > 0:
            context.log.warning(f"Found {duplicates} duplicate company_id records, keeping first occurrence")
            stage_df = stage_df.drop_duplicates(subset=['COMPANY_ID'], keep='first')

        context.log.info(f"Final dataset ready: {len(stage_df)} company profiles")

        # Load to Snowflake STAGE table
        context.log.info("Loading transformed data to STAGE.company_profiles table...")

        # Truncate existing data (for full refresh approach)
        cursor = conn.cursor()
        cursor.execute("USE DATABASE BETTERJOBS_DB")
        cursor.execute("USE SCHEMA STAGE")
        cursor.execute("TRUNCATE TABLE COMPANY_PROFILES")

        # Write data to Snowflake
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=stage_df,
            table_name="COMPANY_PROFILES",
            database="BETTERJOBS_DB",
            schema="STAGE",
            overwrite=False,
            auto_create_table=False
        )

        cursor.close()

        if success:
            context.log.info(f"Successfully loaded {nrows} company profiles to STAGE layer")
        else:
            raise Exception("Failed to load data to Snowflake")

        # Generate summary statistics
        size_distribution = stage_df['COMPANY_SIZE_CATEGORY'].value_counts().to_dict()
        industry_distribution = stage_df['COMPANY_INDUSTRY_STANDARDIZED'].value_counts().head(10).to_dict()

        # Calculate metadata values with explicit type conversion
        total_companies = len(stage_df)
        unique_industries = int(stage_df['COMPANY_INDUSTRY_STANDARDIZED'].nunique())
        companies_with_location = int((stage_df['HEADQUARTERS_LOCATION'].notna() &
                                     (stage_df['HEADQUARTERS_LOCATION'] != '')).sum())
        data_quality_score = float((len(stage_df) - missing_company_name) / len(df)) if len(df) > 0 else 0.0

        # Prepare metadata
        metadata = {
            "total_companies": MetadataValue.int(total_companies),
            "unique_industries": MetadataValue.int(unique_industries),
            "companies_with_location": MetadataValue.int(companies_with_location),
            "size_distribution": MetadataValue.json(size_distribution),
            "top_industries": MetadataValue.json(industry_distribution),
            "duplicates_removed": MetadataValue.int(duplicates),
            "data_quality_score": MetadataValue.float(data_quality_score)
        }

        context.add_output_metadata(metadata)

        context.log.info("Stage company profiles transformation completed successfully")

        return stage_df

    except Exception as e:
        context.log.error(f"Failed to transform company profiles: {str(e)}")
        raise
    finally:
        conn.close()