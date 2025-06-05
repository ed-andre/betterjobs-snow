import os
import pandas as pd
from datetime import datetime
import hashlib
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dagster import asset, AssetExecutionContext, MetadataValue

def generate_company_id(company_name: str) -> str:
    """
    Generate a stable, unique company ID from company name.
    Uses first 8 characters of SHA-256 hash to create a compact ID.
    """
    # Normalize company name (lowercase, remove extra spaces)
    normalized_name = " ".join(company_name.lower().split())
    # Generate hash
    hash_object = hashlib.sha256(normalized_name.encode())
    # Take first 8 characters for a compact but still unique ID
    return hash_object.hexdigest()[:8]

def calculate_file_hash(file_path: str) -> str:
    """Calculate SHA-256 hash of a file."""
    hash_sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()

@asset(
    group_name="adhoc_request",
    kinds={"python", "sql", "snowflake"},
    deps=["snowflake_master_company_urls"],
    required_resource_keys={"snowflake"}
)
def adhoc_company_urls(context: AssetExecutionContext) -> None:
    """
    Processes manually added company URLs from CSV files in the input folder.
    Adds new companies to the master_company_urls table if they don't exist
    or if they have a different ATS URL than what's already in the database.

    CSV files must have the following headers:
    company_name, company_industry, platform, ats_url, career_url, url_verified (optional)
    """
    # Initialize Snowflake connection
    conn = context.resources.snowflake.get_connection()
    database_name = os.getenv("SNOWFLAKE_DATABASE", "BETTERJOBS_DB")
    schema_name = os.getenv("SNOWFLAKE_RAW_SCHEMA", "RAW")

    # Get input folder path from environment variable or use relative path as fallback
    input_folder = os.getenv("ADHOC_INPUT_FOLDER")

    if not input_folder:
        # Use relative path as fallback
        # Assuming the code is run from the project root
        input_folder = os.path.join("pipeline", "dagster_betterjobs", "input")
        context.log.info(f"ADHOC_INPUT_FOLDER environment variable not set, using relative path: {input_folder}")

    if not os.path.exists(input_folder):
        context.log.error(f"Input folder not found at: {input_folder}")
        conn.close()
        return None

    # List directory contents for debugging
    try:
        context.log.info(f"Contents of input folder {input_folder}: {os.listdir(input_folder)}")
    except Exception as e:
        context.log.error(f"Error listing input folder: {str(e)}")

    # Get list of CSV files in input folder
    csv_files = [f for f in os.listdir(input_folder) if f.endswith('.csv')]

    if not csv_files:
        context.log.info("No CSV files found in input folder")
        conn.close()
        return None

    context.log.info(f"Found {len(csv_files)} CSV files in input folder")

    # Read and process each CSV file
    all_companies = []
    for csv_file in csv_files:
        file_path = os.path.join(input_folder, csv_file)
        try:
            df = pd.read_csv(file_path)
            required_columns = ["company_name", "company_industry", "platform", "ats_url", "career_url"]

            # Check if all required columns are present
            if not all(col in df.columns for col in required_columns):
                missing_cols = [col for col in required_columns if col not in df.columns]
                context.log.warning(f"File {csv_file} is missing required columns: {missing_cols}")
                continue

            # Filter out rows with missing company_name or ats_url
            df = df[df['company_name'].notna() & df['ats_url'].notna()]

            if len(df) == 0:
                context.log.warning(f"No valid rows found in {csv_file} after filtering")
                continue

            # Handle url_verified column (set to True if not present or if present use its value)
            if 'url_verified' not in df.columns:
                df['url_verified'] = True
            else:
                # Convert string values to boolean
                df['url_verified'] = df['url_verified'].apply(lambda x:
                    str(x).lower() in ['true', '1', 'yes', 'y'] if pd.notna(x) else True
                )

            # Generate company_id for each row
            df['company_id'] = df['company_name'].apply(generate_company_id)

            # Calculate file hash and add source metadata
            file_hash = calculate_file_hash(file_path)
            df['source_file'] = os.path.basename(file_path)
            df['file_hash'] = file_hash

            # Current timestamp for new records
            current_time = datetime.now()
            df['ingested_at'] = current_time

            context.log.info(f"Processed {len(df)} companies from {csv_file}")

            # Include all relevant columns including source metadata
            columns_to_keep = required_columns + ['url_verified', 'company_id', 'source_file', 'file_hash', 'ingested_at']
            all_companies.append(df[columns_to_keep])

        except Exception as e:
            context.log.error(f"Error processing file {csv_file}: {str(e)}")

    if not all_companies:
        context.log.info("No valid company data found in CSV files")
        conn.close()
        return None

    # Combine all company data
    combined_df = pd.concat(all_companies, ignore_index=True)

    # Get existing master table data
    try:
        cursor = conn.cursor()
        cursor.execute(f"USE DATABASE {database_name}")
        cursor.execute(f"USE SCHEMA {schema_name}")

        query = f"""
        SELECT
            company_id,
            company_name,
            ats_url,
            date_added,
            last_updated
        FROM {database_name}.{schema_name}.master_company_urls
        """
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        existing_df = pd.DataFrame(results, columns=columns)
        existing_df.columns = existing_df.columns.str.lower()
        cursor.close()
        context.log.info(f"Loaded existing master table with {len(existing_df)} records")
    except Exception as e:
        context.log.warning(f"Error accessing master_company_urls: {str(e)}")
        context.log.info("Will process all companies as new")
        existing_df = pd.DataFrame(columns=["company_id", "company_name", "ats_url"])

    # Identify new companies and companies with changed ATS URLs
    if not existing_df.empty:
        # Create a mapping of company_id to ats_url from existing data
        existing_map = dict(zip(existing_df["company_id"], existing_df["ats_url"]))

        # Filter to keep only new companies or companies with different ats_url
        new_or_changed = []
        for _, row in combined_df.iterrows():
            company_id = row["company_id"]
            if company_id not in existing_map:
                # New company
                new_or_changed.append(row)
                context.log.info(f"New company: {row['company_name']}")
            elif existing_map[company_id] != row["ats_url"]:
                # Existing company with different ATS URL
                new_or_changed.append(row)
                context.log.info(f"Updated ATS URL for: {row['company_name']} (from {existing_map[company_id]} to {row['ats_url']})")

        if not new_or_changed:
            context.log.info("No new or changed companies to add")
            conn.close()
            return None

        # Create DataFrame from the new or changed companies
        new_companies_df = pd.DataFrame(new_or_changed)
    else:
        new_companies_df = combined_df
        context.log.info(f"All {len(new_companies_df)} companies will be processed as new")

    # Set proper timestamps
    current_time = datetime.now()

    # For new companies, set date_added to current time
    # For existing companies being updated, preserve their original date_added (will be handled in UPDATE)
    new_companies_df["date_added"] = current_time
    new_companies_df["last_updated"] = current_time

    # Prepare data for Snowflake
    # Ensure all string columns have values and handle data types
    for col in new_companies_df.select_dtypes(include=['object']).columns:
        if col not in ['date_added', 'last_updated', 'ingested_at']:
            new_companies_df[col] = new_companies_df[col].fillna('').astype(str)

    # Handle boolean column
    if 'url_verified' in new_companies_df.columns:
        new_companies_df['url_verified'] = new_companies_df['url_verified'].astype(bool)

    # Convert datetime columns properly for Snowflake
    for col in ['date_added', 'last_updated', 'ingested_at']:
        if col in new_companies_df.columns:
            new_companies_df[col] = pd.to_datetime(new_companies_df[col], errors='coerce')
            new_companies_df[col] = new_companies_df[col].where(pd.notnull(new_companies_df[col]), None)

    cursor = conn.cursor()
    try:
        cursor.execute(f"USE DATABASE {database_name}")
        cursor.execute(f"USE SCHEMA {schema_name}")

        # Use write_pandas for bulk insert to temporary table first
        temp_table_name = "adhoc_company_urls_temp"

        # Drop temp table if exists
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")

        # Create temporary table with source file tracking
        create_temp_sql = f"""
        CREATE TABLE {temp_table_name} (
            company_id STRING,
            company_name STRING NOT NULL,
            company_industry STRING,
            platform STRING,
            ats_url STRING,
            career_url STRING,
            url_verified BOOLEAN DEFAULT TRUE,
            date_added TIMESTAMP_NTZ,
            last_updated TIMESTAMP_NTZ,
            source_file STRING,
            file_hash STRING,
            ingested_at TIMESTAMP_NTZ
        )
        """
        cursor.execute(create_temp_sql)
        context.log.info("Created temporary table for bulk loading")

        # Use write_pandas for bulk insert
        success, num_chunks, num_rows, output = write_pandas(
            conn,
            new_companies_df,
            temp_table_name,
            database=database_name,
            schema=schema_name,
            auto_create_table=False,
            overwrite=False,
            quote_identifiers=False
        )

        if success:
            context.log.info(f"Successfully loaded {len(new_companies_df)} companies into temporary table")
        else:
            context.log.error(f"Failed to load companies to temporary table: {output}")
            cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            return None

        # Insert new records into master_company_urls
        insert_sql = f"""
        INSERT INTO {database_name}.{schema_name}.master_company_urls (
            company_id,
            company_name,
            company_industry,
            platform,
            ats_url,
            career_url,
            url_verified,
            date_added,
            last_updated,
            source_file,
            file_hash,
            ingested_at
        )
        SELECT
            t.company_id,
            t.company_name,
            t.company_industry,
            t.platform,
            t.ats_url,
            t.career_url,
            t.url_verified,
            t.date_added,
            t.last_updated,
            t.source_file,
            t.file_hash,
            t.ingested_at
        FROM {temp_table_name} t
        LEFT JOIN {database_name}.{schema_name}.master_company_urls m
            ON t.company_id = m.company_id
        WHERE m.company_id IS NULL
        """
        cursor.execute(insert_sql)

        # Get count of inserted records
        cursor.execute("SELECT ROW_COUNT()")
        inserted_count = cursor.fetchone()[0]
        context.log.info(f"Inserted {inserted_count} new companies")

        # Update existing records where ats_url is different
        # Preserve original date_added but update last_updated and other fields
        update_sql = f"""
        UPDATE {database_name}.{schema_name}.master_company_urls
        SET
            ats_url = t.ats_url,
            career_url = t.career_url,
            platform = t.platform,
            company_industry = t.company_industry,
            url_verified = t.url_verified,
            last_updated = t.last_updated,
            source_file = t.source_file,
            file_hash = t.file_hash,
            ingested_at = t.ingested_at
        FROM {temp_table_name} t
        WHERE master_company_urls.company_id = t.company_id
          AND master_company_urls.ats_url != t.ats_url
        """
        cursor.execute(update_sql)

        # Get count of updated records
        cursor.execute("SELECT ROW_COUNT()")
        updated_count = cursor.fetchone()[0]
        context.log.info(f"Updated {updated_count} existing companies")

        # Drop the temporary table
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
        conn.commit()
        context.log.info("Dropped temporary table")

    except Exception as e:
        context.log.error(f"Error processing companies in Snowflake: {str(e)}")
        # Clean up temp table on error
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            conn.commit()
        except:
            pass
        return None
    finally:
        cursor.close()

    # Log summary statistics
    total_processed = len(new_companies_df)

    # Add metadata to the output
    context.add_output_metadata({
        "total_companies_processed": MetadataValue.int(total_processed),
        "new_companies_inserted": MetadataValue.int(inserted_count if 'inserted_count' in locals() else 0),
        "companies_updated": MetadataValue.int(updated_count if 'updated_count' in locals() else 0),
        "snowflake_table": MetadataValue.text(f"{database_name}.{schema_name}.master_company_urls")
    })

    context.log.info(f"Successfully processed {total_processed} companies from manual input files")
    if 'inserted_count' in locals():
        context.log.info(f"  - {inserted_count} new companies inserted")
    if 'updated_count' in locals():
        context.log.info(f"  - {updated_count} existing companies updated")

    # Close Snowflake connection
    conn.close()
    return None