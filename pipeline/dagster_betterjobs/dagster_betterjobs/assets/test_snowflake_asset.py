#!/usr/bin/env python3
"""
Test script for Snowflake master company URLs asset.
This script validates the setup and demonstrates basic functionality.
"""

import os
import sys
from pathlib import Path

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✓ Environment variables loaded from .env file")
except ImportError:
    print("⚠️  python-dotenv not installed, using system environment variables only")

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def check_environment_variables():
    """Check if required environment variables are set."""
    required_vars = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_RAW_SCHEMA",
        "SNOWFLAKE_ROLE"
    ]

    optional_vars = [
        "S3_URI",
        "MAIN_INPUT_FOLDER"
    ]

    print("Checking environment variables...")
    missing_required = []

    for var in required_vars:
        if not os.getenv(var):
            missing_required.append(var)
        else:
            print(f"✓ {var} is set")

    if missing_required:
        print(f"\n❌ Missing required environment variables: {missing_required}")
        return False

    print("\nOptional data source variables:")
    data_sources = []
    for var in optional_vars:
        if os.getenv(var):
            print(f"✓ {var} is set: {os.getenv(var)}")
            data_sources.append(var)
        else:
            print(f"- {var} is not set")

    if not data_sources:
        print("\n⚠️  Warning: No data sources configured. Set either S3_URI or MAIN_INPUT_FOLDER")
        return False

    print("\n✅ Environment variables check passed!")
    return True


def test_snowflake_connection():
    """Test Snowflake connection."""
    try:
        import snowflake.connector

        print("\nTesting Snowflake connection...")

        # Create connection using environment variables
        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_RAW_SCHEMA", "RAW"),
            role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
        )

        cursor = conn.cursor()

        # Test basic query
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()[0]
        print(f"✓ Connected to Snowflake version: {version}")

        # Test warehouse
        cursor.execute("SELECT CURRENT_WAREHOUSE()")
        warehouse = cursor.fetchone()[0]
        print(f"✓ Using warehouse: {warehouse}")

        # Test database and schema
        cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
        db, schema = cursor.fetchone()
        print(f"✓ Using database: {db}, schema: {schema}")

        # Test permissions for schema operations
        try:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS RAW")
            print("✓ Schema creation permissions verified")
        except Exception as e:
            print(f"⚠️  Schema creation test failed: {str(e)}")

        # Test table creation permissions
        try:
            cursor.execute("""
                CREATE OR REPLACE TABLE test_permissions_table (
                    test_column STRING
                )
            """)
            cursor.execute("DROP TABLE test_permissions_table")
            print("✓ Table creation/drop permissions verified")
        except Exception as e:
            print(f"⚠️  Table creation test failed: {str(e)}")

        cursor.close()
        conn.close()

        print("✅ Snowflake connection test passed!")
        return True

    except ImportError:
        print("❌ snowflake-connector-python not installed")
        print("Run: pip install snowflake-connector-python")
        return False
    except Exception as e:
        print(f"❌ Snowflake connection test failed: {str(e)}")
        return False


def test_s3_integration():
    """Test S3 integration if S3_URI is configured."""
    s3_uri = os.getenv("S3_URI")
    if not s3_uri:
        print("\nS3_URI not configured, skipping S3 integration test")
        return True

    try:
        import snowflake.connector

        print(f"\nTesting S3 integration for: {s3_uri}")

        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_RAW_SCHEMA", "RAW"),
            role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
        )

        cursor = conn.cursor()

        # Check if storage integration exists
        cursor.execute("SHOW STORAGE INTEGRATIONS LIKE 'betterjobs_s3_integration'")
        integrations = cursor.fetchall()

        if integrations:
            print("✓ Storage integration 'betterjobs_s3_integration' exists")

            # Test the integration
            try:
                cursor.execute("SELECT SYSTEM$VALIDATE_STORAGE_INTEGRATION('betterjobs_s3_integration')")
                result = cursor.fetchone()[0]
                print(f"✓ Storage integration validation: {result}")
            except Exception as e:
                print(f"⚠️  Storage integration validation failed: {str(e)}")
        else:
            print("⚠️  Storage integration 'betterjobs_s3_integration' not found")
            print("Create it with:")
            print("CREATE STORAGE INTEGRATION betterjobs_s3_integration")
            print("  TYPE = EXTERNAL_STAGE")
            print("  STORAGE_PROVIDER = 'S3'")
            print("  ENABLED = TRUE")
            print("  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::your-account:role/snowflake-s3-role'")
            print(f"  STORAGE_ALLOWED_LOCATIONS = ('{s3_uri}');")

        cursor.close()
        conn.close()

        print("✅ S3 integration test completed!")
        return True

    except Exception as e:
        print(f"❌ S3 integration test failed: {str(e)}")
        return False


def check_data_sources():
    """Check availability of data sources."""
    print("\nChecking data sources...")

    sources_available = False

    # Check S3 URI
    s3_uri = os.getenv("S3_URI")
    if s3_uri:
        print(f"✓ S3 URI configured: {s3_uri}")
        sources_available = True

    # Check local folder
    local_folder = os.getenv("MAIN_INPUT_FOLDER")
    if local_folder:
        if os.path.exists(local_folder):
            csv_files = list(Path(local_folder).glob("*.csv"))
            print(f"✓ Local folder exists: {local_folder}")
            print(f"  Found {len(csv_files)} CSV files")

            # Check if files have correct format
            if csv_files:
                sample_file = csv_files[0]
                try:
                    import pandas as pd
                    df = pd.read_csv(sample_file, nrows=1)
                    required_cols = ['company_name']
                    optional_cols = ['company_industry', 'platform', 'ats_url', 'career_url', 'url_verified']

                    has_required = all(col in df.columns for col in required_cols)
                    if has_required:
                        print(f"  ✓ Sample file {sample_file.name} has required columns")
                    else:
                        print(f"  ⚠️  Sample file {sample_file.name} missing required columns: {required_cols}")

                except Exception as e:
                    print(f"  ⚠️  Could not validate sample file: {str(e)}")

            sources_available = True
        else:
            print(f"⚠️  Local folder configured but doesn't exist: {local_folder}")

    if not sources_available:
        print("❌ No valid data sources found")
        return False

    print("✅ Data sources check passed!")
    return True


def create_test_csv():
    """Create a test CSV file for local testing."""
    test_folder = os.getenv("MAIN_INPUT_FOLDER")

    if not test_folder:
        # Use a default test folder
        test_folder = str(Path(__file__).parent / "test_data")
        print(f"\nMAIN_INPUT_FOLDER not set, creating test data in: {test_folder}")
        os.environ["MAIN_INPUT_FOLDER"] = test_folder

    # Create test folder if it doesn't exist
    Path(test_folder).mkdir(parents=True, exist_ok=True)

    # Create test CSV file with timestamp columns
    test_csv_path = Path(test_folder) / "test_companies.csv"

    test_data = """company_name,company_industry,platform,ats_url,career_url,url_verified,date_added,last_updated
"Test Corp A","Technology","workday","https://testcorpa.workday.com","https://testcorpa.com/careers",true,"2024-01-15 10:30:00","2024-01-15 10:30:00"
"Test Corp B","Software","greenhouse","https://greenhouse.io/testcorpb","https://testcorpb.com/jobs",false,"2024-01-16 09:15:00","2024-01-16 09:15:00"
"Test Corp C","Manufacturing","bamboohr","https://bamboohr.com/testcorpc","https://testcorpc.com/careers",true,"2024-01-17 14:45:00","2024-01-17 14:45:00"
"Test Corp D","Finance","icims","https://icims.com/testcorpd","https://testcorpd.com/careers",false,"2024-01-18 08:20:00","2024-01-18 08:20:00"
"""

    with open(test_csv_path, 'w') as f:
        f.write(test_data)

    print(f"✓ Created test CSV file: {test_csv_path}")
    print(f"  Contains 4 test companies with timestamp data")
    return True


def test_asset_import():
    """Test importing the Snowflake asset."""
    try:
        print("\nTesting asset import...")
        from dagster_betterjobs.assets.snowflake_master_company_urls import (
            snowflake_master_company_urls,
            SnowflakeMasterCompanyUrlsConfig
        )
        print("✓ Successfully imported Snowflake asset and config")

        # Test config creation
        config = SnowflakeMasterCompanyUrlsConfig(
            enable_local_processing=True,
            enable_s3_processing=False,
            batch_size=100,
            deduplicate_on_load=True
        )
        print("✓ Successfully created asset configuration")

        # Test helper functions
        from dagster_betterjobs.assets.snowflake_master_company_urls import (
            generate_company_id,
            get_file_metadata,
            calculate_file_hash
        )

        # Test company ID generation
        test_id = generate_company_id("Test Company")
        if len(test_id) == 8:
            print("✓ Company ID generation working correctly")
        else:
            print(f"⚠️  Company ID generation issue: expected 8 chars, got {len(test_id)}")

        print("✅ Asset import test passed!")
        return True

    except Exception as e:
        print(f"❌ Asset import test failed: {str(e)}")
        return False


def test_required_packages():
    """Test that all required packages are available."""
    print("\nTesting required packages...")

    packages_to_test = [
        ("snowflake.connector", "snowflake-connector-python"),
        ("boto3", "boto3"),
        ("pandas", "pandas"),
        ("pyarrow", "pyarrow"),
        ("dagster", "dagster")
    ]

    missing_packages = []

    for package_name, pip_name in packages_to_test:
        try:
            __import__(package_name)
            print(f"✓ {package_name} is available")
        except ImportError:
            print(f"❌ {package_name} is missing (install with: pip install {pip_name})")
            missing_packages.append(pip_name)

    if missing_packages:
        print(f"\n❌ Missing packages: {missing_packages}")
        return False

    print("✅ All required packages are available!")
    return True


def main():
    """Run all tests."""
    print("🧪 Snowflake Master Company URLs Asset Test")
    print("=" * 50)

    tests = [
        ("Required Packages", test_required_packages),
        ("Environment Variables", check_environment_variables),
        ("Snowflake Connection", test_snowflake_connection),
        ("S3 Integration", test_s3_integration),
        ("Data Sources", check_data_sources),
        ("Asset Import", test_asset_import)
    ]

    # If no data sources configured, create test data
    if not os.getenv("S3_URI") and not os.getenv("MAIN_INPUT_FOLDER"):
        print("\nNo data sources configured. Creating test data...")
        create_test_csv()

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        if test_func():
            passed += 1
        else:
            print(f"❌ {test_name} failed")

    print(f"\n{'='*50}")
    print(f"Test Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed! The Snowflake asset is ready to use.")
        print("\nNext steps:")
        print("1. Run the asset using Dagster UI or CLI:")
        print("   dagster asset materialize -m dagster_betterjobs snowflake_master_company_urls")
        print("2. Monitor the execution logs for any issues")
        print("3. Verify data in Snowflake RAW.master_company_urls table")
        print("4. Check processing log: RAW.master_company_urls_processing_log")

        # Display configuration tips
        print("\n💡 Configuration tips:")
        if os.getenv("S3_URI"):
            print("- S3 processing is enabled")
        if os.getenv("MAIN_INPUT_FOLDER"):
            print("- Local file processing is enabled")
        print("- Use SnowflakeMasterCompanyUrlsConfig to customize behavior")
        print("- Enable/disable S3 or local processing as needed")
        print("- Adjust batch_size for performance optimization")

    else:
        print("⚠️  Some tests failed. Please fix the issues before proceeding.")
        print("\n🔧 Common fixes:")
        print("- Install missing packages: pip install -r requirements.txt")
        print("- Set up environment variables for Snowflake connection")
        print("- Configure S3 storage integration if using S3")
        print("- Create local test data folder with CSV files")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())