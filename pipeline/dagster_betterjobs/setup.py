from setuptools import find_packages, setup

setup(
    name="dagster_betterjobs",
    packages=find_packages(exclude=["dagster_betterjobs_tests"]),
    install_requires=[
        "dagster",
        "dagster-webserver",
        "dagster-cloud",
        "dagster-duckdb",
        "dagster-postgres",
        "dagster-snowflake",
        "dagster-snowflake-pandas",
        "dagster-openai",
        "dagster-gcp",
        "dagster-gcp-pandas",
        "pandas",
        "duckdb",
        "sqlescapy",
        "lxml",
        "html5lib",
        "google-generativeai>=0.3.0",
        "beautifulsoup4>=4.12.0",
        "requests>=2.28.0",
        "python-dotenv>=1.0.0",
        "tqdm>=4.65.0",
        "tabulate>=0.9.0",  # For markdown tables
        # Snowflake requirements
        "snowflake-connector-python>=3.0.0",
        "boto3>=1.26.0",
        "pyarrow>=10.0.0",  # Required for pandas integration with Snowflake
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
    entry_points={
        "console_scripts": [
            "betterjobs=dagster_betterjobs.cli:main",
        ],
    },
)
