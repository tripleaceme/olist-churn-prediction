"""
Load Olist CSV data into Snowflake using snowflake-connector-python.

Usage:
    pip install snowflake-connector-python pandas
    python load_to_snowflake.py
"""

import os
import snowflake.connector
import pandas as pd

# -- Config (update with your Snowflake credentials) --
SNOWFLAKE_CONFIG = {
    "account": os.environ.get("SNOWFLAKE_ACCOUNT", "your_account"),
    "user": os.environ.get("SNOWFLAKE_USER", "your_user"),
    "password": os.environ.get("SNOWFLAKE_PASSWORD", "your_password"),
    "role": os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
}

DATABASE = "OLIST"
SCHEMA = "RAW_DATA"
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

# Mapping: csv filename -> table name
TABLE_MAP = {
    "customers_dataset.csv": "customers",
    "orders_dataset.csv": "orders",
    "order_items_dataset.csv": "order_items",
    "order_payments_dataset.csv": "order_payments",
    "order_reviews_dataset.csv": "order_reviews",
    "products_dataset.csv": "products",
    "sellers_dataset.csv": "sellers",
    "geolocation_dataset.csv": "geolocation",
}


def run_ddl(cursor):
    """Create database, schemas, and tables."""
    ddl_statements = [
        f"CREATE DATABASE IF NOT EXISTS {DATABASE}",
        f"USE DATABASE {DATABASE}",
        f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
        "CREATE SCHEMA IF NOT EXISTS STAGING",
        "CREATE SCHEMA IF NOT EXISTS INTERMEDIATE",
        "CREATE SCHEMA IF NOT EXISTS MARTS",
        f"USE SCHEMA {SCHEMA}",
        """CREATE OR REPLACE FILE FORMAT olist_csv_format
           TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"'
           SKIP_HEADER = 1 NULL_IF = ('', 'NULL', 'null')
           EMPTY_FIELD_AS_NULL = TRUE TRIM_SPACE = TRUE""",
        "CREATE OR REPLACE STAGE olist_stage FILE_FORMAT = olist_csv_format",
    ]
    for stmt in ddl_statements:
        print(f"  Running: {stmt[:60]}...")
        cursor.execute(stmt)
    print("  DDL complete.\n")


def upload_and_load(cursor, csv_file, table_name):
    """PUT a local CSV to stage, then COPY INTO the table."""
    filepath = os.path.join(DATA_DIR, csv_file)
    if not os.path.exists(filepath):
        print(f"  SKIP: {filepath} not found")
        return

    # Read CSV to infer and create table schema
    df = pd.read_csv(filepath, nrows=5)
    col_defs = ", ".join(f'"{c}" VARCHAR' for c in df.columns)
    cursor.execute(f"CREATE OR REPLACE TABLE {table_name} ({col_defs})")
    print(f"  Created table {table_name} ({len(df.columns)} cols)")

    # PUT file to stage
    put_sql = f"PUT file://{filepath} @olist_stage/{table_name}/ AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
    cursor.execute(put_sql)
    print(f"  Uploaded {csv_file} to stage")

    # COPY INTO table
    copy_sql = f"""
        COPY INTO {table_name}
        FROM @olist_stage/{table_name}/
        FILE_FORMAT = olist_csv_format
        ON_ERROR = 'CONTINUE'
    """
    cursor.execute(copy_sql)
    result = cursor.fetchone()
    print(f"  Loaded: {result}\n")


def verify_counts(cursor):
    """Print row counts for all tables."""
    print("=== Row Counts ===")
    for table in TABLE_MAP.values():
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table:20s}: {count:>10,}")


def main():
    print("Connecting to Snowflake...\n")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()

    try:
        print("--- Step 1: Creating database and schemas ---")
        run_ddl(cursor)

        print("--- Step 2: Uploading and loading data ---")
        for csv_file, table_name in TABLE_MAP.items():
            print(f"Processing {csv_file} -> {table_name}")
            upload_and_load(cursor, csv_file, table_name)

        print("--- Step 3: Verifying ---")
        verify_counts(cursor)

        print("\nDone! Data is loaded into OLIST.RAW_DATA")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
