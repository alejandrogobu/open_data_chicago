import duckdb
from pathlib import Path
from dotenv import load_dotenv
import os
import time

# ===========================
# Project Path Configuration
# ===========================

# Get the root project path and the DuckDB database path
project_root = Path(__file__).resolve().parent.parent
duckdb_root = Path(__file__).resolve().parent

print(f"Project Root Path: {project_root}")
print(f"DuckDB Path: {duckdb_root}")

# ===========================
# Load Environment Variables
# ===========================

# Load the .env configuration file located at the project root
load_dotenv(f'{project_root}/config.env')

# Retrieve required environment variables for the connection
access_key_id = os.getenv("access_key_id")
secret_access_key = os.getenv("secret_access_key")
account_id = os.getenv("account_id")
bucket_raw = os.getenv("bucket_raw")

# Check that all necessary environment variables are available
if not all([access_key_id, secret_access_key, account_id, bucket_raw]):
    raise EnvironmentError("Some environment variables are missing: ensure 'access_key_id', 'secret_access_key', 'account_id', and 'bucket_raw' are defined in config.env.")

# ===========================
# Connect to DuckDB and Execute Operations
# ===========================

# Use a context manager to ensure the connection is closed automatically
with duckdb.connect(f'{duckdb_root}/chicago_crimes.db') as con:
    # Install and load the httpfs extension to access remote files if not already installed
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")  # Load the extension

    # Create the R2 secret using the loaded environment variables
    # This sets up the necessary credentials to access cloud storage
    try:
        # Try to create the secret for R2 access
        con.execute(f"""
        CREATE SECRET (
            TYPE R2,
            KEY_ID '{access_key_id}',
            SECRET '{secret_access_key}',
            ACCOUNT_ID '{account_id}'
        );
        """)
        print("R2 secret created successfully.")

    except duckdb.Error as db_err:
        print("Database error occurred while creating the R2 secret:", db_err)
    except Exception as e:
        print("Unexpected error occurred while creating the R2 secret:", e)

    # ===========================
    # Execute Query to Test R2 Integration
    # ===========================
    
    try:
        # Step 1: Create schema if it doesn't exist
        create_schema_query = "CREATE SCHEMA IF NOT EXISTS RAW;"
        con.execute(create_schema_query)
        print("Schema 'RAW' created or already exists.")
        
        # Step 2: Create table from R2 .parquet files
        start_time = time.time()
        create_table_query = f"""
        CREATE OR REPLACE TABLE RAW.CRIMES_2024 AS 
        SELECT * FROM READ_PARQUET('r2://alejandrogb/chicago_crimes/crimes/**/*.parquet', filename=true);
        """
        finish_time = time.time()
        elapsed_time = finish_time - start_time
        print(f"Time taken to create table: {elapsed_time} seconds")
        con.execute(create_table_query)
        print("Table 'RAW.CRIMES' created or already exists.")
        
        # Step 3: Count rows in the table
        count_row_query = "SELECT count(*) FROM RAW.CRIMES_2024;"
        count_row = con.execute(count_row_query).fetchdf()  # .fetchdf() to directly retrieve as DataFrame
        print("Row count in 'RAW.CRIMES':")
        print(count_row)

    except duckdb.Error as db_err:
        print("Database error occurred during query execution:", db_err)
    except FileNotFoundError as fnf_err:
        print("File or path not found:", fnf_err)
    except Exception as e:
        print("An unexpected error occurred during query execution:", e)

# At the end, the connection is automatically closed thanks to the `with` context manager.