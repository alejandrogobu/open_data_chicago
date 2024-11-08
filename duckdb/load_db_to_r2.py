import boto3
from pathlib import Path
from dotenv import load_dotenv
import os

# Load Environment Variables

# Define project root and load environment variables
project_root = Path(__file__).resolve().parent.parent
load_dotenv(f'{project_root}/config.env')

# Retrieve R2 credentials and bucket names from environment variables
access_key_id = os.getenv("access_key_id")
secret_access_key = os.getenv("secret_access_key")
endpoint_url = os.getenv("endpoint_url")
bucket_data = os.getenv("bucket_data")     # Bucket for processed data

# Check that all necessary environment variables are available
if not all([access_key_id, secret_access_key, endpoint_url, bucket_data]):
    raise EnvironmentError("Some environment variables are missing: ensure 'access_key_id', 'secret_access_key', 'endpoint_url' and 'bucket_data' are defined in config.env.")

# Configure the R2 Cloudflare Client

# Set up a session with R2 Cloudflare using the credentials
session = boto3.session.Session()
s3 = session.client(
    's3',
    endpoint_url=endpoint_url,
    aws_access_key_id=access_key_id,
    aws_secret_access_key=secret_access_key,
    region_name='auto'
)

# Define File Path and Upload to R2

# Define the path of the file to upload
duckdb_root = Path(__file__).resolve().parent
file_path = duckdb_root / 'chicago_crimes.db'  # Path to the file to upload

# The object name in R2 will be the same as the file name
object_name = file_path.name

# Upload File to R2

try:
    # Upload file to the specified R2 bucket
    s3.upload_file(str(file_path), bucket_data, object_name)
    print(f'File "{file_path}" successfully uploaded to R2 in bucket "{bucket_data}".')
except FileNotFoundError:
    print(f"File not found: {file_path}")
except boto3.exceptions.S3UploadFailedError as e:
    print("Upload failed:", e)
except Exception as e:
    print(f"An unexpected error occurred while uploading the file: {e}")