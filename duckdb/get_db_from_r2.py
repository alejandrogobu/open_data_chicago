import boto3
from pathlib import Path
from dotenv import load_dotenv
import os

# Load Environment Variables

# Define project root and load environment variables
project_root = Path(__file__).resolve().parent.parent
load_dotenv(f'{project_root}/config.env')

# Retrieve R2 credentials and bucket information from environment variables
access_key_id = os.getenv("access_key_id")
secret_access_key = os.getenv("secret_access_key")
endpoint_url = os.getenv("endpoint_url")
bucket_data = os.getenv("bucket_data")     # Bucket for processed data

# Check that all necessary environment variables are available
if not all([access_key_id, secret_access_key, endpoint_url, bucket_data]):
    raise EnvironmentError("Some environment variables are missing: ensure 'access_key_id', 'secret_access_key', 'endpoint_url', and 'bucket_data' are defined in config.env.")

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

# Define Download Path (Same as Upload Path)

# Define the original file path where it was located during upload
duckdb_root = Path(__file__).resolve().parent
file_path = duckdb_root / 'chicago_crimes.db'  # This is the path for both upload and download

# The object name in the R2 bucket remains the same as the file name
object_name = file_path.name

# Download File from R2

try:
    # Download file to the original path
    s3.download_file(bucket_data, object_name, str(file_path))
    print(f'File "{object_name}" successfully downloaded from R2 bucket "{bucket_data}" to "{file_path}".')
except FileNotFoundError:
    print(f"File not found in bucket: {object_name}")
except boto3.exceptions.S3DownloadError as e:
    print("Download failed:", e)
except Exception as e:
    print(f"An unexpected error occurred while downloading the file: {e}")