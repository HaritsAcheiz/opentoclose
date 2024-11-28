import requests
import urllib.parse
import json
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
import os
import pandas as pd
import time
import shutil

load_dotenv()

# Define a consistent schema
SCHEMA = pa.schema([
    ("id", pa.int64()),
    ("created", pa.string()),
    ("timezone", pa.string()),
    ("field_values", pa.string()),
    ("team_id", pa.int64()),
    ("team_name", pa.string()),
    ("team_user_id", pa.int64()),
    ("team_user_name", pa.string()),
    ("agent_id", pa.int64()),  # Force `agent_id` to be int64
    ("agent_name", pa.string()),
    ("brokerage", pa.string()),
    ("api_data", pa.string()),
    ("inbound_email_address", pa.string()),
])


def fetch_properties(api_token, limit=50, offset=0):
    base_url = "https://api.opentoclose.com/v1/properties"
    params = {"api_token": api_token, "limit": limit, "offset": offset}
    encoded_params = urllib.parse.urlencode(params)
    url = f"{base_url}?{encoded_params}"
    headers = {"Accept": "application/json"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Error: API request failed with status code {response.status_code}")
        return None
    return response.json()


def save_to_parquet_in_chunks(data_chunk, filename_prefix):
    if not data_chunk:
        print("No data to save in this chunk. Skipping.")
        return

    # Convert list of records to DataFrame
    df = pd.DataFrame(data_chunk)

    # Convert nested structures to JSON strings
    for column in df.columns:
        if df[column].dtype == "object":
            df[column] = df[column].apply(
                lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
            )

    # Standardize column types to match the schema
    for field in SCHEMA:
        column = field.name
        if column in df.columns:
            if pa.types.is_int64(field.type):
                df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype('int64')
            elif pa.types.is_string(field.type):
                df[column] = df[column].astype('string')

    # Convert DataFrame to PyArrow Table
    table = pa.Table.from_pandas(df, schema=SCHEMA)

    # Create a unique filename based on timestamp
    timestamp = time.strftime("%Y%m%d%H%M%S")
    filename = f"datas/{filename_prefix}_{timestamp}.parquet"

    # Write data to a new file (no appending to existing files)
    pq.write_table(table, filename, compression='snappy')

    print(f"Written {len(data_chunk)} records to {filename}")


def delete_and_recreate_folder(folder_path):
    try:
        # Check if the folder exists
        if os.path.exists(folder_path):
            # If the folder is not empty, use shutil.rmtree()
            if os.listdir(folder_path):
                shutil.rmtree(folder_path)
                print(f"Folder '{folder_path}' and its contents have been deleted.")

            # If the folder is empty, use os.rmdir()
            else:
                os.rmdir(folder_path)
                print(f"Empty folder '{folder_path}' has been deleted.")

        # Create a new empty folder at the same path
        os.makedirs(folder_path)
        print(f"A new empty folder has been created at '{folder_path}'.")

    except PermissionError:
        print(f"Permission denied: Unable to delete or create folder at '{folder_path}'.")
    except Exception as e:
        print(f"An error occurred: {e}")


def fetch_and_save(api_token, filename_prefix="all_properties", limit=50):
    offset = 15000
    while True:
        data = fetch_properties(api_token, limit, offset)
        if data is None or len(data) == 0:
            break
        print(f"Fetched {len(data)} records, offset={offset}")
        save_to_parquet_in_chunks(data, filename_prefix)
        offset += limit


def execute_fetch_properties():
    api_token = os.getenv('OTC_API_KEY')
    if not api_token:
        print("Error: API token not found in environment variables.")
        return
    delete_and_recreate_folder('./datas')
    fetch_and_save(api_token)
