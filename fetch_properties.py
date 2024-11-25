import requests
import urllib.parse
import json
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
import os
import pandas as pd
import numpy as np

load_dotenv()


def fetch_properties(api_token, limit=50, offset=0):
    base_url = "https://api.opentoclose.com/v1/properties"
    # URL encode the parameters
    params = {"api_token": api_token, "limit": limit, "offset": offset}
    encoded_params = urllib.parse.urlencode(params)
    url = f"{base_url}?{encoded_params}"
    headers = {"Accept": "application/json"}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for bad status codes
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def normalize_data(data):
    """
    Normalize the input data to ensure consistent schema and type handling
    """
    normalized_data = []

    for item in data:
        # Create a normalized dictionary with consistent keys
        normalized_item = {}
        for key, value in item.items():
            # Convert nested structures to JSON strings
            if isinstance(value, (dict, list)):
                normalized_item[key] = json.dumps(value) if value else None
                continue

            # Convert all numeric IDs and similar fields to strings
            if key.endswith('_id') or key == 'id':
                normalized_item[key] = str(value) if value is not None else None
            else:
                normalized_item[key] = value

        normalized_data.append(normalized_item)

    return normalized_data

def save_properties_in_chunks(api_token, output_filename, chunk_size=50):
    """
    Fetch and save properties in chunks to minimize memory usage
    """
    offset = 0
    total_properties_saved = 0

    while True:
        # Fetch a chunk of properties
        data = fetch_properties(api_token, limit=chunk_size, offset=offset)

        if not data:
            break

        # Normalize data (convert IDs and nested structures)
        normalized_data = normalize_data(data)

        # Convert chunk to DataFrame
        df_chunk = pd.DataFrame(normalized_data)

        # Create PyArrow schema with all columns as strings
        schema_fields = [
            pa.field(col, pa.string()) for col in df_chunk.columns
        ]
        schema = pa.schema(schema_fields)

        # Convert DataFrame to PyArrow Table with string schema
        table_chunk = pa.Table.from_pandas(df_chunk, schema=schema)

        # Check if file exists
        if offset == 0:
            # First chunk: write the file
            pq.write_table(table_chunk, output_filename)
        else:
            # Subsequent chunks: read existing file, combine, and write
            try:
                # Read existing Parquet file
                existing_table = pq.read_table(output_filename)

                # Combine existing table with new chunk
                combined_table = pa.concat_tables([existing_table, table_chunk])

                # Write combined table
                pq.write_table(combined_table, output_filename)
            except Exception as e:
                print(f"Error appending data: {e}")
                break

        total_properties_saved += len(data)
        print(f"Saved chunk: {len(data)} properties (Total: {total_properties_saved})")

        # Break if fewer properties returned than chunk size
        if len(data) < chunk_size:
            break

        # Update offset for next iteration
        offset += chunk_size

    print(f"\nTotal properties saved: {total_properties_saved}")

def execute_fetch_properties():
    load_dotenv()
    api_token = os.getenv('OTC_API_KEY')
    save_properties_in_chunks(api_token, "all_properties.parquet")

if __name__ == "__main__":
    execute_fetch_properties()