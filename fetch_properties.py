import requests
import urllib.parse
import json
import pyarrow as pa
import pyarrow.parquet as pq


def fetch_properties(api_token, limit=50, offset=0):
    base_url = "https://api.opentoclose.com/v1/properties"

    # URL encode the parameters
    params = {"api_token": api_token, "limit": limit, "offset": offset}
    encoded_params = urllib.parse.urlencode(params)

    url = f"{base_url}?{encoded_params}"

    headers = {"Accept": "application/json"}

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Error: API request failed with status code {response.status_code}")
        return None

    return response.json()


def fetch_all_properties(api_token):
    offset = 0
    limit = 50
    all_properties = []

    while True:
        data = fetch_properties(api_token, limit, offset)
        print(f"Fetching data: limit={limit}, offset={offset}")
        if data is None:
            break

        all_properties.extend(data)

        if len(data) < limit:
            break

        offset += limit

    return all_properties


def save_to_parquet(properties, filename):
    if not properties:
        print("No properties to save.")
        return

    # Convert the list of dictionaries to a pandas DataFrame
    import pandas as pd

    df = pd.DataFrame(properties)

    # Convert nested structures to string representations
    for column in df.columns:
        if df[column].dtype == "object":
            df[column] = df[column].apply(
                lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
            )

    # Convert DataFrame to PyArrow Table
    table = pa.Table.from_pandas(df)

    # Write the table to a Parquet file
    pq.write_table(table, filename)

    print(f"Data has been written to {filename}")


def execute_fetch_properties():
    # Example usage
    api_token = "K2xpU2owcTFYZmVTbEZWNEJaYytWdz09OkJMMHNkakhGeGMyV3dnYUVkTkNsVjJtNlNZRFllUkFqOjA1MjVkMGVlNDg3Nzg1NmNiODRkODI1ZTM1ZDM2YzIzOWM1ZTA5ZjRmNDg1YmI2MjlmZjEwNDBiNjU5Y2FlZjE="

    properties = fetch_all_properties(api_token)

    print(f"\nTotal properties fetched: {len(properties)}")
    save_to_parquet(properties, "all_properties.parquet")
