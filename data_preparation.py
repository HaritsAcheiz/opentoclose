import duckdb
import csv
import pandas as pd
import json


# def extract_field_values(field_values, key):
#     try:
#         values = json.loads(field_values)
#         for item in values:
#             if isinstance(item, dict) and item.get("label") == key:
#                 return item.get("value")
#     except json.JSONDecodeError:
#         pass
#     return None


# def extract_transaction_source(properties_file_path):
#     print('Extracting transaction data source...', end='')
#     conn = duckdb.connect(database=":memory:")
#     try:
#         query = f"SELECT * FROM '{properties_file_path}'"
#         df = conn.execute(query).fetchdf()

#         transaction_schema = list()
#         with open('field_values.json', 'r') as file:
#             for item in json.load(file):
#                 transaction_schema.append(item['label'])

#         transaction_df = pd.DataFrame()
#         for field in transaction_schema:
#             transaction_df[field] = df['field_values'].apply(lambda x: extract_field_values(x, field))

#         print('Done')
#         return transaction_df

#     except Exception as e:
#         print(f"Error processing data: {e}")
#         return None

#     finally:
#         conn.close()


def extract_field_values_batch(field_values, schema):
    """
    Extract multiple fields from a single JSON string in field_values.
    """
    # try:
    # Parse the JSON string
    values = json.loads(field_values)
    # Prepare a dictionary to store extracted values
    result = {key: None for key in schema}
    # Extract values for all schema fields
    for item in values:
        if isinstance(item, dict) and "label" in item and "value" in item:
            if item["label"] in schema:
                result[item["label"]] = item["value"]

    return result

    # except json.JSONDecodeError:
        # Return None for all fields if JSON parsing fails
        # return {key: None for key in schema}


def extract_transaction_source(properties_file_path):
    print('Extracting transaction data source...', end='')
    conn = duckdb.connect(database=":memory:")
    try:
        # Load data into a pandas DataFrame
        query = f"SELECT * FROM '{properties_file_path}'"
        df = conn.execute(query).fetchdf()

        # Load transaction schema from the JSON file
        with open('field_values.json', 'r') as file:
            transaction_schema = [item['label'] for item in json.load(file)]

        # Extract all field values in one pass
        extracted_data = df['field_values'].map(lambda x: extract_field_values_batch(x, transaction_schema))

        # Convert the list of dictionaries into a DataFrame
        transaction_df = pd.DataFrame(extracted_data.tolist(), columns=transaction_schema)

        print('Done')
        return transaction_df

    except Exception as e:
        print(f"Error processing data: {e}")
        return None

    finally:
        conn.close()


def selecting_field(df):
    unneeded_fields = []
    result = df.drop(columns=unneeded_fields)


if __name__ == '__main__':
    transaction_df = extract_transaction_source('all_properties.parquet')
    print(transaction_df.head())