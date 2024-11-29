import duckdb
import csv
import pandas as pd
import json
from gsheetapi import *
from datetime import datetime

na_filler = datetime(1899, 12, 30)


def extract_field_values(field_values, key):
    try:
        values = json.loads(field_values)
        for item in values:
            if isinstance(item, dict) and item.get("label") == key:
                return item.get("value")
    except json.JSONDecodeError:
        pass
    return None


def extract_field_values_batch(field_values, schema):
    values = json.loads(field_values)
    result = {key: None for key in schema}
    for item in values:
        if isinstance(item, dict) and "label" in item and "value" in item:
            if item["label"] in schema:
                result[item["label"]] = item["value"]

    return result


def create_staging_layer(properties_file_path):
    print('Extracting source report...')
    conn = duckdb.connect(database=":memory:")
    try:
        query = f"SELECT * FROM read_parquet('{properties_file_path}')"
        df = conn.execute(query).fetchdf()

        with open('Columns_Transaction_Source.csv') as file:
            rows = csv.reader(file)
            transaction_schema = [row[0] for row in rows]

        intermediate_trx_data = df['field_values'].map(lambda x: extract_field_values_batch(x, transaction_schema))
        intermediate_df = pd.DataFrame(intermediate_trx_data.tolist(), columns=transaction_schema)
        transaction_df = df[['timezone', 'team_name', 'team_user_name', 'created', 'agent_name']].copy()
        transaction_df.rename({'timezone': 'Time Zone', 'team_name': 'Team', 'team_user_name': 'Team User', 'created': 'Date Created', 'agent_name': 'Created By'}, axis=1, inplace=True)
        transaction_df = pd.concat([transaction_df, intermediate_df], axis=1)
        transaction_df = transaction_df[transaction_df['Contract Status'] != 'AGENT ACCOUNT']
        transaction_df['Date Created'] = pd.to_datetime(transaction_df['Date Created'])
        transaction_df.sort_values('Date Created', inplace=True, ignore_index=True, ascending=False)

        error_df = transaction_df[pd.isna(transaction_df['Empower TC Name'])]
        transaction_df = transaction_df[~pd.isna(transaction_df['Empower TC Name'])]

        agent_account_schema = list()
        with open('Columns_Agent_Account_Source.csv') as file:
            rows = csv.reader(file)
            for row in rows:
                agent_account_schema.append(row[0])

        intermediate_aa_data = df['field_values'].map(lambda x: extract_field_values_batch(x, agent_account_schema))
        intermediate_aa_df = pd.DataFrame(intermediate_aa_data.tolist(), columns=agent_account_schema)
        agent_account_df = df[['timezone', 'team_name', 'team_user_name', 'created', 'agent_name']].copy()
        agent_account_df.rename({'timezone': 'Time Zone', 'team_name': 'Team', 'team_user_name': 'Team User', 'created': 'Date Created', 'agent_name': 'Created By'}, axis=1, inplace=True)
        agent_account_df = pd.concat([agent_account_df, intermediate_aa_df], axis=1)
        agent_account_df = agent_account_df[agent_account_df['Contract Status'] == 'AGENT ACCOUNT']
        agent_account_df['Date Created'] = pd.to_datetime(agent_account_df['Date Created'])
        agent_account_df.sort_values('Date Created', inplace=True, ignore_index=True, ascending=False)
        duplicated_agent_account_df = agent_account_df[agent_account_df.duplicated('Contract Title', keep='first')].copy()
        agent_account_df.drop_duplicates('Contract Title', keep='first', inplace=True, ignore_index=True)

        # Join Transaction with Agent Account
        transaction_df = transaction_df.merge(agent_account_df[['Contract Title', '1st Transaction Date', 'Reassigned Date', 'Brokerage', 'Agent Provided by']], how='left', left_on='Empower Agent Name', right_on='Contract Title')
        transaction_df.rename({'Contract Title_x': 'Contract Title'}, inplace=True, axis=1)
        transaction_df.drop(columns='Contract Title_y', inplace=True)

        # Formating
        with open('trx_order.csv') as file:
            rows = csv.reader(file)
            trx_order_columns = [row[0] for row in rows]
        transaction_df = transaction_df[trx_order_columns]

        with open('trx_date_columns.csv') as file:
            rows = csv.reader(file)
            trx_date_columns = [row[0] for row in rows]
        transaction_df[trx_date_columns] = transaction_df[trx_date_columns].astype('datetime64[ns]')
        transaction_df[trx_date_columns] = transaction_df[trx_date_columns].fillna(na_filler)

        return transaction_df, error_df, agent_account_df, duplicated_agent_account_df

    except Exception as e:
        print(f"Error processing data: {e}")
        return None

    finally:
        conn.close()


if __name__ == '__main__':
    transaction_df, error_df, agent_account_df, duplicated_agent_account_df = create_staging_layer('datas/all_properties_*.parquet')

    dataframes = [transaction_df, error_df, agent_account_df, duplicated_agent_account_df]
    sheet_titles = ["Transaction", "Error Transaction", "Agent Account", "Duplicated Agent Account"]
    spreadsheet_name = "Source Report"
    spreadsheet_id = "1NFgo4enM06OiEEaY9bNe6E3e1O-O_xXI2cvAejpFC9A"

    create_google_sheet(dataframes, sheet_titles, spreadsheet_name, spreadsheet_id)