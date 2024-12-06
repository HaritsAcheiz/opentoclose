import duckdb
import csv
import pandas as pd
import json
from gsheetapi import *
from datetime import datetime

na_filler = datetime(1899, 12, 30)


def correcting_ba_amount(df):
    # Create a copy to avoid modifying the original DataFrame
    df = df.copy()

    # Conditions for Billing Amount replacement
    billing_amount_blank = (pd.isna(df['Billing Amount'])) | (df['Billing Amount'] == 0) | (df['Billing Amount'] == '')

    # Replace with Other Amount if available
    other_amount_valid = (pd.notna(df['Other Amount'])) & (df['Other Amount'] != 0) & (df['Other Amount'] != '')
    df.loc[billing_amount_blank & other_amount_valid, 'Billing Amount'] = df.loc[billing_amount_blank & other_amount_valid, 'Other Amount']

    # Fill remaining blank rows based on Preferred Ai CTC
    remaining_blank = billing_amount_blank & ~other_amount_valid
    df.loc[remaining_blank & (df['Preferred  Ai  CTC'] == 'Yes'), 'Billing Amount'] = 99.00
    df.loc[remaining_blank & (df['Preferred  Ai  CTC'] != 'Yes'), 'Billing Amount'] = 400.00

    return df['Billing Amount'].to_list()


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
        transaction_df = transaction_df.merge(agent_account_df[['Contract Title', '1st Transaction Date', 'Reassigned Date', 'Brokerage', 'Agent Provided by']], how='left', left_on='Empower Agent Name', right_on='Contract Title', indicator=True)
        transaction_df.rename({'Contract Title_x': 'Contract Title'}, inplace=True, axis=1)
        transaction_df.drop(columns='Contract Title_y', inplace=True)

        error_df = transaction_df[transaction_df['_merge'] == 'left_only']
        error_df.drop(columns='_merge', inplace=True)

        transaction_df = transaction_df[transaction_df['_merge'] == 'both']
        transaction_df.drop(columns='_merge', inplace=True)

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

        with open('trx_columns_need_fillna_0.csv') as file:
            rows = csv.reader(file)
            trx_columns_need_fillna_0 = [row[0] for row in rows]

        with open('trx_columns_need_fillna_none.csv') as file:
            rows = csv.reader(file)
            trx_columns_need_fillna_none = [row[0] for row in rows]

        transaction_df[trx_columns_need_fillna_0] = transaction_df[trx_columns_need_fillna_0].fillna(0)
        transaction_df[trx_columns_need_fillna_0] = transaction_df[trx_columns_need_fillna_0].replace('', 0)
        transaction_df[trx_columns_need_fillna_none] = transaction_df[trx_columns_need_fillna_none].fillna('none')
        transaction_df[trx_columns_need_fillna_none] = transaction_df[trx_columns_need_fillna_none].replace('', 'none')

        # Filtering
        transaction_df.drop(
            transaction_df[
                transaction_df['Contract Title'].str.lower().str.contains('test')
            ].index,
            axis=0,
            inplace=True
        )

        transaction_df.drop(
            transaction_df[
                transaction_df['Contract Title'].str.lower().str.contains('training')
            ].index,
            axis=0,
            inplace=True
        )

        transaction_df.drop(
            transaction_df[
                transaction_df['Contract Title'].str.lower().str.contains('delete')
            ].index,
            axis=0,
            inplace=True
        )

        transaction_df.drop(
            transaction_df[
                ((pd.isna(transaction_df['Empower TC Name'])) | (transaction_df['Empower TC Name'] == '')) & (transaction_df['CTC Started with Empower'] == na_filler) & (transaction_df['Listing Started with Empower'] == na_filler) & (transaction_df['Offer Started with Empower'] == na_filler) & (transaction_df['Compliance Started with Empower'] == na_filler)].index,
            axis=0,
            inplace=True
        )

        transaction_df.drop(
            transaction_df[
                ((pd.isna(transaction_df['Empower Agent Name'])) | (transaction_df['Empower Agent Name'] == '')) & (transaction_df['CTC Started with Empower'] == na_filler) & (transaction_df['Listing Started with Empower'] == na_filler) & (transaction_df['Offer Started with Empower'] == na_filler) & (transaction_df['Compliance Started with Empower'] == na_filler)].index,
            axis=0,
            inplace=True
        )

        # Correction
        transaction_df.loc[
            transaction_df[(
                transaction_df['Contract Status'].isin([
                    'Compliance - PAID',
                    'Compliance - Ready to BILL',
                    'CTC - Preferred - Terminated - No Change',
                    'CTC - Terminated - No Change',
                    'CTC - Preferred - Withdrawn',
                    'CTC - Withdrawn',
                    'Listing - PAID',
                    'Listing - Pre-Listing',
                ])
            )].index,
            'Closing'
        ] = na_filler

        print(transaction_df['CTC Started with Empower'].dtype)

        transaction_df.loc[
            transaction_df[
                (transaction_df['CTC Started with Empower'].dt.year >= 2021) & (transaction_df['Listing Started with Empower'].dt.year >= 2021)
            ].index,
            ['Listing Started with Empower', 'Live on MLS Date', 'Listing PAID Date']
        ] = na_filler

        transaction_df.loc[
            transaction_df[
                (transaction_df['CTC Started with Empower'].dt.year >= 2021) & (transaction_df['Listing Started with Empower'].dt.year >= 2021)
            ].index,
            'Listing PAID Amount'
        ] = 0

        billing_status_list = transaction_df.loc[
            transaction_df['Billing Status'] != 'none',
            'Billing Status'].to_list()
        transaction_df.loc[
            transaction_df['Billing Status'] != 'none',
            'Contract Status'] = billing_status_list

        ba_amount_correction = transaction_df.loc[
            ((transaction_df['Billing Amount'] == 0) | (pd.isna(transaction_df['Billing Amount'])) | (transaction_df['Billing Amount'] == '')) & (transaction_df['Closing'] != na_filler),
            ['Billing Amount', 'Other Amount', 'Preferred  Ai  CTC']
        ]

        corrected_ba_amount = correcting_ba_amount(ba_amount_correction)

        transaction_df.loc[
            ((transaction_df['Billing Amount'] == 0) | (pd.isna(transaction_df['Billing Amount'])) | (transaction_df['Billing Amount'] == '')) & (transaction_df['Closing'] != na_filler),
            'Billing Amount'
        ] = corrected_ba_amount

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