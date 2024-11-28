import duckdb
import time
import json
import pandas as pd
from datetime import datetime, date
import csv
from openpyxl import load_workbook
# from google.auth.transport.requests import Request
# from google.oauth2.service_account import Credentials
# from google_auth_oauthlib.flow import InstalledAppFlow
# from googleapiclient.discovery import build
import sys
import os

# setting path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from gsheetapi import *

na_filler = datetime(1990, 1, 1, 0, 0, 0)


def extract_field_values(field_values, key):
    try:
        values = json.loads(field_values)
        for item in values:
            if isinstance(item, dict) and item.get("label") == key:
                return item.get("value")
    except json.JSONDecodeError:
        pass
    return None


def extract_transaction_source(properties_file_path_pattern):
    print('Extracting transaction data source...', end='')
    conn = duckdb.connect(database=":memory:")
    try:
        conn = duckdb.connect(database=":memory:")
        query = f"SELECT * FROM read_parquet('{properties_file_path_pattern}')"
        df = conn.execute(query).fetchdf()
        print("Data extraction complete.")

        transaction_schema = list()
        with open('Columns_Transaction_Source.csv') as file:
            rows = csv.reader(file)
            for row in rows:
                transaction_schema.append(row[0])

        transaction_df = pd.DataFrame()
        for field in transaction_schema:
            transaction_df[field] = df['field_values'].apply(lambda x: extract_field_values(x, field))

        print('Done')
        return transaction_df

    except Exception as e:
        print(f"Error processing data: {e}")
        return None

    finally:
        conn.close()


def update_agent_provided_by(transaction_df, agents_file_path):
    try:
        agents_df = pd.read_csv(agents_file_path, usecols=['Title', 'Agent Provided by'])
        transaction_df = transaction_df.merge(agents_df, how='left', left_on='Empower Agent Name', right_on='Title')
        transaction_df.drop(columns=['Agent Provided by', 'Title'], inplace=True)
        # transaction_df.rename(columns={'Agent Provided by': 'agent_provided_by'}, inplace=True)

        print('Done')
        return transaction_df

    except Exception as e:
        print(f"Error processing data: {e}")
        return None


# def add_tc_commission_rate(transaction_df):
#     transaction_df.rename({'TC Commission Rate': 'TC Commission Rate_1'}, axis=1, inplace=True)
#     transaction_df['TC Commission Rate'] = transaction_df.apply(lambda x: 70/100 if x['agent_provided_by'] == 'TC' else 50/100, axis=1)
#     transaction_df.drop(columns='TC Commission Rate_1', inplace=True)

#     return transaction_df


def get_period(selected_date, mode):
    if isinstance(selected_date, str):
        try:
            selected_date = pd.to_datetime(selected_date)
        except ValueError:
            print("Invalid date format")
            selected_date = None
    if pd.isna(selected_date):
        result = selected_date
    else:
        year = selected_date.year
        month = selected_date.month
        day = selected_date.day
        if mode == 'start':
            if day >= 1 and day <= 15:
                result = datetime(year, month, 1)
            else:
                result = datetime(year, month, 16)
        elif mode == 'end':
            if day >= 1 and day <= 15:
                result = datetime(year, month, 15, 23, 59, 59)
            else:
                if month == 12:
                    result = datetime(year, 12, 31, 23, 59, 59)
                else:
                    next_month = datetime(year, month + 1, 1, 23, 59, 59)
                    result = next_month - pd.DateOffset(days=1)

    return result


def add_period(transaction_df):
    transaction_df['closing_period_start'] = transaction_df['Closing'].apply(lambda x: get_period(x, mode='start'))
    transaction_df['closing_period_end'] = transaction_df['Closing'].apply(lambda x: get_period(x, mode='end'))
    transaction_df['closing_periode'] = transaction_df.apply(lambda x: x['closing_period_start'].strftime('%Y/%m/%d').upper() + ' - ' + x['closing_period_end'].strftime('%Y/%m/%d').upper(), axis=1)

    transaction_df['listing_period_start'] = transaction_df['Listing PAID Date'].apply(lambda x: get_period(x, mode='start'))
    transaction_df['listing_period_end'] = transaction_df['Listing PAID Date'].apply(lambda x: get_period(x, mode='end'))
    transaction_df['listing_periode'] = transaction_df.apply(lambda x: x['listing_period_start'].strftime('%Y/%m/%d').upper() + ' - ' + x['listing_period_end'].strftime('%Y/%m/%d').upper(), axis=1)

    transaction_df['ctc_period_start'] = transaction_df['CTC PAID Date'].apply(lambda x: get_period(x, mode='start'))
    transaction_df['ctc_period_end'] = transaction_df['CTC PAID Date'].apply(lambda x: get_period(x, mode='end'))
    transaction_df['ctc_periode'] = transaction_df.apply(lambda x: x['ctc_period_start'].strftime('%Y/%m/%d').upper() + ' - ' + x['ctc_period_end'].strftime('%Y/%m/%d').upper(), axis=1)

    transaction_df['compliance_period_start'] = transaction_df['Compliance PAID Date'].apply(lambda x: get_period(x, mode='start'))
    transaction_df['compliance_period_end'] = transaction_df['Compliance PAID Date'].apply(lambda x: get_period(x, mode='end'))
    transaction_df['compliance_periode'] = transaction_df.apply(lambda x: x['compliance_period_start'].strftime('%Y/%m/%d').upper() + ' - ' + x['compliance_period_end'].strftime('%Y/%m/%d').upper(), axis=1)

    transaction_df['offer_prep_period_start'] = transaction_df['Offer Prep PAID Date'].apply(lambda x: get_period(x, mode='start'))
    transaction_df['offer_prep_period_end'] = transaction_df['Offer Prep PAID Date'].apply(lambda x: get_period(x, mode='end'))
    transaction_df['offer_prep_periode'] = transaction_df.apply(lambda x: x['offer_prep_period_start'].strftime('%Y/%m/%d').upper() + ' - ' + x['offer_prep_period_end'].strftime('%Y/%m/%d').upper(), axis=1)

    return transaction_df


def add_listing_paid_amount(transaction_df):
    global na_filler
    transaction_df.rename({'Listing PAID Amount': 'Listing PAID Amount_1'}, axis=1, inplace=True)
    transaction_df['Listing PAID Amount'] = transaction_df.apply(lambda x: x['Listing PAID Amount_1'] if ((x['Listing PAID Date'] != na_filler) & (x['Listing PAID Date'] >= x['listing_period_start']) & (x['Listing PAID Date'] <= x['listing_period_end'])) else 0, axis=1)
    transaction_df.drop(columns='Listing PAID Amount_1', inplace=True)

    return transaction_df


def add_ctc_paid_amount(transaction_df):
    global na_filler
    transaction_df.rename({'CTC PAID Amount': 'CTC PAID Amount_1'}, axis=1, inplace=True)
    transaction_df['CTC PAID Amount'] = transaction_df.apply(lambda x: x['CTC PAID Amount_1'] if ((x['CTC PAID Date'] != na_filler) & (x['CTC PAID Date'] >= x['ctc_period_start']) & (x['CTC PAID Date'] <= x['ctc_period_end'])) else 0, axis=1)
    transaction_df.drop(columns='CTC PAID Amount_1', inplace=True)

    return transaction_df


def add_compliance_paid_amount(transaction_df):
    global na_filler
    transaction_df.rename({'Compliance PAID Amount': 'Compliance PAID Amount_1'}, axis=1, inplace=True)
    transaction_df['Compliance PAID Amount'] = transaction_df.apply(lambda x: x['Compliance PAID Amount_1'] if ((x['Compliance PAID Date'] != na_filler) & (x['Compliance PAID Date'] >= x['compliance_period_start']) & (x['Compliance PAID Date'] <= x['compliance_period_end'])) else 0, axis=1)
    transaction_df.drop(columns='Compliance PAID Amount_1', inplace=True)

    return transaction_df


def add_offer_prep_paid_amount(transaction_df):
    global na_filler
    transaction_df.rename({'Offer Prep PAID Amount': 'Offer Prep PAID Amount_1'}, axis=1, inplace=True)
    transaction_df['Offer Prep PAID Amount'] = transaction_df.apply(lambda x: x['Offer Prep PAID Amount_1'] if ((x['Offer Prep PAID Date'] != na_filler) & (x['Offer Prep PAID Date'] >= x['offer_prep_period_start']) & (x['Offer Prep PAID Date'] <= x['offer_prep_period_end'])) else 0, axis=1)
    transaction_df.drop(columns='Offer Prep PAID Amount_1', inplace=True)

    return transaction_df


def add_projected_amount(transaction_df):
    transaction_df['Projected Amount'] = transaction_df.apply(lambda x: x['TC Commission Amount'] if ((x['Closing'] != na_filler) & (x['Closing'] >= x['closing_period_start']) & (x['Closing'] <= x['closing_period_end'])) else 0, axis=1)
    return transaction_df


def add_actual_amount(transaction_df):
    transaction_df['Actual Amount'] = transaction_df.apply(lambda x: x['TC Revenue'] * x['TC Commission Rate'] if ((x['CTC PAID Date'] != na_filler) & (x['CTC PAID Date'] >= x['ctc_period_start']) & (x['CTC PAID Date'] <= x['ctc_period_end'])) else 0, axis=1)
    return transaction_df


def add_tc_revenue_amount(transaction_df):
    transaction_df['TC Revenue Amount'] = transaction_df.apply(lambda x: x['TC Revenue'] if ((x['CTC PAID Date'] != na_filler) & (x['CTC PAID Date'] >= x['ctc_period_start']) & (x['CTC PAID Date'] <= x['ctc_period_end'])) else 0, axis=1)
    return transaction_df


def transform_transaction_source(transaction_df):
    print('Transforming transaction data source...', end='')

    global na_filler
    transaction_df['Closing'] = pd.to_datetime(transaction_df['Closing'])
    transaction_df['Closing'].fillna(na_filler, inplace=True)
    transaction_df['Listing PAID Date'] = pd.to_datetime(transaction_df['Listing PAID Date'])
    transaction_df['Listing PAID Date'].fillna(na_filler, inplace=True)
    transaction_df['CTC PAID Date'] = pd.to_datetime(transaction_df['CTC PAID Date'])
    transaction_df['CTC PAID Date'].fillna(na_filler, inplace=True)
    transaction_df['Offer Prep PAID Date'] = pd.to_datetime(transaction_df['Offer Prep PAID Date'])
    transaction_df['Offer Prep PAID Date'].fillna(na_filler, inplace=True)
    transaction_df['Compliance PAID Date'] = pd.to_datetime(transaction_df['Compliance PAID Date'])
    transaction_df['Compliance PAID Date'].fillna(na_filler, inplace=True)
    transaction_df['Listing Started with Empower'] = pd.to_datetime(transaction_df['Listing Started with Empower'])
    transaction_df['Listing Started with Empower'].fillna(na_filler, inplace=True)
    transaction_df['Offer Started with Empower'] = pd.to_datetime(transaction_df['Offer Started with Empower'])
    transaction_df['Offer Started with Empower'].fillna(na_filler, inplace=True)
    transaction_df['Compliance Started with Empower'] = pd.to_datetime(transaction_df['Compliance Started with Empower'])
    transaction_df['Compliance Started with Empower'].fillna(na_filler, inplace=True)

    # transaction_df = update_agent_provided_by(transaction_df, 'agent_sources.csv')

    # transaction_df = add_tc_commission_rate(transaction_df)
    transaction_df['TC Commission Rate'] = 0.5
    # transaction_df['Billing Amount'].astype('float')
    transaction_df['Billing Amount'] = pd.to_numeric(transaction_df['Billing Amount'], errors='coerce', downcast='float')
    transaction_df['TC Commission Amount'] = transaction_df['Billing Amount'] * transaction_df['TC Commission Rate']
    transaction_df = add_period(transaction_df)
    transaction_df = add_listing_paid_amount(transaction_df)
    transaction_df = add_ctc_paid_amount(transaction_df)
    transaction_df = add_compliance_paid_amount(transaction_df)
    transaction_df = add_offer_prep_paid_amount(transaction_df)
    transaction_df['CTC Projection'] = transaction_df['Closing'].apply(lambda x: 0 if x == na_filler else 1)
    transaction_df['Listing Projection'] = transaction_df.apply(lambda x: 1 if (x['Listing Started with Empower'] != na_filler and x['Listing Started with Empower'] >= x['listing_period_start'] and x['Listing Started with Empower'] <= x['listing_period_end']) else 0, axis=1)
    transaction_df['Offer Projection'] = transaction_df.apply(lambda x: 1 if (x['Offer Started with Empower'] != na_filler and x['Offer Started with Empower'] >= x['offer_prep_period_start'] and x['Offer Started with Empower'] <= x['offer_prep_period_end']) else 0, axis=1)
    transaction_df['Compliance Projection'] = transaction_df.apply(lambda x: 1 if (x['Compliance Started with Empower'] != na_filler and x['Compliance Started with Empower'] >= x['compliance_period_start'] and x['Compliance Started with Empower'] <= x['compliance_period_end']) else 0, axis=1)
    transaction_df['Projection Condition'] = transaction_df.apply(lambda x: 1 if (x['CTC Projection'] == 1 or x['Listing Projection'] == 1 or x['Offer Projection'] == 1 or x['Compliance Projection'] == 1) else 0, axis=1)
    transaction_df['Listing PAID Amount'] = pd.to_numeric(transaction_df['Listing PAID Amount'], errors='coerce', downcast='float')
    transaction_df['CTC PAID Amount'] = pd.to_numeric(transaction_df['CTC PAID Amount'], errors='coerce', downcast='float')
    transaction_df['Offer Prep PAID Amount'] = pd.to_numeric(transaction_df['Offer Prep PAID Amount'], errors='coerce', downcast='float')
    transaction_df['Compliance PAID Amount'] = pd.to_numeric(transaction_df['Compliance PAID Amount'], errors='coerce', downcast='float')
    transaction_df['TC Revenue'] = transaction_df[['Listing PAID Amount', 'CTC PAID Amount', 'Offer Prep PAID Amount', 'Compliance PAID Amount']].sum(axis=1)
    transaction_df = add_projected_amount(transaction_df)
    transaction_df = add_actual_amount(transaction_df)
    transaction_df = add_tc_revenue_amount(transaction_df)

    # transaction_df.to_excel('tc_payroll.xlsx', sheet_name='transaction_source', index=False)

    print('Done')
    return transaction_df


def generate_payroll_report(enriched_transaction_df, mode):
    print('Generating report...', end='')
    try:
        # enriched_transaction_df = pd.read_excel('tc_payroll.xlsx', sheet_name='transaction_source')
        # print(enriched_transaction_df)

        specific_teams = [
            "Christianna Velazquez",
            "Kimberly Lewis",
            "Stephanie Kleinman",
            "Molly Kelley",
            "Jenn McKinley"
        ]

        selected_team_transaction_df = enriched_transaction_df[enriched_transaction_df["Empower TC Name"].isin(specific_teams)]
        if mode == 'p':
            values_name = 'Projected Amount'
            columns_name = 'closing_periode'
        elif mode == 'a':
            values_name = 'Actual Amount'
            columns_name = 'ctc_periode'
        else:
            print("Mode is undefined!")
        summary_df = selected_team_transaction_df.pivot_table(values=values_name, index="Empower TC Name", columns=columns_name, aggfunc='sum', fill_value=0)

        summary_df['Total Result'] = summary_df.sum(axis=1)
        summary_df.loc['Total Result'] = summary_df.sum()
        summary_df.reset_index(inplace=True)

        print('Done')
        return summary_df

    except Exception as e:
        print(f"Error processing data: {e}")
        return None

    finally:
        pass


# def create_and_populate_google_sheet(service, spreadsheet_id, df, sheet_title):
#     """
#     Adds a new sheet to an existing Google Spreadsheet and populates it with data.

#     :param service: Google Sheets API service object.
#     :param spreadsheet_id: ID of the Google Spreadsheet.
#     :param df: Pandas DataFrame containing the data.
#     :param sheet_title: Title for the new sheet.
#     """
#     try:
#         df = df.fillna("")

#         df = df.copy()  # Make a copy to avoid modifying the original DataFrame
#         for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
#             df[col] = df[col].astype(str)

#         # Add new sheet to the spreadsheet
#         requests = [{
#             "addSheet": {
#                 "properties": {
#                     "title": sheet_title
#                 }
#             }
#         }]
#         service.spreadsheets().batchUpdate(
#             spreadsheetId=spreadsheet_id,
#             body={"requests": requests}
#         ).execute()

#         # Convert DataFrame to list of lists for Google Sheets API
#         values = [df.columns.tolist()] + df.values.tolist()

#         # Update the new sheet with data
#         service.spreadsheets().values().update(
#             spreadsheetId=spreadsheet_id,
#             range=f"{sheet_title}!A1",
#             valueInputOption="RAW",
#             body={"values": values},
#         ).execute()

#         print(f"Sheet '{sheet_title}' added and populated successfully.")
#     except Exception as e:
#         print(f"Error populating Google Sheet: {e}")


# def create_google_sheet(dataframes, sheet_titles, spreadsheet_name):
#     """
#     Creates a Google Spreadsheet with multiple sheets, each populated with a different DataFrame.

#     :param dataframes: List of DataFrames.
#     :param sheet_titles: List of sheet titles corresponding to each DataFrame.
#     :param spreadsheet_name: Name for the new Google Spreadsheet.
#     :return: ID of the created Google Spreadsheet.
#     """
#     start_time = time.time()
#     if not dataframes or len(dataframes) != len(sheet_titles):
#         print("DataFrames and sheet titles must be provided and match in length.")
#         return None

#     # Set up Google Sheets API
#     scope = [
#         "https://www.googleapis.com/auth/spreadsheets",
#         "https://www.googleapis.com/auth/drive.file",
#     ]
#     creds = Credentials.from_service_account_file('../credentials.json', scopes=scope)
#     service = build("sheets", "v4", credentials=creds)

#     try:
#         # Create a new spreadsheet
#         spreadsheet = service.spreadsheets().create(
#             body={"properties": {"title": spreadsheet_name}}
#         ).execute()
#         spreadsheet_id = spreadsheet["spreadsheetId"]

#         # Add each DataFrame to a separate sheet
#         for df, title in zip(dataframes, sheet_titles):
#             create_and_populate_google_sheet(service, spreadsheet_id, df, title)

#         # Set permissions to make the spreadsheet accessible
#         drive_service = build('drive', 'v3', credentials=creds)
#         permission_body = {
#             'type': 'anyone',   # Makes it accessible to anyone
#             'role': 'writer'    # Sets the permission to read-only
#         }
#         drive_service.permissions().create(
#             fileId=spreadsheet_id,
#             body=permission_body
#         ).execute()

#         end_time = time.time()
#         print(f"create_google_sheet() took {end_time - start_time:.2f} seconds")
#         print(f"Google Spreadsheet created with ID: {spreadsheet_id}")
#         return spreadsheet_id

#     except Exception as e:
#         print(f"Error creating Google Spreadsheet: {e}")
#         return None


if __name__ == "__main__":
    script_start_time = time.time()

    transaction_df = extract_transaction_source('datas/all_properties_*.parquet')

    enriched_transaction_df = transform_transaction_source(transaction_df)
    projected_payroll_report_df = generate_payroll_report(enriched_transaction_df, mode='p')
    actual_payroll_report_df = generate_payroll_report(enriched_transaction_df, mode='a')

    # sheet_id = create_google_sheet(all_summaries)
    dataframes = [enriched_transaction_df, projected_payroll_report_df, actual_payroll_report_df]
    sheet_titles = ["transaction_data", "projected", "actual"]
    spreadsheet_name = "tc_payroll"
    spreadsheet_id = '1KSmBLfhdCtir3FPad1afDRK4Zrfa1VXEbeHx9Ry36vU'

    create_google_sheet(dataframes, sheet_titles, spreadsheet_name, spreadsheet_id)

    script_end_time = time.time()
    total_execution_time = script_end_time - script_start_time
    print(f"Total script execution time: {total_execution_time:.2f} seconds")