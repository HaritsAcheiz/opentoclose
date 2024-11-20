import duckdb
import csv
import pandas as pd
import json
import logging
from datetime import datetime
import time
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

na_filler = datetime(1990, 1, 1, 0, 0, 0)


def extract_field_values(field_values, label):
    try:
        values = json.loads(field_values)
        for item in values:
            if isinstance(item, dict) and item.get("label") == label:
                return item.get("value")
    except json.JSONDecodeError:
        pass
    return None


def generate_source(properties_file_path):
    logger.info('Reading Properties Data Source...')
    conn = duckdb.connect(database=":memory:")
    try:
        query = f"SELECT * FROM '{properties_file_path}'"
        df = conn.execute(query).fetchdf()

        # Main Data Source
        logger.info('Generating Main Data Source...')
        main_source_schema = list()
        with open('Columns_Transaction_Source.csv') as file:
            rows = csv.reader(file)
            for row in rows:
                main_source_schema.append(row[0])

        main_source_df = pd.DataFrame()
        for field in main_source_schema:
            main_source_df[field] = df['field_values'].apply(lambda x: extract_field_values(x, field))

        main_source_df.to_csv('main_source.csv', index=False)

        return main_source_df

    except Exception as e:
        print(f"Error processing data: {e}")
        return None

    finally:
        conn.close()


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
        if mode == 'start':
            result = datetime(year, month, 1)
        elif mode == 'end':
            if month == 12:
                result = datetime(year, 12, 31, 23, 59, 59)
            else:
                next_month = datetime(year, month + 1, 1, 23, 59, 59)
                result = next_month - pd.DateOffset(days=1)
        elif mode == 'end2':
            result = datetime(year, month, 1) + pd.DateOffset(months=2) - pd.DateOffset(days=1)

    return result


def add_period(df):
    df['CTC Started with Empower Periode Start'] = df['CTC Started with Empower'].apply(lambda x: get_period(x, mode='start'))
    df['CTC Started with Empower Periode End'] = df['CTC Started with Empower'].apply(lambda x: get_period(x, mode='end'))
    df['CTC Started with Empower Periode'] = df.apply(lambda x: x['CTC Started with Empower Periode Start'].strftime('%B %Y').upper(), axis=1)
    df['CTC Started with Empower Periode M1 End'] = df['CTC Started with Empower'].apply(lambda x: get_period(x, mode='end2'))

    df['Closing Periode Start'] = df['Closing'].apply(lambda x: get_period(x, mode='start'))
    df['Closing Periode End'] = df['Closing'].apply(lambda x: get_period(x, mode='end'))
    df['Closing Periode'] = df.apply(lambda x: x['Closing Periode Start'].strftime('%B %Y').upper(), axis=1)
    df['Closing Periode M1 End'] = df['Closing'].apply(lambda x: get_period(x, mode='end2'))
    df['Closing Periode M1'] = df.apply(lambda x: x['Closing Periode M1 End'].strftime('%B %Y').upper(), axis=1)

    df['Listing Started Periode Start'] = df['Listing Started with Empower'].apply(lambda x: get_period(x, mode='start'))
    df['Listing Started Periode End'] = df['Listing Started with Empower'].apply(lambda x: get_period(x, mode='end'))
    df['Listing Started Periode'] = df.apply(lambda x: x['Listing Started Periode Start'].strftime('%B %Y').upper(), axis=1)

    df['Listing Paid Periode Start'] = df['Listing PAID Date'].apply(lambda x: get_period(x, mode='start'))
    df['Listing Paid Periode End'] = df['Listing PAID Date'].apply(lambda x: get_period(x, mode='end'))
    df['Listing Paid Periode'] = df.apply(lambda x: x['Listing Paid Periode Start'].strftime('%B %Y').upper(), axis=1)

    df['Compliance Started Periode Start'] = df['Compliance Started with Empower'].apply(lambda x: get_period(x, mode='start'))
    df['Compliance Started Periode End'] = df['Compliance Started with Empower'].apply(lambda x: get_period(x, mode='end'))
    df['Compliance Started Periode'] = df.apply(lambda x: x['Compliance Started Periode Start'].strftime('%B %Y').upper(), axis=1)

    df['Offer Started Periode Start'] = df['Offer Started with Empower'].apply(lambda x: get_period(x, mode='start'))
    df['Offer Started Periode End'] = df['Offer Started with Empower'].apply(lambda x: get_period(x, mode='end'))
    df['Offer Started Periode'] = df.apply(lambda x: x['Offer Started Periode Start'].strftime('%B %Y').upper(), axis=1)

    df['Onboard Periode Start'] = df['Onboard Call Complete Date'].apply(lambda x: get_period(x, mode='start'))
    df['Onboard Periode End'] = df['Onboard Call Complete Date'].apply(lambda x: get_period(x, mode='end'))
    df['Onboard Periode'] = df.apply(lambda x: x['Onboard Periode Start'].strftime('%B %Y').upper(), axis=1)

    df['First Transaction Periode Start'] = df['1st Transaction Date'].apply(lambda x: get_period(x, mode='start'))
    df['First Transaction Periode End'] = df['1st Transaction Date'].apply(lambda x: get_period(x, mode='end'))
    df['First Transaction Periode'] = df.apply(lambda x: x['First Transaction Periode Start'].strftime('%B %Y').upper(), axis=1)

    return df


def expand_periode_dim(periode):
    try:
        # Convert string period to datetime
        start_periode = pd.to_datetime(periode, format='%B %Y')

        # Current month range
        end_periode = (start_periode + pd.DateOffset(months=1) - pd.Timedelta(seconds=1))

        # Next month range
        start_periode_m1 = start_periode + pd.DateOffset(months=1)
        end_periode_m1 = (start_periode + pd.DateOffset(months=2) - pd.Timedelta(seconds=1))

        return {
            'periode': periode,
            'start_periode': start_periode,
            'end_periode': end_periode,
            'start_periode_m1': start_periode_m1,
            'end_periode_m1': end_periode_m1,
            'current_periode': end_periode if datetime.now() < end_periode else datetime.now()
        }

    except ValueError:
        raise ValueError(f"Invalid period format. Expected 'Month YYYY' (e.g., 'January 2024'), got: {periode}")


def transform_main_source(df, periode):
    print('Transforming main data source...', end='')
    df = df[
        [
            'Empower TC Name', 'CTC Started with Empower', 'Closing', 'Contract Status',
            'Listing Started with Empower', 'Listing PAID Date', 'Live on MLS Date',
            'Compliance Started with Empower', 'Offer Started with Empower',
            'Onboard Call Complete Date', 'Onboarding Status', '1st Transaction Date',
            'Transaction Coordinator', 'Agent Provided by', 'Other Status'
        ]
    ]

    ctc_contract_status = (
        'CTC - Closed - PAID', 'CTC - Pending', 'CTC - PAID',
        'CTC - Terminated - No Charge', 'CTC - Withdrawn',
        'CTC - Terminated - Compliance - PAID'
    )

    preferred_contract_status = (
        'CTC - Preferred - Closed - PAID', 'CTC - Preferred - Pending',
        'CTC - Preferred - PAID', 'CTC - Preferred - Terminated - No Change',
        'CTC - Preferred - Withdrawn'
    )

    lost_agents_return_to_sales = (
        'Lost  Reassigned', 'Lost  Left Empower', 'Return to Sales',
        'Lost - Do Not Contact', 'Lost  Do Not Contact', 'Lost - Left Empower',
        'Lost  Left Empower', 'Lost - Reassigned'
    )

    global na_filler

    df['CTC Started with Empower'] = pd.to_datetime(df['CTC Started with Empower'])
    df['CTC Started with Empower'].fillna(na_filler, inplace=True)
    df['Closing'] = pd.to_datetime(df['Closing'])
    df['Closing'].fillna(na_filler, inplace=True)
    df['Listing Started with Empower'] = pd.to_datetime(df['Listing Started with Empower'])
    df['Listing Started with Empower'].fillna(na_filler, inplace=True)
    df['Listing PAID Date'] = pd.to_datetime(df['Listing PAID Date'])
    df['Listing PAID Date'].fillna(na_filler, inplace=True)
    df['Compliance Started with Empower'] = pd.to_datetime(df['Compliance Started with Empower'])
    df['Compliance Started with Empower'].fillna(na_filler, inplace=True)
    df['Offer Started with Empower'] = pd.to_datetime(df['Offer Started with Empower'])
    df['Offer Started with Empower'].fillna(na_filler, inplace=True)
    df['Onboard Call Complete Date'] = pd.to_datetime(df['Onboard Call Complete Date'])
    df['Onboard Call Complete Date'].fillna(na_filler, inplace=True)
    df['1st Transaction Date'] = pd.to_datetime(df['1st Transaction Date'])
    df['1st Transaction Date'].fillna(na_filler, inplace=True)

    df = add_period(df)
    periode_dim = expand_periode_dim(periode)

    df['Total ACTIVE Files - CTC'] = df.apply(lambda x: 1 if x['Closing'] >= periode_dim['current_periode'] and x['Contract Status'] in (ctc_contract_status) else 0, axis=1)
    df['Total ACTIVE Files - CTC Preferred'] = df.apply(lambda x: 1 if x['Closing'] >= periode_dim['current_periode'] and x['Contract Status'] in (preferred_contract_status) else 0, axis=1)
    df['CTC Started for this month'] = df.apply(lambda x: 1 if x['CTC Started with Empower'] >= periode_dim['start_periode'] and x['CTC Started with Empower'] <= periode_dim['end_periode'] and x['Contract Status'] in (ctc_contract_status) else 0, axis=1)
    df['CTC - Preferred Started'] = df.apply(lambda x: 1 if x['CTC Started with Empower'] >= periode_dim['start_periode'] and x['CTC Started with Empower'] <= periode_dim['end_periode'] and x['Contract Status'] in (preferred_contract_status) else 0, axis=1)
    df['Closings for this month'] = df.apply(lambda x: 1 if x['Closing'] >= periode_dim['start_periode'] and x['Closing'] <= periode_dim['end_periode'] and x['Contract Status'] in (ctc_contract_status) else 0, axis=1)
    df['CTC - Preferred Closings'] = df.apply(lambda x: 1 if x['Closing'] >= periode_dim['start_periode'] and x['Closing'] <= periode_dim['end_periode'] and x['Contract Status'] in (preferred_contract_status)else 0, axis=1)
    df['CTC Pending for this month'] = df.apply(lambda x: 1 if x['Closing'] >= periode_dim['start_periode'] and x['Closing'] <= periode_dim['end_periode'] and x['Contract Status'] == 'CTC - Pending' else 0, axis=1)
    df['CTC - Preferred Pending for this month'] = df.apply(lambda x: 1 if x['Closing'] >= periode_dim['start_periode'] and x['Closing'] <= periode_dim['end_periode'] and x['Contract Status'] == 'CTC - Preferred - Pending' else 0, axis=1)
    df['CTC Pending for next month'] = df.apply(lambda x: 1 if x['Closing'] > periode_dim['start_periode_m1'] and x['Closing'] <= periode_dim['end_periode_m1'] and x['Contract Status'] == 'CTC - Pending' else 0, axis=1)
    df['CTC - Preferred Pending for next month'] = df.apply(lambda x: 1 if x['Closing'] > periode_dim['start_periode_m1'] and x['Closing'] <= periode_dim['end_periode_m1'] and x['Contract Status'] == 'CTC - Preferred - Pending' else 0, axis=1)
    df['CTC Pending for other months'] = df.apply(lambda x: 1 if x['Closing'] > periode_dim['end_periode_m1'] and x['Contract Status'] == 'CTC - Pending' else 0, axis=1)
    df['CTC - Preferred Pending for other months'] = df.apply(lambda x: 1 if x['Closing'] > periode_dim['end_periode_m1'] and x['Contract Status'] == 'CTC - Preferred - Pending' else 0, axis=1)
    df['CTC - Preferred Pending for other months'] = df.apply(lambda x: 1 if x['Closing'] > periode_dim['end_periode_m1'] and x['Contract Status'] == 'CTC - Preferred - Pending' else 0, axis=1)
    df['Listing Started'] = df.apply(lambda x: 1 if x['Listing Started with Empower'] >= periode_dim['start_periode'] and x['Listing Started with Empower'] <= periode_dim['end_periode'] else 0, axis=1)
    df['Listing PAID'] = df.apply(lambda x: 1 if x['Listing PAID Date'] >= periode_dim['start_periode'] and x['Listing PAID Date'] <= periode_dim['end_periode'] else 0, axis=1)
    df['Offers Started this month'] = df.apply(lambda x: 1 if x['Offer Started with Empower'] >= periode_dim['start_periode'] and x['Offer Started with Empower'] <= periode_dim['end_periode'] else 0, axis=1)
    df['Compliance Started this month'] = df.apply(lambda x: 1 if x['Compliance Started with Empower'] >= periode_dim['start_periode'] and x['Compliance Started with Empower'] <= periode_dim['end_periode'] else 0, axis=1)
    df['TC Generated Agents'] = df.apply(lambda x: 1 if x['Onboard Call Complete Date'] <= periode_dim['start_periode'] and x['Agent Provided by'] == 'TC' else 0, axis=1)
    df['SALES Generated Agents'] = df.apply(lambda x: 1 if x['Onboard Call Complete Date'] <= periode_dim['start_periode'] and x['Agent Provided by'] == 'Empower' else 0, axis=1)
    df['TOTAL Agents'] = df.apply(lambda x: 1 if x['Onboard Call Complete Date'] <= periode_dim['start_periode'] and x['Agent Provided by'] in ('Empower', 'TC') else 0, axis=1)
    df['1st Transaction Agents'] = df.apply(lambda x: 1 if x['Onboard Call Complete Date'] <= periode_dim['start_periode'] and x['Onboarding Status'] == '1st Transaction' else 0, axis=1)
    df['Lost Agents/return to Sales'] = df.apply(lambda x: 1 if x['Onboard Call Complete Date'] <= periode_dim['start_periode'] and x['Other Status'] in (lost_agents_return_to_sales) else 0, axis=1)
    df['OB for This Month'] = df.apply(lambda x: 1 if x['Onboard Call Complete Date'] >= periode_dim['start_periode'] and x['Onboard Call Complete Date'] <= periode_dim['end_periode'] else 0, axis=1)
    df['1st Transactions for This Month'] = df.apply(lambda x: 1 if x['1st Transaction Date'] >= periode_dim['start_periode'] and x['1st Transaction Date'] <= periode_dim['end_periode'] else 0, axis=1)

    print('Done')
    return df


def create_and_populate_google_sheet(service, spreadsheet_id, df, sheet_title):
    """
    Adds a new sheet or updates existing sheet in Google Spreadsheet and populates it with data.
    :param service: Google Sheets API service object.
    :param spreadsheet_id: ID of the Google Spreadsheet.
    :param df: Pandas DataFrame containing the data.
    :param sheet_title: Title for the sheet.
    """
    try:
        # Prepare DataFrame
        df = df.fillna("")
        df = df.copy()
        for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
            df[col] = df[col].astype(str)

        # Get all sheets in the spreadsheet
        sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = sheet_metadata.get('sheets', '')
        sheet_exists = False
        sheet_id = None

        # Check if sheet already exists
        for sheet in sheets:
            if sheet['properties']['title'] == sheet_title:
                sheet_exists = True
                sheet_id = sheet['properties']['sheetId']
                break

        if not sheet_exists:
            # Create new sheet if it doesn't exist
            requests = [{
                "addSheet": {
                    "properties": {
                        "title": sheet_title
                    }
                }
            }]
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": requests}
            ).execute()
            print(f"Sheet '{sheet_title}' created.")
        else:
            # Clear existing sheet if it exists
            range_name = f"{sheet_title}!A1:ZZ"
            service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=range_name
            ).execute()
            print(f"Existing sheet '{sheet_title}' cleared.")

        # Convert DataFrame to list of lists for Google Sheets API
        values = [df.columns.tolist()] + df.values.tolist()

        # Update the sheet with data
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_title}!A1",
            valueInputOption="RAW",
            body={"values": values},
        ).execute()

        print(f"Sheet '{sheet_title}' populated successfully.")

    except Exception as e:
        print(f"Error working with Google Sheet: {e}")


def create_google_sheet(dataframes, sheet_titles, spreadsheet_name):
    """
    Creates a Google Spreadsheet with multiple sheets, each populated with a different DataFrame.

    :param dataframes: List of DataFrames.
    :param sheet_titles: List of sheet titles corresponding to each DataFrame.
    :param spreadsheet_name: Name for the new Google Spreadsheet.
    :return: ID of the created Google Spreadsheet.
    """
    if not dataframes or len(dataframes) != len(sheet_titles):
        print("DataFrames and sheet titles must be provided and match in length.")
        return None

    # Set up Google Sheets API
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
    ]
    creds = Credentials.from_service_account_file('../credentials.json', scopes=scope)
    service = build("sheets", "v4", credentials=creds)

    try:
        # Create a new spreadsheet
        # spreadsheet = service.spreadsheets().create(
        #     body={"properties": {"title": spreadsheet_name}}
        # ).execute()
        # spreadsheet_id = spreadsheet["spreadsheetId"]
        spreadsheet_id = '1weGRTk5Tzg1VDVDYVuYHpQru-_-oG7l-FpLyZeq3bsU'

        # Add each DataFrame to a separate sheet
        for df, title in zip(dataframes, sheet_titles):
            create_and_populate_google_sheet(service, spreadsheet_id, df, title)

        # Set permissions to make the spreadsheet accessible
        drive_service = build('drive', 'v3', credentials=creds)
        permission_body = {
            'type': 'anyone',   # Makes it accessible to anyone
            'role': 'writer'    # Sets the permission to read-only
        }
        drive_service.permissions().create(
            fileId=spreadsheet_id,
            body=permission_body
        ).execute()

        print(f"Google Spreadsheet created with ID: {spreadsheet_id}")
        return spreadsheet_id

    except Exception as e:
        print(f"Error creating Google Spreadsheet: {e}")
        return None


def generate_daily_update_report(df):
    print('Generating report...', end='')
    # try:
    specific_team = df['Empower TC Name'].unique().tolist()

    dfs = list()
    sheet_titles = list()
    df_report_template = pd.DataFrame(
        index=specific_team
    )

    global na_filler

    df['CTC Started with Empower'] = pd.to_datetime(df['CTC Started with Empower'])
    df['CTC Started with Empower'].fillna(na_filler, inplace=True)
    df['CTC Started with Empower Periode'] = df['CTC Started with Empower'].apply(lambda x: x.strftime('%B %Y').upper())

    all_periodes = df['CTC Started with Empower Periode'].unique().tolist()
    periodes = [x for x in all_periodes if "2024" in x]

    for periode in periodes:
        enriched_df = transform_main_source(df, periode)
        dateformat_periode = datetime.strptime(periode, '%B %Y')
        report_df = df_report_template.copy()
        selected_team_df = enriched_df[enriched_df["Empower TC Name"].isin(specific_team)].copy()

        started_df = selected_team_df[selected_team_df["CTC Started with Empower Periode"] == periode].copy()
        summary_started_df = started_df.pivot_table(
            values=[
                'CTC Started for this month',
                'CTC - Preferred Started'],
            index='Empower TC Name',
            aggfunc='sum',
            fill_value=0
        )

        closing_df = selected_team_df[selected_team_df["Closing Periode"] == periode].copy()
        summary_closing_df = closing_df.pivot_table(
            values=[
                'Total ACTIVE Files - CTC',
                'Total ACTIVE Files - CTC Preferred',
                'Closings for this month',
                'CTC - Preferred Closings',
                'CTC Pending for this month',
                'CTC - Preferred Pending for this month',
                'CTC Pending for next month',
                'CTC - Preferred Pending for next month',
                'CTC Pending for other months',
                'CTC - Preferred Pending for other months'
            ],
            index='Empower TC Name',
            aggfunc='sum',
            fill_value=0
        )

        listing_started_df = selected_team_df[selected_team_df["Listing Started Periode"] == periode].copy()
        if listing_started_df.empty:
            summary_listing_started_df = pd.DataFrame(
                index=pd.Index(specific_team, name='Empower TC Name'),
                data={'Listing Started': [0] * len(specific_team)}
            )
        else:
            summary_listing_started_df = listing_started_df.pivot_table(
                values='Listing Started',
                index='Empower TC Name',
                aggfunc='sum',
                fill_value=0
            )

        listing_paid_df = selected_team_df[selected_team_df["Listing Paid Periode"] == periode].copy()
        if listing_paid_df.empty:
            summary_listing_paid_df = pd.DataFrame(
                index=pd.Index(specific_team, name='Empower TC Name'),
                data={'Listing PAID': [0] * len(specific_team)}
            )
        else:
            summary_listing_paid_df = listing_paid_df.pivot_table(
                values='Listing PAID',
                index='Empower TC Name',
                aggfunc='sum',
                fill_value=0
            )

        offer_started_df = selected_team_df[selected_team_df["Offer Started Periode"] == periode].copy()
        if offer_started_df.empty:
            summary_offer_started_df = pd.DataFrame(
                index=pd.Index(specific_team, name='Empower TC Name'),
                data={'Offers Started this month': [0] * len(specific_team)}
            )
        else:
            summary_offer_started_df = offer_started_df.pivot_table(
                values='Offers Started this month',
                index='Empower TC Name',
                aggfunc='sum',
                fill_value=0
            )

        compliance_started_df = selected_team_df[selected_team_df["Compliance Started Periode"] == periode].copy()
        if compliance_started_df.empty:
            summary_compliance_started_df = pd.DataFrame(
                index=pd.Index(specific_team, name='Empower TC Name'),
                data={'Compliance Started this month': [0] * len(specific_team)}
            )
        else:
            summary_compliance_started_df = compliance_started_df.pivot_table(
                values='Compliance Started this month',
                index='Empower TC Name',
                aggfunc='sum',
                fill_value=0
            )

        onboard_call_complete_date_df = selected_team_df[selected_team_df["Onboard Periode Start"] <= dateformat_periode].copy()
        if onboard_call_complete_date_df.empty:
            summary_onboard_call_complete_date_df = pd.DataFrame(
                index=pd.Index(specific_team, name='Transaction Coordinator'),
                data={
                    'TC Generated Agents': [0] * len(specific_team),
                    'SALES Generated Agents': [0] * len(specific_team),
                    'TOTAL Agents': [0] * len(specific_team),
                    '1st Transaction Agents': [0] * len(specific_team),
                    'Lost Agents/return to Sales': [0] * len(specific_team)
                }
            )
        else:
            summary_onboard_call_complete_date_df = onboard_call_complete_date_df.pivot_table(
                values=[
                    'TC Generated Agents', 'SALES Generated Agents', 'TOTAL Agents',
                    '1st Transaction Agents', 'Lost Agents/return to Sales'
                ],
                index='Transaction Coordinator',
                aggfunc='sum',
                fill_value=0
            )

        ob_date_df = selected_team_df[selected_team_df["Onboard Periode"] == periode].copy()
        if ob_date_df.empty:
            summary_ob_date_df = pd.DataFrame(
                index=pd.Index(specific_team, name='Transaction Coordinator'),
                data={
                    'OB for This Month': [0] * len(specific_team)
                }
            )
        else:
            summary_ob_date_df = ob_date_df.pivot_table(
                values='OB for This Month',
                index='Transaction Coordinator',
                aggfunc='sum',
                fill_value=0
            )

        first_transaction_date_df = selected_team_df[selected_team_df["Onboard Periode"] == periode].copy()
        if first_transaction_date_df.empty:
            summary_first_transaction_date_df = pd.DataFrame(
                index=pd.Index(specific_team, name='Transaction Coordinator'),
                data={
                    '1st Transactions for This Month': [0] * len(specific_team)
                }
            )
        else:
            summary_first_transaction_date_df = ob_date_df.pivot_table(
                values='1st Transactions for This Month',
                index='Transaction Coordinator',
                aggfunc='sum',
                fill_value=0
            )

        report_df.reset_index(inplace=True, names='Empower TC Name')
        report_df = report_df.merge(summary_started_df, how='left', on='Empower TC Name')
        report_df = report_df.merge(summary_closing_df, how='left', on='Empower TC Name')
        report_df = report_df.merge(summary_listing_started_df, how='left', on='Empower TC Name')
        report_df = report_df.merge(summary_listing_paid_df, how='left', on='Empower TC Name')
        report_df = report_df.merge(summary_offer_started_df, how='left', on='Empower TC Name')
        report_df = report_df.merge(summary_compliance_started_df, how='left', on='Empower TC Name')
        report_df = report_df.merge(summary_onboard_call_complete_date_df, how='left', left_on='Empower TC Name', right_on='Transaction Coordinator')
        report_df = report_df.merge(summary_ob_date_df, how='left', left_on='Empower TC Name', right_on='Transaction Coordinator')
        report_df = report_df.merge(summary_first_transaction_date_df, how='left', left_on='Empower TC Name', right_on='Transaction Coordinator')

        report_df.fillna(0, inplace=True)
        report_df = report_df.astype('int64', errors='ignore')

        report_df = report_df[
            [
                'Empower TC Name',
                'Total ACTIVE Files - CTC',
                'Total ACTIVE Files - CTC Preferred',
                'CTC Started for this month', 'CTC - Preferred Started',
                'Closings for this month', 'CTC - Preferred Closings',
                'CTC Pending for this month', 'CTC - Preferred Pending for this month',
                'CTC Pending for next month', 'CTC - Preferred Pending for next month',
                'CTC Pending for other months', 'CTC - Preferred Pending for other months',
                'Offers Started this month', 'Compliance Started this month',
                'Listing Started', 'Listing PAID', 'TC Generated Agents',
                'SALES Generated Agents', 'TOTAL Agents', '1st Transaction Agents',
                'Lost Agents/return to Sales', 'OB for This Month', '1st Transactions for This Month'
            ]
        ]

        dfs.append(report_df.copy())
        sheet_titles.append(periode)

    spreadsheet_name = "tc_daily_update"

    print('Done')
    print('len of dfs = {}, len of sheet title = {}'.format(len(dfs), len(sheet_titles)))
    create_google_sheet(dfs, sheet_titles, spreadsheet_name)

    # except Exception as e:
    #     print(f"Error processing data: {e}")
    #     print(listing_paid_df)
    #     return None

    # finally:
    #     pass


if __name__ == '__main__':
    # df = generate_source('../all_properties.parquet')
    script_start_time = time.time()

    df = pd.read_csv('main_source.csv')
    generate_daily_update_report(df)

    script_end_time = time.time()
    total_execution_time = script_end_time - script_start_time
    print(f"Total script execution time: {total_execution_time:.2f} seconds")
