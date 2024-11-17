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

    df['Listing Started Periode Start'] = df['CTC Started with Empower'].apply(lambda x: get_period(x, mode='start'))
    df['Listing Started Periode End'] = df['CTC Started with Empower'].apply(lambda x: get_period(x, mode='end'))
    df['Listing Started Periode'] = df.apply(lambda x: x['CTC Started with Empower Periode Start'].strftime('%B %Y').upper(), axis=1)

    # df['listing_period_start'] = df['listing_paid_date'].apply(lambda x: get_period(x, mode='start'))
    # df['listing_period_end'] = df['listing_paid_date'].apply(lambda x: get_period(x, mode='end'))
    # df['listing_periode'] = df.apply(lambda x: x['listing_period_start'].strftime('%Y/%m/%d').upper() + ' - ' + x['listing_period_end'].strftime('%Y/%m/%d').upper(), axis=1)

    # df['ctc_period_start'] = df['ctc_paid_date'].apply(lambda x: get_period(x, mode='start'))
    # df['ctc_period_end'] = df['ctc_paid_date'].apply(lambda x: get_period(x, mode='end'))
    # df['ctc_periode'] = df.apply(lambda x: x['ctc_period_start'].strftime('%Y/%m/%d').upper() + ' - ' + x['ctc_period_end'].strftime('%Y/%m/%d').upper(), axis=1)

    # df['compliance_period_start'] = df['compliance_paid_date'].apply(lambda x: get_period(x, mode='start'))
    # df['compliance_period_end'] = df['compliance_paid_date'].apply(lambda x: get_period(x, mode='end'))
    # df['compliance_periode'] = df.apply(lambda x: x['compliance_period_start'].strftime('%Y/%m/%d').upper() + ' - ' + x['compliance_period_end'].strftime('%Y/%m/%d').upper(), axis=1)

    # df['offer_prep_period_start'] = df['offer_prep_paid_date'].apply(lambda x: get_period(x, mode='start'))
    # df['offer_prep_period_end'] = df['offer_prep_paid_date'].apply(lambda x: get_period(x, mode='end'))
    # df['offer_prep_periode'] = df.apply(lambda x: x['offer_prep_period_start'].strftime('%Y/%m/%d').upper() + ' - ' + x['offer_prep_period_end'].strftime('%Y/%m/%d').upper(), axis=1)

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
            'end_periode_m1': end_periode_m1
        }
    except ValueError as e:
        raise ValueError(f"Invalid period format. Expected 'Month YYYY' (e.g., 'January 2024'), got: {periode}")


def transform_main_source(df, periode):
    print('Transforming main data source...', end='')
    df = df[['Empower TC Name', 'CTC Started with Empower', 'Closing', 'Contract Status']]

    global na_filler
    df['CTC Started with Empower'] = pd.to_datetime(df['CTC Started with Empower'])
    df['CTC Started with Empower'].fillna(na_filler, inplace=True)
    df['Closing'] = pd.to_datetime(df['Closing'])
    df['Closing'].fillna(na_filler, inplace=True)

    # df['1st Transaction Date'] = pd.to_datetime(df['1st Transaction Date'])
    # df['1st Transaction Date'].fillna(na_filler, inplace=True)
    # df['CC - Terms of Use Date'] = pd.to_datetime(df['CC - Terms of Use Date'])
    # df['CC - Terms of Use Date'].fillna(na_filler, inplace=True)
    # df['Reassigned Date'] = pd.to_datetime(df['Reassigned Date'])
    # df['Reassigned Date'].fillna(na_filler, inplace=True)

    # df['listing_paid_date'] = pd.to_datetime(df['listing_paid_date'])
    # df['listing_paid_date'].fillna(na_filler, inplace=True)
    # df['ctc_paid_date'] = pd.to_datetime(df['ctc_paid_date'])
    # df['ctc_paid_date'].fillna(na_filler, inplace=True)
    # df['offer_prep_paid_date'] = pd.to_datetime(df['offer_prep_paid_date'])
    # df['offer_prep_paid_date'].fillna(na_filler, inplace=True)
    # df['compliance_paid_date'] = pd.to_datetime(df['compliance_paid_date'])
    # df['compliance_paid_date'].fillna(na_filler, inplace=True)
    # df['listing_started_with_empower'] = pd.to_datetime(df['listing_started_with_empower'])
    # df['listing_started_with_empower'].fillna(na_filler, inplace=True)
    # df['offer_started_with_empower'] = pd.to_datetime(df['offer_started_with_empower'])
    # df['offer_started_with_empower'].fillna(na_filler, inplace=True)
    # df['compliance_started_with_empower'] = pd.to_datetime(df['compliance_started_with_empower'])
    # df['compliance_started_with_empower'].fillna(na_filler, inplace=True)

    df = add_period(df)
    periode_dim = expand_periode_dim(periode)

    df['CTC Started for the month'] = df.apply(lambda x: 1 if x['CTC Started with Empower'] >= periode_dim['start_periode'] and x['CTC Started with Empower'] <= periode_dim['end_periode'] else 0, axis=1)
    df['Closings for the month'] = df.apply(lambda x: 1 if x['Closing'] >= periode_dim['start_periode'] and x['Closing'] <= periode_dim['end_periode'] else 0, axis=1)
    df['Pending for the month'] = df.apply(lambda x: 1 if x['Closing'] >= periode_dim['start_periode'] and x['Closing'] <= periode_dim['end_periode'] and x['Contract Status'] == 'CTC - Pending' else 0, axis=1)
    df['Pending for next month'] = df.apply(lambda x: 1 if x['Closing'] > periode_dim['start_periode_m1'] and x['Closing'] <= periode_dim['end_periode_m1'] and x['Contract Status'] == 'CTC - Pending' else 0, axis=1)
    df['Pending for other months'] = df.apply(lambda x: 1 if x['Closing'] > periode_dim['end_periode_m1'] and x['Contract Status'] == 'CTC - Pending' else 0, axis=1)

    # df['listing_projection'] = df.apply(lambda x: 1 if (x['listing_started_with_empower'] != na_filler and x['listing_started_with_empower'] >= x['listing_period_start'] and x['listing_started_with_empower'] <= x['listing_period_end']) else 0, axis=1)
    # df['offer_projection'] = df.apply(lambda x: 1 if (x['offer_started_with_empower'] != na_filler and x['offer_started_with_empower'] >= x['offer_prep_period_start'] and x['offer_started_with_empower'] <= x['offer_prep_period_end']) else 0, axis=1)
    # df['compliance_projection'] = df.apply(lambda x: 1 if (x['compliance_started_with_empower'] != na_filler and x['compliance_started_with_empower'] >= x['compliance_period_start'] and x['compliance_started_with_empower'] <= x['compliance_period_end']) else 0, axis=1)
    # df['projection_condition'] = df.apply(lambda x: 1 if (x['ctc_projection'] == 1 or x['listing_projection'] == 1 or x['offer_projection'] == 1 or x['compliance_projection'] == 1) else 0, axis=1)

    # df.to_excel('tc_payroll.xlsx', sheet_name='transaction_source', index=False)

    print('Done')
    return df


def create_and_populate_google_sheet(service, spreadsheet_id, df, sheet_title):
    """
    Adds a new sheet to an existing Google Spreadsheet and populates it with data.

    :param service: Google Sheets API service object.
    :param spreadsheet_id: ID of the Google Spreadsheet.
    :param df: Pandas DataFrame containing the data.
    :param sheet_title: Title for the new sheet.
    """
    try:
        df = df.fillna("")

        df = df.copy()  # Make a copy to avoid modifying the original DataFrame
        for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
            df[col] = df[col].astype(str)

        # Add new sheet to the spreadsheet
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

        # Convert DataFrame to list of lists for Google Sheets API
        values = [df.columns.tolist()] + df.values.tolist()

        # Update the new sheet with data
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_title}!A1",
            valueInputOption="RAW",
            body={"values": values},
        ).execute()

        print(f"Sheet '{sheet_title}' added and populated successfully.")
    except Exception as e:
        print(f"Error populating Google Sheet: {e}")


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
        spreadsheet = service.spreadsheets().create(
            body={"properties": {"title": spreadsheet_name}}
        ).execute()
        spreadsheet_id = spreadsheet["spreadsheetId"]
        # spreadsheet_id = '1E4HF5_m-lBtOIV16NWRI3uNjLxPBVNmSHrJjEBGXhdg'

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
    try:
        specific_teams = {
            "ctc": (
                "Christianna Velazquez",
                "Kimberly Lewis",
                "Stephanie Kleinman",
                "Molly Kelley",
                "Jenn McKinley"
            ),
            "preferred": (
                "Marrisa Anderson",
                "Epique TC",
                "Epique EST",
                "Epique CST",
                "Epique CA"
            )
        }

        all_teams = df['Empower TC Name'].unique().tolist()

        dfs = list()
        sheet_titles = list()
        df_report_template = pd.DataFrame(
            index=all_teams
            # data={
            #     'CTC Started for the month': len(all_teams) * [0],
            #     'CTC - Preferred Started': len(all_teams) * [0],
            #     'Closings for the month': len(all_teams) * [0],
            #     'CTC - Preferred Closing': len(all_teams) * [0],
            #     'Pending for the month': len(all_teams) * [0],
            #     'CTC Pending for month - Preferred': len(all_teams) * [0],
            #     'Pending for next month': len(all_teams) * [0],
            #     'CTC Pending for next month - Preferred': len(all_teams) * [0],
            #     'Pending for other months': len(all_teams) * [0],
            #     'CTC Pending for other months - Preferred': len(all_teams) * [0]
            # }
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
            for key in specific_teams:

                if key == "ctc":
                    specific_team = specific_teams[key]
                    selected_team_df = enriched_df[enriched_df["Empower TC Name"].isin(specific_team)].copy()

                    ctc_started_df = selected_team_df[selected_team_df["CTC Started with Empower Periode"] == periode].copy()
                    summary_ctc_started_df = ctc_started_df.pivot_table(values='CTC Started for the month', index='Empower TC Name', aggfunc='sum', fill_value=0)

                    closing_df = selected_team_df[selected_team_df["Closing Periode"] == periode].copy()
                    summary_closing_df = closing_df.pivot_table(values=['Closings for the month', 'Pending for the month'], index='Empower TC Name', aggfunc='sum', fill_value=0)

                    closing_m1_df = selected_team_df[selected_team_df["Closing Periode M1"] == periode].copy()
                    if periode == 'OCTOBER 2024':
                        print('Save')
                        closing_m1_df.to_csv('cek.csv', index=False)
                    summary_closing_m1_df = closing_m1_df.pivot_table(values='Pending for next month', index='Empower TC Name', aggfunc='sum', fill_value=0)

                    closing_other_df = selected_team_df[selected_team_df["Closing Periode M1 End"] > dateformat_periode].copy()
                    summary_closing_other_df = closing_other_df.pivot_table(values='Pending for other months', index='Empower TC Name', aggfunc='sum', fill_value=0)

                elif key == 'preferred':
                    specific_team = specific_teams[key]
                    selected_team_df = enriched_df[enriched_df["Empower TC Name"].isin(specific_team)].copy()

                    ctc_preferred_started_df = selected_team_df[selected_team_df["CTC Started with Empower Periode"] == periode].copy()
                    summary_ctc_preferred_started_df = ctc_preferred_started_df.pivot_table(values='CTC Started for the month', index='Empower TC Name', aggfunc='sum', fill_value=0)
                    summary_ctc_preferred_started_df.rename({'CTC Started for the month': 'CTC - Preferred Started'}, axis=1, inplace=True)

                    ctc_preferred_closing_df = selected_team_df[selected_team_df["Closing Periode"] == periode].copy()
                    summary_ctc_preferred_closing_df = ctc_preferred_closing_df.pivot_table(values=['Closings for the month', 'Pending for the month', 'Pending for next month', 'Pending for other months'], index='Empower TC Name', aggfunc='sum', fill_value=0)
                    summary_ctc_preferred_closing_df.rename({'Closings for the month': 'CTC - Preferred Closing', 'Pending for the month': 'CTC Pending for month - Preferred'}, axis=1, inplace=True)

                    ctc_preferred_closing_m1_df = selected_team_df[selected_team_df["Closing Periode M1"] == periode].copy()
                    summary_ctc_preferred_closing_m1_df = ctc_preferred_closing_m1_df.pivot_table(values='Pending for next month', index='Empower TC Name', aggfunc='sum', fill_value=0)
                    summary_ctc_preferred_closing_m1_df.rename({'Pending for next month': 'CTC Pending for next month - Preferred'}, axis=1, inplace=True)

                    ctc_preferred_closing_other_df = selected_team_df[selected_team_df["Closing Periode M1 End"] > dateformat_periode].copy()
                    summary_ctc_preferred_closing_other_df = ctc_preferred_closing_other_df.pivot_table(values='Pending for other months', index='Empower TC Name', aggfunc='sum', fill_value=0)
                    summary_ctc_preferred_closing_other_df.rename({'Pending for other months': 'CTC Pending for other months - Preferred'}, axis=1, inplace=True)

            # summary_df['Total Result'] = summary_df.sum(axis=1)
            # summary_df.loc['Total Result'] = summary_df.sum()

            # report_df.update(summary_ctc_started_df)
            # report_df.update(summary_ctc_preferred_started_df)
            # report_df.update(summary_closing_df)
            # report_df.update(summary_ctc_preferred_closing_df)
            # report_df.update(summary_closing_m1_df)
            # report_df.update(summary_ctc_preferred_closing_m1_df)
            # report_df.update(summary_closing_other_df)
            # report_df.update(summary_ctc_preferred_closing_other_df)

            report_df.reset_index(inplace=True, names='Empower TC Name')
            report_df = report_df.merge(summary_ctc_started_df, how='left', on='Empower TC Name')
            report_df = report_df.merge(summary_ctc_preferred_started_df, how='left', on='Empower TC Name')
            report_df = report_df.merge(summary_closing_df, how='left', on='Empower TC Name')
            report_df = report_df.merge(summary_ctc_preferred_closing_df, how='left', on='Empower TC Name')
            report_df = report_df.merge(summary_closing_m1_df, how='left', on='Empower TC Name')
            report_df = report_df.merge(summary_ctc_preferred_closing_m1_df, how='left', on='Empower TC Name')
            report_df = report_df.merge(summary_closing_other_df, how='left', on='Empower TC Name')
            report_df = report_df.merge(summary_ctc_preferred_closing_other_df, how='left', on='Empower TC Name')
            report_df.fillna(0, inplace=True)
            report_df = report_df.astype('int64', errors='ignore')
            dfs.append(report_df.copy())
            sheet_titles.append(periode)

        spreadsheet_name = "tc_daily_update"

        print('Done')
        print('len of dfs = {}, len of sheet title = {}'.format(len(dfs), len(sheet_titles)))
        create_google_sheet(dfs, sheet_titles, spreadsheet_name)

    except Exception as e:
        print(f"Error processing data: {e}")
        return None

    finally:
        pass


if __name__ == '__main__':
    # df = generate_source('../all_properties.parquet')
    script_start_time = time.time()

    df = pd.read_csv('main_source.csv')
    generate_daily_update_report(df)

    script_end_time = time.time()
    total_execution_time = script_end_time - script_start_time
    print(f"Total script execution time: {total_execution_time:.2f} seconds")
