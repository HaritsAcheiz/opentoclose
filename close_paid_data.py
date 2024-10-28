import duckdb
import json
import os
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.oauth2.service_account import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import gspread

import pandas as pd
from datetime import datetime, timedelta
import numpy as np


def read_parquet_and_create_google_sheets(parquet_file_path, sheet_name_prefix):
    """
    Reads data from a Parquet file using DuckDB, filters it based on closing date and contract status,
    and creates two Google Sheets for the previous month's data.

    :param parquet_file_path: Path to the Parquet file.
    :param sheet_name_prefix: Prefix for the Google Sheet names.
    :return: IDs of the created Google Sheets.
    """
    conn = duckdb.connect(database=":memory:")

    try:
        query = f"SELECT * FROM '{parquet_file_path}'"
        df = conn.execute(query).fetchdf()

        def get_closing_date_and_status(field_values):
            try:
                values = json.loads(field_values)
                closing_date = None
                status = None
                for item in values:
                    if isinstance(item, dict):
                        if item.get("key") == "closing_date":
                            closing_date = item.get("value")
                        elif item.get("key") == "contract_status":
                            status = item.get("value")
                return closing_date, status == "CTC - Closed - PAID"
            except json.JSONDecodeError:
                pass
            return None, False

        df["closing_date"], df["is_closed_paid"] = zip(
            *df["field_values"].apply(get_closing_date_and_status)
        )
        df["closing_date"] = pd.to_datetime(df["closing_date"], errors="coerce")

        # Filter for the previous month and closed-paid status
        today = datetime.now()
        first_day_of_current_month = today.replace(day=1)
        last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
        first_day_of_previous_month = last_day_of_previous_month.replace(day=1)

        # List of specific teams to include
        specific_teams = [
            "Team Christianna Velazquez",
            "Team Kimberly Lewis",
            "Team Stephanie Kleinman",
            "Team Molly Kelley",
            "Jenn McKinley",
            "Team Jenn McKinley",
        ]

        filtered_df = df[
            (df["closing_date"] >= first_day_of_previous_month)
            & (df["closing_date"] <= last_day_of_previous_month)
            & (df["team_name"].isin(specific_teams))
        ]
        if filtered_df.empty:
            print(
                "No matching records found for the previous month with closed-paid status."
            )
            return None, None

        # Remove unnecessary columns
        filtered_df = filtered_df.drop(columns=["field_values"])

        # Convert datetime columns to string
        for col in filtered_df.select_dtypes(include=["datetime64"]).columns:
            filtered_df[col] = filtered_df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

        # Split the data into two parts
        first_half = filtered_df[pd.to_datetime(filtered_df["closing_date"]).dt.day <= 15]
        second_half = filtered_df[pd.to_datetime(filtered_df["closing_date"]).dt.day > 15]

        # Set up Google Sheets API
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
        ]
        creds = None

        # if os.path.exists("token.json"):
        #     creds = Credentials.from_authorized_user_file("token.json", SCOPES)
        # if not creds or not creds.valid:
        #     if creds and creds.expired and creds.refresh_token:
        #         creds.refresh(Request())
        #     else:
        #         flow = InstalledAppFlow.from_client_secrets_file(
        #             "credentials.json", SCOPES
        #         )
        #         creds = flow.run_local_server(port=0)
        #     with open("token.json", "w") as token:
        #         token.write(creds.to_json())

        creds = Credentials.from_service_account_file('credentials.json', scopes=scope)
        # service = gspread.authorize(creds)

        service = build("sheets", "v4", credentials=creds)

        # Create and populate the Google Sheets
        month_year = first_day_of_previous_month.strftime("%B %Y")
        sheet_id_1 = create_and_populate_google_sheet(
            service, first_half, f"{sheet_name_prefix} 1-15 {month_year}"
        )
        sheet_id_2 = create_and_populate_google_sheet(
            service,
            second_half,
            f"{sheet_name_prefix} 16-{last_day_of_previous_month.day} {month_year}",
        )

        drive_service = build('drive', 'v3', credentials=creds)

        # Step 5: Set public sharing permissions for the Google Sheet
        permission_body = {
            'type': 'anyone',   # Makes it accessible to anyone
            'role': 'reader'    # Sets the permission to read-only
        }

        drive_service.permissions().create(
            fileId=sheet_id_1,
            body=permission_body
        ).execute()

        drive_service.permissions().create(
            fileId=sheet_id_2,
            body=permission_body
        ).execute()

        print(f"Google Sheets created with IDs: {sheet_id_1} and {sheet_id_2}")

        return sheet_id_1, sheet_id_2

    except Exception as e:
        print(f"Error processing data: {e}")
        return None, None
    finally:
        conn.close()


def create_and_populate_google_sheet(service, data, sheet_name):
    """
    Creates a new Google Sheet and populates it with data.

    :param service: Google Sheets API service object.
    :param data: Pandas DataFrame containing the data to be added to the sheet.
    :param sheet_name: Name for the new Google Sheet.
    :return: ID of the created Google Sheet.
    """
    try:
        spreadsheet = (
            service.spreadsheets()
            .create(body={"properties": {"title": sheet_name}})
            .execute()
        )
        spreadsheet_id = spreadsheet["spreadsheetId"]

        # Group the data by team
        grouped_data = data.groupby("team_name")

        def clean_value(val):
            if isinstance(val, float) and np.isnan(val):
                return ""
            return str(val).replace("\n", " ").replace("\r", "")

        for team, team_data in grouped_data:
            # Create a new sheet for each team
            request = {
                "addSheet": {"properties": {"title": f"{team} - Closed Properties"}}
            }
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id, body={"requests": [request]}
            ).execute()

            # Prepare the data for batch update
            values = [team_data.columns.tolist()] + [
                [clean_value(val) for val in row] for row in team_data.values.tolist()
            ]

            # Update the sheet with data
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"'{team} - Closed Properties'!A1",
                valueInputOption="RAW",
                body={"values": values},
            ).execute()
        print(f"Google Sheet '{sheet_name}' created and populated successfully.")

        return spreadsheet_id

    except Exception as e:
        print(f"Error creating and populating Google Sheet: {e}")
        return None


def execute_read_parquet_and_create_google_sheets():
    parquet_file_path = "all_properties.parquet"
    sheet_name_prefix = "Closed Properties"
    sheet_id_1, sheet_id_2 = read_parquet_and_create_google_sheets(
        parquet_file_path, sheet_name_prefix
    )
    if sheet_id_1 and sheet_id_2:
        print(f"Google Sheets created with IDs: {sheet_id_1} and {sheet_id_2}")
    else:
        print("Failed to create Google Sheets.")


if __name__ == "__main__":
    execute_read_parquet_and_create_google_sheets()
