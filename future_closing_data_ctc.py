import duckdb
import json
import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import pandas as pd
from datetime import datetime, timedelta
import numpy as np


def read_parquet_and_create_google_sheets(parquet_file_path, sheet_name):
    """
    Reads data from a Parquet file using DuckDB, filters it based on closing date and specific teams,
    and creates a Google Sheet for future closings (excluding current and previous month).

    :param parquet_file_path: Path to the Parquet file.
    :param sheet_name: Name for the Google Sheet.
    :return: ID of the created Google Sheet.
    """
    conn = duckdb.connect(database=":memory:")

    try:
        query = f"SELECT * FROM '{parquet_file_path}'"
        df = conn.execute(query).fetchdf()

        def get_closing_date(field_values):
            try:
                values = json.loads(field_values)
                for item in values:
                    if isinstance(item, dict) and item.get("key") == "closing_date":
                        return item.get("value")
            except json.JSONDecodeError:
                pass
            return None

        df["closing_date"] = df["field_values"].apply(get_closing_date)
        df["closing_date"] = pd.to_datetime(df["closing_date"], errors="coerce")

        # Filter for future months (excluding current and previous month)
        today = datetime.now()
        first_day_of_next_month = (today.replace(day=1) + timedelta(days=32)).replace(
            day=1
        )

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
            (df["closing_date"] >= first_day_of_next_month)
            & (df["team_name"].isin(specific_teams))
        ]

        if filtered_df.empty:
            print("No matching records found for future closings.")
            return None

        # Remove unnecessary columns
        filtered_df = filtered_df.drop(columns=["field_values"])

        # Convert datetime columns to string
        for col in filtered_df.select_dtypes(include=["datetime64"]).columns:
            filtered_df[col] = filtered_df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

        # Set up Google Sheets API
        SCOPES = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
        ]
        creds = None
        if os.path.exists("token.json"):
            creds = Credentials.from_authorized_user_file("token.json", SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    "credentials.json", SCOPES
                )
                creds = flow.run_local_server(port=0)
            with open("token.json", "w") as token:
                token.write(creds.to_json())

        service = build("sheets", "v4", credentials=creds)

        # Create and populate the Google Sheet
        sheet_id = create_and_populate_google_sheet(service, filtered_df, sheet_name)
        print(f"Google Sheet created with ID: {sheet_id}")

        return sheet_id

    except Exception as e:
        print(f"Error processing data: {e}")
        return None
    finally:
        conn.close()


def create_and_populate_google_sheet(service, data, sheet_name):
    """
    Creates a new Google Sheet and populates it with data on a single sheet.

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

        def clean_value(val):
            if isinstance(val, float) and np.isnan(val):
                return ""
            return str(val).replace("\n", " ").replace("\r", "")

        # Prepare the data for batch update
        values = [data.columns.tolist()] + [
            [clean_value(val) for val in row] for row in data.values.tolist()
        ]

        # Update the sheet with data
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range="Sheet1!A1",
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
    sheet_name = "Future Closings-CTC, All Other Months"
    sheet_id = read_parquet_and_create_google_sheets(parquet_file_path, sheet_name)
    if sheet_id:
        print(f"Google Sheet created with ID: {sheet_id}")
    else:
        print("Failed to create Google Sheet.")


if __name__ == "__main__":
    execute_read_parquet_and_create_google_sheets()
