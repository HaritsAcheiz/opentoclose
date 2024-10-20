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
    Reads data from a Parquet file using DuckDB, filters it based on closing date for next month and specific teams,
    and creates a Google Sheet in the OTC folder.

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

        # Filter for next month only
        today = datetime.now()
        first_day_of_next_month = (today.replace(day=1) + timedelta(days=32)).replace(
            day=1
        )
        last_day_of_next_month = (first_day_of_next_month + timedelta(days=32)).replace(
            day=1
        ) - timedelta(days=1)

        specific_teams = [
            "Team Molly Kelley",
            "Preferred CTC Team",
            "Team Marrisa Anderson",
            "Team EpiqueTC",
            "Team EpiqueTC AA",
            "Team EpiqueEST",
            "Team EpiqueEST AA",
            "Team EpiqueCST",
            "Team EpiqueCST AA",
            "Team EpiqueCA",
            "Team EpiqueCA AA",
        ]
        filtered_df = df[
            (df["closing_date"] >= first_day_of_next_month)
            & (df["closing_date"] <= last_day_of_next_month)
            & (df["team_name"].isin(specific_teams))
        ]

        if filtered_df.empty:
            print("No matching records found for next month's closings.")
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
            "https://www.googleapis.com/auth/drive",
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
        sheet_id = create_and_populate_google_sheet(
            service, filtered_df, sheet_name, creds
        )
        print(f"Google Sheet created with ID: {sheet_id}")

        return sheet_id

    except Exception as e:
        print(f"Error processing data: {e}")
        return None
    finally:
        conn.close()


def create_and_populate_google_sheet(service, data, sheet_name, creds):
    """
    Creates a new Google Sheet, populates it with data, and moves it to the "OTC" folder.

    :param service: Google Sheets API service object.
    :param data: Pandas DataFrame containing the data to be added to the sheet.
    :param sheet_name: Name for the new Google Sheet.
    :param creds: Credentials object for Google APIs.
    :return: ID of the created Google Sheet.
    """
    try:
        # Create the spreadsheet
        spreadsheet = (
            service.spreadsheets()
            .create(body={"properties": {"title": sheet_name}})
            .execute()
        )
        spreadsheet_id = spreadsheet["spreadsheetId"]

        # Populate the spreadsheet with data
        def clean_value(val):
            if isinstance(val, float) and np.isnan(val):
                return ""
            return str(val).replace("\n", " ").replace("\r", "")

        values = [data.columns.tolist()] + [
            [clean_value(val) for val in row] for row in data.values.tolist()
        ]

        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range="Sheet1!A1",
            valueInputOption="RAW",
            body={"values": values},
        ).execute()

        # Move the spreadsheet to the "OTC" folder
        drive_service = build("drive", "v3", credentials=creds)

        # Find the "OTC" folder
        folder_name = "OTC"
        folders = (
            drive_service.files()
            .list(
                q=f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}' and trashed=false",
                spaces="drive",
                fields="files(id, name)",
            )
            .execute()
            .get("files", [])
        )

        if not folders:
            print(f"Folder '{folder_name}' not found. Creating it...")
            folder_metadata = {
                "name": folder_name,
                "mimeType": "application/vnd.google-apps.folder",
            }
            folder = (
                drive_service.files().create(body=folder_metadata, fields="id").execute()
            )
            folder_id = folder.get("id")
        else:
            folder_id = folders[0]["id"]

        # Move the file to the OTC folder
        file = (
            drive_service.files().get(fileId=spreadsheet_id, fields="parents").execute()
        )
        previous_parents = ",".join(file.get("parents"))
        file = (
            drive_service.files()
            .update(
                fileId=spreadsheet_id,
                addParents=folder_id,
                removeParents=previous_parents,
                fields="id, parents",
            )
            .execute()
        )

        print(
            f"Google Sheet '{sheet_name}' created, populated, and moved to the OTC folder successfully."
        )
        return spreadsheet_id
    except Exception as e:
        print(f"Error creating, populating, or moving Google Sheet: {e}")
        return None


def execute_read_parquet_and_create_google_sheets():
    parquet_file_path = "all_properties.parquet"
    sheet_name = "Future Closings-Preferred, Next Month"
    sheet_id = read_parquet_and_create_google_sheets(parquet_file_path, sheet_name)
    if sheet_id:
        print(f"Google Sheet created with ID: {sheet_id}")
    else:
        print("Failed to create Google Sheet.")


if __name__ == "__main__":
    execute_read_parquet_and_create_google_sheets()
