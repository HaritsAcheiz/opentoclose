import json
import csv
import time
import os
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import pandas as pd

from ctc_buyer_closing_summary import get_ctc_closing_summary_buyer
from ctc_closing_summary import get_closing_summary
from ctc_seller_closing_summary import get_ctc_closing_summary_seller
from ctc_started_summary import get_started_summary
from ctc_terminated_summary import get_terminated_summary
from ctc_withdrawn_summary import get_withdrawn_summary
from preferred_seller_closing_summary import get_preferred_closing_summary_seller
from preferred_buyer_closing_summary import get_preferred_closing_summary_buyer
from preferred_closing_summary import get_preferred_closing_summary
from preferred_started_summary import get_preferred_started_summary
from listing_started_summary import get_listing_started_summary
from listing_paid_summary import get_listing_paid_summary
from compliance_started_summary import get_compliance_started_summary
from compliance_paid_summary import get_compliance_paid_summary
from all_closing_current_month_summary import get_all_closing_current_month_summary
from preferred_future_closing_summary import get_preferred_future_closing_summary
from ctc_future_closing_summary import get_ctc_future_closing_summary
from preferred_closing_all_other_month_summary import (
    get_preferred_closing_all_other_month_summary,
)
from ctc_closing_all_other_month_summary import (
    get_ctc_closing_all_other_month_summary,
)


def concatenate_summaries():
    start_time = time.time()
    parquet_file_path = "../all_properties.parquet"
    summaries = [
        get_closing_summary(parquet_file_path),
        get_started_summary(parquet_file_path),
        get_withdrawn_summary(parquet_file_path),
        get_preferred_started_summary(parquet_file_path),
        get_ctc_closing_summary_buyer(parquet_file_path),
        get_ctc_closing_summary_seller(parquet_file_path),
        get_preferred_closing_summary_seller(parquet_file_path),
        get_preferred_closing_summary_buyer(parquet_file_path),
        get_terminated_summary(parquet_file_path),
        get_preferred_closing_summary(parquet_file_path),
        get_listing_started_summary(parquet_file_path),
        get_listing_paid_summary(parquet_file_path),
        get_compliance_started_summary(parquet_file_path),
        get_compliance_paid_summary(parquet_file_path),
        get_all_closing_current_month_summary(parquet_file_path),
        get_ctc_future_closing_summary(parquet_file_path),
        get_preferred_future_closing_summary(parquet_file_path),
        get_preferred_closing_all_other_month_summary(parquet_file_path),
        get_ctc_closing_all_other_month_summary(parquet_file_path),
    ]
    summaries = [summary for summary in summaries if summary]
    end_time = time.time()
    print(f"concatenate_summaries() took {end_time - start_time:.2f} seconds")
    return summaries


def create_and_populate_google_sheet(service, data, sheet_name):
    """
    Creates a new Google Sheet and populates it with data.

    :param service: Google Sheets API service object.
    :param data: List of dictionaries containing the summary data.
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

        # Prepare the data for batch update
        df = pd.DataFrame(data)
        values = [df.columns.tolist()] + df.values.tolist()

        # Update the sheet with data
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range="A1",
            valueInputOption="RAW",
            body={"values": values},
        ).execute()

        print(f"Google Sheet '{sheet_name}' created and populated successfully.")
        return spreadsheet_id
    except Exception as e:
        print(f"Error creating and populating Google Sheet: {e}")
        return None


def create_google_sheet(summaries, sheet_name="Daily Contract Count - All States YOY"):
    start_time = time.time()
    if not summaries:
        print("No summaries to write to Google Sheet.")
        return

    # Set up Google Sheets API
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
    ]
    creds = Credentials.from_service_account_file('../credentials.json', scopes=scope)
    service = build("sheets", "v4", credentials=creds)
    sheet_id = create_and_populate_google_sheet(service, summaries, sheet_name)
    drive_service = build('drive', 'v3', credentials=creds)

    permission_body = {
        'type': 'anyone',   # Makes it accessible to anyone
        'role': 'reader'    # Sets the permission to read-only
    }

    drive_service.permissions().create(
        fileId=sheet_id,
        body=permission_body
    ).execute()

    end_time = time.time()
    print(f"create_google_sheet() took {end_time - start_time:.2f} seconds")

    if sheet_id:
        print(f"Google Sheet created with ID: {sheet_id}")
    else:
        print("Failed to create Google Sheet.")

    return sheet_id


if __name__ == "__main__":
    script_start_time = time.time()

    all_summaries = concatenate_summaries()
    sheet_id = create_google_sheet(all_summaries)

    script_end_time = time.time()
    total_execution_time = script_end_time - script_start_time
    print(f"Total script execution time: {total_execution_time:.2f} seconds")
