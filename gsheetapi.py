from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


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


def create_google_sheet(dataframes, sheet_titles, spreadsheet_name, spreadsheet_id=None):
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
    creds = Credentials.from_service_account_file('credentials.json', scopes=scope)
    service = build("sheets", "v4", credentials=creds)

    try:
        if not spreadsheet_id:
            # Create a new spreadsheet
            spreadsheet = service.spreadsheets().create(
                body={"properties": {"title": spreadsheet_name}}
            ).execute()
            spreadsheet_id = spreadsheet["spreadsheetId"]

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
