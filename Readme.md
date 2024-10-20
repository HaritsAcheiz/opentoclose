
This will start the pipeline, which consists of two main parts:

#### Part 1: Fetch Properties (`execute_fetch_properties`)

1. The script uses the API token to authenticate with the OpenToClose API.
2. It fetches property data in batches of 50 properties at a time.
3. The fetched data is collected into a list of all properties.
4. Finally, the data is saved to a Parquet file named `all_properties.parquet`.

#### Part 2: Process Data and Create Google Sheet (`execute_read_parquet_and_create_google_sheet`)

1. The script reads the `all_properties.parquet` file using DuckDB.
2. It filters the data to include only properties with a "CTC - Closed - PAID" status.
3. The 'field_values' column is removed from the filtered data.
4. The script then authenticates with the Google Sheets API:
   - It looks for existing credentials in `token.json`.
   - If not found or invalid, it initiates the OAuth2 flow using `credentials.json`.
   - This may open a browser window for you to authorize the application.
5. A new Google Sheet named "Closed Paid Properties" is created.
6. The filtered data is written to the Google Sheet in chunks to avoid size limits.

### What Happens Behind the Scenes

1. `main.py` orchestrates the entire pipeline:
   - It sets up logging to track the progress.
   - Calls `execute_fetch_properties()` from `fetch_properties.py`.
   - Then calls `execute_read_parquet_and_create_google_sheet()` from `close_paid_data.py`.

2. `fetch_properties.py`:
   - Handles the API communication with OpenToClose.
   - Fetches all properties in batches.
   - Saves the raw data to a Parquet file.

3. `close_paid_data.py`:
   - Reads the Parquet file created by `fetch_properties.py`.
   - Filters the data for closed and paid properties.
   - Handles the Google Sheets API authentication and data upload.

### Output

After running the pipeline:
1. You'll see log messages in the console indicating the progress of each step.
2. A file named `all_properties.parquet` will be created in your working directory.
3. A new Google Sheet named "Closed Paid Properties" will be created in your Google Drive.
4. The console will display the ID of the created Google Sheet.

### Error Handling

- The pipeline includes error handling to catch and log any exceptions that occur during execution.
- If any step fails, an error message will be displayed in the console.

### Note

- Ensure you have proper internet connectivity throughout the process.
- The Google Sheets authentication process requires a web browser for the first run or if the credentials expire.
- The `credentials.json` file is crucial for Google Sheets API access. Make sure it's present and valid.

By following these steps and understanding the process, you can successfully run the entire pipeline to fetch property data, process it, and create a Google Sheet with the results.