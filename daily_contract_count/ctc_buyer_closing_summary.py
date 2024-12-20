import duckdb
import json
from datetime import datetime
import pandas as pd
import calendar
import csv


def get_ctc_closing_summary_buyer(parquet_file_path):
    """
    Reads data from a Parquet file using DuckDB, filters it for the current year,
    and creates a summary of closing counts by month for CTC teams with Buyer client type.

    :param parquet_file_path: Path to the Parquet file.
    :return: Dictionary containing the summary data.
    """
    conn = duckdb.connect(database=":memory:")

    try:
        query = f"SELECT * FROM read_parquet('{parquet_file_path}')"
        df = conn.execute(query).fetchdf()

        def get_contract_status(field_values):
            try:
                values = json.loads(field_values)
                for item in values:
                    if isinstance(item, dict) and item.get("key") == "contract_status":
                        return item.get("value")
            except json.JSONDecodeError:
                pass
            return None

        def get_closing_date_and_client_type(field_values):
            try:
                values = json.loads(field_values)
                closing_date = None
                client_type = None
                for item in values:
                    if isinstance(item, dict):
                        if item.get("key") == "closing_date":
                            closing_date = item.get("value")
                        elif item.get("key") == "contract_client_type":
                            client_type = item.get("value")
                return closing_date, client_type
            except json.JSONDecodeError:
                pass
            return None, None

        df["contract_status"] = df["field_values"].apply(get_contract_status)

        df["closing_date"], df["client_type"] = zip(
            *df["field_values"].apply(get_closing_date_and_client_type)
        )
        df["closing_date"] = pd.to_datetime(df["closing_date"], errors="coerce")

        # Filter for current year, Buyer client type only
        current_year = datetime.now().year
        current_month = datetime.now().month
        df = df[
            (df["closing_date"].dt.year == current_year)
            & (df["closing_date"].dt.month <= current_month)
            & (df["client_type"] == "Buyer")
        ]

        specific_teams = list()
        with open('ctc_teams.csv') as file:
            rows = csv.reader(file)
            for row in rows:
                specific_teams.append(row[0])

        filtered_df = df[df["team_name"].isin(specific_teams) & (df["contract_status"] == "CTC - Closed - PAID")]

        # Group by month and count
        monthly_counts = filtered_df.groupby(
            filtered_df["closing_date"].dt.to_period("M")
        ).size()

        # Create the summary dictionary with all months
        summary = {"state": "Client Type-Buyer"}
        current_year = datetime.now().year
        current_month = datetime.now().month

        for month in range(1, current_month + 1):
            month_name = calendar.month_abbr[month]
            summary[f"{month_name} {current_year}"] = 0

        # Update the summary with actual counts
        for month, count in monthly_counts.items():
            month_name = calendar.month_abbr[month.month]
            summary[f"{month_name} {current_year}"] = int(count)

        return summary

    except Exception as e:
        print(f"Error processing data: {e}")
        return None
    finally:
        conn.close()


def execute_ctc_closing_summary_buyer():
    parquet_file_path = "all_properties.parquet"
    summary = get_ctc_closing_summary_buyer(parquet_file_path)
    if summary:
        print(json.dumps(summary, indent=2))
    else:
        print("Failed to generate summary.")


if __name__ == "__main__":
    execute_ctc_closing_summary_buyer()
