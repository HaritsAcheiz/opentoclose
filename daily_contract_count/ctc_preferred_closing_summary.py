import duckdb
import json
from datetime import datetime
import pandas as pd
import calendar
import csv


def get_ctc_preferred_closing_summary(parquet_file_path):
    """
    Reads data from a Parquet file using DuckDB, filters it for the current year,
    and creates a summary of closing counts by month for preferred teams.

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

        def get_closing_date(field_values):
            try:
                values = json.loads(field_values)
                for item in values:
                    if isinstance(item, dict) and item.get("key") == "closing_date":
                        return item.get("value")
            except json.JSONDecodeError:
                pass
            return None

        df['contract_status'] = df['field_values'].apply(get_contract_status)

        df["closing_date"] = df["field_values"].apply(get_closing_date)
        df["closing_date"] = pd.to_datetime(df["closing_date"], errors="coerce")

        # Filter for current year only
        current_year = datetime.now().year
        current_month = datetime.now().month
        df = df[
            (df["closing_date"].dt.year == current_year)
            & (df["closing_date"].dt.month <= current_month)
        ]

        specific_teams = list()
        with open('preferred_teams.csv') as file:
            rows = csv.reader(file)
            for row in rows:
                specific_teams.append(row[0])

        filtered_df = df[df["team_name"].isin(specific_teams) & (df["contract_status"] == "CTC - Preferred - Closed - Ready to BILL")]

        # Group by month and count
        monthly_counts = filtered_df.groupby(
            filtered_df["closing_date"].dt.to_period("M")
        ).size()

        # Create the summary dictionary with all months
        summary = {"state": "CTC - Preferred Closing"}
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


def execute_ctc_preferred_closing_summary():
    parquet_file_path = "all_properties.parquet"
    summary = get_ctc_preferred_closing_summary(parquet_file_path)
    if summary:
        print(json.dumps(summary, indent=2))
    else:
        print("Failed to generate summary.")


if __name__ == "__main__":
    execute_ctc_preferred_closing_summary()
