import duckdb
import json
from datetime import datetime
import pandas as pd
import calendar
import csv


def get_ctc_future_closing_summary(parquet_file_path):
    """
    Reads data from a Parquet file using DuckDB, filters it for the current year,
    and creates a summary of closing counts by month for specific teams,
    where each month's count represents the closings for the next month.

    :param parquet_file_path: Path to the Parquet file.
    :return: Dictionary containing the summary data.
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

        # Filter for current year and include next month
        current_year = datetime.now().year
        current_month = datetime.now().month
        df = df[
            (df["closing_date"].dt.year == current_year)
            & (df["closing_date"].dt.month <= current_month + 1)
        ]

        specific_teams = list()
        with open('../ctc_teams.csv') as file:
            rows = csv.reader(file)
            for row in rows:
                specific_teams.append(row[0])
        filtered_df = df[df["team_name"].isin(specific_teams)]

        # Group by month and count
        monthly_counts = filtered_df.groupby(
            filtered_df["closing_date"].dt.to_period("M")
        ).size()

        # Create the summary dictionary with all months
        summary = {"state": "Future Closing Next Month - CTC"}
        current_year = datetime.now().year
        current_month = datetime.now().month

        # Initialize all months up to the current month with 0
        for month in range(1, current_month + 1):
            month_name = calendar.month_abbr[month]
            summary[f"{month_name} {current_year}"] = 0

        # Assign each month's count to the previous month
        for month, count in monthly_counts.items():
            if (
                month.month > 1
            ):  # Skip January as it would represent December's future closings
                prev_month = month.to_timestamp() - pd.DateOffset(months=1)
                prev_month_name = calendar.month_abbr[prev_month.month]
                summary[f"{prev_month_name} {current_year}"] = int(count)

        return summary

    except Exception as e:
        print(f"Error processing data: {e}")
        return None
    finally:
        conn.close()


def execute_closing_summary():
    parquet_file_path = "../all_properties.parquet"
    summary = get_ctc_future_closing_summary(parquet_file_path)
    if summary:
        print(json.dumps(summary, indent=2))
    else:
        print("Failed to generate summary.")


if __name__ == "__main__":
    execute_closing_summary()
