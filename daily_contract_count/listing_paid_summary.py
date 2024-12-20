import duckdb
import json
from datetime import datetime
import pandas as pd
import calendar


def get_listing_paid_summary(parquet_file_path):
    """
    Reads data from a Parquet file using DuckDB, filters it for the current year,
    and creates a summary of listing paid counts by month.

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

        def get_listing_paid_date(field_values):
            try:
                values = json.loads(field_values)
                for item in values:
                    if isinstance(item, dict) and item.get("key") == "listing_paid_date":
                        return item.get("value")
            except json.JSONDecodeError:
                pass
            return None

        df["contract_status"] = df["field_values"].apply(get_contract_status)

        df["listing_paid_date"] = df["field_values"].apply(get_listing_paid_date)
        df["listing_paid_date"] = pd.to_datetime(df["listing_paid_date"], errors="coerce")

        # Filter for current year
        current_year = datetime.now().year
        current_month = datetime.now().month
        df = df[
            (df["listing_paid_date"].dt.year == current_year)
            & (df["listing_paid_date"].dt.month <= current_month)
            & (df["contract_status"] == "Listing - PAID")
        ]

        # Group by month and count
        monthly_counts = df.groupby(df["listing_paid_date"].dt.to_period("M")).size()

        # Create the summary dictionary with all months
        summary = {"state": "Listing - Paid"}
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


def execute_listing_paid_summary():
    parquet_file_path = "all_properties.parquet"
    summary = get_listing_paid_summary(parquet_file_path)
    if summary:
        print(json.dumps(summary, indent=2))
    else:
        print("Failed to generate summary.")


if __name__ == "__main__":
    execute_listing_paid_summary()
