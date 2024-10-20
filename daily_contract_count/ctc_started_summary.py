import duckdb
import json
from datetime import datetime
import pandas as pd
import calendar


def get_started_summary(parquet_file_path):
    """
    Reads data from a Parquet file using DuckDB, filters it for the current year up to the current month,
    and creates a summary of started counts by month for specific teams.

    :param parquet_file_path: Path to the Parquet file.
    :return: Dictionary containing the summary data.
    """
    conn = duckdb.connect(database=":memory:")

    try:
        query = f"SELECT * FROM '{parquet_file_path}'"
        df = conn.execute(query).fetchdf()

        def get_ctc_started_with_empower(field_values):
            try:
                values = json.loads(field_values)
                for item in values:
                    if (
                        isinstance(item, dict)
                        and item.get("key") == "ctc_started_with_empower"
                    ):
                        return item.get("value")
            except json.JSONDecodeError:
                pass
            return None

        df["ctc_started_with_empower"] = df["field_values"].apply(
            get_ctc_started_with_empower
        )
        df["ctc_started_with_empower"] = pd.to_datetime(
            df["ctc_started_with_empower"], errors="coerce"
        )

        # Filter for current year up to the current month
        current_year = datetime.now().year
        current_month = datetime.now().month
        df = df[
            (df["ctc_started_with_empower"].dt.year == current_year)
            & (df["ctc_started_with_empower"].dt.month <= current_month)
        ]

        specific_teams = [
            "Team Christianna Velazquez",
            "Team Kimberly Lewis",
            "Team Stephanie Kleinman",
            "Team Molly Kelley",
            "Jenn McKinley",
            "Team Jenn McKinley",
        ]
        filtered_df = df[df["team_name"].isin(specific_teams)]

        # Group by month and count
        monthly_counts = filtered_df.groupby(
            filtered_df["ctc_started_with_empower"].dt.to_period("M")
        ).size()

        # Create the summary dictionary with all months
        summary = {"state": "CTC - Started"}
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


def execute_started_summary():
    parquet_file_path = "../all_properties.parquet"
    summary = get_started_summary(parquet_file_path)
    if summary:
        print(json.dumps(summary, indent=2))
    else:
        print("Failed to generate summary.")


if __name__ == "__main__":
    execute_started_summary()
