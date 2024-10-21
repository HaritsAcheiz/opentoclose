import duckdb
import json
from datetime import datetime
import pandas as pd
import calendar


def get_preferred_closing_all_other_month_summary(parquet_file_path):
    """
    Reads data from a Parquet file using DuckDB, filters it for the current year,
    and creates a summary of closing counts by month for specific teams,
    where each month's count represents the closings for the rest of the year.

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

        # Filter for current year
        current_year = datetime.now().year
        df = df[df["closing_date"].dt.year == current_year]

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
        filtered_df = df[df["team_name"].isin(specific_teams)]

        # Create the summary dictionary
        summary = {"state": "Future Closing All Other Month - CTC"}
        month = datetime.now().month

        # Calculate counts for each month
        for month in range(1, month + 1):
            month_name = calendar.month_abbr[month]
            rest_of_year = filtered_df[filtered_df["closing_date"].dt.month > month]
            count = len(rest_of_year)
            summary[f"{month_name} {current_year}"] = int(count)

        return summary

    except Exception as e:
        print(f"Error processing data: {e}")
        return None
    finally:
        conn.close()


def execute_closing_summary():
    parquet_file_path = "../all_properties.parquet"
    summary = get_preferred_closing_all_other_month_summary(parquet_file_path)
    if summary:
        print(json.dumps(summary, indent=2))
    else:
        print("Failed to generate summary.")


if __name__ == "__main__":
    execute_closing_summary()