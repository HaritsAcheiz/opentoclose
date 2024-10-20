import duckdb
import json
from datetime import datetime
import pandas as pd
import calendar


def get_preferred_closing_summary_buyer(parquet_file_path):
    """
    Reads data from a Parquet file using DuckDB, filters it for the current year,
    and creates a summary of closing counts by month for preferred teams with Buyer client type.

    :param parquet_file_path: Path to the Parquet file.
    :return: Dictionary containing the summary data.
    """
    conn = duckdb.connect(database=":memory:")

    try:
        query = f"SELECT * FROM '{parquet_file_path}'"
        df = conn.execute(query).fetchdf()

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

        # Group by month and count
        monthly_counts = filtered_df.groupby(
            filtered_df["closing_date"].dt.to_period("M")
        ).size()

        # Create the summary dictionary with all months
        summary = {"state": "Preferred (epique) Buyer Closed"}
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


def execute_preferred_closing_summary_buyer():
    parquet_file_path = "../all_properties.parquet"
    summary = get_preferred_closing_summary_buyer(parquet_file_path)
    if summary:
        print(json.dumps(summary, indent=2))
    else:
        print("Failed to generate summary.")


if __name__ == "__main__":
    execute_preferred_closing_summary_buyer()
