import duckdb
import json


def get_all_contract_statuses(parquet_file_path):
    """
    Reads data from a Parquet file using DuckDB and extracts all unique contract statuses.

    :param parquet_file_path: Path to the Parquet file.
    :return: List of unique contract statuses.
    """
    conn = duckdb.connect(database=":memory:")

    try:
        query = f"SELECT * FROM '{parquet_file_path}'"
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

        df["contract_status"] = df["field_values"].apply(get_contract_status)

        # Get unique contract statuses
        unique_statuses = df["contract_status"].dropna().unique().tolist()

        return unique_statuses

    except Exception as e:
        print(f"Error processing data: {e}")
        return None
    finally:
        conn.close()


def execute_all_contract_statuses():
    parquet_file_path = "all_properties.parquet"
    statuses = get_all_contract_statuses(parquet_file_path)
    if statuses:
        print("Unique Contract Statuses:")
        for status in sorted(statuses):
            print(f"- {status}")
    else:
        print("Failed to generate contract statuses.")


if __name__ == "__main__":
    execute_all_contract_statuses()
