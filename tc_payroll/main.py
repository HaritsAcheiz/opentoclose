import duckdb
import time
import json
import pandas as pd
from datetime import datetime
import csv

na_filler = datetime(1990, 1, 1, 0, 0, 0)


def extract_field_values(field_values, key):
    try:
        values = json.loads(field_values)
        for item in values:
            if isinstance(item, dict) and item.get("key") == key:
                return item.get("value")
    except json.JSONDecodeError:
        pass
    return None


def extract_transaction_source(properties_file_path):
    print('Extracting transaction data source...', end='')
    conn = duckdb.connect(database=":memory:")
    try:
        query = f"SELECT * FROM '{properties_file_path}'"
        df = conn.execute(query).fetchdf()
        df[0:10].to_csv('properties_template.csv', index=False)

        transaction_schema = list()
        with open('transaction_schema.csv') as file:
            rows = csv.reader(file)
            for row in rows:
                transaction_schema.append(row[0])

        transaction_df = pd.DataFrame()
        for field in transaction_schema:
            transaction_df[field] = df['field_values'].apply(lambda x: extract_field_values(x, field))

        transaction_df.to_csv('transaction_source.csv', index=False)

        print('Done')
        return transaction_df

    except Exception as e:
        print(f"Error processing data: {e}")
        return None

    finally:
        conn.close()


def add_tc_commission_rate(transaction_df):
    transaction_df.rename({'tc_commission_rate': 'tc_commission_rate_1'}, axis=1, inplace=True)
    transaction_df['tc_commission_rate'] = transaction_df.apply(lambda x: 70/100 if x['agent_provided_by'] == 'TC' else x['tc_commission_rate_1']/100, axis=1)
    transaction_df.drop(columns='tc_commission_rate_1', inplace=True)

    return transaction_df


def get_period(date, mode):
    if isinstance(date, str):
        try:
            date = pd.to_datetime(date)
        except ValueError:
            print("Invalid date format")
            date = None
    if pd.isna(date):
        result = date
    else:
        year = date.year
        month = date.month
        day = date.day
        if mode == 'start':
            if day >= 1 and day <= 15:
                result = datetime(year, month, 1)
            else:
                result = datetime(year, month, 16)
        elif mode == 'end':
            if day >= 1 and day <= 15:
                result = datetime(year, month, 15)
            else:
                if month == 12:
                    result = datetime(year, 12, 31)
                else:
                    next_month = datetime(year, month + 1, 1)
                    result = next_month - pd.DateOffset(days=1)

    return result


def add_period(transaction_df):
    transaction_df['closing_date']
    transaction_df['period_start'] = transaction_df['closing_date'].apply(lambda x: get_period(x, mode='start'))
    transaction_df['period_end'] = transaction_df['closing_date'].apply(lambda x: get_period(x, mode='end'))

    return transaction_df


def add_listing_paid_amount(transaction_df):
    global na_filler
    transaction_df.rename({'listing_paid_amount': 'listing_paid_amount_1'}, axis=1, inplace=True)
    transaction_df['listing_paid_amount'] = transaction_df.apply(lambda x: x['listing_paid_amount_1'] if ((x['listing_paid_date'] != na_filler) & (x['listing_paid_date'] >= x['period_start']) & (x['listing_paid_date'] <= x['period_end'])) else 0, axis=1)
    transaction_df.drop(columns='listing_paid_amount_1', inplace=True)

    return transaction_df


def add_ctc_paid_amount(transaction_df):
    global na_filler
    transaction_df.rename({'ctc_paid_amount': 'ctc_paid_amount_1'}, axis=1, inplace=True)
    transaction_df['ctc_paid_amount'] = transaction_df.apply(lambda x: x['ctc_paid_amount_1'] if ((x['ctc_paid_date'] != na_filler) & (x['ctc_paid_date'] >= x['period_start']) & (x['ctc_paid_date'] <= x['period_end'])) else 0, axis=1)
    transaction_df.drop(columns='ctc_paid_amount_1', inplace=True)

    return transaction_df


def add_compliance_paid_amount(transaction_df):
    global na_filler
    transaction_df.rename({'compliance_paid_amount': 'compliance_paid_amount_1'}, axis=1, inplace=True)
    transaction_df['compliance_paid_amount'] = transaction_df.apply(lambda x: x['compliance_paid_amount_1'] if ((x['compliance_paid_date'] != na_filler) & (x['compliance_paid_date'] >= x['period_start']) & (x['compliance_paid_date'] <= x['period_end'])) else 0, axis=1)
    transaction_df.drop(columns='compliance_paid_amount_1', inplace=True)

    return transaction_df


def transform_transaction_source(transaction_df):
    print('Transforming transaction data source...', end='')
    # try:
    global na_filler
    transaction_df['closing_date'] = pd.to_datetime(transaction_df['closing_date'])
    transaction_df['closing_date'].fillna(na_filler, inplace=True)
    transaction_df['listing_paid_date'] = pd.to_datetime(transaction_df['listing_paid_date'])
    transaction_df['listing_paid_date'].fillna(na_filler, inplace=True)
    transaction_df['ctc_paid_date'] = pd.to_datetime(transaction_df['ctc_paid_date'])
    transaction_df['ctc_paid_date'].fillna(na_filler, inplace=True)
    transaction_df['offer_prep_paid_date'] = pd.to_datetime(transaction_df['offer_prep_paid_date'])
    transaction_df['offer_prep_paid_date'].fillna(na_filler, inplace=True)
    transaction_df['compliance_paid_date'] = pd.to_datetime(transaction_df['compliance_paid_date'])
    transaction_df['compliance_paid_date'].fillna(na_filler, inplace=True)
    transaction_df['listing_started_with_empower'] = pd.to_datetime(transaction_df['listing_started_with_empower'])
    transaction_df['listing_started_with_empower'].fillna(na_filler, inplace=True)
    transaction_df['offer_started_with_empower'] = pd.to_datetime(transaction_df['offer_started_with_empower'])
    transaction_df['offer_started_with_empower'].fillna(na_filler, inplace=True)
    transaction_df['compliance_started_with_empower'] = pd.to_datetime(transaction_df['compliance_started_with_empower'])
    transaction_df['compliance_started_with_empower'].fillna(na_filler, inplace=True)

    transaction_df = add_tc_commission_rate(transaction_df)
    transaction_df = add_period(transaction_df)
    transaction_df = add_listing_paid_amount(transaction_df)
    transaction_df = add_ctc_paid_amount(transaction_df)
    transaction_df['ctc_projection'] = transaction_df['closing_date'].apply(lambda x: 0 if x == na_filler else 1)
    transaction_df['listing_projection'] = transaction_df.apply(lambda x: 1 if (x['listing_started_with_empower'] != na_filler and x['listing_started_with_empower'] >= x['period_start'] and x['listing_started_with_empower'] <= x['period_end']) else 0, axis=1)
    transaction_df['offer_projection'] = transaction_df.apply(lambda x: 1 if (x['offer_started_with_empower'] != na_filler and x['offer_started_with_empower'] >= x['period_start'] and x['offer_started_with_empower'] <= x['period_end']) else 0, axis=1)
    transaction_df['compliance_projection'] = transaction_df.apply(lambda x: 1 if (x['compliance_started_with_empower'] != na_filler and x['compliance_started_with_empower'] >= x['period_start'] and x['compliance_started_with_empower'] <= x['period_end']) else 0, axis=1)
    transaction_df['projection_condition'] = transaction_df.apply(lambda x: 1 if (x['ctc_projection'] == 1 or x['listing_projection'] == 1 or x['offer_projection'] == 1 or x['compliance_projection'] == 1) else 0, axis=1)
    transaction_df['tc_revenue'] = transaction_df['listing_paid_amount'] + transaction_df['ctc_paid_amount'] + transaction_df['offer_prep_paid_amount'] + transaction_df['compliance_paid_amount']

    transaction_df.to_excel('tc_payroll.xlsx', sheet_name='transaction_source', index=False)

    print('Done')
    return transaction_df

    # #     # Filter for current year only
    #     current_year = datetime.now().year
    #     current_month = datetime.now().month
    #     df = df[
    #         (df["closing_date"].dt.year == current_year)
    #         & (df["closing_date"].dt.month <= current_month)
    #     ]

    #     specific_teams = [
    #         "Team Christianna Velazquez",
    #         "Team Kimberly Lewis",
    #         "Team Stephanie Kleinman",
    #         "Team Molly Kelley",
    #         "Jenn McKinley",
    #         "Team Jenn McKinley",
    #     ]

    #     filtered_df = df[df["team_name"].isin(specific_teams)]

    #     print(filtered_df)
        # # Group by month and count
        # monthly_counts = filtered_df.groupby(
        #     filtered_df["closing_date"].dt.to_period("M")
        # ).size()

    #     # Create the summary dictionary with all months
    #     summary = {"state": "CTC - Closing"}
    #     current_year = datetime.now().year
    #     current_month = datetime.now().month

    #     for month in range(1, current_month + 1):
    #         month_name = calendar.month_abbr[month]
    #         summary[f"{month_name} {current_year}"] = 0

    #     # Update the summary with actual counts
    #     for month, count in monthly_counts.items():
    #         month_name = calendar.month_abbr[month.month]
    #         summary[f"{month_name} {current_year}"] = int(count)

    #     return summary

    # except Exception as e:
    #     print(f"Error processing data: {e}")
    #     return None

    # finally:
    #     pass
        # conn.close()


# def generate_payroll_report(enriched_transaction_df):
#     print('Generating report...', end='')
#     try:

#         print('Done')
#         return payroll_report_df

#     except Exception as e:
#         print(f"Error processing data: {e}")
#         return None

#     finally:
#         pass


def load_payroll_report(payroll_report_df):
    pass


if __name__ == "__main__":
    script_start_time = time.time()

    # transaction_df = extract_transaction_source('../all_properties.parquet')
    transaction_df = pd.read_csv('transaction_source.csv')

    enriched_transaction_df = transform_transaction_source(transaction_df)

    # payroll_report_df = generate_payroll_report(enriched_transaction_df)

    # load_payroll_report(payroll_report_df)

    # sheet_id = create_google_sheet(all_summaries)

    script_end_time = time.time()
    total_execution_time = script_end_time - script_start_time
    print(f"Total script execution time: {total_execution_time:.2f} seconds")