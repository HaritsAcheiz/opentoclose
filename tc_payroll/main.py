import duckdb
import time
import json
import pandas as pd
from datetime import datetime, date
import csv
from openpyxl import load_workbook

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


def update_agent_provided_by(transaction_df, agents_file_path):
    try:
        agents_df = pd.read_csv(agents_file_path, usecols=['Title', 'Agent Provided by'])
        transaction_df = transaction_df.merge(agents_df, how='left', left_on='empower_agent_name', right_on='Title')
        transaction_df.drop(columns=['agent_provided_by', 'Title'], inplace=True)
        transaction_df.rename(columns={'Agent Provided by': 'agent_provided_by'}, inplace=True)

        print('Done')
        return transaction_df

    except Exception as e:
        print(f"Error processing data: {e}")
        return None


def add_tc_commission_rate(transaction_df):
    transaction_df.rename({'tc_commission_rate': 'tc_commission_rate_1'}, axis=1, inplace=True)
    transaction_df['tc_commission_rate'] = transaction_df.apply(lambda x: 70/100 if x['agent_provided_by'] == 'TC' else x['tc_commission_rate_1']/100, axis=1)
    transaction_df.drop(columns='tc_commission_rate_1', inplace=True)

    return transaction_df


def get_period(selected_date, mode):
    if isinstance(selected_date, str):
        try:
            selected_date = pd.to_datetime(selected_date)
        except ValueError:
            print("Invalid date format")
            selected_date = None
    if pd.isna(selected_date):
        result = selected_date
    else:
        year = selected_date.year
        month = selected_date.month
        day = selected_date.day
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
    transaction_df['closing_period_start'] = transaction_df['closing_date'].apply(lambda x: get_period(x, mode='start'))
    transaction_df['closing_period_end'] = transaction_df['closing_date'].apply(lambda x: get_period(x, mode='end'))
    transaction_df['closing_periode'] = transaction_df.apply(lambda x: x['closing_period_start'].strftime('%Y %m %d').upper() + ' - ' + x['closing_period_end'].strftime('%Y %m %d').upper(), axis=1)

    transaction_df['listing_period_start'] = transaction_df['listing_paid_date'].apply(lambda x: get_period(x, mode='start'))
    transaction_df['listing_period_end'] = transaction_df['listing_paid_date'].apply(lambda x: get_period(x, mode='end'))
    transaction_df['listing_periode'] = transaction_df.apply(lambda x: x['listing_period_start'].strftime('%Y %m %d').upper() + ' - ' + x['listing_period_end'].strftime('%Y %m %d').upper(), axis=1)

    transaction_df['ctc_period_start'] = transaction_df['ctc_paid_date'].apply(lambda x: get_period(x, mode='start'))
    transaction_df['ctc_period_end'] = transaction_df['ctc_paid_date'].apply(lambda x: get_period(x, mode='end'))
    transaction_df['ctc_periode'] = transaction_df.apply(lambda x: x['ctc_period_start'].strftime('%Y %m %d').upper() + ' - ' + x['ctc_period_end'].strftime('%Y %m %d').upper(), axis=1)

    transaction_df['compliance_period_start'] = transaction_df['compliance_paid_date'].apply(lambda x: get_period(x, mode='start'))
    transaction_df['compliance_period_end'] = transaction_df['compliance_paid_date'].apply(lambda x: get_period(x, mode='end'))
    transaction_df['compliance_periode'] = transaction_df.apply(lambda x: x['compliance_period_start'].strftime('%Y %m %d').upper() + ' - ' + x['compliance_period_end'].strftime('%Y %m %d').upper(), axis=1)

    transaction_df['offer_prep_period_start'] = transaction_df['offer_prep_paid_date'].apply(lambda x: get_period(x, mode='start'))
    transaction_df['offer_prep_period_end'] = transaction_df['offer_prep_paid_date'].apply(lambda x: get_period(x, mode='end'))
    transaction_df['offer_prep_periode'] = transaction_df.apply(lambda x: x['offer_prep_period_start'].strftime('%Y %m %d').upper() + ' - ' + x['offer_prep_period_end'].strftime('%Y %m %d').upper(), axis=1)

    return transaction_df


def add_listing_paid_amount(transaction_df):
    global na_filler
    transaction_df.rename({'listing_paid_amount': 'listing_paid_amount_1'}, axis=1, inplace=True)
    transaction_df['listing_paid_amount'] = transaction_df.apply(lambda x: x['listing_paid_amount_1'] if ((x['listing_paid_date'] != na_filler) & (x['listing_paid_date'] >= x['listing_period_start']) & (x['listing_paid_date'] <= x['listing_period_end'])) else 0, axis=1)
    transaction_df.drop(columns='listing_paid_amount_1', inplace=True)

    return transaction_df


def add_ctc_paid_amount(transaction_df):
    global na_filler
    transaction_df.rename({'ctc_paid_amount': 'ctc_paid_amount_1'}, axis=1, inplace=True)
    transaction_df['ctc_paid_amount'] = transaction_df.apply(lambda x: x['ctc_paid_amount_1'] if ((x['ctc_paid_date'] != na_filler) & (x['ctc_paid_date'] >= x['ctc_period_start']) & (x['ctc_paid_date'] <= x['ctc_period_end'])) else 0, axis=1)
    transaction_df.drop(columns='ctc_paid_amount_1', inplace=True)

    return transaction_df


def add_compliance_paid_amount(transaction_df):
    global na_filler
    transaction_df.rename({'compliance_paid_amount': 'compliance_paid_amount_1'}, axis=1, inplace=True)
    transaction_df['compliance_paid_amount'] = transaction_df.apply(lambda x: x['compliance_paid_amount_1'] if ((x['compliance_paid_date'] != na_filler) & (x['compliance_paid_date'] >= x['compliance_period_start']) & (x['compliance_paid_date'] <= x['compliance_period_end'])) else 0, axis=1)
    transaction_df.drop(columns='compliance_paid_amount_1', inplace=True)

    return transaction_df


def add_offer_prep_paid_amount(transaction_df):
    global na_filler
    transaction_df.rename({'offer_prep_paid_amount': 'offer_prep_paid_amount_1'}, axis=1, inplace=True)
    transaction_df['offer_prep_paid_amount'] = transaction_df.apply(lambda x: x['offer_prep_paid_amount_1'] if ((x['offer_prep_paid_date'] != na_filler) & (x['offer_prep_paid_date'] >= x['offer_prep_period_start']) & (x['offer_prep_paid_date'] <= x['offer_prep_period_end'])) else 0, axis=1)
    transaction_df.drop(columns='offer_prep_paid_amount_1', inplace=True)

    return transaction_df


def add_projected_amount(transaction_df):
    transaction_df['projected_amount'] = transaction_df.apply(lambda x: x['tc_commission_amount'] if ((x['closing_date'] != na_filler) & (x['closing_date'] >= x['closing_period_start']) & (x['closing_date'] <= x['closing_period_end'])) else 0, axis=1)
    return transaction_df


def add_actual_amount(transaction_df):
    transaction_df['actual_amount'] = transaction_df.apply(lambda x: x['tc_revenue'] * x['tc_commission_rate'] if ((x['ctc_paid_date'] != na_filler) & (x['ctc_paid_date'] >= x['ctc_period_start']) & (x['ctc_paid_date'] <= x['ctc_period_end'])) else 0, axis=1)
    return transaction_df


def add_tc_revenue_amount(transaction_df):
    transaction_df['tc_revenue_amount'] = transaction_df.apply(lambda x: x['tc_revenue'] if ((x['ctc_paid_date'] != na_filler) & (x['ctc_paid_date'] >= x['ctc_period_start']) & (x['ctc_paid_date'] <= x['ctc_period_end'])) else 0, axis=1)
    return transaction_df


def transform_transaction_source(transaction_df):
    print('Transforming transaction data source...', end='')

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

    transaction_df = update_agent_provided_by(transaction_df, 'agent_sources.csv')

    transaction_df = add_tc_commission_rate(transaction_df)
    transaction_df = add_period(transaction_df)
    transaction_df = add_listing_paid_amount(transaction_df)
    transaction_df = add_ctc_paid_amount(transaction_df)
    transaction_df = add_compliance_paid_amount(transaction_df)
    transaction_df = add_offer_prep_paid_amount(transaction_df)
    transaction_df['ctc_projection'] = transaction_df['closing_date'].apply(lambda x: 0 if x == na_filler else 1)
    transaction_df['listing_projection'] = transaction_df.apply(lambda x: 1 if (x['listing_started_with_empower'] != na_filler and x['listing_started_with_empower'] >= x['listing_period_start'] and x['listing_started_with_empower'] <= x['listing_period_end']) else 0, axis=1)
    transaction_df['offer_projection'] = transaction_df.apply(lambda x: 1 if (x['offer_started_with_empower'] != na_filler and x['offer_started_with_empower'] >= x['offer_prep_period_start'] and x['offer_started_with_empower'] <= x['offer_prep_period_end']) else 0, axis=1)
    transaction_df['compliance_projection'] = transaction_df.apply(lambda x: 1 if (x['compliance_started_with_empower'] != na_filler and x['compliance_started_with_empower'] >= x['compliance_period_start'] and x['compliance_started_with_empower'] <= x['compliance_period_end']) else 0, axis=1)
    transaction_df['projection_condition'] = transaction_df.apply(lambda x: 1 if (x['ctc_projection'] == 1 or x['listing_projection'] == 1 or x['offer_projection'] == 1 or x['compliance_projection'] == 1) else 0, axis=1)
    transaction_df['tc_revenue'] = transaction_df[['listing_paid_amount', 'ctc_paid_amount', 'offer_prep_paid_amount', 'compliance_paid_amount']].sum(axis=1)
    transaction_df = add_projected_amount(transaction_df)
    transaction_df = add_actual_amount(transaction_df)
    transaction_df = add_tc_revenue_amount(transaction_df)

    # transaction_df.to_excel('tc_payroll.xlsx', sheet_name='transaction_source', index=False)

    print('Done')
    return transaction_df


def generate_payroll_report(enriched_transaction_df, mode):
    print('Generating report...', end='')
    try:
        # enriched_transaction_df = pd.read_excel('tc_payroll.xlsx', sheet_name='transaction_source')
        # print(enriched_transaction_df)

        specific_teams = [
            "Christianna Velazquez",
            "Kimberly Lewis",
            "Stephanie Kleinman",
            "Molly Kelley",
            "Jenn McKinley"
        ]

        selected_team_transaction_df = enriched_transaction_df[enriched_transaction_df["empower_tc_name"].isin(specific_teams)]
        if mode == 'p':
            values_name = 'projected_amount'
            columns_name = 'closing_periode'
        elif mode == 'a':
            values_name = 'actual_amount'
            columns_name = 'ctc_periode'
        else:
            print("Mode is undefined!")
        summary_df = selected_team_transaction_df.pivot_table(values=values_name, index='empower_tc_name', columns=columns_name, aggfunc='sum', fill_value=0)
        print(summary_df)
        summary_df['Total Result'] = summary_df.sum(axis=1)
        summary_df.loc['Total Result'] = summary_df.sum()
        summary_df.reset_index(inplace=True)

        print('Done')
        return summary_df

    except Exception as e:
        print(f"Error processing data: {e}")
        return None

    finally:
        pass


def load_payroll_report(payroll_report_df):
    pass


if __name__ == "__main__":
    script_start_time = time.time()

    # transaction_df = extract_transaction_source('../all_properties.parquet')
    transaction_df = pd.read_csv('transaction_source.csv')
    enriched_transaction_df = transform_transaction_source(transaction_df)
    projected_payroll_report_df = generate_payroll_report(enriched_transaction_df, mode='p')
    actual_payroll_report_df = generate_payroll_report(enriched_transaction_df, mode='a')

    path = 'tc_payroll.xlsx'
    with pd.ExcelWriter(path, engine='openpyxl') as writer:
        enriched_transaction_df.to_excel(writer, sheet_name='Transaction Source', index=False)
        projected_payroll_report_df.to_excel(writer, sheet_name='TC Payroll Consolidated Projected')
        actual_payroll_report_df.to_excel(writer, sheet_name='TC Payroll Consolidated Actual')

    # load_payroll_report(payroll_report_df)

    # sheet_id = create_google_sheet(all_summaries)

    script_end_time = time.time()
    total_execution_time = script_end_time - script_start_time
    print(f"Total script execution time: {total_execution_time:.2f} seconds")