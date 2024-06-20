from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from etl_script import load_s3, load_redshift

default_args = {
    'owner': 'hospital_admissions',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

import pandas as pd
import boto3
from io import BytesIO
import psycopg2
from datetime import datetime
from dateutil.parser import parse
import logging
import cred


def extract_transform():
        
    # Read the CSV 
    adm_df = pd.read_csv('Admission Data.csv')
    mort_df = pd.read_csv('Mortality Data.csv')
    poll_df = pd.read_csv('Pollution Data.csv')

    ## Print the resulting DataFrame
    # print(adm_df.head(50))
    # print(mort_df.head(50))
    # print(poll_df.head(50))
        
    ## Check for missing values and drop them

    # adm_df.isnull().sum()

    # mort_df.isnull().sum()

    # poll_df.isnull().sum()


    adm_df.fillna(0, inplace=True)


    poll_df = poll_df.dropna()



    # # Identify duplicate rows
    # duplicateRows = adm_df[adm_df.duplicated()]
    # print(duplicateRows)
    # duplicateRows = mort_df[adm_df.duplicated()]
    # print(duplicateRows)
    # duplicateRows = adm_df[adm_df.duplicated()]
    # print(duplicateRows)



    # Changing column to date format


    def time_format(df: pd.DataFrame, col: str) -> pd.DataFrame:
        def parse_with_dayfirst(date_str):
            if pd.isna(date_str):
                return pd.NaT
            try:
                # Try parsing with month first (default behavior)
                return parse(date_str)
            except ValueError:
                try:
                    # If month first fails, try with day first
                    return parse(date_str, dayfirst=True)
                except ValueError:
                    # If both fail, return NaT
                    return pd.NaT

        # Apply the parsing function and ensure the result is datetime
        df[col] = pd.to_datetime(df[col].apply(parse_with_dayfirst), errors='coerce')

        # Identify rows with invalid dates
        invalid_dates = df[df[col].isna()]
        if not invalid_dates.empty:
            print(f"Warning: {len(invalid_dates)} rows have invalid dates in column {col}.")
            # print(invalid_dates)

        # Check if all values are NaT
        if df[col].isna().all():
            # print(f"Error: All values in column {col} are invalid dates.")
            
            # 1. Fill with a default date
            df[col] = pd.to_datetime('2000-01-01')
            # 2. Fill with the current date
            # df[col] = pd.to_datetime(datetime.now().date())
            # 3. Or just return the DataFrame with NaT values
            return df

        # Convert datetime values to strings in the desired format
        df[col] = df[col].dt.strftime('%Y-%m-%d')

        return df




    adm_df = time_format(adm_df, 'D.O.A')
    adm_df = time_format(adm_df, 'D.O.D')
    adm_df = adm_df.dropna()

    adm_values_to_drop = [3253, 4053, 4357, 4572, 4622, 10165]
    adm_df = adm_df.drop(adm_df[adm_df['SNO'].isin(adm_values_to_drop)].index)

    # # print(adm_df.head(50))

    mort_df = time_format(mort_df, 'DATE OF BROUGHT DEAD')

    # # print(mort_df.head(50))

    poll_df = time_format(poll_df, 'DATE')
    poll_values_to_drop = ['2018-11-16', '2019-01-23']
    poll_df = poll_df.drop(poll_df[poll_df['DATE'].isin(poll_values_to_drop)].index)
    # # print(poll_df.head(50))

    adm_df.isnull().sum()

    # mort_df.isnull().sum()

    # poll_df.isnull().sum()

    # Preprocessed file

    return adm_df.to_csv('clean/admission_data.csv', index=False, header=True, sep=','), mort_df.to_csv('clean/mortality_data.csv', index=False, header=True, sep=','), poll_df.to_csv('clean/pollution_data.csv', index=False, header=True, sep=',')


with DAG('hosp_pipeline', default_args=default_args, schedule_interval=None) as dag:

    extract_transform_task = PythonOperator(
        task_id='extract_transform_data',
        python_callable=extract_transform
    )

    load_s3_task = PythonOperator(
        task_id='load_s3',
        python_callable=load_s3
    )

    load_redshift_task = PythonOperator(
        task_id='load_redshift',
        python_callable=load_redshift
    )

    extract_transform_task >> load_s3_task >> load_redshift_task