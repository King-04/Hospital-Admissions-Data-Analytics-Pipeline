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
    adm_df = pd.read_csv('source_data/Admission Data.csv')
    mort_df = pd.read_csv('source_data/Mortality Data.csv')
    poll_df = pd.read_csv('source_data/Pollution Data.csv')

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

def load_s3():
        
    # Persist data in S3 bucket
    s3 = boto3.client('s3')
    bucket_name = cred.bucket_name
    file_name = ["clean/admission_data.csv", "clean/mortality_data.csv", "clean/pollution_data.csv"]

    for local_file in file_name:
        s3.upload_file(local_file, bucket_name, local_file)
        print(f"Uploaded {local_file} to {bucket_name}")

# # Getting column names to build table

# column_names = list(adm_df.columns)
# for column in column_names:
#     print(column)

# column_names = list(mort_df.columns)
# for column in column_names:
#     print(column)

# column_names = list(poll_df.columns)
# for column in column_names:
#     print(column)

def load_redshift():
        
    # Redshift Connection
    try:
        con = psycopg2.connect(dbname=cred.dbname,
                            host=cred.host,
                            port=cred.port,
                            user=cred.user,
                            password=cred.password)
        logging.info('Redshift connection succeeded')
    except Exception as e:
        logging.exception(e)

    con.autocommit = True
    cur = con.cursor()

    # Load data to redshift
    def load_data_to_redshift(schema_name, table_name, s3_location):
        """
        Loads data from an S3 location into a Redshift table.

        Args:
            schema_name (str): Name of the Redshift schema.
            table_name (str): Name of the target table.
            s3_location (str): S3 location of the CSV file.

        Returns:
            None
        """
        try:
            copy_query = f"""
                COPY {schema_name}.{table_name}
                FROM '{s3_location}'
                IAM_ROLE '{cred.IAM_ROLE}'
                CSV
                IGNOREHEADER 1;
            """
            cur.execute(copy_query)
            print(f"Data loaded into {schema_name}.{table_name} successfully.")
        except psycopg2.Error as e:
            print(f"Error loading data into {schema_name}.{table_name}: {e}")


    schema_name = 'hospital_data'
    load_data_to_redshift(schema_name, 'admission_data', cred.s3_admis)
    load_data_to_redshift(schema_name, 'mortality_data', cred.s3_mort)
    load_data_to_redshift(schema_name, 'pollution_data', cred.s3_poll)
