'''
Final Project - Data Engineer
This script is designed to be run in an Apache Airflow environment.
The dataset is fetched from a PostgreSQL database, cleaned, and then indexed into an Elasticsearch instance.
The objective of the project is to optimize the Ads Spend for better marketing strategies.
'''

# Libraries Import
import pandas as pd
import psycopg2 as db
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch, exceptions
from datetime import datetime, timedelta

# Fetching Data from PostgreSQL

def fetch_from_postgres():
    '''
    Connects to the PostgreSQL database running in the Docker container.
    The connection string functions as a placeholder and should be replaced with the actual connection string.
    Fetches the data from the table 'table_m3' and saves it as a CSV file.
    '''

    conn_string = "dbname='postgres' user='airflow' host='postgres' password='airflow' port='5432'"
    conn = db.connect(conn_string)
    df = pd.read_sql("SELECT * FROM table_m3", conn)
    df.to_csv('/opt/airflow/dags/raw_data.csv', index=False)

def clean_data():
    '''
    Processes the raw data fetched from PostgreSQL.
    Cleans the data by removing duplicates, handling missing values, and normalizing column names.
    The cleaned data is saved as a CSV file for further processing and saved to the path where the DAG is located. 
    '''
    df = pd.read_csv('/opt/airflow/dags/raw_data.csv')

    # Normalize column names
    def normalize_column(col):
        '''
        Normalizes column names by stripping whitespace, converting to lowercase, using underscores instead of spaces.
        '''
        col = str(col).strip().lower().replace(' ', '_')
        col = ''.join(e for e in col if e.isalnum() or e == '_')
        return col
    df.columns = [normalize_column(col) for col in df.columns]


    # Create new column `transaction_date_update` from extracted Year-Month from the `transaction_date` column
    df['transaction_date'] = pd.to_datetime(df['transaction_date'], format='%Y-%m-%d')
    df['transaction_date_update'] = df['transaction_date'].dt.strftime('%b-%Y')  # e.g., 'Jan-2023'

    # Create new column `profit` from `revenue` and `ad_spend`
    df['profit'] = df['revenue'] - df['ad_spend']

    # Create new column `roi` from `profit` and `ad_spend`
    df['roi'] = (df['profit'] / df['ad_spend']) * 100  # ROI in percentage

    # Drop `transaction_date` columns
    df.drop(columns=['transaction_date'], inplace=True, errors='ignore')

    # Drop duplicates and missing values
    df.drop_duplicates(inplace=True)
    df.dropna(inplace=True)

    # Save cleaned data to CSV
    df.to_csv('/opt/airflow/dags/cleaned_data.csv', index=False)

def post_to_elasticsearch():
    '''
    Connects to the Elasticsearch instance running in the Docker container.
    The connection string functions as a placeholder and should be replaced with the actual connection string.
    Waits for the Elasticsearch instance to become available before proceeding into the next step.
    Reads the cleaned data from the CSV file and indexes it into Elasticsearch.
    The data is indexed into the 'bike_sales' index, with each record being identified by its unique transaction_id so that there will be no duplicates.
    '''
    es = Elasticsearch('http://elasticsearch:9200')

    # Wait for ES to become available
    for i in range(10):
        try:
            if es.ping():
                print('Connected to Elasticsearch!')
                break
        except exceptions.ConnectionError:
            print('Waiting for Elasticsearch to be ready...')
        time.sleep(5)
    else:
        raise Exception('Elasticsearch is not reachable after waiting.')

    df = pd.read_csv('/opt/airflow/dags/cleaned_data.csv')
    for _, r in df.iterrows():
        doc = r.to_dict()
        es.index(index='final_project', id=doc['transaction_id'], body=doc)


default_args = {
    'owner': 'nadhira', # The person responsible for the DAG
    'retries': 1, # Number of retries in case of failure
    'retry_delay': timedelta(minutes=1) # Delay between retries
}

with DAG(
    dag_id='P2M3_nadhira_gunawan_DAG', # Identifier for the DAG
    default_args=default_args, # Default parameter for the DAG
    start_date=datetime(2024, 11, 1, 9, 10) - timedelta(hours=7), # Start date of the DAG, which is 11 November 2024 at 09:10 AM WIB(GMT+7)
    schedule_interval='10-30/10 9 * * 6',  # Every Saturday from 09:10 to 09:30, every 10 min
    catchup=False, # Fill the gap between the start date and the current date
    max_active_runs=10, # Maximum number of active runs to avoid overloading the system that could cause the system to crash
) as dag:
    # Define the tasks in the DAG

    # Fetch Data from PostgreSQL
    task_1 = PythonOperator(
        task_id='fetch_from_postgresql',
        python_callable=fetch_from_postgres
    )

    # Clean Data
    task_2 = PythonOperator(
        task_id='data_cleaning',
        python_callable=clean_data
    )

    # Post Data to Elasticsearch
    task_3 = PythonOperator(
        task_id='post_to_elasticsearch',
        python_callable=post_to_elasticsearch
    )

    # Set the task dependencies, the task will run sequentially and in order
    task_1 >> task_2 >> task_3

