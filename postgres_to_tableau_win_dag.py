from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import tableauhyperapi as hyper
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def extract_from_postgres_to_tableau():
    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgresql_130')

    # Your PostgreSQL query
    query = """
    SELECT *
    FROM nmc.f_patient_encounter
    WHERE visit_date >= CURRENT_DATE - INTERVAL '70 days'
    """

    # Execute query and load results into a pandas DataFrame
    df = pg_hook.get_pandas_df(query)

    # Define the path for the Hyper file
    hyper_file_path = '/path/to/your/extract.hyper'

    # Ensure the directory exists
    os.makedirs(os.path.dirname(hyper_file_path), exist_ok=True)

    # Create Hyper file
    with hyper.HyperProcess(telemetry=hyper.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper_process:
        with hyper.Connection(hyper_process.endpoint,
                              hyper_file_path,
                              create_mode=hyper.CreateMode.CREATE_AND_REPLACE) as connection:
            # Create table definition
            table_def = hyper.TableDefinition(
                table_name="Extract",
                columns=[
                    # Add columns based on your DataFrame structure
                    # For example:
                    hyper.TableDefinition.Column('column1', hyper.SqlType.text()),
                    hyper.TableDefinition.Column('column2', hyper.SqlType.int()),
                    # Add more columns as needed
                ]
            )

            # Create the table in the Hyper file
            connection.catalog.create_table(table_def)

            # Insert data into the table
            with connection.execute_insert(table_def) as insert:
                for _, row in df.iterrows():
                    insert.add_row([row['column1'], row['column2']])  # Adjust column names as needed

    print(f"Tableau extract created successfully at {hyper_file_path}")


with DAG('postgres_to_tableau_extract_local',
         default_args=default_args,
         description='Extract data from PostgreSQL and create Tableau extract locally',
         schedule_interval=timedelta(days=1),
         catchup=False) as dag:
    create_extract = PythonOperator(
        task_id='create_tableau_extract',
        python_callable=extract_from_postgres_to_tableau,
    )

create_extract