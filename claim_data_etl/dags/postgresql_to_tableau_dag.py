from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException

from datetime import datetime, timedelta
import logging
import pandas as pd
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, Inserter
import tableauserverclient as TSC
from typing import List, Dict, Any, Optional
import gc
import os
import subprocess

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

dag = DAG(
    'postgres_to_tableau_etl',
    default_args=default_args,
    description='Production ETL process from Postgres to Tableau Server',
    catchup=False,
    tags=['postgres', 'tableau', 'etl'],
)

def get_column_types():
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgresql_local')
        schema_query = """
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = 'nmc' 
        AND table_name = 'tmp_2023_claims'
        ORDER BY ordinal_position
        """
        return {row[0]: row[1] for row in pg_hook.get_records(schema_query)}
    except Exception as e:
        logger.error(f"Error getting column types: {str(e)}")
        raise AirflowException(f"Failed to get column types: {str(e)}")

def get_hyper_type(pg_type: str):
    type_map = {
        'integer': SqlType.int(),
        'bigint': SqlType.big_int(),
        'numeric': SqlType.double(),
        'double precision': SqlType.double(),
        'real': SqlType.double(),
        'timestamp without time zone': SqlType.timestamp(),
        'timestamp with time zone': SqlType.timestamp_tz(),
        'date': SqlType.date(),
        'boolean': SqlType.bool(),
        'character varying': SqlType.text(),
        'text': SqlType.text(),
        'uuid': SqlType.text(),
        'json': SqlType.text(),
        'jsonb': SqlType.text()
    }
    return type_map.get(pg_type.lower(), SqlType.text())

def process_chunk(chunk_df: pd.DataFrame, inserter: Inserter):
    try:
        records = chunk_df.replace({pd.NA: None}).values.tolist()
        for record in records:
            inserter.add_row(record)
        del records
        gc.collect()
    except Exception as e:
        logger.error(f"Error processing chunk: {str(e)}")
        raise AirflowException(f"Failed to process data chunk: {str(e)}")

def extract_and_load(**context):
    CHUNK_SIZE = 50000
    pg_hook = PostgresHook(postgres_conn_id='postgresql_local')
    
    # Set up directories with proper permissions
    base_dir = os.path.expanduser('~/airflow/tableau_extracts')
    temp_dir = os.path.join(base_dir, 'temp')
    log_dir = os.path.join(base_dir, 'logs')
    
    # Create directories with proper permissions
    for d in [base_dir, temp_dir, log_dir]:
        if not os.path.exists(d):
            os.makedirs(d, mode=0o755)
        os.chmod(d, 0o755)
    
    try:
        # Set environment variables for Hyper
        os.environ['TAB_SDK_LOGDIR'] = log_dir
        os.environ['TAB_SDK_TMPDIR'] = temp_dir
        
        column_types = get_column_types()
        count_query = "SELECT COUNT(*) FROM nmc.tmp_2023_claims"
        total_rows = pg_hook.get_first(count_query)[0]
        logger.info(f"Total rows to process: {total_rows}")
        
        extract_file = os.path.join(base_dir, 'claims_extract.hyper')
        
        if os.path.exists(extract_file):
            os.remove(extract_file)
        
        table_def = TableDefinition(
            table_name="Extract",
            columns=[
                TableDefinition.Column(name, get_hyper_type(dtype), nullability=NULLABLE)
                for name, dtype in column_types.items()
            ]
        )
        
        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            with Connection(endpoint=hyper.endpoint,
                          database=extract_file,
                          create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
                
                connection.catalog.create_table(table_def)
                
                with Inserter(connection, table_def) as inserter:
                    for offset in range(0, total_rows, CHUNK_SIZE):
                        query = f"""
                        SELECT *
                        FROM nmc.tmp_2023_claims
                        ORDER BY id
                        LIMIT {CHUNK_SIZE} OFFSET {offset}
                        """
                        chunk_df = pg_hook.get_pandas_df(query)
                        process_chunk(chunk_df, inserter)
                        logger.info(f"Processed rows {offset} to {offset + len(chunk_df)}")
                        del chunk_df
                        gc.collect()

        logger.info("Successfully created Hyper extract")

        # Copy to Tableau server
        tableau_server = "10.8.55.135"
        tableau_user = "tabuser"
        remote_path = "/tableau_extracts/claims_extract.hyper"
        
        copy_command = f"scp {extract_file} {tableau_user}@{tableau_server}:{remote_path}"
        result = subprocess.run(copy_command, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise AirflowException(f"Failed to copy file to Tableau server: {result.stderr}")
        
        logger.info(f"Extract copied to Tableau server at {remote_path}")
        
        if os.path.exists(extract_file):
            os.remove(extract_file)
            logger.info("Cleaned up local extract file")
        
        return remote_path
        
    except Exception as e:
        logger.error(f"Error in extract_and_load: {str(e)}")
        if os.path.exists(extract_file):
            try:
                os.remove(extract_file)
                logger.info("Cleaned up local extract file after error")
            except Exception as cleanup_error:
                logger.error(f"Failed to clean up extract file: {cleanup_error}")
        raise AirflowException(f"Extract and load process failed: {str(e)}")

def publish_to_tableau(hyper_file_path: str, **context):
    try:
        tableau_auth = TSC.TableauAuth(
            'tabuser',
            'tabuser@nmc',
            'Development'
        )
        
        server_url = 'http://10.8.55.135'
        
        with TSC.Server(server_url, use_server_version=True) as server:
            server.auth.sign_in(tableau_auth)
            
            all_projects = list(TSC.Pager(server.projects))
            project = next((p for p in all_projects if p.name == 'DataSources'), None)
            
            if not project:
                raise ValueError("Project 'DataSources' not found")
            
            datasource = TSC.DatasourceItem(project.id)
            datasource.name = 'claims'
            
            logger.info("Publishing to project: DataSources")
            datasource = server.datasources.publish(
                datasource,
                hyper_file_path,
                mode=TSC.Server.PublishMode.Overwrite
            )
            logger.info(f"Successfully published datasource. ID: {datasource.id}")
            
    except Exception as e:
        logger.error(f"Error in publish_to_tableau: {str(e)}")
        raise AirflowException(f"Publishing to Tableau Server failed: {str(e)}")

extract_and_load_task = PythonOperator(
    task_id='extract_and_load',
    python_callable=extract_and_load,
    provide_context=True,
    dag=dag,
)

publish_task = PythonOperator(
    task_id='publish_to_tableau',
    python_callable=publish_to_tableau,
    provide_context=True,
    op_kwargs={'hyper_file_path': "{{ task_instance.xcom_pull(task_ids='extract_and_load') }}"},
    dag=dag,
)

extract_and_load_task >> publish_task
