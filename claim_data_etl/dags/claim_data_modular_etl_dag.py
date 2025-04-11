from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable, Connection
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.utils.email import send_email
from pyspark.sql import SparkSession

import logging
import pandas as pd
from datetime import datetime, timedelta
import os
import pendulum
from typing import Dict, Any, List
import json
import tempfile
import socket
import subprocess
import sys
import zipfile
import io

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Define the DAG
dag = DAG(
    'claim_data_modular_etl',
    default_args=default_args,
    description='Modular ETL process for claim data with separate extraction and transformation steps',
    schedule=None,  # Manual trigger (can be changed to a schedule if needed)
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
    tags=['postgres', 'etl', 'spark', 'claim_data'],
    params={
        'from_date': Param('2025-01-01', type='string', description='Start date for data extraction (YYYY-MM-DD)'),
        'to_date': Param('2025-01-31', type='string', description='End date for data extraction (YYYY-MM-DD)'),
        'center_id': Param('', type='string', description='Center ID filter (leave blank for all centers)'),
        'email_to': Param('', type='string', description='Email recipient(s) for CSV export (comma-separated)'),
    },
)

# Function to detect if running on local or remote environment
def is_local_environment():
    """Determine if we're running on local or remote environment"""
    hostname = socket.gethostname()
    # Assume we're local if the hostname matches the local machine name
    # Adjust this logic if needed
    return "manjunath" in hostname.lower()

# Function to initialize database connections from Airflow connections
def get_connection_params(**context):
    """
    Get connection parameters from Airflow connections and return them as a dictionary
    to be stored in XCom for other tasks to use.
    """
    try:
        # Get source connection details
        source_conn = Connection.get_connection_from_secrets("postgres_source")
        
        # Parse extra field if it exists
        source_schema = "nmc_trng"  # Default to nmc_trng for source
        
        # Try to extract schema from extra field
        if source_conn.extra:
            try:
                extra_dict = source_conn.extra_dejson
                if isinstance(extra_dict, dict):
                    if 'schema' in extra_dict:
                        source_schema = extra_dict['schema']
                    elif 'options' in extra_dict:
                        options_str = extra_dict['options']
                        if 'search_path=' in options_str:
                            import re
                            search_path_match = re.search(r'search_path=(\w+)', options_str)
                            if search_path_match:
                                source_schema = search_path_match.group(1)
            except Exception as e:
                logger.warning(f"Error parsing source extra field: {str(e)}")
                
        logger.info(f"Source connection - Extra field: {source_conn.extra}")
        logger.info(f"Source connection - Extracted schema: {source_schema}")
        
        source_params = {
            "SOURCE_HOST": source_conn.host,
            "SOURCE_DB": source_conn.schema,
            "SOURCE_USER": source_conn.login,
            "SOURCE_PWD": source_conn.password,
            "SOURCE_SCHEMA": source_schema,
            "SOURCE_PORT": source_conn.port or 5432
        }
        
        # If there's a backup/secondary source connection
        try:
            sec_source_conn = Connection.get_connection_from_secrets("postgres_sec_source")
            source_params["SEC_SOURCE_HOST"] = sec_source_conn.host
        except:
            source_params["SEC_SOURCE_HOST"] = source_params["SOURCE_HOST"]
        
        # Get BI source connection details
        try:
            bi_source_conn = Connection.get_connection_from_secrets("postgres_bi_source")
            bi_source_schema = "nmc"  # Default to nmc for bi_source
            
            # Try to extract schema from extra field for BI source
            if bi_source_conn.extra:
                try:
                    bi_extra_dict = bi_source_conn.extra_dejson
                    if isinstance(bi_extra_dict, dict):
                        if 'schema' in bi_extra_dict:
                            bi_source_schema = bi_extra_dict['schema']
                        elif 'options' in bi_extra_dict:
                            options_str = bi_extra_dict['options']
                            if 'search_path=' in options_str:
                                import re
                                search_path_match = re.search(r'search_path=(\w+)', options_str)
                                if search_path_match:
                                    bi_source_schema = search_path_match.group(1)
                except Exception as e:
                    logger.warning(f"Error parsing BI source extra field: {str(e)}")
            
            logger.info(f"BI Source connection - Extra field: {bi_source_conn.extra}")
            logger.info(f"BI Source connection - Extracted schema: {bi_source_schema}")
            
            source_params["BI_SOURCE_HOST"] = bi_source_conn.host
            source_params["BI_SOURCE_DB"] = bi_source_conn.schema
            source_params["BI_SOURCE_USER"] = bi_source_conn.login
            source_params["BI_SOURCE_PWD"] = bi_source_conn.password
            source_params["BI_SOURCE_SCHEMA"] = bi_source_schema
        except:
            source_params["BI_SOURCE_HOST"] = source_params["SOURCE_HOST"]
            source_params["BI_SOURCE_DB"] = source_params["SOURCE_DB"]
            source_params["BI_SOURCE_USER"] = source_params["SOURCE_USER"]
            source_params["BI_SOURCE_PWD"] = source_params["SOURCE_PWD"]
            source_params["BI_SOURCE_SCHEMA"] = source_params["SOURCE_SCHEMA"]
        
        # Get target connection details
        target_conn = Connection.get_connection_from_secrets("postgres_target")
        target_schema = "nmc"  # Default to nmc for target
        
        # Try to extract schema from extra field for target
        if target_conn.extra:
            try:
                target_extra_dict = target_conn.extra_dejson
                if isinstance(target_extra_dict, dict):
                    if 'schema' in target_extra_dict:
                        target_schema = target_extra_dict['schema']
                    elif 'options' in target_extra_dict:
                        options_str = target_extra_dict['options']
                        if 'search_path=' in options_str:
                            import re
                            search_path_match = re.search(r'search_path=(\w+)', options_str)
                            if search_path_match:
                                target_schema = search_path_match.group(1)
            except Exception as e:
                logger.warning(f"Error parsing target extra field: {str(e)}")
        
        logger.info(f"Target connection - Extra field: {target_conn.extra}")
        logger.info(f"Target connection - Extracted schema: {target_schema}")
        
        target_params = {
            "TARGET_HOST": target_conn.host,
            "TARGET_DB": target_conn.schema,
            "TARGET_USER": target_conn.login,
            "TARGET_PWD": target_conn.password,
            "TARGET_SCHEMA": target_schema,
            "TARGET_PORT": target_conn.port or 5432
        }
        
        # Log connection details for debugging
        logger.info(f"Source DB: {source_params['SOURCE_DB']}, Source Schema: {source_params['SOURCE_SCHEMA']}")
        logger.info(f"Target DB: {target_params['TARGET_DB']}, Target Schema: {target_params['TARGET_SCHEMA']}")
        
        # Combine all parameters
        all_params = {**source_params, **target_params}
        
        # Add runtime parameters
        all_params["FROM_DATE"] = context["params"]["from_date"]
        all_params["TO_DATE"] = context["params"]["to_date"]
        all_params["CENTER_ID"] = context["params"]["center_id"]
        
        # Write parameters to temporary JSON file for Spark jobs
        temp_file_path = os.path.join('/tmp', 'claim_data_params.json')
        with open(temp_file_path, 'w') as f:
            json.dump(all_params, f)
        
        logger.info("Successfully retrieved connection parameters")
        return all_params
        
    except Exception as e:
        logger.error(f"Error getting connection parameters: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise AirflowException(f"Failed to retrieve database connection parameters: {str(e)}")

def create_required_tables(**context):
    """
    Execute the table creation script to ensure all required tables exist
    """
    ti = context['ti']
    params = ti.xcom_pull(task_ids='initialize_params')
    
    # Path to the create tables script
    script_path = "/home/airflow/airflow/scripts/sql/create_tables.py"
    
    # Parameters JSON file
    params_file = "/tmp/claim_data_params.json"
    
    try:
        logger.info(f"Executing create_tables.py script with params from {params_file}")
        
        # Execute the script as a subprocess with drop_existing flag
        result = subprocess.run(
            [sys.executable, script_path, "--params_file", params_file, "--drop_existing"],
            check=True,
            capture_output=True,
            text=True
        )
        
        logger.info(f"Table creation script output: {result.stdout}")
        if result.stderr:
            logger.warning(f"Table creation script stderr: {result.stderr}")
            
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error executing table creation script: {e.stderr}")
        raise AirflowException(f"Failed to create required tables: {str(e)}")
    except Exception as e:
        logger.error(f"Error creating tables: {str(e)}")
        raise AirflowException(f"Failed to create required tables: {str(e)}")

def debug_spark_environment(**context):
    """Debug function to check Spark configuration and environment"""
    try:
        # Get the Spark connection
        spark_conn = Connection.get_connection_from_secrets("spark_default")
        
        # Log connection details
        logger.info("Spark Connection Details:")
        logger.info(f"Connection ID: {spark_conn.conn_id}")
        logger.info(f"Connection Type: {spark_conn.conn_type}")
        logger.info(f"Host: {spark_conn.host}")
        logger.info(f"Schema: {spark_conn.schema}")
        logger.info(f"Login: {spark_conn.login}")
        logger.info(f"Port: {spark_conn.port}")
        logger.info(f"Extra: {spark_conn.extra}")
        
        # Check if application files exist
        extraction_scripts = [
            "/home/airflow/airflow/scripts/spark/extraction/extract_patient_data.py",
            "/home/airflow/airflow/scripts/spark/extraction/extract_diagnosis_data.py",
            "/home/airflow/airflow/scripts/spark/extraction/extract_fact_data.py"
        ]
        
        transformation_scripts = [
            "/home/airflow/airflow/scripts/spark/transformation/create_diagnosis_pivot.py",
            "/home/airflow/airflow/scripts/spark/transformation/create_observations_pivot.py",
            "/home/airflow/airflow/scripts/spark/transformation/update_claim_data.py"
        ]
        
        logger.info(f"Checking if Spark scripts exist:")
        
        for script in extraction_scripts + transformation_scripts:
            logger.info(f"Script {script} exists: {os.path.exists(script)}")
            if os.path.exists(script):
                logger.info(f"Script permissions: {oct(os.stat(script).st_mode)[-3:]}")
                logger.info(f"Script size: {os.path.getsize(script)} bytes")
        
        # Check if params file exists and is readable
        params_file = "/tmp/claim_data_params.json"
        logger.info(f"Params file exists: {os.path.exists(params_file)}")
        if os.path.exists(params_file):
            logger.info(f"Params file permissions: {oct(os.stat(params_file).st_mode)[-3:]}")
            logger.info(f"Params file size: {os.path.getsize(params_file)} bytes")
            try:
                with open(params_file, 'r') as f:
                    params_content = f.read()
                    logger.info(f"Params file content length: {len(params_content)}")
            except Exception as e:
                logger.error(f"Error reading params file: {str(e)}")
        
        # Check environment variables
        logger.info("Environment Variables:")
        logger.info(f"SPARK_HOME: {os.environ.get('SPARK_HOME', 'Not set')}")
        logger.info(f"HADOOP_HOME: {os.environ.get('HADOOP_HOME', 'Not set')}")
        logger.info(f"JAVA_HOME: {os.environ.get('JAVA_HOME', 'Not set')}")
        logger.info(f"PATH: {os.environ.get('PATH', 'Not set')}")
        
        return True
    except Exception as e:
        logger.error(f"Error in debug_spark_environment: {str(e)}")
        raise AirflowException(f"Failed to debug Spark environment: {str(e)}")

def export_and_email_claim_data(**context):
    """
    Export the claim data to a compressed CSV file and email it to the recipients
    """
    ti = context['ti']
    params = ti.xcom_pull(task_ids='initialize_params')
    
    # Get the email recipients from context
    email_to = context["params"]["email_to"]
    from_date = context["params"]["from_date"]
    to_date = context["params"]["to_date"]
    center_id = context["params"]["center_id"]
    
    if not email_to:
        logger.info("No email recipients specified, skipping email task")
        return True
    
    # Create filename with date range and center_id
    center_suffix = f"_center_{center_id}" if center_id else "_all_centers"
    filename = f"claim_data_{from_date}_to_{to_date}{center_suffix}.csv"
    zip_filename = f"{filename}.zip"
    
    # Temporary paths for files
    temp_csv_path = os.path.join('/tmp', filename)
    temp_zip_path = os.path.join('/tmp', zip_filename)
    
    try:
        # Create postgres hook
        pg_hook = PostgresHook(postgres_conn_id="postgres_target")
        
        # Build SQL query to fetch data
        sql_query = f"""
        SELECT * FROM {params['TARGET_SCHEMA']}.rcm_claim_data 
        WHERE submission_date::date BETWEEN '{from_date}' AND '{to_date}'
        """
        
        if center_id:
            sql_query += f" AND center_id = '{center_id}'"
            
        # Fetch data
        logger.info(f"Executing query: {sql_query}")
        df = pg_hook.get_pandas_df(sql_query)
        
        if df.empty:
            logger.warning("No data found for the specified parameters")
            send_email(
                to=email_to.split(','),
                subject=f"Claim Data Export - No Data Found",
                html_content=f"No claim data found for date range {from_date} to {to_date} {center_suffix}.",
                mime_charset='utf-8'
            )
            return True
        
        # Export to CSV
        logger.info(f"Exporting {len(df)} rows to CSV")
        df.to_csv(temp_csv_path, index=False)
        
        # Compress the CSV file
        with zipfile.ZipFile(temp_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(temp_csv_path, arcname=filename)
        
        # Calculate file sizes for logging
        csv_size = os.path.getsize(temp_csv_path) / (1024 * 1024)  # MB
        zip_size = os.path.getsize(temp_zip_path) / (1024 * 1024)  # MB
        
        logger.info(f"Created CSV file ({csv_size:.2f} MB) and ZIP file ({zip_size:.2f} MB)")
        
        # Send email with attachment
        email_subject = f"Claim Data Export {from_date} to {to_date}{center_suffix}"
        email_body = f"""
        <p>Please find attached the claim data export for the following parameters:</p>
        <ul>
            <li>Date Range: {from_date} to {to_date}</li>
            <li>Center ID: {center_id if center_id else 'All Centers'}</li>
            <li>Total Records: {len(df)}</li>
        </ul>
        <p>The file is attached as a ZIP archive.</p>
        """
        
        # Parse email recipients
        recipients = [email.strip() for email in email_to.split(',')]
        
        logger.info(f"Sending email to: {recipients}")
        
        # Read the zip file content
        with open(temp_zip_path, 'rb') as f:
            zip_content = f.read()
        
        # Send email with attachment
        send_email(
            to=recipients,
            subject=email_subject,
            html_content=email_body,
            files=[(zip_filename, zip_content, 'application/zip')],
            mime_charset='utf-8'
        )
        
        logger.info(f"Email sent successfully to {email_to}")
        
        # Clean up temporary files
        os.remove(temp_csv_path)
        os.remove(temp_zip_path)
        
        return True
    except Exception as e:
        logger.error(f"Error in export_and_email_claim_data: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise AirflowException(f"Failed to export and email claim data: {str(e)}")

# Tasks definition
init_params_task = PythonOperator(
    task_id="initialize_params",
    python_callable=get_connection_params,
    dag=dag,
)

# Replace the old create_tables_task with new one that uses the script
# create_tables_task = PythonOperator(
#     task_id="create_staging_tables",
#     python_callable=create_required_tables,
#     dag=dag,
# )

# Add debug task before Spark tasks
# debug_spark_task = PythonOperator(
#     task_id="debug_spark_environment",
#     python_callable=debug_spark_environment,
#     dag=dag,
# )

# Extraction Tasks - Updated order and memory settings

extract_fact_data_task = SparkSubmitOperator(
    task_id="extract_claim_data_first",
    application="/home/airflow/airflow/scripts/spark/extraction/extract_fact_data.py",
    conn_id="spark_default",
    application_args=[
        "--params_file", "/tmp/claim_data_params.json",
        "--num_partitions", "12"
    ],
    verbose=True,
    env_vars={
        "JAVA_HOME": "/usr/lib/jvm/java-8-openjdk-amd64",
        "SPARK_HOME": "/opt/spark",
        "HADOOP_HOME": "/usr/local/hadoop",
        "PYTHONPATH": "/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip",
        "PATH": "/opt/spark/bin:/usr/local/hadoop/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin:/usr/bin:/sbin:/bin"
    },
    spark_binary="/opt/spark/bin/spark-submit",
    conf={
        "spark.master": "local[*]",
        "spark.driver.memory": "6g",
        "spark.executor.memory": "14g",
        "spark.driver.extraJavaOptions": "-Dfs.defaultFS=file:/// -XX:+UseG1GC -XX:+UseCompressedOops",
        "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:+UseCompressedOops", 
        "spark.memory.fraction": "0.8",
        "spark.memory.storageFraction": "0.3",
        "spark.sql.shuffle.partitions": "12",
        "spark.default.parallelism": "12",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.broadcastTimeout": "300",
        "spark.hadoop.fs.defaultFS": "file:///"
    },
    dag=dag,
)

extract_patient_data_task = SparkSubmitOperator(
    task_id="extract_patient_data_filtered",
    application="/home/airflow/airflow/scripts/spark/extraction/extract_patient_data.py",
    conn_id="spark_default",
    application_args=[
        "--params_file", "/tmp/claim_data_params.json",
        "--num_partitions", "12"
    ],
    verbose=True,
    env_vars={
        "JAVA_HOME": "/usr/lib/jvm/java-8-openjdk-amd64",
        "SPARK_HOME": "/opt/spark",
        "HADOOP_HOME": "/usr/local/hadoop",
        "PYTHONPATH": "/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip",
        "PATH": "/opt/spark/bin:/usr/local/hadoop/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin:/usr/bin:/sbin:/bin"
    },
    spark_binary="/opt/spark/bin/spark-submit",
    conf={
        "spark.master": "local[*]",
        "spark.driver.memory": "5g",
        "spark.executor.memory": "8g", 
        "spark.driver.extraJavaOptions": "-Dfs.defaultFS=file:/// -XX:+UseG1GC -XX:+UseCompressedOops",
        "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:+UseCompressedOops",
        "spark.memory.fraction": "0.8",
        "spark.memory.storageFraction": "0.3",
        "spark.sql.shuffle.partitions": "8",
        "spark.default.parallelism": "8",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.hadoop.fs.defaultFS": "file:///"
    },
    dag=dag,
)

extract_diagnosis_data_task = SparkSubmitOperator(
    task_id="extract_diagnosis_and_observations_filtered",
    application="/home/airflow/airflow/scripts/spark/extraction/extract_diagnosis_data.py",
    conn_id="spark_default",
    application_args=[
        "--params_file", "/tmp/claim_data_params.json"
    ],
    verbose=True,
    env_vars={
        "JAVA_HOME": "/usr/lib/jvm/java-8-openjdk-amd64",
        "SPARK_HOME": "/opt/spark",
        "HADOOP_HOME": "/usr/local/hadoop",
        "PYTHONPATH": "/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip",
        "PATH": "/opt/spark/bin:/usr/local/hadoop/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin:/usr/bin:/sbin:/bin"
    },
    spark_binary="/opt/spark/bin/spark-submit",
    conf={
        "spark.master": "local[*]",
        "spark.driver.memory": "4g",
        "spark.executor.memory": "6g",
        "spark.hadoop.fs.defaultFS": "file:///",
        "spark.driver.extraJavaOptions": "-Dfs.defaultFS=file:///",
        "spark.sql.shuffle.partitions": "4",
        "spark.default.parallelism": "4",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    },
    dag=dag,
)

# Transformation Tasks - Update memory settings

create_diagnosis_pivot_task = SparkSubmitOperator(
    task_id="create_diagnosis_pivot",
    application="/home/airflow/airflow/scripts/spark/transformation/create_diagnosis_pivot.py",
    conn_id="spark_default",
    application_args=[
        "--params_file", "/tmp/claim_data_params.json"
    ],
    verbose=True,
    env_vars={
        "JAVA_HOME": "/usr/lib/jvm/java-8-openjdk-amd64",
        "SPARK_HOME": "/opt/spark",
        "HADOOP_HOME": "/usr/local/hadoop",
        "PYTHONPATH": "/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip",
        "PATH": "/opt/spark/bin:/usr/local/hadoop/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin:/usr/bin:/sbin:/bin"
    },
    spark_binary="/opt/spark/bin/spark-submit",
    conf={
        "spark.master": "local[*]",
        "spark.driver.memory": "3g",
        "spark.executor.memory": "5g",
        "spark.hadoop.fs.defaultFS": "file:///",
        "spark.driver.extraJavaOptions": "-Dfs.defaultFS=file:///",
        "spark.sql.shuffle.partitions": "4",
        "spark.default.parallelism": "4",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    },
    dag=dag,
)

create_observations_pivot_task = SparkSubmitOperator(
    task_id="create_observations_pivot",
    application="/home/airflow/airflow/scripts/spark/transformation/create_observations_pivot.py",
    conn_id="spark_default",
    application_args=[
        "--params_file", "/tmp/claim_data_params.json"
    ],
    verbose=True,
    env_vars={
        "JAVA_HOME": "/usr/lib/jvm/java-8-openjdk-amd64",
        "SPARK_HOME": "/opt/spark",
        "HADOOP_HOME": "/usr/local/hadoop",
        "PYTHONPATH": "/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip",
        "PATH": "/opt/spark/bin:/usr/local/hadoop/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin:/usr/bin:/sbin:/bin"
    },
    spark_binary="/opt/spark/bin/spark-submit",
    conf={
        "spark.master": "local[*]",
        "spark.driver.memory": "3g",
        "spark.executor.memory": "5g",
        "spark.hadoop.fs.defaultFS": "file:///",
        "spark.driver.extraJavaOptions": "-Dfs.defaultFS=file:///",
        "spark.sql.shuffle.partitions": "4",
        "spark.default.parallelism": "4",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    },
    dag=dag,
)

update_claim_data_task = SparkSubmitOperator(
    task_id="update_claim_data",
    application="/home/airflow/airflow/scripts/spark/transformation/update_claim_data.py",
    conn_id="spark_default",
    application_args=[
        "--params_file", "/tmp/claim_data_params.json"
    ],
    verbose=True,
    env_vars={
        "JAVA_HOME": "/usr/lib/jvm/java-8-openjdk-amd64",
        "SPARK_HOME": "/opt/spark",
        "HADOOP_HOME": "/usr/local/hadoop",
        "PYTHONPATH": "/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip",
        "PATH": "/opt/spark/bin:/usr/local/hadoop/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin:/usr/bin:/sbin:/bin"
    },
    spark_binary="/opt/spark/bin/spark-submit",
    conf={
        "spark.master": "local[*]",
        "spark.driver.memory": "3g",
        "spark.executor.memory": "5g",
        "spark.hadoop.fs.defaultFS": "file:///",
        "spark.driver.extraJavaOptions": "-Dfs.defaultFS=file:///",
        "spark.sql.shuffle.partitions": "4",
        "spark.default.parallelism": "4",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    },
    dag=dag,
)

# Add the export and email task
export_email_task = PythonOperator(
    task_id="export_and_email_claim_data",
    python_callable=export_and_email_claim_data,
    dag=dag,
)

# Define the task dependencies with new order - extract fact data first
init_params_task >> extract_fact_data_task

# First extract fact data to get reference IDs
# debug_spark_task >> extract_fact_data_task

# Then extract patient and diagnosis data using reference IDs
extract_fact_data_task >> extract_patient_data_task
extract_fact_data_task >> extract_diagnosis_data_task   

# Transformation phase after all extractions
extract_diagnosis_data_task >> create_diagnosis_pivot_task
extract_diagnosis_data_task >> create_observations_pivot_task

# Connect all transformations to final update
create_diagnosis_pivot_task >> update_claim_data_task
create_observations_pivot_task >> update_claim_data_task

# Add email task as the final step
update_claim_data_task >> export_email_task
