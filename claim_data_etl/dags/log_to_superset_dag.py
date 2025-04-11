from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException

from datetime import datetime, timedelta
import logging
import pandas as pd
import os
import sys
import glob
import json
import sqlalchemy
from sqlalchemy.engine import create_engine

# Add scripts directory to path to import LogProcessor
scripts_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'scripts')
if scripts_dir not in sys.path:
    sys.path.append(scripts_dir)
    
from log_processor import LogProcessor

logger = logging.getLogger(__name__)

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# Define DAG
dag = DAG(
    'log_to_superset_etl',
    default_args=default_args,
    description='ETL process for HMS log files to be analyzed in Superset',
    #schedule_interval='0 * * * *',  # Run hourly
    catchup=False,
    tags=['logs', 'superset', 'etl'],
)

def get_processed_files():
    """
    Get list of previously processed files from metadata file
    """
    metadata_file = os.path.join(os.path.expanduser('~/airflow'), 'processed_data', 'processed_files.json')
    if os.path.exists(metadata_file):
        try:
            with open(metadata_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error reading processed files metadata: {str(e)}")
            return {'processed_files': [], 'last_processed_time': None}
    else:
        return {'processed_files': [], 'last_processed_time': None}

def save_processed_files(metadata):
    """
    Save list of processed files to metadata file
    """
    metadata_file = os.path.join(os.path.expanduser('~/airflow'), 'processed_data', 'processed_files.json')
    os.makedirs(os.path.dirname(metadata_file), exist_ok=True)
    try:
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f)
    except Exception as e:
        logger.error(f"Error saving processed files metadata: {str(e)}")

def extract_log_data(**context):
    """
    Extract structured data from log files in hms_logs folder
    Handles incremental processing of new files only
    """
    try:
        # Get log files from the hms_logs directory
        airflow_home = os.path.expanduser('~/airflow')
        logs_dir = os.path.join(airflow_home, 'hms_logs')
        
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir, exist_ok=True)
            logger.warning(f"Created logs directory at {logs_dir}, but no logs found")
            return {'files_processed': [], 'is_full_load': False}
        
        # Get previously processed files
        metadata = get_processed_files()
        processed_files = set(metadata['processed_files'])
        
        # Find all log files in the directory
        log_files = glob.glob(os.path.join(logs_dir, '*.log'))
        
        # Check if we have new files to process
        new_log_files = [f for f in log_files if os.path.basename(f) not in processed_files]
        
        if not new_log_files:
            logger.info("No new log files to process")
            return {'files_processed': [], 'is_full_load': False}
        
        # Process each new log file
        processed = []
        output_csvs = []
        
        for log_file_path in new_log_files:
            try:
                logger.info(f"Processing log file: {os.path.basename(log_file_path)}")
                
                # Initialize the log processor for this file
                processor = LogProcessor(log_file_path)
                
                # Extract log data
                output_csv = processor.extract()
                output_csvs.append(output_csv)
                
                # Add to processed files list
                processed.append(os.path.basename(log_file_path))
                
                logger.info(f"Log data extracted successfully from {os.path.basename(log_file_path)} to {output_csv}")
            except Exception as e:
                logger.error(f"Error processing log file {os.path.basename(log_file_path)}: {str(e)}")
        
        # Update processed files metadata if successful
        if processed:
            metadata['processed_files'].extend(processed)
            metadata['last_processed_time'] = datetime.now().isoformat()
            save_processed_files(metadata)
        
        result = {
            'files_processed': processed,
            'output_csvs': output_csvs,
            'is_full_load': False
        }
        
        return result
    
    except Exception as e:
        logger.error(f"Error extracting log data: {str(e)}")
        raise AirflowException(f"Failed to extract log data: {str(e)}")

def perform_full_load(**context):
    """
    Perform a full load of all log files in the hms_logs folder
    Used as a fallback when incremental load fails
    """
    try:
        # Get log files from the hms_logs directory
        airflow_home = os.path.expanduser('~/airflow')
        logs_dir = os.path.join(airflow_home, 'hms_logs')
        
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir, exist_ok=True)
            logger.warning(f"Created logs directory at {logs_dir}, but no logs found")
            return {'files_processed': [], 'is_full_load': True}
        
        # Find all log files in the directory
        log_files = glob.glob(os.path.join(logs_dir, '*.log'))
        
        if not log_files:
            logger.info("No log files found for full load")
            return {'files_processed': [], 'is_full_load': True}
        
        # Process each log file
        processed = []
        output_csvs = []
        
        for log_file_path in log_files:
            try:
                logger.info(f"Full load processing: {os.path.basename(log_file_path)}")
                
                # Initialize the log processor for this file
                processor = LogProcessor(log_file_path)
                
                # Extract log data
                output_csv = processor.extract()
                output_csvs.append(output_csv)
                
                # Add to processed files list
                processed.append(os.path.basename(log_file_path))
                
                logger.info(f"Log data extracted successfully from {os.path.basename(log_file_path)} to {output_csv}")
            except Exception as e:
                logger.error(f"Error processing log file {os.path.basename(log_file_path)}: {str(e)}")
        
        # Create new metadata
        metadata = {
            'processed_files': [os.path.basename(f) for f in log_files],
            'last_processed_time': datetime.now().isoformat()
        }
        save_processed_files(metadata)
        
        result = {
            'files_processed': processed,
            'output_csvs': output_csvs,
            'is_full_load': True
        }
        
        return result
    
    except Exception as e:
        logger.error(f"Error performing full load: {str(e)}")
        raise AirflowException(f"Failed to perform full load: {str(e)}")

def extract_task_decision(**context):
    """
    Decision task to determine whether to do incremental or full load
    """
    try:
        # Try incremental load first
        result = extract_log_data(**context)
        if result['files_processed']:
            return result
        else:
            # If no files processed in incremental, just return empty result
            # No need for full load if there are no new files
            return result
    except Exception as e:
        # On failure, fallback to full load
        logger.error(f"Incremental load failed. Falling back to full load: {str(e)}")
        return perform_full_load(**context)

def transform_log_data(**context):
    """
    Transform the extracted log data using LogProcessor
    """
    try:
        ti = context['ti']
        extract_result = ti.xcom_pull(task_ids='extract_log_data')
        
        if not extract_result or not extract_result.get('files_processed'):
            logger.info("No files processed, skipping transformation")
            return {'transformed_files': [], 'is_full_load': extract_result.get('is_full_load', False)}
        
        # Process each output CSV file
        output_csvs = extract_result.get('output_csvs', [])
        transformed_results = []
        
        for csv_file_path in output_csvs:
            try:
                # Get the log file path (necessary to initialize LogProcessor)
                airflow_home = os.path.expanduser('~/airflow')
                log_file_path = os.path.join(airflow_home, 'hms_logs', 'placeholder.log')  # Just for initialization
                
                # Initialize the log processor
                processor = LogProcessor(log_file_path)
                
                # Transform the data
                transformed_csv, analysis_files = processor.transform(csv_file_path)
                
                # Generate statistics
                stats_csv = processor.generate_stats(transformed_csv)
                
                transformed_results.append({
                    'transformed_csv': transformed_csv,
                    'analysis_files': analysis_files,
                    'stats_csv': stats_csv
                })
                
                logger.info(f"Log data transformed successfully to {transformed_csv}")
            except Exception as e:
                logger.error(f"Error transforming CSV {csv_file_path}: {str(e)}")
        
        result = {
            'transformed_files': transformed_results,
            'is_full_load': extract_result.get('is_full_load', False)
        }
        
        return result
    
    except Exception as e:
        logger.error(f"Error transforming log data: {str(e)}")
        raise AirflowException(f"Failed to transform log data: {str(e)}")

def load_to_postgres(**context):
    """
    Load the transformed data into PostgreSQL for Superset
    Handles both incremental and full load scenarios
    """
    try:
        ti = context['ti']
        transform_result = ti.xcom_pull(task_ids='transform_log_data')
        
        if not transform_result or not transform_result.get('transformed_files'):
            logger.info("No transformed files to load")
            return {"loaded_files": 0, "is_full_load": transform_result.get('is_full_load', False)}
        
        is_full_load = transform_result.get('is_full_load', False)
        transformed_files = transform_result.get('transformed_files', [])
        
        # Use PostgresHook with specific connection details
        hook = PostgresHook(
            postgres_conn_id='postgres_superset'
        )
        
        # If this is a full load, truncate all tables first
        if is_full_load:
            logger.info("Performing full load - truncating existing tables")
            truncate_queries = [
                "TRUNCATE TABLE log_entries",
                "TRUNCATE TABLE log_stats",
                "TRUNCATE TABLE log_hourly_summary",
                "TRUNCATE TABLE log_user_summary",
                "TRUNCATE TABLE log_ip_summary"
            ]
            
            # Also truncate level-specific tables
            for level in ['error', 'info', 'warn', 'debug']:
                truncate_queries.append(f"TRUNCATE TABLE IF EXISTS log_{level}_counts")
            
            # Execute truncate queries
            for query in truncate_queries:
                try:
                    hook.run(query)
                except Exception as e:
                    logger.warning(f"Error truncating table: {str(e)}")
        
        # Create tables if they don't exist (needed for first run or after schema changes)
        # Create the main log table with updated structure
        main_table_query = """
        CREATE TABLE IF NOT EXISTS log_entries (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            ip VARCHAR(20),
            username VARCHAR(100),
            log_level VARCHAR(10),
            message TEXT,
            details TEXT,
            function VARCHAR(100),
            file_info VARCHAR(200),
            clean_message TEXT,
            date DATE,
            hour INTEGER,
            minute INTEGER,
            time_diff_seconds FLOAT
        )
        """
        
        # Create the stats table
        stats_table_query = """
        CREATE TABLE IF NOT EXISTS log_stats (
            id SERIAL PRIMARY KEY,
            metric VARCHAR(50),
            value TEXT
        )
        """
        
        # Create hourly summary table
        hourly_table_query = """
        CREATE TABLE IF NOT EXISTS log_hourly_summary (
            id SERIAL PRIMARY KEY,
            date DATE,
            hour INTEGER,
            log_count INTEGER,
            avg_time_diff FLOAT,
            max_time_diff FLOAT
        )
        """
        
        # Create user summary table
        user_table_query = """
        CREATE TABLE IF NOT EXISTS log_user_summary (
            id SERIAL PRIMARY KEY,
            username VARCHAR(100),
            count INTEGER
        )
        """
        
        # Create IP summary table
        ip_table_query = """
        CREATE TABLE IF NOT EXISTS log_ip_summary (
            id SERIAL PRIMARY KEY,
            ip VARCHAR(20),
            count INTEGER
        )
        """
        
        # Execute table creation queries
        hook.run(main_table_query)
        hook.run(stats_table_query)
        hook.run(hourly_table_query)
        hook.run(user_table_query)
        hook.run(ip_table_query)
        
        # Process each transformed file result
        loaded_count = 0
        
        for transform_file in transformed_files:
            try:
                transformed_csv = transform_file['transformed_csv']
                analysis_files = transform_file['analysis_files']
                stats_csv = transform_file['stats_csv']
                
                # Load main data using bulk insert approach
                df = pd.read_csv(transformed_csv)
                
                # Debug: Print column names to verify
                logger.info(f"DataFrame columns: {df.columns.tolist()}")
                
                # For full load or for first file in incremental, replace stats data
                if is_full_load or loaded_count == 0:
                    # Load statistics data (replace)
                    stats_df = pd.read_csv(stats_csv)
                    if is_full_load:
                        hook.run("DELETE FROM log_stats")
                    hook.insert_rows(
                        table="log_stats",
                        rows=stats_df.values.tolist(),
                        target_fields=list(stats_df.columns),
                        commit_every=1000
                    )
                
                # Insert log entries (always append)
                hook.insert_rows(
                    table="log_entries",
                    rows=df.values.tolist(),
                    target_fields=list(df.columns),
                    commit_every=1000
                )
                
                # For hourly summary, IP summary, and user summary:
                # - Full load: Replace data
                # - Incremental: Append and then run aggregation query
                
                # Load hourly summary data
                hourly_df = pd.read_csv(analysis_files['hourly_summary'])
                if is_full_load:
                    hook.run("DELETE FROM log_hourly_summary")
                hook.insert_rows(
                    table="log_hourly_summary",
                    rows=hourly_df.values.tolist(),
                    target_fields=list(hourly_df.columns),
                    commit_every=1000
                )
                
                # Load user summary data
                if 'username_summary' in analysis_files:
                    username_df = pd.read_csv(analysis_files['username_summary'])
                    if is_full_load:
                        hook.run("DELETE FROM log_user_summary")
                    hook.insert_rows(
                        table="log_user_summary",
                        rows=username_df.values.tolist(),
                        target_fields=list(username_df.columns),
                        commit_every=1000
                    )
                
                # Load IP summary data
                if 'ip_summary' in analysis_files:
                    ip_df = pd.read_csv(analysis_files['ip_summary'])
                    if is_full_load:
                        hook.run("DELETE FROM log_ip_summary")
                    hook.insert_rows(
                        table="log_ip_summary",
                        rows=ip_df.values.tolist(),
                        target_fields=list(ip_df.columns),
                        commit_every=1000
                    )
                
                # Load level counts for each log level
                for level_key, level_file in analysis_files.items():
                    if level_key in ['hourly_summary', 'username_summary', 'ip_summary']:
                        continue
                        
                    level_name = level_key.replace('_counts', '')
                    level_df = pd.read_csv(level_file)
                    
                    # Create level count table with updated column name
                    level_table_query = f"""
                    CREATE TABLE IF NOT EXISTS log_{level_name}_counts (
                        id SERIAL PRIMARY KEY,
                        function VARCHAR(100),
                        {level_name}_count INTEGER
                    )
                    """
                    hook.run(level_table_query)
                    
                    if is_full_load:
                        hook.run(f"DELETE FROM log_{level_name}_counts")
                    
                    hook.insert_rows(
                        table=f"log_{level_name}_counts",
                        rows=level_df.values.tolist(),
                        target_fields=list(level_df.columns),
                        commit_every=1000
                    )
                
                loaded_count += 1
                logger.info(f"Loaded data from {transformed_csv} into PostgreSQL")
            
            except Exception as e:
                logger.error(f"Error loading file {transform_file.get('transformed_csv')}: {str(e)}")
        
        # After incremental load, refresh summary tables by aggregation
        if not is_full_load and loaded_count > 0:
            try:
                # Refresh hourly summary with aggregated data
                refresh_hourly_query = """
                TRUNCATE TABLE log_hourly_summary;
                INSERT INTO log_hourly_summary (date, hour, log_count, avg_time_diff, max_time_diff)
                SELECT date, hour, COUNT(*) as log_count, AVG(time_diff_seconds) as avg_time_diff, MAX(time_diff_seconds) as max_time_diff
                FROM log_entries
                GROUP BY date, hour;
                """
                hook.run(refresh_hourly_query)
                
                # Refresh user summary
                refresh_user_query = """
                TRUNCATE TABLE log_user_summary;
                INSERT INTO log_user_summary (username, count)
                SELECT username, COUNT(*) as count
                FROM log_entries
                GROUP BY username;
                """
                hook.run(refresh_user_query)
                
                # Refresh IP summary
                refresh_ip_query = """
                TRUNCATE TABLE log_ip_summary;
                INSERT INTO log_ip_summary (ip, count)
                SELECT ip, COUNT(*) as count
                FROM log_entries
                GROUP BY ip;
                """
                hook.run(refresh_ip_query)
                
                # Refresh level count tables
                for level in ['error', 'info', 'warn', 'debug']:
                    try:
                        refresh_level_query = f"""
                        TRUNCATE TABLE log_{level}_counts;
                        INSERT INTO log_{level}_counts (function, {level}_count)
                        SELECT function, COUNT(*) as {level}_count
                        FROM log_entries
                        WHERE LOWER(log_level) = '{level}'
                        GROUP BY function;
                        """
                        hook.run(refresh_level_query)
                    except Exception as e:
                        logger.warning(f"Error refreshing {level} counts: {str(e)}")
                
                logger.info("Refreshed summary tables with aggregated data after incremental load")
            except Exception as e:
                logger.error(f"Error refreshing summary tables: {str(e)}")
        
        logger.info(f"Loaded {loaded_count} files into PostgreSQL for Superset analysis")
        return {"loaded_files": loaded_count, "is_full_load": is_full_load}
        
    except Exception as e:
        logger.error(f"Error loading data to PostgreSQL: {str(e)}")
        raise AirflowException(f"Failed to load data to PostgreSQL: {str(e)}")

# Define tasks
extract_task = PythonOperator(
    task_id='extract_log_data',
    python_callable=extract_task_decision,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_log_data',
    python_callable=transform_log_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
extract_task >> transform_task >> load_task