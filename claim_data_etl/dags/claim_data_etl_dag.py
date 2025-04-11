from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable, Connection
from airflow.exceptions import AirflowException
from airflow.models.param import Param

import logging
import pandas as pd
from datetime import datetime, timedelta
import os
import pendulum
from typing import Dict, Any

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# Define the DAG
dag = DAG(
    'claim_data_etl',
    default_args=default_args,
    description='ETL process for claim data with source and target in PostgreSQL',
    schedule=None,  # Manual trigger (can be changed to a schedule if needed)
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
    tags=['postgres', 'etl', 'claim_data'],
    params={
        'from_date': Param('2023-01-01', type='string', description='Start date for data extraction (YYYY-MM-DD)'),
        'to_date': Param('2023-12-31', type='string', description='End date for data extraction (YYYY-MM-DD)'),
        'center_id': Param('', type='string', description='Center ID filter (leave blank for all centers)'),
    },
)

# SQL for creating staging tables
create_staging_tables_sql = """
-- Create staging tables for claim data ETL
CREATE TABLE IF NOT EXISTS {{params.target_schema}}.staging_tables (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create masters table for reference data
CREATE TABLE IF NOT EXISTS {{params.target_schema}}.masters (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    record_count INT,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Create diagnosis pivot table
CREATE TABLE IF NOT EXISTS {{params.target_schema}}.mrd_diag_pivot (
    visit_id VARCHAR(100) PRIMARY KEY,
    p1 VARCHAR(100),
    s1 VARCHAR(100),
    s2 VARCHAR(100),
    s3 VARCHAR(100),
    s4 VARCHAR(100),
    s5 VARCHAR(100),
    s6 VARCHAR(100),
    s7 VARCHAR(100),
    s8 VARCHAR(100),
    s9 VARCHAR(100),
    s10 VARCHAR(100),
    s11 VARCHAR(100),
    s12 VARCHAR(100),
    s13 VARCHAR(100),
    s14 VARCHAR(100),
    s15 VARCHAR(100)
);

-- Create observations pivot table
CREATE TABLE IF NOT EXISTS {{params.target_schema}}.observations_pivot (
    visit_id VARCHAR(100) PRIMARY KEY,
    observation_date TIMESTAMP,
    observation_code VARCHAR(100),
    observation_value VARCHAR(200)
);

-- Create main claim data table
CREATE TABLE IF NOT EXISTS {{params.target_schema}}.rcm_claim_data (
    id SERIAL PRIMARY KEY,
    patientid VARCHAR(100),
    patientname VARCHAR(200),
    patientbirthdate VARCHAR(100),
    age INT,
    gender VARCHAR(10),
    nationality VARCHAR(100),
    passport_no VARCHAR(100),
    visit_id VARCHAR(100) UNIQUE,
    doctorname VARCHAR(200),
    doctorsdepartment VARCHAR(100),
    Encounterdiagnosisprincipal VARCHAR(100),
    Encounterdiagnosissecondary1 VARCHAR(100),
    Encounterdiagnosissecondary2 VARCHAR(100),
    Encounterdiagnosissecondary3 VARCHAR(100),
    Encounterdiagnosissecondary4 VARCHAR(100),
    Encounterdiagnosissecondary5 VARCHAR(100),
    Encounterdiagnosissecondary6 VARCHAR(100),
    Encounterdiagnosissecondary7 VARCHAR(100),
    Encounterdiagnosissecondary8 VARCHAR(100),
    Encounterdiagnosissecondary9 VARCHAR(100),
    Encounterdiagnosissecondary10 VARCHAR(100),
    Encounterdiagnosissecondary11 VARCHAR(100),
    Encounterdiagnosissecondary12 VARCHAR(100),
    Encounterdiagnosissecondary13 VARCHAR(100),
    Encounterdiagnosissecondary14 VARCHAR(100),
    Encounterdiagnosissecondary15 VARCHAR(100),
    Encountertransfersource VARCHAR(100),
    Encountertransferdestination VARCHAR(100),
    insurance_company VARCHAR(100),
    insurance_plan VARCHAR(100),
    registration_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);
"""

# Function to initialize database connections from Airflow connections
def get_connection_params(**context):
    """
    Get connection parameters from Airflow connections and return them as a dictionary
    to be stored in XCom for other tasks to use.
    """
    try:
        # Get source connection details
        source_conn = Connection.get_connection_from_secrets("postgres_source")
        source_params = {
            "SOURCE_HOST": source_conn.host,
            "SOURCE_DB": source_conn.schema,
            "SOURCE_USER": source_conn.login,
            "SOURCE_PWD": source_conn.password,
            "SOURCE_SCHEMA": source_conn.extra_dejson.get("schema", "public")
        }
        
        # If there's a backup/secondary source connection
        try:
            sec_source_conn = Connection.get_connection_from_secrets("postgres_sec_source")
            source_params["SEC_SOURCE_HOST"] = sec_source_conn.host
        except:
            source_params["SEC_SOURCE_HOST"] = source_params["SOURCE_HOST"]
        
        # Get BI source connection details (if different from primary source)
        try:
            bi_source_conn = Connection.get_connection_from_secrets("postgres_bi_source")
            source_params["BI_SOURCE_HOST"] = bi_source_conn.host
            source_params["BI_SOURCE_DB"] = bi_source_conn.schema
            source_params["BI_SOURCE_USER"] = bi_source_conn.login
            source_params["BI_SOURCE_PWD"] = bi_source_conn.password
            source_params["BI_SOURCE_SCHEMA"] = bi_source_conn.extra_dejson.get("schema", "public")
        except:
            # Use primary source if BI source not configured
            source_params["BI_SOURCE_HOST"] = source_params["SOURCE_HOST"]
            source_params["BI_SOURCE_DB"] = source_params["SOURCE_DB"]
            source_params["BI_SOURCE_USER"] = source_params["SOURCE_USER"]
            source_params["BI_SOURCE_PWD"] = source_params["SOURCE_PWD"]
            source_params["BI_SOURCE_SCHEMA"] = source_params["SOURCE_SCHEMA"]
        
        # Get target connection details
        target_conn = Connection.get_connection_from_secrets("postgres_target")
        target_params = {
            "TARGET_HOST": target_conn.host,
            "TARGET_DB": target_conn.schema,
            "TARGET_USER": target_conn.login,
            "TARGET_PWD": target_conn.password,
            "TARGET_SCHEMA": target_conn.extra_dejson.get("schema", "public")
        }
        
        # Combine all parameters
        all_params = {**source_params, **target_params}
        
        # Add runtime parameters
        all_params["FromDate"] = context["params"]["from_date"]
        all_params["ToDate"] = context["params"]["to_date"]
        all_params["CenterID"] = context["params"]["center_id"]
        
        logger.info("Successfully retrieved connection parameters")
        return all_params
        
    except Exception as e:
        logger.error(f"Error getting connection parameters: {str(e)}")
        raise AirflowException(f"Failed to retrieve database connection parameters: {str(e)}")

# Function to load staging tables
def load_staging_tables(**context):
    """
    Load data into staging tables
    """
    conn_params = context["ti"].xcom_pull(task_ids="initialize_params")
    pg_hook = PostgresHook(postgres_conn_id="postgres_source")
    
    try:
        # Get list of patients based on date range
        patient_query = f"""
        SELECT patient_id, visit_date
        FROM {conn_params['SOURCE_SCHEMA']}.patient_visits
        WHERE visit_date BETWEEN '{conn_params['FromDate']}' AND '{conn_params['ToDate']}'
        """
        
        if conn_params["CenterID"]:
            patient_query += f" AND center_id = '{conn_params['CenterID']}'"
            
        df = pg_hook.get_pandas_df(patient_query)
        
        # Setup target hook for loading
        target_hook = PostgresHook(postgres_conn_id="postgres_target")
        
        # Load data to staging table
        target_hook.insert_rows(
            table=f"{conn_params['TARGET_SCHEMA']}.patient_staging",
            rows=df.values.tolist(),
            target_fields=list(df.columns),
            commit_every=1000
        )
        
        return f"Loaded {len(df)} records to staging tables"
    
    except Exception as e:
        logger.error(f"Error loading staging tables: {str(e)}")
        raise AirflowException(f"Failed to load staging tables: {str(e)}")

# Function to create diagnosis pivot
def create_diagnosis_pivot(**context):
    """
    Create pivot table for diagnosis data
    """
    conn_params = context["ti"].xcom_pull(task_ids="initialize_params")
    pg_hook = PostgresHook(postgres_conn_id="postgres_target")
    
    try:
        # Run the pivot query
        pivot_query = f"""
        INSERT INTO {conn_params['TARGET_SCHEMA']}.mrd_diag_pivot (visit_id, p1, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15)
        SELECT 
            visit_id,
            MAX(CASE WHEN diag_order = 1 THEN diagnosis_code END) as p1,
            MAX(CASE WHEN diag_order = 2 THEN diagnosis_code END) as s1,
            MAX(CASE WHEN diag_order = 3 THEN diagnosis_code END) as s2,
            MAX(CASE WHEN diag_order = 4 THEN diagnosis_code END) as s3,
            MAX(CASE WHEN diag_order = 5 THEN diagnosis_code END) as s4,
            MAX(CASE WHEN diag_order = 6 THEN diagnosis_code END) as s5,
            MAX(CASE WHEN diag_order = 7 THEN diagnosis_code END) as s6,
            MAX(CASE WHEN diag_order = 8 THEN diagnosis_code END) as s7,
            MAX(CASE WHEN diag_order = 9 THEN diagnosis_code END) as s8,
            MAX(CASE WHEN diag_order = 10 THEN diagnosis_code END) as s9,
            MAX(CASE WHEN diag_order = 11 THEN diagnosis_code END) as s10,
            MAX(CASE WHEN diag_order = 12 THEN diagnosis_code END) as s11,
            MAX(CASE WHEN diag_order = 13 THEN diagnosis_code END) as s12,
            MAX(CASE WHEN diag_order = 14 THEN diagnosis_code END) as s13,
            MAX(CASE WHEN diag_order = 15 THEN diagnosis_code END) as s14,
            MAX(CASE WHEN diag_order = 16 THEN diagnosis_code END) as s15
        FROM {conn_params['TARGET_SCHEMA']}.patient_diagnosis
        GROUP BY visit_id
        ON CONFLICT (visit_id) 
        DO UPDATE SET
            p1 = EXCLUDED.p1,
            s1 = EXCLUDED.s1,
            s2 = EXCLUDED.s2,
            s3 = EXCLUDED.s3,
            s4 = EXCLUDED.s4,
            s5 = EXCLUDED.s5,
            s6 = EXCLUDED.s6,
            s7 = EXCLUDED.s7,
            s8 = EXCLUDED.s8,
            s9 = EXCLUDED.s9,
            s10 = EXCLUDED.s10,
            s11 = EXCLUDED.s11,
            s12 = EXCLUDED.s12,
            s13 = EXCLUDED.s13,
            s14 = EXCLUDED.s14,
            s15 = EXCLUDED.s15
        """
        
        pg_hook.run(pivot_query)
        return "Successfully created diagnosis pivot"
    
    except Exception as e:
        logger.error(f"Error creating diagnosis pivot: {str(e)}")
        raise AirflowException(f"Failed to create diagnosis pivot: {str(e)}")

# Function to create observations pivot
def create_observations_pivot(**context):
    """
    Create pivot table for observations data
    """
    conn_params = context["ti"].xcom_pull(task_ids="initialize_params")
    pg_hook = PostgresHook(postgres_conn_id="postgres_target")
    
    try:
        # Run the observations pivot query
        pivot_query = f"""
        INSERT INTO {conn_params['TARGET_SCHEMA']}.observations_pivot (visit_id, observation_date, observation_code, observation_value)
        SELECT 
            visit_id,
            observation_date,
            observation_code,
            observation_value
        FROM {conn_params['TARGET_SCHEMA']}.patient_observations
        ON CONFLICT (visit_id) 
        DO UPDATE SET
            observation_date = EXCLUDED.observation_date,
            observation_code = EXCLUDED.observation_code,
            observation_value = EXCLUDED.observation_value
        """
        
        pg_hook.run(pivot_query)
        return "Successfully created observations pivot"
    
    except Exception as e:
        logger.error(f"Error creating observations pivot: {str(e)}")
        raise AirflowException(f"Failed to create observations pivot: {str(e)}")

# SQL for updating patient details
update_patient_details_sql = """
UPDATE {{params.target_schema}}.rcm_claim_data x
SET patientname = pd.patient_full_name,
    patientbirthdate = pd.date ,
    age = pd.age,
    gender = pd.gender,
    nationality = pd.nationality,
    passport_no = pd.custom_field2
FROM (
    SELECT 
        mr_no,
        patient_name || ' ' || COALESCE(middle_name, '') || CASE WHEN COALESCE(middle_name, '') != '' THEN ' ' ELSE '' END
        || COALESCE(last_name, '') AS patient_full_name,
        TO_CHAR(COALESCE(expected_dob, dateofbirth), 'Mon-DD-YYYY') AS date, 
        EXTRACT(YEAR FROM AGE(COALESCE(expected_dob, dateofbirth))) AS age,
        patient_gender AS gender,
        custom_list1_value AS nationality,
        passport_no,
        custom_field2 
    FROM {{params.target_schema}}.patient_details
) pd
WHERE x.patientid = pd.mr_no;
"""

# SQL for updating doctor details
update_doctor_details_sql = """
UPDATE {{params.target_schema}}.rcm_claim_data x
SET doctorname = d.doctor_name,
    doctorsdepartment = dp.dept_name,
    Encountertransfersource = pr.transfer_source,
    Encountertransferdestination = pr.transfer_destination,
    registration_date = pr.reg_date + pr.reg_time
FROM {{params.target_schema}}.patient_registration_full pr
JOIN {{params.target_schema}}.doctors d ON d.doctor_id = pr.doctor
JOIN {{params.target_schema}}.department dp ON (d.dept_id = dp.dept_id)
WHERE x.visit_id = pr.patient_id;
"""

# SQL for updating diagnosis
update_diagnosis_sql = """
UPDATE {{params.target_schema}}.rcm_claim_data jt
SET Encounterdiagnosisprincipal = mdp.p1,
    Encounterdiagnosissecondary1 = mdp.s1,
    Encounterdiagnosissecondary2 = mdp.s2,
    Encounterdiagnosissecondary3 = mdp.s3,
    Encounterdiagnosissecondary4 = mdp.s4,
    Encounterdiagnosissecondary5 = mdp.s5,
    Encounterdiagnosissecondary6 = mdp.s6,
    Encounterdiagnosissecondary7 = mdp.s7,
    Encounterdiagnosissecondary8 = mdp.s8,
    Encounterdiagnosissecondary9 = mdp.s9,
    Encounterdiagnosissecondary10 = mdp.s10,
    Encounterdiagnosissecondary11 = mdp.s11,
    Encounterdiagnosissecondary12 = mdp.s12,
    Encounterdiagnosissecondary13 = mdp.s13,
    Encounterdiagnosissecondary14 = mdp.s14,
    Encounterdiagnosissecondary15 = mdp.s15
FROM {{params.target_schema}}.mrd_diag_pivot mdp
WHERE jt.visit_id = mdp.visit_id;
"""

# SQL for updating observations
update_observations_sql = """
UPDATE {{params.target_schema}}.rcm_claim_data jt
SET observation_details = op.observation_value
FROM {{params.target_schema}}.observations_pivot op
WHERE jt.visit_id = op.visit_id;
"""

# SQL for updating insurance company
update_insurance_sql = """
UPDATE {{params.target_schema}}.rcm_claim_data jt
SET insurance_company = i.company_name,
    insurance_plan = i.plan_name
FROM {{params.target_schema}}.insurance_details i
WHERE jt.visit_id = i.visit_id;
"""

# Tasks definition
init_params_task = PythonOperator(
    task_id="initialize_params",
    python_callable=get_connection_params,
    dag=dag,
)

create_tables_task = SQLExecuteQueryOperator(
    task_id="create_staging_tables",
    conn_id="postgres_target",
    sql=create_staging_tables_sql,
    hook_params={'schema': "{{ ti.xcom_pull(task_ids='initialize_params')['TARGET_SCHEMA'] }}"},
    params={
        "target_schema": "{{ ti.xcom_pull(task_ids='initialize_params')['TARGET_SCHEMA'] }}"
    },
    dag=dag,
)

staging_tables_task = PythonOperator(
    task_id="load_staging_tables",
    python_callable=load_staging_tables,
    dag=dag,
)

master_tables_task = SQLExecuteQueryOperator(
    task_id="load_master_tables",
    conn_id="postgres_target",
    sql="INSERT INTO {{ ti.xcom_pull(task_ids='initialize_params')['TARGET_SCHEMA'] }}.masters (table_name, record_count) VALUES ('master_load', (SELECT COUNT(*) FROM {{ ti.xcom_pull(task_ids='initialize_params')['TARGET_SCHEMA'] }}.patient_staging))",
    hook_params={'schema': "{{ ti.xcom_pull(task_ids='initialize_params')['TARGET_SCHEMA'] }}"},
    dag=dag,
)

diagnosis_pivot_task = PythonOperator(
    task_id="create_diagnosis_pivot",
    python_callable=create_diagnosis_pivot,
    dag=dag,
)

observations_pivot_task = PythonOperator(
    task_id="create_observations_pivot",
    python_callable=create_observations_pivot,
    dag=dag,
)

create_datafrom_fact_task = SQLExecuteQueryOperator(
    task_id="create_datafrom_fact",
    conn_id="postgres_target",
    sql="INSERT INTO {{ ti.xcom_pull(task_ids='initialize_params')['TARGET_SCHEMA'] }}.rcm_claim_data (patientid, visit_id) SELECT patient_id, visit_id FROM {{ ti.xcom_pull(task_ids='initialize_params')['TARGET_SCHEMA'] }}.patient_staging",
    hook_params={'schema': "{{ ti.xcom_pull(task_ids='initialize_params')['TARGET_SCHEMA'] }}"},
    dag=dag,
)

update_patient_details_task = SQLExecuteQueryOperator(
    task_id="update_patient_details",
    conn_id="postgres_target",
    sql=update_patient_details_sql,
    hook_params={'schema': "{{ ti.xcom_pull(task_ids='initialize_params')['TARGET_SCHEMA'] }}"},
    params={
        "target_schema": "{{ ti.xcom_pull(task_ids='initialize_params')['TARGET_SCHEMA'] }}"
    },
    dag=dag,
)

update_doctor_details_task = SQLExecuteQueryOperator(
    task_id="update_doctor_dept",
    conn_id="postgres_target",
    sql=update_doctor_details_sql,
    hook_params={'schema': "{{ ti.xcom_pull(task_ids='initialize_params')['TARGET_SCHEMA'] }}"},
    params={
        "target_schema": "{{ ti.xcom_pull(task_ids='initialize_params')['TARGET_SCHEMA'] }}"
    },
    dag=dag,
)

update_diagnosis_task = SQLExecuteQueryOperator(
    task_id="update_diagnosis",
    conn_id="postgres_target",
    sql=update_diagnosis_sql,
    hook_params={'schema': "{{ ti.xcom_pull(task_ids='initialize_params')['TARGET_SCHEMA'] }}"},
    params={
        "target_schema": "{{ ti.xcom_pull(task_ids='initialize_params')['TARGET_SCHEMA'] }}"
    },
    dag=dag,
)

update_observations_task = SQLExecuteQueryOperator(
    task_id="update_observations",
    conn_id="postgres_target",
    sql=update_observations_sql,
    hook_params={'schema': "{{ ti.xcom_pull(task_ids='initialize_params')['TARGET_SCHEMA'] }}"},
    params={
        "target_schema": "{{ ti.xcom_pull(task_ids='initialize_params')['TARGET_SCHEMA'] }}"
    },
    dag=dag,
)

update_insurance_task = SQLExecuteQueryOperator(
    task_id="update_insurance_co",
    conn_id="postgres_target",
    sql=update_insurance_sql,
    hook_params={'schema': "{{ ti.xcom_pull(task_ids='initialize_params')['TARGET_SCHEMA'] }}"},
    params={
        "target_schema": "{{ ti.xcom_pull(task_ids='initialize_params')['TARGET_SCHEMA'] }}"
    },
    dag=dag,
)

# Define the task flow
init_params_task >> create_tables_task >> staging_tables_task >> master_tables_task

master_tables_task >> diagnosis_pivot_task
master_tables_task >> observations_pivot_task
master_tables_task >> create_datafrom_fact_task

create_datafrom_fact_task >> update_patient_details_task
create_datafrom_fact_task >> update_doctor_details_task 
diagnosis_pivot_task >> update_diagnosis_task
observations_pivot_task >> update_observations_task
create_datafrom_fact_task >> update_insurance_task

# All updates must complete for the DAG to finish
[
    update_patient_details_task, 
    update_doctor_details_task, 
    update_diagnosis_task, 
    update_observations_task, 
    update_insurance_task
] 