from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable, Connection
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from pyspark.sql import SparkSession

import logging
import pandas as pd
from datetime import datetime, timedelta
import os
import pendulum
from typing import Dict, Any
import json
import tempfile
import socket

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
    'claim_data_spark_etl',
    default_args=default_args,
    description='ETL process for claim data with Spark processing for improved performance',
    schedule=None,  # Manual trigger (can be changed to a schedule if needed)
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
    tags=['postgres', 'etl', 'spark', 'claim_data'],
    params={
        'from_date': Param('2025-01-01', type='string', description='Start date for data extraction (YYYY-MM-DD)'),
        'to_date': Param('2025-01-31', type='string', description='End date for data extraction (YYYY-MM-DD)'),
        'center_id': Param('', type='string', description='Center ID filter (leave blank for all centers)'),
    },
)

# SQL for creating staging tables
create_staging_tables_sql = """
-- Create jawda_patient_details table
CREATE TABLE IF NOT EXISTS {target_schema}.jawda_patient_details (
    mr_no VARCHAR,
    name VARCHAR,
    sex VARCHAR,
    birthdate DATE,
    age INT,
    nationality VARCHAR,
    country VARCHAR,
    phone VARCHAR,
    id_type VARCHAR,
    id_number VARCHAR,
    sponsor_id VARCHAR,
    residence_type VARCHAR,
    email VARCHAR,
    passport VARCHAR,
    address VARCHAR,
    PRIMARY KEY (mr_no)
);

-- Create jawda_mrd_observations table
CREATE TABLE IF NOT EXISTS {target_schema}.jawda_mrd_observations (
    charge_id VARCHAR,
    observation_type VARCHAR,
    code VARCHAR,
    value VARCHAR,
    value_type VARCHAR
);

-- Create jawda_bill_charge_claim table
CREATE TABLE IF NOT EXISTS {target_schema}.jawda_bill_charge_claim (
    bill_no VARCHAR,
    charge_id VARCHAR,
    claim_id VARCHAR,
    service_id VARCHAR,
    billed_amount NUMERIC,
    charge_amt NUMERIC,
    bill_charge_line_id VARCHAR,
    billed_date TIMESTAMP,
    billed_date_only DATE,
    dept_id VARCHAR,
    charge_status VARCHAR,
    patientshare NUMERIC,
    netclaim NUMERIC,
    paid_flag VARCHAR,
    bill_rate_plan_id VARCHAR
);

-- Create jawda_mrd_diagnosis table
CREATE TABLE IF NOT EXISTS {target_schema}.jawda_mrd_diagnosis (
    visit_id VARCHAR,
    id VARCHAR,
    description VARCHAR,
    icd_code VARCHAR,
    code_type VARCHAR,
    diag_type VARCHAR
);


-- Create a pivot table for diagnosis data
CREATE TABLE IF NOT EXISTS {target_schema}.mrd_diag_pivot (
    visit_id VARCHAR,
    p1 VARCHAR,
    s1 VARCHAR,
    s2 VARCHAR,
    s3 VARCHAR,
    s4 VARCHAR,
    s5 VARCHAR,
    s6 VARCHAR,
    s7 VARCHAR,
    s8 VARCHAR,
    s9 VARCHAR,
    s10 VARCHAR,
    s11 VARCHAR,
    s12 VARCHAR,
    s13 VARCHAR,
    s14 VARCHAR,
    s15 VARCHAR
);

-- Create a pivot table for observations data
CREATE TABLE IF NOT EXISTS {target_schema}.mrd_observs_pivot (
    charge_id VARCHAR,
    code1 VARCHAR,
    code2 VARCHAR,
    code3 VARCHAR,
    code4 VARCHAR,
    code5 VARCHAR,
    obstype1 VARCHAR,
    obstype2 VARCHAR,
    obstype3 VARCHAR,
    obstype4 VARCHAR,
    obstype5 VARCHAR,
    value1 VARCHAR,
    value2 VARCHAR,
    value3 VARCHAR,
    value4 VARCHAR,
    value5 VARCHAR,
    valuetype1 VARCHAR,
    valuetype2 VARCHAR,
    valuetype3 VARCHAR,
    valuetype4 VARCHAR,
    valuetype5 VARCHAR
);

-- Create s_patient_registration_full table for doctor details updates
CREATE TABLE IF NOT EXISTS {target_schema}.s_patient_registration_full (
    mr_no VARCHAR,
    patient_id VARCHAR,
    reg_date DATE,
    reg_time TIME, 
    doctor VARCHAR,
    dept_name VARCHAR,
    transfer_source VARCHAR,
    transfer_destination VARCHAR
);

-- Create masters tables
CREATE TABLE IF NOT EXISTS {target_schema}.m_doctors (
    doctor_id VARCHAR,
    doctor_name VARCHAR,
    dept_id VARCHAR
);

CREATE TABLE IF NOT EXISTS {target_schema}.m_department (
    dept_id VARCHAR,
    dept_name VARCHAR
);

-- Create main claim data table
CREATE TABLE IF NOT EXISTS {target_schema}.rcm_claim_data (
    claimid VARCHAR,
    insurancecompany VARCHAR,
    tpa_name VARCHAR,
    receiverid VARCHAR,
    transactiondate TIMESTAMP,
    memberid VARCHAR,
    payerid VARCHAR,
    providerid VARCHAR,
    emiratesidnumber VARCHAR,
    gross NUMERIC,
    patientshare NUMERIC,
    netclaim NUMERIC,
    encountertype VARCHAR,
    patientid VARCHAR,
    patientname VARCHAR,
    patientbirthdate DATE,
    age INT,
    gender VARCHAR,
    encounterstart TIMESTAMP,
    encounterend TIMESTAMP,
    encounterstarttype VARCHAR,
    encounterendtype VARCHAR,
    encountertransfersource VARCHAR,
    encountertransferdestination VARCHAR,
    encounterdiagnosisprincipal VARCHAR,
    encounterdiagnosissecondary1 VARCHAR,
    encounterdiagnosissecondary2 VARCHAR,
    encounterdiagnosissecondary3 VARCHAR,
    encounterdiagnosissecondary4 VARCHAR,
    encounterdiagnosissecondary5 VARCHAR,
    encounterdiagnosissecondary6 VARCHAR,
    encounterdiagnosissecondary7 VARCHAR,
    encounterdiagnosissecondary8 VARCHAR,
    encounterdiagnosissecondary9 VARCHAR,
    encounterdiagnosissecondary10 VARCHAR,
    encounterdiagnosissecondary11 VARCHAR,
    encounterdiagnosissecondary12 VARCHAR,
    encounterdiagnosissecondary13 VARCHAR,
    encounterdiagnosissecondary14 VARCHAR,
    encounterdiagnosissecondary15 VARCHAR,
    activityid VARCHAR,
    activitycode VARCHAR,
    activitydescription VARCHAR,
    activitytype VARCHAR,
    activitydate DATE,
    activitynotes VARCHAR,
    activityunitcost NUMERIC,
    activityquantity INT,
    activitynet NUMERIC,
    obscode1 VARCHAR,
    obstype1 VARCHAR,
    obsvalue1 VARCHAR,
    obsvaluetype1 VARCHAR,
    obscode2 VARCHAR,
    obstype2 VARCHAR,
    obsvalue2 VARCHAR,
    obsvaluetype2 VARCHAR,
    obscode3 VARCHAR,
    obstype3 VARCHAR,
    obsvalue3 VARCHAR,
    obsvaluetype3 VARCHAR,
    obscode4 VARCHAR,
    obstype4 VARCHAR,
    obsvalue4 VARCHAR,
    obsvaluetype4 VARCHAR,
    obscode5 VARCHAR,
    obstype5 VARCHAR,
    obsvalue5 VARCHAR,
    obsvaluetype5 VARCHAR,
    doctorname VARCHAR,
    doctorsdepartment VARCHAR,
    primarydiagnosis VARCHAR,
    secondarydiagnosis VARCHAR,
    insurance_co_id VARCHAR,
    visit_id VARCHAR,
    charge_id VARCHAR,
    admitting_dr_id VARCHAR,
    tpa_id VARCHAR,
    center_id VARCHAR,
    plan_id VARCHAR,
    centername VARCHAR,
    registration_date TIMESTAMP,
    insurance_plan VARCHAR,
    insurance_plan_type VARCHAR,
    rate_plan VARCHAR,
    rate_plan_id VARCHAR
);
"""

# SQL for creating diagnosis pivot table
create_diagnosis_pivot_sql = """
SELECT * FROM crosstab('select visit_id,diag_type,icd_code from (select visit_id ,icd_code ,diag_type ||row_number as diag_type from (select visit_id,icd_code,diag_type,row_number() over (partition by visit_id order by diag_type) from {target_schema}.jawda_mrd_diagnosis md) as foo)as f') 
AS (visit_id character varying, P1 character varying ,S1 character varying, S2 character varying, S3 character varying,S4 character varying,S5 character varying,S6 character varying,S7 character varying,S8 character varying,S9 character varying,S10 character varying,S11 character varying,S12 character varying,S13 character varying,S14 character varying,S15 character varying)
"""

# SQL for creating observations pivot table
create_observations_pivot_sql = """
SELECT * FROM crosstab('select charge_id, row_number() over () as seq, observation_type, code, value, value_type
from {target_schema}.jawda_mrd_observations order by charge_id, observation_type') 
AS (charge_id character varying, code1 character varying, code2 character varying, code3 character varying, code4 character varying, code5 character varying,
obstype1 character varying, obstype2 character varying, obstype3 character varying, obstype4 character varying, obstype5 character varying,
value1 character varying, value2 character varying, value3 character varying, value4 character varying, value5 character varying,
valuetype1 character varying, valuetype2 character varying, valuetype3 character varying, valuetype4 character varying, valuetype5 character varying)
"""

# SQL for creating fact data
create_fact_data_sql = """
INSERT INTO {target_schema}.rcm_claim_data 
(claimid, insurancecompany, tpa_name, patientid, patientname, patientbirthdate, 
encounterdiagnosisprincipal, visit_id, insurance_co_id, admitting_dr_id, doctorname, 
tpa_id, center_id, plan_id, registration_date, centername, insurance_plan)
SELECT distinct claimid, null::text AS insurancecompany, null::text AS tpa_name,
patientid, null::text AS patientname, null::text AS patientbirthdate, 
primary_icd AS Encounterdiagnosisprincipal, null::text as primary_diagnosis,
visit_id, insurance_co_id, admitting_dr_id, null::text AS doctorname, 
tpa_id, center_id, plan_id, registration_date,
null::text as CenterName, null::text as Insurance_Plan
FROM {source_schema}.f_claim_submission
WHERE center_id::integer in ({center_id}) 
      AND registration_date::date >= '{from_date}' 
      AND registration_date::date <= '{to_date}'
      AND org_sub_seq = 1 
UNION ALL
SELECT distinct claim_id as claimid, null::text AS insurancecompany, null::text AS tpa_name,
patient_id as patientid, null::text AS patientname, null::text AS patientbirthdate, 
null AS Encounterdiagnosisprincipal, null::text as primary_diagnosis,
visit_id, insurance_co_id, admitting_dr_id, null::text AS doctorname, 
tpa_id, center_id::text as center_id, plan_id::text as plan_id, 
null::timestamp as registration_date,
null::text as CenterName, null::text as Insurance_Plan
FROM {source_schema}.f_open_claims
WHERE center_id::integer in ({center_id});
"""

# Function to detect if running on local or remote environment
def is_local_environment():
    """Determine if we're running on local or remote environment"""
    hostname = socket.gethostname()
    # Assume we're local if the hostname matches the local machine name
    # Adjust this logic if needed
    return "manjunath" in hostname.lower()

def create_fact_data(**context):
    """
    Extract claim fact data and insert into rcm_claim_data table using the same approach as in jawdaDatafromFact.ktr
    """
    try:
        # Get params from XCom
        ti = context['ti']
        params = ti.xcom_pull(task_ids='initialize_params')
        target_schema = params['TARGET_SCHEMA']
        source_schema = params['SOURCE_SCHEMA']
        center_id = params['CENTER_ID']
        from_date = params['FROM_DATE']
        to_date = params['TO_DATE']
        
        # Format the SQL with parameters
        sql = create_fact_data_sql.format(
            target_schema=target_schema,
            source_schema=source_schema,
            center_id=center_id,
            from_date=from_date,
            to_date=to_date
        )
        
        # Create a connection to the target database
        target_hook = PostgresHook(postgres_conn_id='postgres_target')
        
        # Execute the query
        logger.info("Creating fact data in rcm_claim_data table")
        target_hook.run(sql)
        logger.info("Fact data creation completed successfully")
        
        return True
    except Exception as e:
        logger.error(f"Error creating fact data: {str(e)}")
        raise AirflowException(f"Failed to create fact data: {str(e)}")

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
        extra = source_conn.extra_dejson
        source_schema = "public"  # Default schema
        
        # Try to extract schema from extra field
        if isinstance(extra, dict):
            if 'schema' in extra:
                source_schema = extra.get('schema')
            elif 'extra' in extra and isinstance(extra['extra'], dict) and 'schema' in extra['extra']:
                source_schema = extra['extra'].get('schema')
        
        # If no schema found, try to extract from URI query parameters
        if source_schema == "public" and hasattr(source_conn, 'extra') and source_conn.extra:
            import urllib.parse
            try:
                parsed = urllib.parse.parse_qs(source_conn.extra.lstrip('?'))
                if 'options' in parsed and parsed['options']:
                    options_str = parsed['options'][0]
                    if '-c search_path=' in options_str:
                        import re
                        search_path_match = re.search(r'-c search_path=(\w+)', options_str)
                        if search_path_match:
                            source_schema = search_path_match.group(1)
            except:
                pass  # If parsing fails, use default
                
        # Log what we found
        logger.info(f"Source connection - Schema field: {source_conn.schema}, Extracted schema: {source_schema}")
        
        source_params = {
            "SOURCE_HOST": source_conn.host,
            "SOURCE_DB": source_conn.schema,  # In Airflow UI, schema field is used for database name
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
        
        # Get BI source connection details (if different from primary source)
        try:
            bi_source_conn = Connection.get_connection_from_secrets("postgres_bi_source")
            bi_source_schema = "public"
            
            # Try to extract schema from extra field for BI source
            bi_extra = bi_source_conn.extra_dejson
            if isinstance(bi_extra, dict):
                if 'schema' in bi_extra:
                    bi_source_schema = bi_extra.get('schema')
                elif 'extra' in bi_extra and isinstance(bi_extra['extra'], dict) and 'schema' in bi_extra['extra']:
                    bi_source_schema = bi_extra['extra'].get('schema')
            
            source_params["BI_SOURCE_HOST"] = bi_source_conn.host
            source_params["BI_SOURCE_DB"] = bi_source_conn.schema
            source_params["BI_SOURCE_USER"] = bi_source_conn.login
            source_params["BI_SOURCE_PWD"] = bi_source_conn.password
            source_params["BI_SOURCE_SCHEMA"] = bi_source_schema
        except:
            # Use primary source if BI source not configured
            source_params["BI_SOURCE_HOST"] = source_params["SOURCE_HOST"]
            source_params["BI_SOURCE_DB"] = source_params["SOURCE_DB"]
            source_params["BI_SOURCE_USER"] = source_params["SOURCE_USER"]
            source_params["BI_SOURCE_PWD"] = source_params["SOURCE_PWD"]
            source_params["BI_SOURCE_SCHEMA"] = source_params["SOURCE_SCHEMA"]
        
        # Get target connection details
        target_conn = Connection.get_connection_from_secrets("postgres_target")
        
        # Parse target extra field
        target_extra = target_conn.extra_dejson
        target_schema = "public"  # Default schema
        
        # Try to extract schema from extra field
        if isinstance(target_extra, dict):
            if 'schema' in target_extra:
                target_schema = target_extra.get('schema')
            elif 'extra' in target_extra and isinstance(target_extra['extra'], dict) and 'schema' in target_extra['extra']:
                target_schema = target_extra['extra'].get('schema')
        
        # If no schema found, try to extract from URI query parameters
        if target_schema == "public" and hasattr(target_conn, 'extra') and target_conn.extra:
            import urllib.parse
            try:
                parsed = urllib.parse.parse_qs(target_conn.extra.lstrip('?'))
                if 'options' in parsed and parsed['options']:
                    options_str = parsed['options'][0]
                    if '-c search_path=' in options_str:
                        import re
                        search_path_match = re.search(r'-c search_path=(\w+)', options_str)
                        if search_path_match:
                            target_schema = search_path_match.group(1)
            except:
                pass  # If parsing fails, use default
        
        # Additionally, check for search_path in options
        if hasattr(target_conn, 'extra') and target_conn.extra and '"options"' in target_conn.extra:
            import re
            import json
            try:
                # Try to parse the options as JSON
                match = re.search(r'"options"\s*:\s*"([^"]+)"', target_conn.extra)
                if match:
                    options_str = match.group(1)
                    if '-c search_path=' in options_str:
                        search_path_match = re.search(r'-c search_path=(\w+)', options_str)
                        if search_path_match:
                            target_schema = search_path_match.group(1)
            except:
                pass  # If parsing fails, use default
                
        # Log what we found
        logger.info(f"Target connection - Schema field: {target_conn.schema}, Extracted schema: {target_schema}")
        
        target_params = {
            "TARGET_HOST": target_conn.host,
            "TARGET_DB": target_conn.schema,  # In Airflow UI, schema field is used for database name
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

# Function to extract patient data
def extract_patient_data(**context):
    """Extract patient visit data from source DB"""
    try:
        # Get params from XCom
        ti = context['ti']
        params = ti.xcom_pull(task_ids='initialize_params')
        source_schema = params['SOURCE_SCHEMA']
        center_id = params['CENTER_ID']
        from_date = params['FROM_DATE']
        to_date = params['TO_DATE']

        # Create a connection to the source database
        source_hook = PostgresHook(postgres_conn_id='postgres_source')
        
        # Log the extraction parameters
        logger.info(f"Extracting patient data from schema: {source_schema}")
        logger.info(f"Parameters: CenterID = {center_id}, FromDate = {from_date}, ToDate = {to_date}")
        
        # Use the exact same query from the KTR file
        patient_query = f"""
        SELECT  pr.mr_no,
    		pr.patient_id ,
    		pr.reg_date ,
    		pr.reg_time , 
    		pr.doctor ,
    		pr.dept_name ,
    		pr.transfer_source ,
    		pr.transfer_destination 
        FROM insurance_submission_batch isb
        LEFT JOIN insurance_claim ic on(ic.submission_batch_id=isb.submission_batch_id)
        JOIN patient_registration pr on ic.patient_id=pr.patient_id
        JOIN hospital_center_master hcm on hcm.center_id=pr.center_id
        WHERE hcm.center_id in ({center_id})
        AND isb.submission_date::date >= '{from_date}' AND isb.submission_date::date <= '{to_date}';
        """
        
        logger.info("Executing patient data query")
        result = source_hook.get_pandas_df(patient_query)
        
        if result.empty:
            logger.warning("No patient data found for the specified criteria")
            return []
        
        # Save results to a temporary file for Spark processing
        logger.info(f"Retrieved {len(result)} patient records")
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        result.to_csv(temp_file.name, index=False)
        logger.info(f"Patient data saved to {temp_file.name}")
        
        # Pass the filepath via XCom
        return temp_file.name
        
    except Exception as e:
        logger.error(f"Error extracting patient data: {str(e)}")
        raise AirflowException(f"Failed to extract patient data: {str(e)}")

# Function to extract diagnosis data
def extract_diagnosis_data(**context):
    """Extract diagnosis data for the selected patients"""
    try:
        # Get params from XCom
        ti = context['ti']
        params = ti.xcom_pull(task_ids='initialize_params')
        source_schema = params['SOURCE_SCHEMA']
        center_id = params['CENTER_ID']
        from_date = params['FROM_DATE']
        to_date = params['TO_DATE']
        
        # Log source schema being used
        logger.info(f"Using source schema: {source_schema} for diagnosis data extraction")
        
        # Create a connection to the source database
        source_hook = PostgresHook(postgres_conn_id='postgres_source')
        
        # Use the exact same query from the KTR file
        diagnosis_query = f"""
        SELECT md.visit_id,md.id,md.description,md.icd_code,md.code_type,md.diag_type
        FROM insurance_submission_batch isb
        LEFT JOIN insurance_claim ic on(ic.submission_batch_id=isb.submission_batch_id)
        JOIN patient_registration pr on ic.patient_id=pr.patient_id
        JOIN mrd_diagnosis md ON md.visit_id=pr.patient_id 
        JOIN hospital_center_master hcm on hcm.center_id=pr.center_id
        WHERE hcm.center_id in ({center_id}) 
        AND isb.submission_date::date >= '{from_date}' 
        AND isb.submission_date::date <= '{to_date}';
        """
        
        logger.info("Executing diagnosis data query")
        result = source_hook.get_pandas_df(diagnosis_query)
        
        if result.empty:
            logger.warning("No diagnosis data found for the specified criteria")
            return []
        
        # Save results to a temporary file for Spark processing
        logger.info(f"Retrieved {len(result)} diagnosis records")
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        result.to_csv(temp_file.name, index=False)
        logger.info(f"Diagnosis data saved to {temp_file.name}")
        
        # Pass the filepath via XCom
        return temp_file.name
        
    except Exception as e:
        error_msg = f"Failed to extract diagnosis data from schema '{source_schema}': {str(e)}"
        logger.error(error_msg)
        raise AirflowException(error_msg)

# SQL for updating patient details
update_patient_details_sql = """
UPDATE {target_schema}.rcm_claim_data x
SET patientname = pd.patient_full_name,
    patientbirthdate = pd.date,
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
    FROM {target_schema}.jawda_patient_details
) pd
WHERE x.patientid = pd.mr_no;
"""

# SQL for updating insurance company
update_insurance_sql = """
UPDATE {target_schema}.rcm_claim_data jt
SET insurance_company = i.insurance_co_name
FROM {target_schema}.m_insurance_company_master i
WHERE jt.insurance_co_id = i.insurance_co_id;
"""

# SQL for updating diagnosis from mrd_diag_pivot
update_diagnosis_sql = """
UPDATE {target_schema}.rcm_claim_data jt
SET Encounterdiagnosisprincipal=mdp.P1,
Encounterdiagnosissecondary1=mdp.S1,
Encounterdiagnosissecondary2=mdp.S2,
Encounterdiagnosissecondary3=mdp.S3,
Encounterdiagnosissecondary4=mdp.S4,
Encounterdiagnosissecondary5=mdp.S5,
Encounterdiagnosissecondary6=mdp.S6,
Encounterdiagnosissecondary7=mdp.S7,
Encounterdiagnosissecondary8=mdp.S8,
Encounterdiagnosissecondary9=mdp.S9,
Encounterdiagnosissecondary10=mdp.S10,
Encounterdiagnosissecondary11=mdp.S11,
Encounterdiagnosissecondary12=mdp.S12,
Encounterdiagnosissecondary13=mdp.S13,
Encounterdiagnosissecondary14=mdp.S14,
Encounterdiagnosissecondary15=mdp.S15
FROM {target_schema}.mrd_diag_pivot mdp
WHERE jt.visit_id=mdp.visit_id;
"""

# SQL for updating doctor details
update_doctor_details_sql = """
UPDATE {target_schema}.rcm_claim_data x
SET doctorname=d.doctor_name,
doctorsdepartment=dp.dept_name,
Encountertransfersource=pr.transfer_source,
Encountertransferdestination=pr.transfer_destination,
registration_date=pr.reg_date+pr.reg_time
FROM {target_schema}.s_patient_registration_full pr
JOIN {target_schema}.m_doctors d ON d.doctor_id=pr.doctor
JOIN {target_schema}.m_department dp ON (d.dept_id=dp.dept_id)
WHERE x.visit_id=pr.patient_id;
"""

# SQL for updating observations
update_observations_sql = """
UPDATE {target_schema}.rcm_claim_data j
SET obscode1=c.code1,
obscode2=c.code2,
obscode3=c.code3,
obscode4=c.code4,
obscode5=c.code5,
obstype1=c.obstype1,
obstype2=c.obstype2,
obstype3=c.obstype3,
obstype4=c.obstype4,
obstype5=c.obstype5,
obsvalue1=c.value1,
obsvalue2=c.value2,
obsvalue3=c.value3,
obsvalue4=c.value4,
obsvalue5=c.value5,
obsvaluetype1=c.valuetype1,
obsvaluetype2=c.valuetype2,
obsvaluetype3=c.valuetype3,
obsvaluetype4=c.valuetype4,
obsvaluetype5=c.valuetype5
FROM {target_schema}.mrd_observs_pivot c
WHERE j.charge_id=c.charge_id;
"""

def execute_sql_with_schema(**context):
    """
    Custom function to execute SQL statements with the schema from XCom.
    Will create the schema if it doesn't exist.
    """
    ti = context['ti']
    # Get target schema from xcom
    params = ti.xcom_pull(task_ids='initialize_params')
    target_schema = params['TARGET_SCHEMA']
    target_db = params['TARGET_DB']
    
    # Get pre-defined SQL and use format to insert the schema
    sql = context['templates_dict']['sql']
    rendered_sql = sql.format(target_schema=target_schema)
    
    logger.info(f"Executing SQL with database: {target_db}, schema: {target_schema}")
    logger.info(f"SQL sample: {rendered_sql[:200]}...")
    
    # Create a PostgreSQL hook
    hook = PostgresHook(postgres_conn_id='postgres_target')
    
    # First, create the schema if it doesn't exist
    create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {target_schema};"
    logger.info(f"Creating schema if not exists: {target_schema}")
    
    try:
        hook.run(create_schema_sql)
        logger.info(f"Schema {target_schema} created or already exists")
        
        # Now execute the main SQL
        hook.run(rendered_sql)
        logger.info("SQL executed successfully")
        return True
    except Exception as e:
        logger.error(f"Error executing SQL: {str(e)}")
        raise AirflowException(f"Failed to execute SQL: {str(e)}")

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
        patient_script = "/home/airflow/airflow/scripts/spark/process_patient_data.py"
        diagnosis_script = "/home/airflow/airflow/scripts/spark/process_diagnosis_data.py"
        
        logger.info(f"Checking if Spark scripts exist:")
        logger.info(f"Patient script exists: {os.path.exists(patient_script)}")
        if os.path.exists(patient_script):
            logger.info(f"Patient script permissions: {oct(os.stat(patient_script).st_mode)[-3:]}")
            logger.info(f"Patient script size: {os.path.getsize(patient_script)} bytes")
        
        logger.info(f"Diagnosis script exists: {os.path.exists(diagnosis_script)}")
        if os.path.exists(diagnosis_script):
            logger.info(f"Diagnosis script permissions: {oct(os.stat(diagnosis_script).st_mode)[-3:]}")
            logger.info(f"Diagnosis script size: {os.path.getsize(diagnosis_script)} bytes")
        
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
        
        # Check if spark-submit is available
        try:
            import subprocess
            spark_submit_path = "/home/airflow/airflow/airflow_venv/bin/spark-submit"
            if os.path.exists(spark_submit_path):
                logger.info(f"spark-submit exists at: {spark_submit_path}")
                logger.info(f"spark-submit permissions: {oct(os.stat(spark_submit_path).st_mode)[-3:]}")
                # Try to get spark-submit version
                try:
                    version = subprocess.check_output([spark_submit_path, '--version']).decode()
                    logger.info(f"Spark version: {version}")
                except:
                    logger.warning("Could not get Spark version")
            else:
                logger.warning(f"spark-submit not found at: {spark_submit_path}")
        except Exception as e:
            logger.error(f"Error checking spark-submit: {str(e)}")
        
        return True
    except Exception as e:
        logger.error(f"Error in debug_spark_environment: {str(e)}")
        raise AirflowException(f"Failed to debug Spark environment: {str(e)}")

# Tasks definition
init_params_task = PythonOperator(
    task_id="initialize_params",
    python_callable=get_connection_params,
    dag=dag,
)

create_tables_task = PythonOperator(
    task_id="create_staging_tables",
    python_callable=execute_sql_with_schema,
    templates_dict={'sql': create_staging_tables_sql},
    dag=dag,
)

extract_patient_task = PythonOperator(
    task_id="extract_patient_data",
    python_callable=extract_patient_data,
    dag=dag,
)

extract_diagnosis_task = PythonOperator(
    task_id="extract_diagnosis_data",
    python_callable=extract_diagnosis_data,
    dag=dag,
)

# Define Spark tasks with minimal configuration
process_patient_data_task = SparkSubmitOperator(
    task_id="process_patient_data",
    application="/home/airflow/airflow/scripts/spark/process_patient_data.py",
    conn_id="spark_default",
    application_args=[
        "--params_file", "/tmp/claim_data_params.json",
        "--patient_file", "{{ ti.xcom_pull(task_ids='extract_patient_data') }}"
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
        "spark.driver.memory": "1g",
        "spark.executor.memory": "2g",
        "spark.hadoop.fs.defaultFS": "file:///",
        "spark.driver.extraJavaOptions": "-Dfs.defaultFS=file:///",
        "spark.sql.shuffle.partitions": "2",
        "spark.default.parallelism": "2",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    },
    dag=dag,
)

process_diagnosis_data_task = SparkSubmitOperator(
    task_id="process_diagnosis_data",
    application="/home/airflow/airflow/scripts/spark/process_diagnosis_data.py",
    conn_id="spark_default",
    application_args=[
        "--params_file", "/tmp/claim_data_params.json",
        "--diagnosis_file", "{{ ti.xcom_pull(task_ids='extract_diagnosis_data') }}"
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
        "spark.driver.memory": "1g",
        "spark.executor.memory": "2g",
        "spark.hadoop.fs.defaultFS": "file:///",
        "spark.driver.extraJavaOptions": "-Dfs.defaultFS=file:///",
        "spark.sql.shuffle.partitions": "2",
        "spark.default.parallelism": "2",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    },
    dag=dag,
)

# Create the diagnosis pivot task - use SQL directly instead of Spark
create_diagnosis_pivot_task = PythonOperator(
    task_id="create_diagnosis_pivot",
    python_callable=execute_sql_with_schema,
    templates_dict={'sql': create_diagnosis_pivot_sql},
    dag=dag,
)

# Replace the Spark task with a direct SQL task
create_fact_data_task = PythonOperator(
    task_id="create_fact_data",
    python_callable=create_fact_data,
    dag=dag,
)

update_patient_details_task = PythonOperator(
    task_id="update_patient_details",
    python_callable=execute_sql_with_schema,
    templates_dict={'sql': update_patient_details_sql},
    dag=dag,
)

update_insurance_task = PythonOperator(
    task_id="update_insurance_co",
    python_callable=execute_sql_with_schema,
    templates_dict={'sql': update_insurance_sql},
    dag=dag,
)

update_diagnosis_task = PythonOperator(
    task_id="update_diagnosis",
    python_callable=execute_sql_with_schema,
    templates_dict={'sql': update_diagnosis_sql},
    dag=dag,
)

update_doctor_details_task = PythonOperator(
    task_id="update_doctor_details",
    python_callable=execute_sql_with_schema,
    templates_dict={'sql': update_doctor_details_sql},
    dag=dag,
)

update_observations_task = PythonOperator(
    task_id="update_observations",
    python_callable=execute_sql_with_schema,
    templates_dict={'sql': update_observations_sql},
    dag=dag,
)

# Create the observations pivot task
create_observations_pivot_task = PythonOperator(
    task_id="create_observations_pivot",
    python_callable=execute_sql_with_schema,
    templates_dict={'sql': create_observations_pivot_sql},
    dag=dag,
)

# Add debug task before Spark tasks
debug_spark_task = PythonOperator(
    task_id="debug_spark_environment",
    python_callable=debug_spark_environment,
    dag=dag,
)

# Define the task flow
init_params_task >> create_tables_task >> extract_patient_task >> extract_diagnosis_task
extract_patient_task >> debug_spark_task >> process_patient_data_task
extract_diagnosis_task >> debug_spark_task >> process_diagnosis_data_task
process_patient_data_task >> create_fact_data_task
process_diagnosis_data_task >> create_diagnosis_pivot_task
create_fact_data_task >> update_patient_details_task
create_fact_data_task >> update_insurance_task
create_diagnosis_pivot_task >> update_diagnosis_task
create_fact_data_task >> update_doctor_details_task
create_fact_data_task >> create_observations_pivot_task
create_observations_pivot_task >> update_observations_task 
