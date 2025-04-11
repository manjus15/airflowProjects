#!/usr/bin/env python3
# Spark script to update claim data fields from pivot and master tables

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
import argparse
import json
import os
import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create and configure Spark session"""
    builder = SparkSession.builder \
        .appName("Claim Data Update Transformation") \
        .config("spark.sql.files.ignoreMissingFiles", "true") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .config("spark.driver.extraJavaOptions", "-Dfs.defaultFS=file:///") \
        .config("spark.driver.memory", "3g") \
        .config("spark.executor.memory", "5g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.default.parallelism", "4") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .master("local[*]")
    
    # Add PostgreSQL JDBC driver if available
    jdbc_jar_path = "/opt/spark/jars/postgresql-42.2.18.jar"
    if os.path.exists(jdbc_jar_path):
        builder = builder.config("spark.driver.extraClassPath", jdbc_jar_path) \
                         .config("spark.jars", jdbc_jar_path)
    
    return builder.getOrCreate()

def execute_update_query(spark, jdbc_url, connection_properties, query, description):
    """Execute an update query on the target database using Spark"""
    try:
        logger.info(f"Executing {description}")
        
        # Configure JDBC properties for better write performance
        write_properties = connection_properties.copy()
        
        # Import required Java classes for direct JDBC execution
        from pyspark.sql import SparkSession
        from py4j.java_gateway import java_import
        
        # Get the Java JDBC driver
        java_import(spark._jvm, "java.sql.DriverManager")
        java_import(spark._jvm, "java.sql.Connection")
        java_import(spark._jvm, "java.util.Properties")
        java_import(spark._jvm, "java.sql.SQLException")
        
        # Create properties object
        props = spark._jvm.java.util.Properties()
        for key, value in write_properties.items():
            props.setProperty(key, value)
        
        # Create connection and execute update
        conn = None
        stmt = None
        try:
            conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, props)
            stmt = conn.createStatement()
            
            # Execute the update query
            logger.info(f"Executing SQL: {query}")
            stmt.executeUpdate(query)
            
            logger.info(f"{description} completed successfully")
        finally:
            # Close resources
            if stmt is not None:
                stmt.close()
            if conn is not None:
                conn.close()
    except Exception as e:
        logger.error(f"Error executing {description}: {str(e)}")
        raise

def main():
    """Main transformation function for updating claim data"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Update claim data from pivot and master tables')
    parser.add_argument('--params_file', required=True, help='Path to JSON file with connection parameters')
    args = parser.parse_args()
    
    # Load connection parameters
    logger.info(f"Loading parameters from {args.params_file}")
    try:
        with open(args.params_file, 'r') as f:
            params = json.load(f)
        
        target_schema = params['TARGET_SCHEMA']
        target_host = params['TARGET_HOST']
        target_db = params['TARGET_DB']
        target_port = params['TARGET_PORT']
        target_user = params['TARGET_USER']
        target_pwd = params['TARGET_PWD']
        
        logger.info(f"Loaded parameters: Target DB={target_db}, Target Schema={target_schema}")
    except Exception as e:
        logger.error(f"Error loading parameters: {str(e)}")
        sys.exit(1)
    
    # Create Spark session
    logger.info("Initializing Spark session")
    spark = create_spark_session()
    
    # PostgreSQL connection properties and JDBC URL
    target_jdbc_url = f"jdbc:postgresql://{target_host}:{target_port}/{target_db}"
    connection_properties = {
        "user": target_user,
        "password": target_pwd,
        "driver": "org.postgresql.Driver",
        "currentSchema": target_schema  # Set the current schema for the connection
    }
    
    try:
        # 1. Update patient details
        update_patient_sql = f"""
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
        WHERE x.patientid = pd.mr_no
        """
        
        execute_update_query(
            spark,
            target_jdbc_url,
            connection_properties,
            update_patient_sql,
            "Patient details update"
        )
        
        # 2. Update insurance company
        update_insurance_sql = f"""
        UPDATE {target_schema}.rcm_claim_data jt
        SET insurancecompany = i.insurance_co_name
        FROM {target_schema}.m_insurance_company_master i
        WHERE jt.insurance_co_id = i.insurance_co_id
        """
        
        execute_update_query(
            spark,
            target_jdbc_url,
            connection_properties,
            update_insurance_sql,
            "Insurance company update"
        )
        
        # 3. Update diagnosis from pivot
        update_diagnosis_sql = f"""
        UPDATE {target_schema}.rcm_claim_data jt
        SET encounterdiagnosisprincipal=mdp.p1,
            encounterdiagnosissecondary1=mdp.s1,
            encounterdiagnosissecondary2=mdp.s2,
            encounterdiagnosissecondary3=mdp.s3,
            encounterdiagnosissecondary4=mdp.s4,
            encounterdiagnosissecondary5=mdp.s5,
            encounterdiagnosissecondary6=mdp.s6,
            encounterdiagnosissecondary7=mdp.s7,
            encounterdiagnosissecondary8=mdp.s8,
            encounterdiagnosissecondary9=mdp.s9,
            encounterdiagnosissecondary10=mdp.s10,
            encounterdiagnosissecondary11=mdp.s11,
            encounterdiagnosissecondary12=mdp.s12,
            encounterdiagnosissecondary13=mdp.s13,
            encounterdiagnosissecondary14=mdp.s14,
            encounterdiagnosissecondary15=mdp.s15
        FROM {target_schema}.mrd_diag_pivot mdp
        WHERE jt.visit_id=mdp.visit_id
        """
        
        execute_update_query(
            spark,
            target_jdbc_url,
            connection_properties,
            update_diagnosis_sql,
            "Diagnosis update from pivot"
        )
        
        # 4. Update doctor details
        update_doctor_sql = f"""
        UPDATE {target_schema}.rcm_claim_data x
        SET doctorname=d.doctor_name,
            doctorsdepartment=dp.dept_name,
            encountertransfersource=pr.transfer_source,
            encountertransferdestination=pr.transfer_destination,
            registration_date=pr.reg_date+pr.reg_time
        FROM {target_schema}.s_patient_registration_full pr
        JOIN {target_schema}.m_doctors d ON d.doctor_id=pr.doctor
        JOIN {target_schema}.m_department dp ON (d.dept_id=dp.dept_id)
        WHERE x.visit_id=pr.patient_id
        """
        
        execute_update_query(
            spark,
            target_jdbc_url,
            connection_properties,
            update_doctor_sql,
            "Doctor details update"
        )
        
        # 5. Update observations from pivot
        update_observations_sql = f"""
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
        WHERE j.charge_id=c.charge_id
        """
        
        execute_update_query(
            spark,
            target_jdbc_url,
            connection_properties,
            update_observations_sql,
            "Observations update from pivot"
        )
        
        logger.info("All updates completed successfully")
    except Exception as e:
        logger.error(f"Error updating claim data: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        # Stop Spark session
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main() 