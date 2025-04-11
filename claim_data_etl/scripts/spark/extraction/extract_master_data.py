#!/usr/bin/env python3
# Spark script to extract master data and load it into staging tables

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import argparse
import json
import os
import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatabaseConnector:
    """Helper class to handle database connections and operations"""
    
    def __init__(self, host, port, db, user, pwd, schema):
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.pwd = pwd if pwd is not None else ""
        self.schema = schema
        self.jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"
        self.properties = {
            "user": user,
            "password": self.pwd,
            "driver": "org.postgresql.Driver"
        }
        
    def get_jdbc_url(self):
        return self.jdbc_url
        
    def get_properties(self):
        return self.properties
        
    def with_search_path(self):
        """Return connection properties with search_path set"""
        props = self.properties.copy()
        props["SQL_CONNECT"] = f"SET search_path TO {self.schema}"
        return props
        
    def find_schema_for_table(self, spark, table_name):
        """Find the schema that contains the specified table"""
        query = f"""
        SELECT table_schema 
        FROM information_schema.tables 
        WHERE table_name = '{table_name}'
        """
        
        df = spark.read.jdbc(
            url=self.jdbc_url, 
            table=f"({query}) as tmp",
            properties=self.properties
        )
        
        if df.count() > 0:
            return df.first()['table_schema']
        return self.schema  # fallback to default schema

def create_spark_session():
    """Create and configure Spark session"""
    builder = SparkSession.builder \
        .appName("Master Data Extraction") \
        .config("spark.sql.files.ignoreMissingFiles", "true") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .config("spark.driver.extraJavaOptions", "-Dfs.defaultFS=file:///") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "6g") \
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

def extract_data(spark, source_connector, query):
    """Extract data using the provided query"""
    try:
        # Try with schema qualification first
        df = spark.read.jdbc(
            url=source_connector.get_jdbc_url(),
            table=f"({query}) as tmp",
            properties=source_connector.get_properties()
        )
        return df
    except Exception as e:
        logger.warning(f"Failed with standard properties: {str(e)}")
        
        # Try with search_path
        try:
            df = spark.read.jdbc(
                url=source_connector.get_jdbc_url(),
                table=f"({query}) as tmp",
                properties=source_connector.with_search_path()
            )
            return df
        except Exception as e2:
            logger.error(f"Failed with search_path: {str(e2)}")
            raise e2

def write_master_data(spark, df, target_connector, table_name):
    """Write dataframe to master table by deleting existing data and inserting new data.
    This preserves table dependencies while refreshing the data."""
    if df.count() > 0:
        logger.info(f"Writing {df.count()} rows to master table {table_name}")
        
        # Get JDBC connection for executing SQL commands
        jdbc_url = target_connector.get_jdbc_url()
        properties = target_connector.get_properties()
        qualified_table = f"{target_connector.schema}.{table_name}"
        
        try:
            # First delete all existing data from the table
            delete_query = f"DELETE FROM {qualified_table}"
            logger.info(f"Deleting existing data from master table: {delete_query}")
            
            # Execute delete using JDBC
            delete_df = spark.read.jdbc(
                url=jdbc_url,
                table=f"({delete_query}; SELECT 1 as success) as tmp",
                properties=properties
            )
            
            # Now insert the data from our dataframe using spark's native JDBC capabilities
            logger.info(f"Inserting data into master table {qualified_table}")
            df.write.mode("append").jdbc(
                url=jdbc_url,
                table=qualified_table,
                properties=properties
            )
            
            logger.info(f"Successfully updated master table {table_name}")
        except Exception as e:
            logger.error(f"Error updating master table {table_name}: {str(e)}")
            # Fallback to regular write if delete fails
            logger.info(f"Falling back to regular table write for {table_name}")
            df.write.mode("overwrite").jdbc(
                url=jdbc_url,
                table=qualified_table,
                properties=properties
            )
    else:
        logger.warning(f"No data to write to master table {table_name}")

def main():
    """Main extraction function for master data"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Extract master data and load to staging tables')
    parser.add_argument('--params_file', required=True, help='Path to JSON file with connection parameters')
    args = parser.parse_args()
    
    # Load connection parameters
    logger.info(f"Loading parameters from {args.params_file}")
    try:
        with open(args.params_file, 'r') as f:
            params = json.load(f)
        
        # Source and target connection details
        source_connector = DatabaseConnector(
            host=params.get('SOURCE_HOST'),
            port=params.get('SOURCE_PORT', 5432),
            db=params.get('SOURCE_DB'),
            user=params.get('SOURCE_USER'),
            pwd=params.get('SOURCE_PWD'),
            schema=params.get('SOURCE_SCHEMA')
        )
        
        target_connector = DatabaseConnector(
            host=params.get('TARGET_HOST'),
            port=params.get('TARGET_PORT', 5432),
            db=params.get('TARGET_DB'),
            user=params.get('TARGET_USER'),
            pwd=params.get('TARGET_PWD'),
            schema=params.get('TARGET_SCHEMA')
        )
        
        logger.info(f"Source DB: {source_connector.db}, Source Schema: {source_connector.schema}")
        logger.info(f"Target DB: {target_connector.db}, Target Schema: {target_connector.schema}")
    except Exception as e:
        logger.error(f"Error loading parameters: {str(e)}")
        sys.exit(1)
    
    # Create Spark session
    logger.info("Initializing Spark session")
    spark = create_spark_session()
    
    try:
        # Find the schema that contains our main table
        actual_schema = source_connector.find_schema_for_table(spark, 'insurance_submission_batch')
        logger.info(f"Using schema: {actual_schema} for queries")
        
        # Extract doctors master data
        logger.info("Extracting doctors master data")
        doctors_query = f"SELECT doctor_id, doctor_name, dept_id FROM {actual_schema}.doctors"
        doctors_df = extract_data(spark, source_connector, doctors_query)
        
        # Check if doctors data was loaded properly
        doctors_row_count = doctors_df.count()
        logger.info(f"Extracted {doctors_row_count} doctor records")
        
        if doctors_row_count > 0:
            # Write to doctors table in target DB using truncate/insert
            write_master_data(spark, doctors_df, target_connector, "m_doctors")
            logger.info("Doctors master data extraction completed successfully")
        else:
            logger.warning("No doctors data was found")
            
        # Extract departments master data
        logger.info("Extracting departments master data")
        dept_query = f"SELECT dept_id, dept_name FROM {actual_schema}.department"
        dept_df = extract_data(spark, source_connector, dept_query)
        
        # Check if departments data was loaded properly
        dept_row_count = dept_df.count()
        logger.info(f"Extracted {dept_row_count} department records")
        
        if dept_row_count > 0:
            # Write to departments table in target DB using truncate/insert
            write_master_data(spark, dept_df, target_connector, "m_department")
            logger.info("Department master data extraction completed successfully")
        else:
            logger.warning("No department data was found")
            
        # Extract insurance company master data
        logger.info("Extracting insurance company master data")
        insurance_query = f"SELECT insurance_co_id, insurance_co_name FROM {actual_schema}.insurance_company_master"
        insurance_df = extract_data(spark, source_connector, insurance_query)
        
        # Check if insurance company data was loaded properly
        insurance_row_count = insurance_df.count()
        logger.info(f"Extracted {insurance_row_count} insurance company records")
        
        if insurance_row_count > 0:
            # Write to insurance company table in target DB using truncate/insert
            write_master_data(spark, insurance_df, target_connector, "m_insurance_company_master")
            logger.info("Insurance company master data extraction completed successfully")
        else:
            logger.warning("No insurance company data was found")
        
    except Exception as e:
        logger.error(f"Error processing master data: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        # Stop Spark session
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main() 