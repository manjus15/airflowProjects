#!/usr/bin/env python3
# Spark script to create observations pivot table from extracted observation data

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
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
        .appName("Observations Pivot Transformation") \
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

def write_to_postgres(spark, df, table_name, jdbc_url, connection_properties, key_column):
    """Write DataFrame to PostgreSQL table with truncation"""
    try:
        logger.info(f"Writing data to {table_name}")
        row_count = df.count()
        logger.info(f"Writing {row_count} rows to {table_name}")
        
        if row_count > 0:
            # Configure JDBC properties for better write performance
            write_properties = connection_properties.copy()
            write_properties.update({
                "batchsize": "10000",  # Number of rows to write in each batch
                "rewriteBatchedStatements": "true",  # Enable batch statement rewriting
                "useServerPrepStmts": "true"  # Use server-side prepared statements
            })
            
            # Split schema and table name
            if "." in table_name:
                schema, pure_table = table_name.split(".")
                # Set currentSchema in properties
                write_properties["currentSchema"] = schema
            else:
                pure_table = table_name
                
            # First use mode("overwrite") instead of truncating
            # This will drop and recreate the table with the new data
            logger.info(f"Overwriting table {pure_table}")
            
            # Now insert the data with overwrite mode
            df.write.mode("overwrite").jdbc(
                url=jdbc_url,
                table=table_name,
                properties=write_properties
            )
            logger.info(f"Successfully wrote data to {table_name}")
        else:
            logger.warning(f"No data to write to {table_name}")
    except Exception as e:
        logger.error(f"Error writing to {table_name}: {str(e)}")
        raise

def main():
    """Main transformation function for creating observations pivot"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Create observations pivot table from extracted data')
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
    
    # PostgreSQL connection properties for target
    target_jdbc_url = f"jdbc:postgresql://{target_host}:{target_port}/{target_db}"
    target_connection_properties = {
        "user": target_user,
        "password": target_pwd,
        "driver": "org.postgresql.Driver",
        "currentSchema": target_schema
    }
    
    try:
        # Execute pivot query similar to the KTR file
        logger.info(f"Executing observations pivot query")
        
        # Define the pivot query to match KTR approach, but using Spark SQL instead
        pivot_query = """
        SELECT a.charge_id,
        MAX(CASE WHEN a.r_number = 1 then a.code end) as code1,
        MAX(CASE WHEN a.r_number = 2 then a.code end) as code2,
        MAX(CASE WHEN a.r_number = 3 then a.code end) as code3,
        MAX(CASE WHEN a.r_number = 4 then a.code end) as code4,
        MAX(CASE WHEN a.r_number = 5 then a.code end) as code5,
        MAX(CASE WHEN a.r_number = 1 then a.observation_type end) as obstype1,
        MAX(CASE WHEN a.r_number = 2 then a.observation_type end) as obstype2,
        MAX(CASE WHEN a.r_number = 3 then a.observation_type end) as obstype3,
        MAX(CASE WHEN a.r_number = 4 then a.observation_type end) as obstype4,
        MAX(CASE WHEN a.r_number = 5 then a.observation_type end) as obstype5,
        MAX(CASE WHEN a.r_number = 1 then a.value end) as value1,
        MAX(CASE WHEN a.r_number = 2 then a.value end) as value2,
        MAX(CASE WHEN a.r_number = 3 then a.value end) as value3,
        MAX(CASE WHEN a.r_number = 4 then a.value end) as value4,
        MAX(CASE WHEN a.r_number = 5 then a.value end) as value5,
        MAX(CASE WHEN a.r_number = 1 then a.value_type end) as valuetype1,
        MAX(CASE WHEN a.r_number = 2 then a.value_type end) as valuetype2,
        MAX(CASE WHEN a.r_number = 3 then a.value_type end) as valuetype3,
        MAX(CASE WHEN a.r_number = 4 then a.value_type end) as valuetype4,
        MAX(CASE WHEN a.r_number = 5 then a.value_type end) as valuetype5
        FROM (
            SELECT md.charge_id, md.code, md.observation_type, md.value, md.value_type, 
            row_number() over (partition by md.charge_id order by observation_type) as r_number 
            FROM jawda_mrd_observations md
        ) as a
        WHERE a.r_number BETWEEN 1 AND 5
        GROUP BY a.charge_id
        """
        
        # Execute the query
        pivot_df = spark.read.jdbc(
            url=target_jdbc_url,
            table=f"({pivot_query}) as obs_pivot_tmp",
            properties=target_connection_properties
        )
        
        # Check if pivot data was created properly
        pivot_row_count = pivot_df.count()
        logger.info(f"Created {pivot_row_count} pivot rows")
        
        if pivot_row_count > 0:
            # Display sample of the pivot data
            logger.info("Sample pivot data:")
            pivot_df.show(5, truncate=False)
            
            # Write to mrd_observs_pivot table in target DB (matching KTR table name)
            write_to_postgres(
                spark,
                pivot_df,
                f"{target_schema}.mrd_observs_pivot",
                target_jdbc_url,
                target_connection_properties,
                "charge_id"
            )
            
            logger.info("Observations pivot transformation completed successfully")
        else:
            logger.warning("No pivot data was created")
        
    except Exception as e:
        logger.error(f"Error creating observations pivot: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        # Stop Spark session
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main() 