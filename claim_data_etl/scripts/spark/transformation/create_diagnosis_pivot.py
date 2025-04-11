#!/usr/bin/env python3
# Spark script to create diagnosis pivot table from extracted diagnosis data

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
        .appName("Diagnosis Pivot Transformation") \
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
    """Main transformation function for creating diagnosis pivot"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Create diagnosis pivot table from extracted data')
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
        # Execute the diagnosis pivot query using row_number and CASE statements instead of crosstab
        logger.info(f"Executing diagnosis pivot query")
        
        # Define pivot query using row number and CASE statements approach
        pivot_query = """
        SELECT 
            a.visit_id,
            MAX(CASE WHEN a.diag_type = 'P' THEN a.icd_code END) as P1,
            MAX(CASE WHEN a.diag_type_with_row = 'S1' THEN a.icd_code END) as S1,
            MAX(CASE WHEN a.diag_type_with_row = 'S2' THEN a.icd_code END) as S2,
            MAX(CASE WHEN a.diag_type_with_row = 'S3' THEN a.icd_code END) as S3,
            MAX(CASE WHEN a.diag_type_with_row = 'S4' THEN a.icd_code END) as S4,
            MAX(CASE WHEN a.diag_type_with_row = 'S5' THEN a.icd_code END) as S5,
            MAX(CASE WHEN a.diag_type_with_row = 'S6' THEN a.icd_code END) as S6,
            MAX(CASE WHEN a.diag_type_with_row = 'S7' THEN a.icd_code END) as S7,
            MAX(CASE WHEN a.diag_type_with_row = 'S8' THEN a.icd_code END) as S8,
            MAX(CASE WHEN a.diag_type_with_row = 'S9' THEN a.icd_code END) as S9,
            MAX(CASE WHEN a.diag_type_with_row = 'S10' THEN a.icd_code END) as S10,
            MAX(CASE WHEN a.diag_type_with_row = 'S11' THEN a.icd_code END) as S11,
            MAX(CASE WHEN a.diag_type_with_row = 'S12' THEN a.icd_code END) as S12,
            MAX(CASE WHEN a.diag_type_with_row = 'S13' THEN a.icd_code END) as S13,
            MAX(CASE WHEN a.diag_type_with_row = 'S14' THEN a.icd_code END) as S14,
            MAX(CASE WHEN a.diag_type_with_row = 'S15' THEN a.icd_code END) as S15
        FROM (
            SELECT 
                md.visit_id, 
                md.icd_code, 
                md.diag_type,
                CASE 
                    WHEN md.diag_type = 'S' THEN md.diag_type || r_number::text
                    ELSE md.diag_type
                END as diag_type_with_row
            FROM (
                SELECT 
                    visit_id, 
                    icd_code, 
                    diag_type,
                    row_number() OVER (PARTITION BY visit_id, diag_type ORDER BY diag_type) as r_number
                FROM jawda_mrd_diagnosis
            ) md
        ) a
        GROUP BY a.visit_id
        """
        
        # Execute the pivot query
        pivot_df = spark.read.jdbc(
            url=target_jdbc_url,
            table=f"({pivot_query}) as diag_pivot_tmp",
            properties=target_connection_properties
        )
        
        # Check if pivot data was created properly
        pivot_row_count = pivot_df.count()
        logger.info(f"Created {pivot_row_count} pivot rows")
        
        if pivot_row_count > 0:
            # Display sample of the pivot data
            logger.info("Sample pivot data:")
            pivot_df.show(5, truncate=False)
            
            # Write to mrd_diag_pivot table in target DB
            write_to_postgres(
                spark,
                pivot_df,
                f"{target_schema}.mrd_diag_pivot",
                target_jdbc_url,
                target_connection_properties,
                "visit_id"
            )
            
            logger.info("Diagnosis pivot transformation completed successfully")
        else:
            logger.warning("No pivot data was created")
        
    except Exception as e:
        logger.error(f"Error creating diagnosis pivot: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        # Stop Spark session
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main() 