#!/usr/bin/env python3
# Spark script to extract diagnosis data and load it into staging tables

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
            "driver": "org.postgresql.Driver",
            "fetchSize": "50000",  # Increased fetch size for better performance
            "batchSize": "50000",  # Increased batch size for better performance
            "numPartitions": "8",  # Increased partitions for better parallelism
            "connectionPool": "HikariCP",  # Use connection pooling
            "maximumPoolSize": "10",  # Maximum number of connections in pool
            "minimumIdle": "5",  # Minimum number of idle connections
            "idleTimeout": "300000",  # 5 minutes idle timeout
            "connectionTimeout": "30000",  # 30 seconds connection timeout
            "maxLifetime": "1800000",  # 30 minutes max lifetime
            "useServerPrepStmts": "true",  # Use server-side prepared statements
            "cachePrepStmts": "true",  # Cache prepared statements
            "prepStmtCacheSize": "250",  # Size of prepared statement cache
            "prepStmtCacheSqlLimit": "2048"  # Maximum length of SQL to cache
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
        .appName("Diagnosis Data Extraction") \
        .config("spark.sql.files.ignoreMissingFiles", "true") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .config("spark.driver.extraJavaOptions", "-Dfs.defaultFS=file:///") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "16g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.default.parallelism", "8") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
        .config("spark.sql.inMemoryColumnarStorage.compressed", "true") \
        .config("spark.sql.inMemoryColumnarStorage.batchSize", "10000") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.parquet.filterPushdown", "true") \
        .config("spark.sql.parquet.mergeSchema", "false") \
        .config("spark.sql.parquet.enableVectorizedReader", "true") \
        .config("spark.sql.parquet.recordLevelFilter.enabled", "true") \
        .config("spark.sql.parquet.columnarReaderBatchSize", "4096") \
        .config("spark.sql.parquet.enableDictionary", "true") \
        .config("spark.sql.parquet.dictionary.page.size", "1048576") \
        .config("spark.sql.parquet.page.size", "1048576") \
        .config("spark.sql.parquet.block.size", "16777216") \
        .master("local[*]")
    
    # Add PostgreSQL JDBC driver if available
    jdbc_jar_path = "/opt/spark/jars/postgresql-42.2.18.jar"
    if os.path.exists(jdbc_jar_path):
        builder = builder.config("spark.driver.extraClassPath", jdbc_jar_path) \
                         .config("spark.jars", jdbc_jar_path)
    
    return builder.getOrCreate()

def extract_data(spark, source_connector, query):
    """Extract data using the provided query with optimized settings"""
    try:
        # Try with schema qualification first
        df = spark.read.jdbc(
            url=source_connector.get_jdbc_url(),
            table=f"({query}) as tmp",
            properties=source_connector.get_properties(),
            numPartitions=8  # Number of partitions for parallel processing
        )
        return df
    except Exception as e:
        logger.warning(f"Failed with standard properties: {str(e)}")
        
        # Try with search_path
        try:
            df = spark.read.jdbc(
                url=source_connector.get_jdbc_url(),
                table=f"({query}) as tmp",
                properties=source_connector.with_search_path(),
                numPartitions=8
            )
            return df
        except Exception as e2:
            logger.error(f"Failed with search_path: {str(e2)}")
            raise e2

def write_data(spark, df, target_connector, table_name):
    """Write dataframe to target table with optimized settings"""
    if df.count() > 0:
        logger.info(f"Writing {df.count()} rows to {table_name}")
        
        # Configure JDBC properties for better write performance
        write_properties = target_connector.get_properties()
        write_properties.update({
            "batchsize": "50000",  # Increased batch size
            "rewriteBatchedStatements": "true",  # Enable batch statement rewriting
            "useServerPrepStmts": "true",  # Use server-side prepared statements
            "cachePrepStmts": "true",  # Cache prepared statements
            "prepStmtCacheSize": "250",  # Size of prepared statement cache
            "prepStmtCacheSqlLimit": "2048"  # Maximum length of SQL to cache
        })
        
        # Repartition data for better write performance
        df = df.repartition(8)  # Match with spark.sql.shuffle.partitions
        
        # Write data with overwrite mode (this will truncate the table)
        df.write.mode("overwrite").jdbc(
            url=target_connector.get_jdbc_url(),
            table=f"{target_connector.schema}.{table_name}",
            properties=write_properties
        )
        logger.info(f"Successfully wrote data to {table_name}")
    else:
        logger.warning(f"No data to write to {table_name}")

def write_master_data(spark, df, target_connector, table_name):
    """Write dataframe to master table by truncating and inserting instead of drop/create
    This preserves table dependencies while refreshing the data."""
    if df.count() > 0:
        logger.info(f"Writing {df.count()} rows to master table {table_name}")
        
        # Create a temporary view of the data
        df.createOrReplaceTempView("temp_data")
        
        # Get JDBC connection for executing SQL commands
        jdbc_url = target_connector.get_jdbc_url()
        properties = target_connector.get_properties()
        qualified_table = f"{target_connector.schema}.{table_name}"
        
        try:
            # First truncate the table
            truncate_query = f"TRUNCATE {qualified_table}"
            logger.info(f"Truncating master table: {truncate_query}")
            
            # Execute truncate using JDBC
            truncate_df = spark.read.jdbc(
                url=jdbc_url,
                table=f"({truncate_query}; SELECT 1 as success) as tmp",
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
            # Fallback to regular write if truncate fails (table might not exist yet)
            logger.info(f"Falling back to regular table write for {table_name}")
            write_data(spark, df, target_connector, table_name)
    else:
        logger.warning(f"No data to write to master table {table_name}")

def load_reference_ids(file_path):
    """Load reference IDs from temporary file"""
    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
        try:
            with open(file_path, 'r') as f:
                id_list = json.load(f)
            logger.info(f"Loaded {len(id_list)} reference IDs from {file_path}")
            return id_list
        except Exception as e:
            logger.error(f"Error loading reference IDs from {file_path}: {str(e)}")
            return []
    else:
        logger.warning(f"Reference ID file {file_path} does not exist or is empty")
        return []

def format_id_list_for_sql(id_list):
    """Format a list of IDs for use in SQL IN clause, with proper quoting"""
    if not id_list:
        return "''"  # Return a dummy value to avoid SQL errors
        
    # Format each ID with single quotes and join with commas
    formatted_ids = [f"'{id_val}'" for id_val in id_list]
    return ",".join(formatted_ids)

def main():
    """Main extraction function for diagnosis data"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Extract diagnosis data and load to staging tables')
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
        
        from_date = params.get('FROM_DATE')
        to_date = params.get('TO_DATE')
        center_id = params.get('CENTER_ID')
        
        logger.info(f"Loaded parameters for date range: {from_date} to {to_date}, Center ID: {center_id}")
        logger.info(f"Source DB: {source_connector.db}, Source Schema: {source_connector.schema}")
        logger.info(f"Target DB: {target_connector.db}, Target Schema: {target_connector.schema}")
    except Exception as e:
        logger.error(f"Error loading parameters: {str(e)}")
        sys.exit(1)
    
    # Create Spark session
    logger.info("Initializing Spark session")
    spark = create_spark_session()
    
    try:
        # Load reference IDs from temporary files
        visit_ids = load_reference_ids("/tmp/visit_ids.json")
        charge_ids = load_reference_ids("/tmp/charge_ids.json")
        
        # Validate we have IDs to process
        if not visit_ids and not charge_ids:
            logger.warning("No reference IDs found. No diagnosis or observation data will be extracted.")
            sys.exit(0)
            
        # Format ID lists for SQL query
        visit_ids_str = format_id_list_for_sql(visit_ids)
        charge_ids_str = format_id_list_for_sql(charge_ids)
        
        # Find the schema that contains our main table
        actual_schema = source_connector.find_schema_for_table(spark, 'insurance_submission_batch')
        logger.info(f"Using schema: {actual_schema} for queries")
        
        # Extract diagnosis data from source
        logger.info("Extracting diagnosis data")
        logger.info(f"Filtering by {len(visit_ids)} visit IDs")
        
        # Build diagnosis query with appropriate schema and filtered by visit IDs
        diagnosis_query = f"""
        SELECT md.visit_id, md.id, md.description, md.icd_code, md.code_type, md.diag_type
        FROM {actual_schema}.mrd_diagnosis md
        JOIN {actual_schema}.hospital_center_master hcm ON hcm.center_id IN ({center_id})
        WHERE md.visit_id IN ({visit_ids_str})
        """
        
        # Extract diagnosis data
        diagnosis_df = extract_data(spark, source_connector, diagnosis_query)
        
        # Check if data was loaded properly
        row_count = diagnosis_df.count()
        logger.info(f"Extracted {row_count} diagnosis records")
        
        if row_count > 0:
            # Display sample of the data
            logger.info("Sample data schema:")
            diagnosis_df.printSchema()
            logger.info("Sample records:")
            diagnosis_df.show(5, truncate=False)
            
            # Write to diagnosis table in target DB
            write_data(spark, diagnosis_df, target_connector, "jawda_mrd_diagnosis")
            logger.info("Diagnosis data extraction completed successfully")
        else:
            logger.warning("No diagnosis data was found for the specified criteria")
        
        # Extract observation data
        logger.info("Extracting observation data")
        logger.info(f"Filtering by {len(charge_ids)} charge IDs")
        
        # Query for observation data with schema and filtered by charge IDs
        observation_query = f"""
        SELECT mo.charge_id, mo.observation_type, mo.code, mo.value, mo.value_type
        FROM {actual_schema}.mrd_observations mo
        WHERE mo.charge_id IN ({charge_ids_str})
        """
        
        # Extract observation data
        observation_df = extract_data(spark, source_connector, observation_query)
        
        # Check if observation data was loaded properly
        obs_row_count = observation_df.count()
        logger.info(f"Extracted {obs_row_count} observation records")
        
        if obs_row_count > 0:
            # Display sample of the observation data
            logger.info("Sample observation data:")
            observation_df.show(5, truncate=False)
            
            # Write to observation table in target DB
            write_data(spark, observation_df, target_connector, "jawda_mrd_observations")
            logger.info("Observation data extraction completed successfully")
        else:
            logger.warning("No observation data was found for the specified criteria")
        
    except Exception as e:
        logger.error(f"Error processing diagnosis and observation data: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        # Stop Spark session
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main() 