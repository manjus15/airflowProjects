#!/usr/bin/env python3
# Spark script to extract patient data and load it into staging tables

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import argparse
import json
import os
import logging
import sys
from pyspark.storagelevel import StorageLevel

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
        .appName("Patient Data Extraction") \
        .config("spark.sql.files.ignoreMissingFiles", "true") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .config("spark.driver.extraJavaOptions", "-Dfs.defaultFS=file:/// -XX:+UseG1GC -XX:+UseCompressedOops") \
        .config("spark.driver.memory", "10g") \
        .config("spark.executor.memory", "16g") \
        .config("spark.executor.cores", "4") \
        .config("spark.sql.shuffle.partitions", "12") \
        .config("spark.default.parallelism", "12") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.sql.inMemoryColumnarStorage.compressed", "true") \
        .config("spark.sql.inMemoryColumnarStorage.batchSize", "20000") \
        .config("spark.sql.autoBroadcastJoinThreshold", "10MB") \
        .config("spark.sql.broadcastTimeout", "300") \
        .master("local[*]")
    
    # Add PostgreSQL JDBC driver if available
    jdbc_jar_path = "/opt/spark/jars/postgresql-42.2.18.jar"
    if os.path.exists(jdbc_jar_path):
        builder = builder.config("spark.driver.extraClassPath", jdbc_jar_path) \
                         .config("spark.jars", jdbc_jar_path)
    
    return builder.getOrCreate()

def extract_data(spark, source_connector, query, num_partitions=12):
    """Extract data using the provided query with optimized settings"""
    try:
        # Try with schema qualification first
        properties = source_connector.get_properties()
        
        # Add query timeout to avoid hanging connections
        properties["queryTimeout"] = "3600"  # 1 hour timeout
        properties["socketTimeout"] = "3600"  # 1 hour socket timeout
        
        # Increase fetch size for better performance
        properties["fetchSize"] = "100000"  # Doubled from 50000
        
        # Enable connection pooling with higher limits
        properties["maximumPoolSize"] = "20"  # Doubled from 10
        properties["minimumIdle"] = "10"  # Doubled from 5
        
        df = spark.read.jdbc(
            url=source_connector.get_jdbc_url(),
            table=f"({query}) as tmp",
            properties=properties,
            numPartitions=num_partitions  # Increased number of partitions for parallel processing
        )
        return df
    except Exception as e:
        logger.warning(f"Failed with standard properties: {str(e)}")
        
        # Try with search_path
        try:
            properties = source_connector.with_search_path()
            properties["queryTimeout"] = "3600"  # 1 hour timeout
            properties["socketTimeout"] = "3600"  # 1 hour socket timeout
            properties["fetchSize"] = "100000"  # Increased fetch size
            properties["maximumPoolSize"] = "20"  # Increased pool size
            
            df = spark.read.jdbc(
                url=source_connector.get_jdbc_url(),
                table=f"({query}) as tmp",
                properties=properties,
                numPartitions=num_partitions
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
            "prepStmtCacheSqlLimit": "2048",  # Maximum length of SQL to cache
            "socketTimeout": "3600"  # Socket timeout for write operations
        })
        
        # Optimize data for writing by coalescing to a reasonable number of partitions
        row_count = df.count()
        optimal_partitions = min(12, max(4, row_count // 50000))
        df = df.coalesce(optimal_partitions)
        
        # Write data with overwrite mode (this will truncate the table)
        df.write.mode("overwrite").jdbc(
            url=target_connector.get_jdbc_url(),
            table=f"{target_connector.schema}.{table_name}",
            properties=write_properties
        )
        logger.info(f"Successfully wrote data to {table_name}")
    else:
        logger.warning(f"No data to write to {table_name}")

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
    """Main extraction function for patient data"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Extract patient data and load to staging tables')
    parser.add_argument('--params_file', required=True, help='Path to JSON file with connection parameters')
    parser.add_argument('--num_partitions', type=int, default=12, help='Number of partitions for data processing')
    args = parser.parse_args()
    
    # Set number of partitions
    num_partitions = args.num_partitions
    
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
    
    # Create Spark session with optimized settings
    logger.info("Initializing Spark session")
    spark = create_spark_session()
    
    try:
        # Load reference IDs from temporary files
        patient_ids = load_reference_ids("/tmp/patient_ids.json")
        visit_ids = load_reference_ids("/tmp/visit_ids.json")
        
        # Validate we have IDs to process
        if not patient_ids and not visit_ids:
            logger.warning("No reference IDs found. No patient data will be extracted.")
            sys.exit(0)
            
        # Count total IDs for optimizing partitioning
        total_ids = len(patient_ids) + len(visit_ids)
        logger.info(f"Total IDs to process: {total_ids}")
        
        # Dynamically adjust partitions based on data volume
        optimal_partitions = min(num_partitions, max(8, total_ids // 5000))
        logger.info(f"Using {optimal_partitions} partitions based on data volume")
            
        # Format ID lists for SQL query 
        patient_ids_str = format_id_list_for_sql(patient_ids)
        visit_ids_str = format_id_list_for_sql(visit_ids)
        
        # Find the schema that contains our main table
        actual_schema = source_connector.find_schema_for_table(spark, 'insurance_submission_batch')
        logger.info(f"Using schema: {actual_schema} for queries")
        
        # Build patient data query with appropriate schema and filtered by patient IDs
        patient_query = f"""
        SELECT  pr.mr_no,
                pr.patient_id,
                pr.reg_date,
                pr.reg_time,
                pr.doctor,
                pr.dept_name,
                pr.transfer_source,
                pr.transfer_destination,
                pd.patient_name,
                pd.middle_name,
                pd.last_name,
                pd.dateofbirth,
                pd.expected_dob,
                pd.patient_gender,
                pd.custom_list1_value,
                pd.passport_no,
                pd.custom_field2,
                -- Add a partition column for better data distribution
                (ROW_NUMBER() OVER (ORDER BY pr.mr_no) % {optimal_partitions}) + 1 as partition_key
        FROM {actual_schema}.patient_registration pr
        LEFT JOIN {actual_schema}.patient_details pd on pd.mr_no=pr.mr_no
        JOIN {actual_schema}.hospital_center_master hcm on hcm.center_id=pr.center_id
        WHERE hcm.center_id in ({center_id})
        AND (pr.mr_no IN ({patient_ids_str}) OR pr.patient_id IN ({visit_ids_str}))
        """
        
        # Extract patient data with optimized settings
        logger.info("Extracting patient data")
        logger.info(f"Filtering by {len(patient_ids)} patient IDs and {len(visit_ids)} visit IDs")
        
        # Use the partition_key column for better data distribution
        patient_df = extract_data(spark, source_connector, patient_query, optimal_partitions)
        
        # Persist dataframe in memory for multiple operations
        patient_df = patient_df.persist(StorageLevel.MEMORY_AND_DISK)
        
        # Check if data was loaded properly
        row_count = patient_df.count()
        logger.info(f"Extracted {row_count} patient records")
        
        if row_count > 0:
            # Display sample of the data
            logger.info("Sample data schema:")
            patient_df.printSchema()
            logger.info("Sample records:")
            patient_df.show(5, truncate=False)
            
            # Create and write to registration table in target DB
            logger.info("Extracting patient registration data")
            registration_df = patient_df.select(
                "mr_no", "patient_id", "reg_date", "reg_time", "doctor", 
                "dept_name", "transfer_source", "transfer_destination"
            )
            
            # Write registration data to target DB
            write_data(spark, registration_df, target_connector, "jawda_patient_registration")
            
            # Create and write to patient details table in target DB
            logger.info("Extracting patient details data")
            patient_details_df = patient_df.select(
                "mr_no", "patient_name", "middle_name", "last_name", 
                "dateofbirth", "expected_dob", "patient_gender", 
                "custom_list1_value", "passport_no", "custom_field2"
            )
            
            # Write patient details data to target DB
            write_data(spark, patient_details_df, target_connector, "jawda_patient_details")
            
            logger.info("Patient data extraction completed successfully")
        else:
            logger.warning("No patient data was found for the specified criteria")
        
        # Release memory
        patient_df.unpersist()
        
    except Exception as e:
        logger.error(f"Error processing patient data: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        # Stop Spark session
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main() 