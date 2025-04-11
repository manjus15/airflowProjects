#!/usr/bin/env python3
# Spark script to extract fact claim data and load it into staging tables

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from functools import reduce
import argparse
import json
import os
import logging
import sys
from pyspark.storagelevel import StorageLevel
#import jaydebeapi

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
        .appName("Claim Fact Data Extraction") \
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
        .config("spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin", "0.2") \
        .config("spark.sql.autoBroadcastJoinThreshold", "10MB") \
        .config("spark.sql.inMemoryColumnarStorage.compressed", "true") \
        .config("spark.sql.inMemoryColumnarStorage.batchSize", "20000") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .config("spark.sql.files.openCostInBytes", "4MB") \
        .config("spark.sql.broadcastTimeout", "300") \
        .config("spark.sql.sources.parallelPartitionDiscovery.threshold", "32") \
        .master("local[*]")
    
    # Add PostgreSQL JDBC driver if available
    jdbc_jar_path = "/opt/spark/jars/postgresql-42.2.18.jar"
    if os.path.exists(jdbc_jar_path):
        builder = builder.config("spark.driver.extraClassPath", jdbc_jar_path) \
                         .config("spark.jars", jdbc_jar_path)
    
    return builder.getOrCreate()

def extract_data(spark, source_connector, query, partitioned=False, partition_column=None, num_partitions=8):
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
        
        if partitioned and partition_column:
            logger.info(f"Using SQL partitioning on column {partition_column} with {num_partitions} partitions")
            # Use SQL partitioning for better distribution
            df = spark.read.jdbc(
                url=source_connector.get_jdbc_url(),
                table=f"({query}) as tmp",
                column=partition_column,
                lowerBound=1,
                upperBound=num_partitions+1,
                numPartitions=num_partitions,
                properties=properties
            )
        else:
            # Use standard numPartitions approach
            df = spark.read.jdbc(
                url=source_connector.get_jdbc_url(),
                table=f"({query}) as tmp",
                properties=properties,
                numPartitions=num_partitions  # Number of partitions for parallel processing
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
            
            if partitioned and partition_column:
                df = spark.read.jdbc(
                    url=source_connector.get_jdbc_url(),
                    table=f"({query}) as tmp",
                    column=partition_column,
                    lowerBound=1,
                    upperBound=num_partitions+1,
                    numPartitions=num_partitions,
                    properties=properties
                )
            else:
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

def save_reference_ids(df, output_path, id_column):
    """Save distinct IDs from dataframe to a temporary file for reference"""
    distinct_ids_df = df.select(id_column).distinct()
    id_count = distinct_ids_df.count()
    
    if id_count > 0:
        logger.info(f"Saving {id_count} distinct {id_column} values to {output_path}")
        
        # Convert to list for easier handling in other scripts
        id_list = [row[id_column] for row in distinct_ids_df.collect() if row[id_column] is not None]
        
        # Save to file
        with open(output_path, 'w') as f:
            json.dump(id_list, f)
            
        logger.info(f"Successfully saved {len(id_list)} distinct {id_column} values")
        return len(id_list)
    else:
        logger.warning(f"No distinct {id_column} values found to save")
        # Create an empty file to avoid downstream errors
        with open(output_path, 'w') as f:
            json.dump([], f)
        return 0

def split_date_range(start_date, end_date, num_chunks):
    """Split a date range into equal chunks"""
    from datetime import datetime, timedelta
    
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    total_days = (end - start).days + 1
    chunk_size = total_days // num_chunks
    
    ranges = []
    current_start = start
    for i in range(num_chunks):
        if i == num_chunks - 1:
            current_end = end
        else:
            current_end = current_start + timedelta(days=chunk_size - 1)
        ranges.append((current_start.strftime('%Y-%m-%d'), current_end.strftime('%Y-%m-%d')))
        current_start = current_end + timedelta(days=1)
    
    return ranges

def main():
    """Main extraction function for claim fact data"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Extract claim fact data and load to target table')
    parser.add_argument('--params_file', required=True, help='Path to JSON file with connection parameters')
    parser.add_argument('--num_partitions', type=int, default=12, help='Number of partitions for data processing')
    args = parser.parse_args()
    
    # Increase default number of partitions
    num_partitions = args.num_partitions
    
    # Load connection parameters
    logger.info(f"Loading parameters from {args.params_file}")
    try:
        with open(args.params_file, 'r') as f:
            params = json.load(f)
        
        # Source and target connection details
        source_connector = DatabaseConnector(
            host=params.get('BI_SOURCE_HOST'),
            port=params.get('BI_SOURCE_PORT', 5432),
            db=params.get('BI_SOURCE_DB'),
            user=params.get('BI_SOURCE_USER'),
            pwd=params.get('BI_SOURCE_PWD'),
            schema=params.get('BI_SOURCE_SCHEMA')
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
        # First, get the count of records we'll be processing
        count_query = f"""
        SELECT COUNT(*) as record_count 
        FROM {source_connector.schema}.f_claim_submission fcl
        WHERE fcl.center_id = '{center_id}'
        AND submission_date::date >= '{from_date}' 
        AND submission_date::date <= '{to_date}'
        AND org_sub_seq = 1
        """
        
        count_df = extract_data(spark, source_connector, count_query)
        total_records = count_df.first()['record_count']
        logger.info(f"Total records to process: {total_records}")
        
        if total_records == 0:
            logger.warning("No records found for the specified criteria")
            # Create empty reference files
            for path in ["/tmp/claim_ids.json", "/tmp/charge_ids.json", "/tmp/patient_ids.json", "/tmp/visit_ids.json"]:
                with open(path, 'w') as f:
                    json.dump([], f)
            return
        
        # Add dynamic partitioning based on data volume
        optimal_partition_count = min(num_partitions, max(8, total_records // 50000))
        logger.info(f"Using {optimal_partition_count} partitions based on data volume")
        
        # Direct extraction using date-based partitioning through SQL
        # This approach uses a single query with partitioning handled in SQL
        # Taking advantage of the existing indexes on center_id, submission_date
        extraction_query = f"""
SELECT null::text AS insurancecompany, null::text AS tpa_name, receiverid, transactiondate ,claimid,memberid, payerid,providerid, emiratesidnumber , gross, patientshare, netclaim,
encountertype,patientid,null::text AS patientname,null::text AS patientbirthdate,null::text AS age,null::text AS gender,encounterstart,encounterend,
encounterstarttype,encounterendtype,null::text AS Encountertransfersource,null::text AS Encountertransferdestination,primary_icd AS Encounterdiagnosisprincipal,
null::text AS Encounterdiagnosissecondary1,null::text AS Encounterdiagnosissecondary2, null::text AS Encounterdiagnosissecondary3,
null::text AS Encounterdiagnosissecondary4,null::text AS Encounterdiagnosissecondary5,null::text AS Encounterdiagnosissecondary6,
null::text AS Encounterdiagnosissecondary7,null::text AS Encounterdiagnosissecondary8,null::text AS Encounterdiagnosissecondary9,
null::text AS Encounterdiagnosissecondary10,null::text AS Encounterdiagnosissecondary11,null::text AS Encounterdiagnosissecondary12,
null::text AS Encounterdiagnosissecondary13,null::text AS Encounterdiagnosissecondary14,null::text AS Encounterdiagnosissecondary15,activityid, 
activitystart,activitytype,activitycode,activityquantity,activitynet,orderingclinician,prior_auth_id AS PriorAuthorizationID,null::text AS Obstype1,
null::text AS Obscode1,null::text AS Obsvalue1,null::text AS Obsvaluetype1,null::text AS Obstype2,null::text AS Obscode2,null::text AS Obsvalue2,
null::text AS Obsvaluetype2,null::text AS Obstype3,null::text AS Obscode3,null::text AS Obsvalue3,null::text AS Obsvaluetype3,null::text AS Obstype4,
null::text AS Obscode4,null::text AS Obsvalue4,null::text AS Obsvaluetype4,null::text AS Obstype5,null::text AS Obscode5,null::text AS Obsvalue5,
null::text AS Obsvaluetype5,clinician, visit_id,charge_id ,insurance_co_id,admitting_dr_id,null::text AS doctorname,null::text AS doctorsdepartment,
category, submission_date, tpa_id, fcl.center_id, act_description, plan_id, registration_date,
org_id as rate_plan_id,insurance_category_id , null::text as CenterName, null::text as Rate_Plan, null::text as Insurance_Plan,
null::text as Insurance_Plan_type,null::text as nationality,null::text as passport_no,fcl."patient_type",
            -- Add a partition key for better distribution
            (ROW_NUMBER() OVER (ORDER BY submission_date) % {optimal_partition_count}) + 1 as partition_key
        FROM {source_connector.schema}.f_claim_submission fcl
        WHERE fcl.center_id = '{center_id}'
        AND submission_date::date >= '{from_date}' 
        AND submission_date::date <= '{to_date}'
        AND org_sub_seq = 1
        """
        
        logger.info("Extracting claim data with optimized query")
        
        # Extract data with optimized settings
        final_df = extract_data(spark, source_connector, extraction_query, 
                                partitioned=True, 
                                partition_column="partition_key", 
                                num_partitions=optimal_partition_count)
        
        # Cache the dataframe for multiple operations with MEMORY_AND_DISK storage level
        final_df.persist(StorageLevel.MEMORY_AND_DISK)
        
        # Check if data was loaded properly
        row_count = final_df.count()
        logger.info(f"Extracted {row_count} claim records from f_claim_submission")
        
        if row_count > 0:
            # Extract minimal columns for ID extraction to reduce memory pressure
            ids_df = final_df.select("claimid", "charge_id", "patientid", "visit_id").cache()
            
            # Extract and save distinct IDs in parallel with increased thread pool
            from concurrent.futures import ThreadPoolExecutor
            
            def save_ids(df, column, output_path):
                id_count = save_reference_ids(df, output_path, column)
                logger.info(f"Saved {id_count} distinct {column} values to {output_path}")
                return id_count
            
            # Create a thread pool for parallel ID extraction with more workers
            with ThreadPoolExecutor(max_workers=12) as executor:  # Increased from 8 to 12
                futures = [
                    executor.submit(save_ids, ids_df, "claimid", "/tmp/claim_ids.json"),
                    executor.submit(save_ids, ids_df, "charge_id", "/tmp/charge_ids.json"),
                    executor.submit(save_ids, ids_df, "patientid", "/tmp/patient_ids.json"),
                    executor.submit(save_ids, ids_df, "visit_id", "/tmp/visit_ids.json")
                ]
                
                # Wait for all tasks to complete
                for future in futures:
                    future.result()
            
            # Release memory from the ids dataframe
            ids_df.unpersist()
                
            # Optimize the dataframe before writing to increase write speed
            final_df = final_df.coalesce(min(8, optimal_partition_count))
            
            # Write to target DB with optimized settings
            logger.info("Writing data to target database")
            write_data(spark, final_df, target_connector, "rcm_claim_data")
            
            logger.info("Claim fact data extraction completed successfully")
        else:
            logger.warning("No claim fact data was found for the specified criteria")
            # Create empty reference files
            for path in ["/tmp/claim_ids.json", "/tmp/charge_ids.json", "/tmp/patient_ids.json", "/tmp/visit_ids.json"]:
                with open(path, 'w') as f:
                    json.dump([], f)
        
    except Exception as e:
        logger.error(f"Error processing claim fact data: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        # Unpersist cached dataframe
        if 'final_df' in locals():
            final_df.unpersist()
        if 'ids_df' in locals() and not ids_df.is_cached:
            ids_df.unpersist()
        # Stop Spark session
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main() 