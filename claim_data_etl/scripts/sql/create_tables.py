#!/usr/bin/env python3
"""
Script to create required tables for JAWDA claim data ETL
This script can be run standalone or called from Airflow
"""

import os
import sys
import argparse
import json
import logging
import psycopg2
from psycopg2 import sql

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def execute_sql_script(target_host, target_port, target_db, target_user, target_pwd, target_schema, sql_file_path, drop_existing=False):
    """Execute an SQL script from file with target schema substitution"""
    
    # Read the SQL script
    try:
        with open(sql_file_path, 'r') as f:
            sql_script = f.read()
    except Exception as e:
        logger.error(f"Error reading SQL file {sql_file_path}: {str(e)}")
        return False
    
    # Format the SQL script with target schema
    sql_script = sql_script.format(target_schema=target_schema)
    
    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(
            host=target_host,
            port=target_port,
            database=target_db,
            user=target_user,
            password=target_pwd
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Create schema if it doesn't exist
        create_schema_sql = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(target_schema))
        cursor.execute(create_schema_sql)
        logger.info(f"Ensured schema {target_schema} exists")
        
        # If drop_existing is True, drop the tables first
        if drop_existing:
            logger.info("Dropping existing tables before recreating them")
            
            # Extract table names from SQL script using a basic parser
            import re
            table_names = re.findall(r'CREATE TABLE IF NOT EXISTS\s+{target_schema}\.([^\s\(]+)', sql_script)
            
            # Drop tables in reverse order to handle dependencies
            for table_name in reversed(table_names):
                try:
                    drop_table_sql = sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                        sql.Identifier(target_schema),
                        sql.Identifier(table_name)
                    )
                    cursor.execute(drop_table_sql)
                    logger.info(f"Dropped table {table_name}")
                except Exception as e:
                    logger.warning(f"Error dropping table {table_name}: {str(e)}")
        
        # Execute the SQL script
        cursor.execute(sql_script)
        logger.info(f"Successfully executed SQL script {sql_file_path}")
        
        # Close connection
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error executing SQL script: {str(e)}")
        return False
        
def main():
    """Main function to execute the script"""
    parser = argparse.ArgumentParser(description='Create tables for JAWDA claim data ETL')
    parser.add_argument('--params_file', required=False, help='Path to JSON file with connection parameters')
    parser.add_argument('--target_host', required=False, help='Target database host')
    parser.add_argument('--target_port', required=False, type=int, default=5432, help='Target database port')
    parser.add_argument('--target_db', required=False, help='Target database name')
    parser.add_argument('--target_user', required=False, help='Target database user')
    parser.add_argument('--target_pwd', required=False, help='Target database password')
    parser.add_argument('--target_schema', required=False, help='Target database schema')
    parser.add_argument('--drop_existing', action='store_true', help='Drop existing tables before recreating them')
    args = parser.parse_args()
    
    # Get connection parameters
    if args.params_file:
        try:
            with open(args.params_file, 'r') as f:
                params = json.load(f)
            
            target_host = params.get('TARGET_HOST')
            target_port = params.get('TARGET_PORT', 5432)
            target_db = params.get('TARGET_DB')
            target_user = params.get('TARGET_USER')
            target_pwd = params.get('TARGET_PWD')
            target_schema = params.get('TARGET_SCHEMA')
        except Exception as e:
            logger.error(f"Error loading parameters from {args.params_file}: {str(e)}")
            return False
    else:
        # Use command-line arguments
        target_host = args.target_host
        target_port = args.target_port
        target_db = args.target_db
        target_user = args.target_user
        target_pwd = args.target_pwd
        target_schema = args.target_schema
    
    # Check if all required parameters are provided
    if not all([target_host, target_db, target_user, target_pwd, target_schema]):
        logger.error("Missing required parameters. Please provide all connection parameters.")
        return False
    
    # Get the SQL file path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    sql_file_path = os.path.join(script_dir, 'create_jawda_tables.sql')
    
    # Execute the SQL script
    return execute_sql_script(
        target_host, 
        target_port, 
        target_db, 
        target_user, 
        target_pwd, 
        target_schema, 
        sql_file_path,
        args.drop_existing
    )
    
if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 