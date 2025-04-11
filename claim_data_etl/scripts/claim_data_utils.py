#!/usr/bin/env python3
"""
Utility functions for Claim Data ETL process
"""

import logging
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from psycopg2.extras import execute_values
from typing import Dict, List, Any, Optional, Tuple
import time

# Configure logging
logger = logging.getLogger(__name__)

class DatabaseUtils:
    """
    Utility class for database operations in the Claim Data ETL process
    """
    
    @staticmethod
    def create_sqlalchemy_engine(
        host: str, 
        database: str, 
        user: str, 
        password: str, 
        schema: str = "public",
        port: str = "5432"
    ) -> Any:
        """
        Create a SQLAlchemy engine with the specified connection parameters
        
        Args:
            host: Database host
            database: Database name
            user: Database user
            password: Database password
            schema: Database schema
            port: Database port
            
        Returns:
            SQLAlchemy engine object
        """
        connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(
            connection_string,
            connect_args={"options": f"-csearch_path={schema}"}
        )
        return engine
    
    @staticmethod
    def get_connection(
        host: str, 
        database: str, 
        user: str, 
        password: str, 
        schema: str = "public",
        port: str = "5432"
    ) -> Any:
        """
        Get a psycopg2 connection with the specified parameters
        
        Args:
            host: Database host
            database: Database name
            user: Database user
            password: Database password
            schema: Database schema
            port: Database port
            
        Returns:
            psycopg2 connection object
        """
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        
        # Set search path
        with conn.cursor() as cursor:
            cursor.execute(f"SET search_path TO {schema}")
            
        return conn
    
    @staticmethod
    def get_data_with_query(conn: Any, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Execute a query and return results as a pandas DataFrame
        
        Args:
            conn: Database connection
            query: SQL query to execute
            params: Query parameters (optional)
            
        Returns:
            DataFrame with query results
        """
        start_time = time.time()
        try:
            if params:
                df = pd.read_sql_query(query, conn, params=params)
            else:
                df = pd.read_sql_query(query, conn)
                
            logger.info(f"Query executed in {time.time() - start_time:.2f} seconds, returned {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            logger.error(f"Query: {query}")
            raise
    
    @staticmethod
    def bulk_insert(
        conn: Any, 
        table_name: str, 
        data: pd.DataFrame, 
        chunk_size: int = 10000,
        on_conflict: Optional[str] = None
    ) -> int:
        """
        Efficiently insert large amounts of data into a table
        
        Args:
            conn: Database connection
            table_name: Target table name
            data: DataFrame with data to insert
            chunk_size: Number of rows per chunk
            on_conflict: Optional ON CONFLICT clause for UPSERT operations
            
        Returns:
            Number of rows inserted
        """
        if data.empty:
            logger.warning(f"No data to insert into {table_name}")
            return 0
            
        total_rows = len(data)
        columns = list(data.columns)
        
        # Convert DataFrame to list of tuples
        tuples = [tuple(x) for x in data.to_numpy()]
        
        # Prepare SQL query
        column_names = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        
        if on_conflict:
            query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders}) {on_conflict}"
        else:
            query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
        
        start_time = time.time()
        rows_inserted = 0
        
        try:
            with conn.cursor() as cursor:
                # Process in chunks
                for i in range(0, total_rows, chunk_size):
                    chunk = tuples[i:i + chunk_size]
                    execute_values(cursor, query, chunk, template=None, page_size=chunk_size)
                    conn.commit()
                    rows_inserted += len(chunk)
                    
                    logger.info(f"Inserted chunk {i // chunk_size + 1}, progress: {rows_inserted}/{total_rows} rows")
                    
            logger.info(f"Bulk insert completed in {time.time() - start_time:.2f} seconds, {rows_inserted} rows inserted")
            return rows_inserted
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error in bulk insert: {str(e)}")
            raise
    
    @staticmethod
    def run_sql_file(conn: Any, sql_file_path: str) -> None:
        """
        Execute SQL statements from a file
        
        Args:
            conn: Database connection
            sql_file_path: Path to SQL file
            
        Returns:
            None
        """
        try:
            with open(sql_file_path, 'r') as file:
                sql = file.read()
                
            # Split SQL by semicolons (naive approach, doesn't handle all edge cases)
            statements = sql.split(';')
            
            with conn.cursor() as cursor:
                for statement in statements:
                    statement = statement.strip()
                    if statement:
                        cursor.execute(statement)
                conn.commit()
                
            logger.info(f"SQL file {sql_file_path} executed successfully")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error executing SQL file {sql_file_path}: {str(e)}")
            raise

class DataProcessor:
    """
    Utility class for data processing operations
    """
    
    @staticmethod
    def create_pivot_table(
        df: pd.DataFrame,
        index: str,
        columns: str,
        values: str,
        fill_value: Any = None
    ) -> pd.DataFrame:
        """
        Create a pivot table from a DataFrame
        
        Args:
            df: Input DataFrame
            index: Column to use as index
            columns: Column to use as pivot columns
            values: Column with values to aggregate
            fill_value: Value to use for missing cells
            
        Returns:
            Pivot table as DataFrame
        """
        try:
            pivot_df = df.pivot_table(
                index=index,
                columns=columns,
                values=values,
                aggfunc='first',
                fill_value=fill_value
            ).reset_index()
            
            return pivot_df
            
        except Exception as e:
            logger.error(f"Error creating pivot table: {str(e)}")
            raise
    
    @staticmethod
    def process_in_chunks(
        df: pd.DataFrame,
        func: callable,
        chunk_size: int = 10000,
        **kwargs
    ) -> pd.DataFrame:
        """
        Process a large DataFrame in chunks
        
        Args:
            df: Input DataFrame
            func: Function to apply to each chunk
            chunk_size: Number of rows per chunk
            **kwargs: Additional arguments to pass to func
            
        Returns:
            Processed DataFrame
        """
        if df.empty:
            return df
            
        total_rows = len(df)
        result_dfs = []
        
        for i in range(0, total_rows, chunk_size):
            chunk = df.iloc[i:i + chunk_size].copy()
            result = func(chunk, **kwargs)
            result_dfs.append(result)
            
            logger.info(f"Processed chunk {i // chunk_size + 1}, progress: {min(i + chunk_size, total_rows)}/{total_rows} rows")
            
        if result_dfs:
            return pd.concat(result_dfs, ignore_index=True)
        else:
            return pd.DataFrame()
    
    @staticmethod
    def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """
        Perform basic data cleaning operations on a DataFrame
        
        Args:
            df: Input DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        # Make a copy to avoid modifying the original
        df_clean = df.copy()
        
        # Handle missing values
        df_clean = df_clean.fillna('')
        
        # Convert column names to lowercase
        df_clean.columns = [col.lower() for col in df_clean.columns]
        
        # Remove leading/trailing whitespace from string columns
        for col in df_clean.select_dtypes(include=['object']).columns:
            df_clean[col] = df_clean[col].astype(str).str.strip()
            
        return df_clean 