import psycopg2
import pandas as pd
from datetime import datetime


def fetch_data_from_source_db(schema_name='uat'):
    source_db_config = {
        'host': '10.8.55.162',
        'user': 'postgres',
        'database': 'hmsuat'
    }

    try:
        with psycopg2.connect(**source_db_config) as connection:
            query = f'''
            SELECT mrd.*
            FROM {schema_name}.mrd_observations mrd
            JOIN {schema_name}.bill_charge bc ON bc.charge_id=mrd.charge_id
            JOIN {schema_name}.bill b ON b.bill_no=bc.bill_no
            JOIN {schema_name}.patient_registration pr ON pr.patient_id=b.visit_id
            WHERE b.finalized_date::date BETWEEN '2023-01-01' AND '2024-07-31' AND pr.center_id=13
            '''
            df = pd.read_sql(query, connection)
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None


def create_table_if_not_exists(cursor, schema_name, df):
    # Generate a CREATE TABLE statement based on the DataFrame's columns and data types
    column_definitions = []
    for column in df.columns:
        column_name = column.lower().replace(' ', '_')
        if df[column].dtype == 'int64':
            column_definitions.append(f"{column_name} BIGINT")
        elif df[column].dtype == 'float64':
            column_definitions.append(f"{column_name} DOUBLE PRECISION")
        else:
            column_definitions.append(f"{column_name} TEXT")

    create_table_query = f'''
    CREATE TABLE IF NOT EXISTS {schema_name}.target_table_name (
        {', '.join(column_definitions)}
    );
    '''
    cursor.execute(create_table_query)


def write_data_to_target_db(df, schema_name='nmc'):
    target_db_config = {
        'host': '10.8.55.120',
        'user': 'kettle',
        'password': 'pentaho',
        'database': 'hmsbi'
    }

    try:
        with psycopg2.connect(**target_db_config) as connection:
            cursor = connection.cursor()

            # Create table if it does not exist
            create_table_if_not_exists(cursor, schema_name, df)

            # Dynamically build the insert statement based on DataFrame columns
            columns = ', '.join(df.columns)
            values = ', '.join(['%s'] * len(df.columns))
            insert_query = f'INSERT INTO {schema_name}.target_table_name ({columns}) VALUES ({values})'

            for index, row in df.iterrows():
                cursor.execute(insert_query, tuple(row))

            connection.commit()
            print(f'Data written to {schema_name}.target_table_name in target database')

    except Exception as e:
        print(f"Error writing data to target database: {e}")


def etl_process():
    df = fetch_data_from_source_db()
    if df is not None:
        write_data_to_target_db(df)


if __name__ == "__main__":
    etl_process()
