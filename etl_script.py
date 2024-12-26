import psycopg2
import pandas as pd
from datetime import datetime
import os


def fetch_data_from_postgres(schema_name='uat'):
    postgres_config = {
        'host': '10.8.55.162',
        'user': 'postgres',
        'database': 'hmsuat'
    }

    try:
        with psycopg2.connect(**postgres_config) as connection:
            query = f'''
            SELECT hcm.center_name AS "Center Name", 
                   vn.visit_type_name AS "Patient Type", 
                   b.bill_no AS "Bill No.", 
                   TO_CHAR(b.open_date, 'DD/MM/YYYY') AS "Open Date", 
                   TO_CHAR(b.finalized_date, 'DD/MM/YYYY') AS "Finalized Date", 
                   tpa.tpa_name AS "Primary Bill Sponsor", 
                   tpa.claim_format AS "Sponsor Type", 
                   org.org_name AS "Rate Plan", 
                   pipm.plan_name AS "Pri. Plan Name", 
                   icm.category_name AS "Pri. Plan Type", 
                   (b.total_amount - b.primary_total_claim - b.secondary_total_claim - COALESCE(b.total_patient_disc,0) + 
                   COALESCE((CASE WHEN pr.secondary_sponsor_id IS NOT NULL THEN b.secondary_insurance_deduction 
                                  ELSE b.insurance_deduction END),0)) AS "Patient Amt", 
                   (b.total_claim - (CASE WHEN pr.secondary_sponsor_id IS NOT NULL THEN b.secondary_insurance_deduction 
                                          ELSE b.insurance_deduction END)) AS "Total Claim Amt", 
                   b.total_amount AS "Net Amt" 
            FROM {schema_name}.bill b 
            JOIN {schema_name}.patient_registration pr ON pr.patient_id = b.visit_id 
            JOIN {schema_name}.hospital_center_master hcm ON hcm.center_id = pr.center_id 
            LEFT JOIN {schema_name}.tpa_master tpa ON tpa.tpa_id = pr.primary_sponsor_id 
            LEFT JOIN {schema_name}.organization_details org ON org.org_id = pr.org_id 
            LEFT JOIN {schema_name}.insurance_plan_main pipm ON pipm.plan_id = pr.plan_id 
            LEFT JOIN {schema_name}.insurance_category_master icm ON icm.category_id = pipm.category_id 
            LEFT JOIN {schema_name}.visit_type_names vn ON vn.visit_type = pr.visit_type 
            WHERE b.finalized_date::date BETWEEN '2024-01-01' AND '2024-07-31' and hcm.center_id='13'
            '''
            df = pd.read_sql(query, connection)
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None


def write_data_to_file(df, center_name):
    try:
        output_dir = '/home/tabuser/extracts'
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        file_name = f'nmc_{center_name}_{timestamp}.csv'
        file_path = os.path.join(output_dir, file_name)
        df.to_csv(file_path, index=False)
        print(f'Data for {center_name} written to {file_path}')
    except Exception as e:
        print(f"Error writing data to file: {e}")


def etl_process():
    df = fetch_data_from_postgres()
    if df is not None:
        center_names = df['Center Name'].unique()
        for center_name in center_names:
            df_transformed = df[df['Center Name'] == center_name]
            write_data_to_file(df_transformed, center_name)


if __name__ == "__main__":
    etl_process()
