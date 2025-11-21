from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd


#reading csv and inserting it in postgres
def load_sales_csv_to_db():
    df = pd.read_csv("/path/to/sales_today.csv")
    pg_hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    #this is a loop that iterates over the csv to take the needed values before being saved in the new table
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO daily_sales (sale_id, product_name, quantity, unit_price, sale_date)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (sale_id) DO NOTHING;
        """, (row['sale_id'], row['product_name'], row['quantity'], row['unit_price'], row['sale_date']))

    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    dag_id="daily_sales_pipeline_csv",
    description="Load daily sales from CSV into Postgres",
    default_args=default_args,
    schedule_interval="0 17 * * *",
    start_date=datetime(2025, 11, 20),
    dagrun_timeout=timedelta(minutes=45),
    catchup=False
) as dag:
    
# inserting csv file to postgres
    load_sales_today = PythonOperator(
        task_id="load_sales_today",
        python_callable=load_sales_csv_to_db
    )
