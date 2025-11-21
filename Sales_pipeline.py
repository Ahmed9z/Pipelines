from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import pandas as pd

# today_file is removed as it caused the issue

# reading csv and inserting it in postgres
def insert_csv_to_postgres(csv_path, table_name):
    df = pd.read_csv(csv_path)

    pg_hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # this is a loop that iterates over the csv to take the needed values before being saved in the new table
    for _, row in df.iterrows():
        cursor.execute(f"""
            INSERT INTO {table_name} (sale_id, product_name, quantity, unit_price, sale_date)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (sale_id) DO NOTHING;
        """, (row['sale_id'], row['product_name'], row['quantity'], row['unit_price'], row['sale_date']))

    conn.commit()
    cursor.close()
    conn.close()

# inserting csv file to postgres under the name "daily_sales"
def load_sales_today_task(**kwargs):
    exec_date = kwargs['execution_date']
    date_str_for_file = exec_date.strftime('%d-%m-%Y')
    today_file_dynamic = f"/path/to/Sales-{date_str_for_file}.csv"
    insert_csv_to_postgres(today_file_dynamic, "daily_sales")


with DAG(
    dag_id="daily_sales_pipeline_with_sensor",
    description="Waits for a csv to extract its records and save it in postgres",
    schedule_interval="0 17 * * *",
    start_date=datetime(2025, 11, 21),
    dagrun_timeout=timedelta(minutes=45),
    catchup=False
) as dag:

    FILE_PATH_TEMPLATE = "/path/to/Sales-{{ execution_date.strftime('%d-%m-%Y') }}.csv"
    
    # Wait for the daily sales CSV file
    wait_for_csv = FileSensor(
        task_id='wait_for_csv',
        filepath=FILE_PATH_TEMPLATE,
        poke_interval=10,     # checks every 10 sec
        timeout=600           # waits for 10 minutes
    )

    # Load the daily sales CSV into the Postgres database
    load_sales_today = PythonOperator(
        task_id="load_sales_today",
        python_callable=load_sales_today_task
    )

    wait_for_csv >> load_sales_today