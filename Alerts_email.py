from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule

def process_sales():
    print("Processing today's sales records")

with DAG(
    dag_id="daily_sales_monitor",
    description="Monitor daily sales records arrival in Postgres",
    schedule_interval="0 17 * * *",  # runs every day 5 pm
    start_date=datetime(2025, 11, 20),
    catchup=False,
    dagrun_timeout=timedelta(minutes=45)
) as dag:

    # Check if today's sales exist in the database
    check_sales = SqlSensor(
        task_id="check_sales",
        conn_id="postgres_conn",
        sql="SELECT 1 FROM sales WHERE sale_date = CURRENT_DATE LIMIT 1;",
        poke_interval=300,  # check every 5 minutes
        timeout=3600,  # fail after an hour if there is no data
        mode='reschedule'
    )

    # Process sales if they exist
    process_records = PythonOperator(
        task_id="process_records",
        python_callable=process_sales
    )

    # Send an email alert if the sales data is missing
    email_alert = EmailOperator(
        task_id="email_alert",
        to="sales.alert@example.com",
        subject="Daily Sales Data Missing",
        html_content="No sales record found for today",
        trigger_rule=TriggerRule.ONE_FAILED
    )

    check_sales >> process_records
    check_sales >> email_alert
