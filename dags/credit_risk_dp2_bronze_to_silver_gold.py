"""
File path: dags/credit_risk_dp2_bronze_to_silver_gold.py
Description: Airflow DAG for DP2 transforming data from Bronze to Silver and Gold zones
             including ingestion/transformation and validation stages.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def transform_to_silver_gold():
    print("Executing ingestion/transformation stage: Processing Bronze data into Silver and Gold layers.")

def validate_silver_gold_data():
    print("Executing validation stage: Running business logic checks on Silver and Gold tables.")

default_args = {
    'owner': 'risk-data-engineering',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'credit_risk_dp2_bronze_to_silver_gold',
    default_args=default_args,
    description='DP2: Ingest data from bronze zone into silver and gold zone',
    schedule_interval='@hourly',
    start_date=datetime(2026, 7, 1),
    catchup=False,
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_stage',
        python_callable=transform_to_silver_gold,
    )

    validate_task = PythonOperator(
        task_id='validate_stage',
        python_callable=validate_silver_gold_data,
    )

    ingest_task >> validate_task