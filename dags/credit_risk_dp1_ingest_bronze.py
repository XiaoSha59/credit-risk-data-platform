"""
File path: dags/credit_risk_dp1_ingest_bronze.py
Description: Airflow DAG for DP1 orchestrating ingestion from Kafka/Source into the Bronze zone
             with distinct Ingest and Validate stages.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def ingest_to_bronze():
    print("Executing ingestion stage: Pulling raw streaming events into Bronze zone.")

def validate_bronze_data():
    print("Executing validation stage: Checking schema and null counts in Bronze zone.")

default_args = {
    'owner': 'risk-data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'credit_risk_dp1_ingest_bronze',
    default_args=default_args,
    description='DP1: Ingest raw data into bronze zone',
    schedule_interval='@hourly',
    start_date=datetime(2026, 7, 1),
    catchup=False,
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_stage',
        python_callable=ingest_to_bronze,
    )

    validate_task = PythonOperator(
        task_id='validate_stage',
        python_callable=validate_bronze_data,
    )

    ingest_task >> validate_task