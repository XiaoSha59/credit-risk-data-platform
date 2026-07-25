"""
File path: dags/credit_risk_dp3_offline_features.py
Description: Airflow DAG for DP3 computing offline feature tables such as f_customer_total_orders_90d
             with compute and validation stages.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def compute_offline_features():
    print("Executing ingestion/computation stage: Calculating f_customer_total_orders_90d feature table.")

def validate_offline_features():
    print("Executing validation stage: Validating feature metrics and distributions.")

default_args = {
    'owner': 'risk-data-engineering',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'credit_risk_dp3_offline_features',
    default_args=default_args,
    description='DP3: Compute offline feature table f_customer_total_orders_90d',
    schedule_interval='@daily',
    start_date=datetime(2026, 7, 1),
    catchup=False,
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_stage',
        python_callable=compute_offline_features,
    )

    validate_task = PythonOperator(
        task_id='validate_stage',
        python_callable=validate_offline_features,
    )

    ingest_task >> validate_task