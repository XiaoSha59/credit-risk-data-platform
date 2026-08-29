try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except Exception:
    AIRFLOW_AVAILABLE = False
    DAG = object
    PythonOperator = object

from datetime import datetime, timedelta
import sys
import os

# Add root directory to python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Novel_idea.data_quality_gate import run_data_quality_gate



default_args = {
    'owner': 'credit_risk_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 25),
    'retries': 0, # Zero retries to immediately halt pipeline on DQ failure
}

def task_ingest_bronze():
    print("[*] Task 1: Ingesting Raw Data to Bronze Zone...")

def task_data_quality_check():
    print("[*] Task 2: Executing Data Quality Gate Assertions...")
    # By default, runs Data Quality Gate validation
    run_data_quality_gate(simulate_anomaly=False)

def task_transform_silver():
    print("[*] Task 3: Transforming Data from Bronze to Silver (EAD Calculation)...")

def task_gold_dw_sync():
    print("[*] Task 4: Synchronizing Gold Zone to PostgreSQL Data Warehouse...")

if AIRFLOW_AVAILABLE:
    with DAG(
        'credit_risk_dp2_quality_gate',
        default_args=default_args,
        description='Pipeline featuring blocking Data Quality Gate before Silver transformation',
        schedule_interval=None,
        catchup=False
    ) as dag:

        ingest_bronze = PythonOperator(
            task_id='ingest_bronze',
            python_callable=task_ingest_bronze
        )

        data_quality_check = PythonOperator(
            task_id='data_quality_check',
            python_callable=task_data_quality_check
        )

        transform_silver = PythonOperator(
            task_id='transform_silver',
            python_callable=task_transform_silver
        )

        gold_dw_sync = PythonOperator(
            task_id='gold_dw_sync',
            python_callable=task_gold_dw_sync
        )

        # DAG Dependency Chain with Quality Gate Blocking
        ingest_bronze >> data_quality_check >> transform_silver >> gold_dw_sync


if __name__ == '__main__':
    print("[*] Testing Airflow DAG task execution standalone:")
    task_ingest_bronze()
    task_data_quality_check()
    task_transform_silver()
    task_gold_dw_sync()
    print("[+] Airflow Quality Gate DAG Tasks Executed Successfully!")

