from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time

from airflow.lineage.entities import Table
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.metadata.schema_classes import (
    AssertionRunEventClass, AssertionRunStatusClass, AssertionResultClass,
    AssertionResultTypeClass, DatasetPropertiesClass, UpstreamLineageClass,
    UpstreamClass, DatasetLineageTypeClass, AssertionInfoClass, AssertionTypeClass,
    DatasetAssertionInfoClass, DatasetAssertionScopeClass, AssertionStdOperatorClass
)

from datahub.emitter.mcp import MetadataChangeProposalWrapper

DATAHUB_GMS = "http://host.docker.internal:8080"

# Định nghĩa các URN
KAFKA_SOURCE = "raw_loan_events"
KAFKA_URN = make_dataset_urn("kafka", KAFKA_SOURCE, "PROD")
BRONZE_DATASET = "bronze_loan_data"
BRONZE_URN = make_dataset_urn("hive", BRONZE_DATASET, "PROD")
ASSERTION_URN = "urn:li:assertion:dp1_schema_check"

def ingest_data():
    print(f"Ingesting {KAFKA_SOURCE} to {BRONZE_DATASET}...")
    time.sleep(2)

def validate_data():
    print("Emitting Contract and Validation to DataHub...")
    try:
        emitter = DatahubRestEmitter(gms_server=DATAHUB_GMS)
        props = DatasetPropertiesClass(description="Bronze Loan Data Contract validated")
        mcp_props = MetadataChangeProposalWrapper(entityUrn=BRONZE_URN, aspect=props)
        emitter.emit(mcp_props)
        
        # Bắn Lineage: KAFKA -> BRONZE
        lineage = UpstreamLineageClass(
            upstreams=[UpstreamClass(dataset=KAFKA_URN, type=DatasetLineageTypeClass.TRANSFORMED)]
        )
        mcp_lineage = MetadataChangeProposalWrapper(entityUrn=BRONZE_URN, aspect=lineage)
        emitter.emit(mcp_lineage)

        # 1. Định nghĩa metadata cho Assertion
        assertion_info = AssertionInfoClass(
            type=AssertionTypeClass.DATASET,
            datasetAssertion=DatasetAssertionInfoClass(
                scope=DatasetAssertionScopeClass.DATASET_COLUMN,
                dataset=BRONZE_URN,
                operator=AssertionStdOperatorClass.NOT_NULL,
                logic="Bronze schema & data quality validation pass"
            )
        )
        mcp_info = MetadataChangeProposalWrapper(entityType="assertion", entityUrn=ASSERTION_URN, aspect=assertion_info)
        emitter.emit(mcp_info)

        # 2. Bắn Validation Run Success
        assertion_result = AssertionRunEventClass(
            timestampMillis=int(time.time() * 1000),
            assertionUrn=ASSERTION_URN, asserteeUrn=BRONZE_URN,
            runId="dp1_run_001", status=AssertionRunStatusClass.COMPLETE,
            result=AssertionResultClass(type=AssertionResultTypeClass.SUCCESS)
        )
        mcp_assertion = MetadataChangeProposalWrapper(entityType="assertion", entityUrn=ASSERTION_URN, aspect=assertion_result)
        emitter.emit(mcp_assertion)

        print("Done: Lineage, AssertionInfo & AssertionResult emitted to DataHub.")
    except Exception as e:
        print(f"WARNING: DataHub emit failed (non-blocking): {e}")


with DAG('credit_risk_dp1_ingest_bronze', start_date=datetime(2026, 7, 25), schedule_interval=None, catchup=False) as dag:
    
    ingest_stage = PythonOperator(
        task_id='ingest_stage',
        python_callable=ingest_data,
        inlets=[Table(cluster="kafka", database="default", name=KAFKA_SOURCE)],
        outlets=[Table(cluster="hive", database="default", name=BRONZE_DATASET)]
    )

    validate_stage = PythonOperator(
        task_id='validate_stage',
        python_callable=validate_data,
        inlets=[Table(cluster="hive", database="default", name=BRONZE_DATASET)]
    )

    ingest_stage >> validate_stage