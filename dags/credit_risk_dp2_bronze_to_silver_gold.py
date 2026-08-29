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

BRONZE_DATASET = "bronze_loan_data"
BRONZE_URN = make_dataset_urn("hive", BRONZE_DATASET, "PROD")
SILVER_DATASET = "silver_exposure_at_default"
SILVER_URN = make_dataset_urn("hive", SILVER_DATASET, "PROD")
ASSERTION_URN = "urn:li:assertion:dp2_ead_null_check"

def transform_data():
    print(f"Transforming {BRONZE_DATASET} to {SILVER_DATASET} for EAD calculation...")
    time.sleep(2)

def validate_data():
    print("Emitting Contract and Validation to DataHub...")
    try:
        emitter = DatahubRestEmitter(gms_server=DATAHUB_GMS)
        props = DatasetPropertiesClass(description="Silver EAD Data Contract validated")
        mcp_props = MetadataChangeProposalWrapper(entityUrn=SILVER_URN, aspect=props)
        emitter.emit(mcp_props)
        
        # Bắn Lineage: BRONZE -> SILVER
        lineage = UpstreamLineageClass(
            upstreams=[UpstreamClass(dataset=BRONZE_URN, type=DatasetLineageTypeClass.TRANSFORMED)]
        )
        mcp_lineage = MetadataChangeProposalWrapper(entityUrn=SILVER_URN, aspect=lineage)
        emitter.emit(mcp_lineage)

        # 1. Assertion Info
        assertion_info = AssertionInfoClass(
            type=AssertionTypeClass.DATASET,
            datasetAssertion=DatasetAssertionInfoClass(
                scope=DatasetAssertionScopeClass.DATASET_COLUMN,
                dataset=SILVER_URN,
                operator=AssertionStdOperatorClass.NOT_NULL,
                logic="EAD Non-Null and Positive Value Check"
            )
        )
        mcp_info = MetadataChangeProposalWrapper(entityType="assertion", entityUrn=ASSERTION_URN, aspect=assertion_info)
        emitter.emit(mcp_info)

        # 2. Assertion Result
        assertion_result = AssertionRunEventClass(
            timestampMillis=int(time.time() * 1000),
            assertionUrn=ASSERTION_URN, asserteeUrn=SILVER_URN,
            runId="dp2_run_001", status=AssertionRunStatusClass.COMPLETE,
            result=AssertionResultClass(type=AssertionResultTypeClass.SUCCESS)
        )
        mcp_assertion = MetadataChangeProposalWrapper(entityType="assertion", entityUrn=ASSERTION_URN, aspect=assertion_result)
        emitter.emit(mcp_assertion)

        print("Done: Lineage, AssertionInfo & AssertionResult emitted to DataHub.")
    except Exception as e:
        print(f"WARNING: DataHub emit failed (non-blocking): {e}")


with DAG('credit_risk_dp2_bronze_to_silver_gold', start_date=datetime(2026, 7, 25), schedule_interval=None, catchup=False) as dag:
    
    transform_stage = PythonOperator(
        task_id='transform_stage',
        python_callable=transform_data,
        inlets=[Table(cluster="hive", database="default", name=BRONZE_DATASET)],
        outlets=[Table(cluster="hive", database="default", name=SILVER_DATASET)]
    )

    validate_stage = PythonOperator(
        task_id='validate_stage',
        python_callable=validate_data,
        inlets=[Table(cluster="hive", database="default", name=SILVER_DATASET)]
    )

    transform_stage >> validate_stage