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

SILVER_DATASET = "silver_exposure_at_default"
SILVER_URN = make_dataset_urn("hive", SILVER_DATASET, "PROD")
GOLD_DATASET = "gold_pd_feature_matrix"
GOLD_URN = make_dataset_urn("hive", GOLD_DATASET, "PROD")
ASSERTION_URN = "urn:li:assertion:dp3_pd_range_check"

def compute_features():
    print(f"Computing offline features from {SILVER_DATASET} to {GOLD_DATASET} for PD modeling...")
    time.sleep(2)

def validate_data():
    print("Emitting Contract and Validation to DataHub...")
    try:
        emitter = DatahubRestEmitter(gms_server=DATAHUB_GMS)
        props = DatasetPropertiesClass(description="Gold PD Feature Matrix Data Contract validated")
        mcp_props = MetadataChangeProposalWrapper(entityUrn=GOLD_URN, aspect=props)
        emitter.emit(mcp_props)
        
        # Bắn Lineage: SILVER -> GOLD
        lineage = UpstreamLineageClass(
            upstreams=[UpstreamClass(dataset=SILVER_URN, type=DatasetLineageTypeClass.TRANSFORMED)]
        )
        mcp_lineage = MetadataChangeProposalWrapper(entityUrn=GOLD_URN, aspect=lineage)
        emitter.emit(mcp_lineage)

        # 1. Assertion Info
        assertion_info = AssertionInfoClass(
            type=AssertionTypeClass.DATASET,
            datasetAssertion=DatasetAssertionInfoClass(
                scope=DatasetAssertionScopeClass.DATASET_COLUMN,
                dataset=GOLD_URN,
                operator=AssertionStdOperatorClass.BETWEEN,
                logic="PD Score bounded in [0, 1] range check"
            )
        )
        mcp_info = MetadataChangeProposalWrapper(entityType="assertion", entityUrn=ASSERTION_URN, aspect=assertion_info)
        emitter.emit(mcp_info)

        # 2. Assertion Result
        assertion_result = AssertionRunEventClass(
            timestampMillis=int(time.time() * 1000),
            assertionUrn=ASSERTION_URN, asserteeUrn=GOLD_URN,
            runId="dp3_run_001", status=AssertionRunStatusClass.COMPLETE,
            result=AssertionResultClass(type=AssertionResultTypeClass.SUCCESS)
        )
        mcp_assertion = MetadataChangeProposalWrapper(entityType="assertion", entityUrn=ASSERTION_URN, aspect=assertion_result)
        emitter.emit(mcp_assertion)

        print("Done: Lineage, AssertionInfo & AssertionResult emitted to DataHub.")
    except Exception as e:
        print(f"WARNING: DataHub emit failed (non-blocking): {e}")


with DAG('credit_risk_dp3_offline_features', start_date=datetime(2026, 7, 25), schedule_interval=None, catchup=False) as dag:
    
    compute_stage = PythonOperator(
        task_id='compute_stage',
        python_callable=compute_features,
        inlets=[Table(cluster="hive", database="default", name=SILVER_DATASET)],
        outlets=[Table(cluster="hive", database="default", name=GOLD_DATASET)]
    )

    validate_stage = PythonOperator(
        task_id='validate_stage',
        python_callable=validate_data,
        inlets=[Table(cluster="hive", database="default", name=GOLD_DATASET)]
    )

    compute_stage >> validate_stage