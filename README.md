# Credit Risk Data Platform

[![Data Platform](https://img.shields.io/badge/Architecture-Medallion%20Lakehouse-blue.svg)](#2-high-level-system-deployment-diagram)
[![Engine](https://img.shields.io/badge/Streaming-Apache%20Flink%20%7C%20Kafka-orange.svg)](#51-online-streaming-pipeline)
[![Batch Engine](https://img.shields.io/badge/Batch-Apache%20Spark%20%7C%20Delta%20Lake-green.svg)](#52-offline-batch--delta-lakehouse-architecture)
[![Governance](https://img.shields.io/badge/Governance-Acryl%20DataHub-purple.svg)](#55-data-governance-lineage--quality-contracts)

## Table of Contents
1. [Business Domain & Platform Objectives](#1-business-domain--platform-objectives)
2. [High-Level System Deployment Diagram](#2-high-level-system-deployment-diagram)
3. [Repository Structure](#3-repository-structure)
4. [Quick Start: Running Locally](#4-quick-start-running-locally)
5. [Detailed Documentation Index](#5-detailed-documentation-index)

---

## 1. Business Domain & Platform Objectives
The core business domain of this platform is **Credit Risk Management & Analytics** in modern financial institutions.

In banking and quantitative risk frameworks (e.g., Basel II/III), accurately monitoring credit transactions and calculating risk metrics — such as **Probability of Default (PD)**, **Loss Given Default (LGD)**, and **Exposure at Default (EAD)** — requires both low-latency real-time detection and high-throughput batch analytics.

This enterprise data platform implements a hybrid Lambda/Kappa-inspired architecture:
- **Real-Time Streaming & MinIO External Storage:** Captures credit transaction events via **Kafka** (PyFlink 60-second window aggregations with risk classification) and simulates external department batch streaming ingestion via **MinIO S3** (`external-streaming-source` bucket into Bronze Zone).
- **Offline Medallion Lakehouse:** Ingests raw data into a multi-zone **Delta Lake** storage (Bronze, Silver, Gold) orchestrated by **Apache Airflow** and powered by **PySpark** with Compaction and Z-Order optimization.
- **Data Warehouse Serving:** Synchronizes Gold zone One Big Tables (OBT) and Dimensional models into **PostgreSQL** for business intelligence and quantitative analysis (DBeaver).
- **Data Governance:** Automated end-to-end data lineage, schema contracts, and Great Expectations quality metrics via **Acryl DataHub**.

---

## 2. High-Level System Deployment Diagram

Below is the high-level system deployment architecture illustrating the core data processing flows across the enterprise platform:

![Credit Risk Data Platform - High Level Architecture](./docs/images/architecture-diagram.png)

### Architecture Principles & Component Breakdown

1. **Deployable Units Only:** Every box in the deployment diagram represents an independently deployable container, cluster service, or external UI:
   - 🐳 **Data Generator Service (Docker):** Generates synthetic credit transactions with realistic anomaly injections (burst traffic, late arrivals, duplicate retries).
   - 📦 **MinIO S3 External Storage (Docker):** Containerized S3-compatible object storage (`http://localhost:9000`) acting as the external department source bucket (`external-streaming-source`).
   - ⚡ **Apache Kafka Broker (Docker):** Real-time message streaming bus receiving JSON event streams from the producer.
   - 🌊 **Apache PyFlink Engine:** Distributed stream processing engine running 60-second tumbling window aggregations with real-time risk classification (`HIGH`, `MEDIUM`, `LOW`).
   - ⚙️ **Apache Airflow Orchestrator (Docker):** DAG scheduler (`http://localhost:8085`) managing batch ingestion, Delta transformations, and DW sync tasks (`dp1_ingest_raw`, `dp2_silver_transform`, `dp3_gold_dw_sync`).
   - 📊 **PySpark Lakehouse Engine:** Batch processing framework handling Medallion transformations, Delta Lake compaction, and Z-Order spatial clustering.
   - 🐘 **PostgreSQL Data Warehouse (Docker):** Relational serving database hosting Gold zone dimensional schemas (`bronze`, `silver`, `gold`).
   - 🔍 **Acryl DataHub Platform (Docker):** Centralized governance portal capturing automated data lineage, schema contracts, and data quality metrics.

2. **Data Flow & Arrow Directions:** Arrows strictly follow data transmission direction with explicit payload labels on arrow badges:
   - `1.1 Credit Events (JSON) / S3 Batch`: Payload pushed from generator to Kafka topic / MinIO S3 bucket.
   - `1.2 Streaming Events / Bronze Parquet`: Real-time windowed records & MinIO batches written to Bronze zone storage.
   - `2.1 Airflow DAG Trigger`: Orchestration control signals sent to PySpark batch scripts.
   - `2.2 Read Raw Data (Parquet / Delta)`: PySpark reading raw Bronze Parquet files.
   - `2.3 Delta Medallion Transformations`: Cleaning, deduplication, and compaction across Silver and Gold Delta tables.
   - `2.4 Sync Gold OBT`: Bulk loading Gold One Big Tables into PostgreSQL DW schemas.
   - `3.1 SQL Analytics Query`: Analytical SQL queries executed from DBeaver to PostgreSQL DW.
   - `4.1 Metadata & Lineage`: Asynchronous emission of lineage graph, schema contracts, and GE quality test execution logs to DataHub.

3. **Numbered Multi-Flow Lineage:**
   - **Flow 1: Streaming Data & External S3 Storage Flow (Blue Steps 1.1 to 1.3):**
     - *Path 1A (Kafka/Flink Streaming):* Generator -> Kafka Topic -> PyFlink 60s window aggregation -> Bronze Parquet Sink (`data/bronze/online/`).
     - *Path 1B (MinIO Batch Streaming):* Generator -> MinIO S3 bucket (`external-streaming-source`) -> `ingest_online_bronze.py` -> Bronze Zone Storage.
   - **Flow 2: Offline Medallion Batch Flow (Green Steps 2.1 to 2.4):** Airflow-scheduled PySpark batch jobs reading Bronze Parquet files, applying data quality rules, creating Silver Delta Lake tables, performing compaction and Z-Order optimization, building Gold zone 360-degree risk analytical models, and syncing into PostgreSQL DW.
   - **Flow 3: Serving & Analytics Flow (Orange Step 3.1):** End-user Risk Analysts and Quantitative Modelers querying Gold zone DW tables via DBeaver for credit risk reporting.
   - **Flow 4: Data Governance & Lineage Flow (Dashed Purple Step 4.1):** Automated metadata emission across all ingestion, processing, and serving layers into DataHub for complete end-to-end lineage visualization and schema contract monitoring.

4. **Solid vs Dashed Lines:** Primary data processing paths use solid lines (Flows 1, 2, 3); dashed purple lines denote background metadata, lineage, and governance synchronization (Flow 4).

---

## 3. Repository Structure

```text
credit-risk-data-platform/
│
├── dags/                           # Airflow DAG definitions (DP1, DP2, DP3 orchestrations)
│
├── docs/                           # Comprehensive technical documentation & reports
│   ├── airflow_orchestration.md    # [Doc] Airflow DAG design & operational guide
│   ├── data_goverance.md           # [Doc] DataHub lineage, schema contracts & quality report
│   ├── IaC_Docker.md               # [Doc] Multi-stage Docker optimization report
│   ├── Offline_Data_Pipeline_Index.md # [Doc] Offline data pipeline index & specs
│   ├── offline_pipeline_optimization_report.md # [Doc] Delta Lake compaction & Z-Order benchmark
│   ├── Offline_Processing_Index.md # [Doc] Offline processing architecture summary
│   ├── Online_Data_Pipeline_Index.md  # [Doc] Online streaming pipeline & MinIO specs
│   ├── schema_design.md            # [Doc] Multi-Zone DBeaver Schema & ER diagram evidence
│   ├── streaming_pipeline_report.md# [Doc] Flink baseline vs optimized streaming report
│   ├── images/                     # Screenshot assets & architecture diagrams
│   ├── sql/
│   │   └── schema_setup.sql        # PostgreSQL DDL script for Gold Zone DW schema
│   └── study-purpose/
│       ├── Docker_Optimization.md  # Docker multi-stage build reference guide
│       └── storage.md              # Baseline Parquet vs Delta Lake Lakehouse comparison
│
├── generators/                     # Data generator & stream loader scripts
│   ├── credit_events.py            # Simulated credit risk event generator
│   ├── offline_data_gen.py         # Offline batch dataset generator
│   ├── online_stream_gen.py        # Real-time transaction generator with anomalies
│   ├── online_stream_loader.py     # Pushes streaming batches to external MinIO S3 bucket
│   └── send_stream_data.py         # Streaming Kafka producer application
│
├── pipelines/                      # Bronze Zone ingestion & batch pipelines
│   ├── ingest_offline_bronze.py    # Offline batch data Bronze ingestion pipeline
│   ├── ingest_online_bronze.py     # Pulls streaming batches from MinIO S3 into Bronze
│   └── run_offline_pipeline.py     # End-to-end offline pipeline orchestrator
│
├── streaming/                      # Real-time streaming PyFlink applications
│   ├── flink_baseline_job.py       # Baseline PyFlink 30s processing job
│   └── flink_optimzed_job.py       # Optimized PyFlink 60s window job with checkpointing
│
├── storage/                        # Storage layer implementations
│   ├── baseline_storage.py         # Baseline raw Parquet append handler
│   └── optimized_lakehouse.py      # Delta Lakehouse engine (compaction & Z-Order)
│
├── docker-compose.yml              # Multi-container orchestration (Kafka, Zookeeper, Airflow, Postgres)
├── Dockerfile                      # Multi-stage optimized Dockerfile for data generator
├── Dockerfile.non-optimized        # Baseline single-stage Dockerfile for comparison
└── README.md                       # Main platform entry point
```

---

## 4. Quick Start: Running Locally

### Prerequisites
- **Docker Engine** (v20.10+) & **Docker Compose** (v2.0+)
- **Python 3.10+** (with virtual environment `.venv`)

### Step 1: Environment & Infrastructure Setup
Clone the repository and launch the containerized infrastructure:
```bash
# Clone repository
git clone https://github.com/XiaoSha59/credit-risk-data-platform.git
cd credit-risk-data-platform

# Build optimized Data Generator image
docker build -f Dockerfile -t credit-risk-platform:optimized .

# Launch core services (Kafka, Zookeeper, Postgres, Airflow)
docker-compose up -d

# Spin up MinIO container (External S3 object storage for streaming batches)
docker run -d --name minio_streaming_source -p 9000:9000 -p 9001:9001 -e "MINIO_ROOT_USER=minioadmin" -e "MINIO_ROOT_PASSWORD=minioadmin" minio/minio server /data --console-address ":9001"

# Verify container status
docker ps
```

### Step 2: Running Real-Time Streaming & MinIO Pipeline
To run the streaming data pipelines:

```bash
# --- Option A: External MinIO S3 Batch Streaming Flow ---
# 1. Generate streaming transaction batches and push to MinIO S3 bucket (external-streaming-source)
.venv\Scripts\python.exe generators/online_stream_loader.py

# 2. Ingest streaming batches from MinIO S3 into Bronze Zone (data/bronze/online/)
.venv\Scripts\python.exe pipelines/ingest_online_bronze.py

# --- Option B: Apache Kafka & PyFlink Streaming Flow ---
# Terminal 1 — Launch Flink Baseline Stream Job (Port 8081)
.venv\Scripts\python.exe streaming/flink_baseline_job.py

# Terminal 2 — Launch Flink Optimized Stream Job (Port 8082 with Checkpointing & Risk Classification)
.venv\Scripts\python.exe streaming/flink_optimzed_job.py

# Terminal 3 — Produce 100 streaming credit risk events to Kafka
.venv\Scripts\python.exe generators/send_stream_data.py
```

*Access Web & Console Interfaces:*
- **MinIO Console:** `http://localhost:9001` (User: `minioadmin` / Password: `minioadmin`)
- **Flink Dashboard:** `http://localhost:8081` or `http://localhost:8082`

### Step 3: Running Airflow Batch DAGs & Data Warehouse Sync
Access the Airflow Web UI to monitor and trigger batch data pipelines:
- **Airflow Web UI:** `http://localhost:8085` (User: `admin` / Password: `standalone_password`)
- **Trigger DAGs:** `dp1_ingest_raw`, `dp2_silver_transform`, `dp3_gold_dw_sync`

### Step 4: Connecting DBeaver to PostgreSQL DW
Connect your SQL client (e.g., DBeaver) to inspect Gold zone tables:
- **Host:** `localhost` | **Port:** `5432`
- **Database:** `credit_risk_dw` | **User:** `creditrisk` | **Password:** `creditrisk123`
- **Schemas:** `bronze`, `silver`, `gold` (Tables: `dim_customers`, `fact_loans`, `feat_credit_scores`, `obt_credit_risk_360`)

---

## 5. Detailed Documentation Index

For exhaustive technical deep-dives, benchmark reports, and operational guides, consult the dedicated reports below:

| Focus Area | Document Link | Description |
|---|---|---|
| **System Architecture Diagram** | [architecture-diagram.png](./docs/images/architecture-diagram.png) | High-res deployable unit architecture diagram image. |
| **Real-Time Streaming** | [streaming_pipeline_report.md](./docs/streaming_pipeline_report.md) | PyFlink baseline vs optimized streaming report (burst handling, late arrival, checkpointing). |
| **Streaming Specs Index** | [Online_Data_Pipeline_Index.md](./docs/Online_Data_Pipeline_Index.md) | Online streaming pipeline specs and event schema descriptions. |
| **Offline Pipeline Optimization** | [offline_pipeline_optimization_report.md](./docs/offline_pipeline_optimization_report.md) | Benchmark analysis of Delta Lake compaction & Z-Order clustering performance. |
| **Storage Architecture Study** | [storage.md](./docs/study-purpose/storage.md) | Technical comparison between Raw Parquet baseline and Delta Lakehouse storage. |
| **Offline Pipeline Specs** | [Offline_Data_Pipeline_Index.md](./docs/Offline_Data_Pipeline_Index.md) | Detailed breakdown of batch ingestion, Silver cleaning, and Gold feature engineering. |
| **Offline Processing Overview** | [Offline_Processing_Index.md](./docs/Offline_Processing_Index.md) | High-level summary of batch processing stages and data transformations. |
| **Airflow Orchestration** | [airflow_orchestration.md](./docs/airflow_orchestration.md) | Complete operational guide for Airflow DAGs (DP1, DP2, DP3) and retry policies. |
| **Data Governance & Contracts** | [data_goverance.md](./docs/data_goverance.md) | Implementation report for DataHub lineage, schema contracts, and Great Expectations quality audit. |
| **Warehouse & ER Diagram** | [schema_design.md](./docs/schema_design.md) | Multi-zone database navigation evidence and DBeaver ER diagram for Gold zone tables. |
| **SQL DW DDL Setup** | [schema_setup.sql](./docs/sql/schema_setup.sql) | DDL script setting up `bronze`, `silver`, and `gold` schemas in PostgreSQL DW. |
| **IaC & Containerization** | [IaC_Docker.md](./docs/IaC_Docker.md) | Multi-stage Docker build footprint analysis and image optimization benchmark. |
| **Docker Study Guide** | [Docker_Optimization.md](./docs/study-purpose/Docker_Optimization.md) | Educational reference on container multi-staging and Alpine base image selection. |
