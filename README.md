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
- **Real-Time Streaming:** Captures credit transaction events via **Kafka** and processes 60-second window aggregations with risk classification (`HIGH`, `MEDIUM`, `LOW`) using **PyFlink**.
- **Offline Medallion Lakehouse:** Ingests raw data into a multi-zone **Delta Lake** storage (Bronze, Silver, Gold) orchestrated by **Apache Airflow** and powered by **PySpark** with Compaction and Z-Order optimization.
- **Data Warehouse Serving:** Synchronizes Gold zone One Big Tables (OBT) and Dimensional models into **PostgreSQL** for business intelligence and quantitative analysis (DBeaver).
- **Data Governance:** Automated end-to-end data lineage, schema contracts, and Great Expectations quality metrics via **Acryl DataHub**.

---

## 2. High-Level System Deployment Diagram

Below is the deployable unit-based architecture diagram illustrating the 4 core numbered operational flows:

![System Deployment Diagram](./docs/images/architecture-diagram.png)

```mermaid
graph TD
    classDef deployable fill:#1E293B,stroke:#38BDF8,stroke-width:2px,color:#F8FAFC;
    classDef batchUnit fill:#1E293B,stroke:#4ADE80,stroke-width:2px,color:#F8FAFC;
    classDef govUnit fill:#1E293B,stroke:#C084FC,stroke-width:2px,color:#F8FAFC;
    classDef userUnit fill:#1E293B,stroke:#FB923C,stroke-width:2px,color:#F8FAFC;
    classDef monitorUnit fill:#1E293B,stroke:#FACC15,stroke-width:2px,color:#F8FAFC;

    %% -------------------------------------------------------------------------
    %% DEPLOYABLE UNITS (CONTAINERS / SERVICES)
    %% -------------------------------------------------------------------------
    subgraph Ingestion_and_Streaming ["Real-Time Streaming Infrastructure"]
        DataGen["Data Generator Service<br/><i>(Docker: platform-data-generator)</i>"]:::deployable
        Zookeeper["Zookeeper Service<br/><i>(Docker: platform-zookeeper)</i>"]:::deployable
        Kafka["Kafka Message Broker<br/><i>(Docker: platform-kafka)</i>"]:::deployable
        FlinkEngine["PyFlink Stream Engine<br/><i>(Flink Cluster / Stream Worker)</i>"]:::deployable
    end

    subgraph Batch_and_Lakehouse ["Offline Batch & Lakehouse Infrastructure"]
        Airflow["Airflow Orchestrator<br/><i>(Docker: credit_risk_airflow)</i>"]:::batchUnit
        SparkEngine["PySpark Batch Engine<br/><i>(Spark Cluster Engine)</i>"]:::batchUnit
        Lakehouse["Delta Lakehouse Storage<br/><i>(Storage Volume: Bronze/Silver/Gold)</i>"]:::batchUnit
    end

    subgraph Serving_and_Governance ["Serving & Governance Infrastructure"]
        PostgresDW["PostgreSQL DW<br/><i>(Docker: credit_risk_postgres)</i>"]:::userUnit
        DataHubGMS["DataHub Governance Server<br/><i>(Docker: platform-datahub-gms)</i>"]:::govUnit
    end

    subgraph Users_and_Monitoring ["Interfaces & Operations"]
        RiskAnalyst["Risk Analyst / DBeaver<br/><i>(External SQL Client)</i>"]:::userUnit
        WebUI["Flink & Airflow Web UI<br/><i>(Dashboard & Monitoring)</i>"]:::monitorUnit
        DataHubUI["DataHub Web UI<br/><i>(Governance Catalog UI)</i>"]:::govUnit
    end

    %% -------------------------------------------------------------------------
    %% INTERNAL METADATA SYNC (Dashed)
    %% -------------------------------------------------------------------------
    Zookeeper -.-|Cluster State Sync| Kafka

    %% -------------------------------------------------------------------------
    %% FLOW 1: ONLINE REAL-TIME STREAMING PIPELINE (Blue Arrows)
    %% -------------------------------------------------------------------------
    DataGen -->|1.1: Raw Streaming Credit Events| Kafka
    Kafka -->|1.2: Topic credit_risk_events| FlinkEngine
    FlinkEngine -->|1.3: Stream Metrics & Window Logs| WebUI
    FlinkEngine -->|1.4: Stream Checkpoints & Parquet Sinks| Lakehouse

    %% -------------------------------------------------------------------------
    %% FLOW 2: OFFLINE BATCH & LAKEHOUSE PIPELINE (Green Arrows)
    %% -------------------------------------------------------------------------
    Airflow -->|2.1: Triggers Ingest & ETL DAGs DP1/DP2/DP3| SparkEngine
    Lakehouse -->|2.2: Reads Raw Source Parquet/CSV| SparkEngine
    SparkEngine -->|2.3: Writes Bronze, Silver & Gold Delta Tables| Lakehouse
    SparkEngine -->|2.4: Syncs Gold DW OBT & Fact/Dim Tables| PostgresDW

    %% -------------------------------------------------------------------------
    %% FLOW 3: DATA GOVERNANCE & QUALITY AUDIT (Purple Arrows)
    %% -------------------------------------------------------------------------
    SparkEngine -->|3.1: Lineage, Schema Contracts & Quality Meta| DataHubGMS
    Airflow -->|3.1: DAG Orchestration Metadata| DataHubGMS
    DataHubGMS -->|3.2: Lineage Graph & Catalog Metadata| DataHubUI

    %% -------------------------------------------------------------------------
    %% FLOW 4: USER ANALYTICS & OPERATIONAL CONTROL (Orange/Yellow Arrows)
    %% -------------------------------------------------------------------------
    RiskAnalyst -->|4.1: SQL Queries on Gold 360 Risk Tables| PostgresDW
    Airflow -->|4.2: Execution Status & Logs| WebUI
```

### Architecture Principles & Rubric Compliance
1. **Deployable Units Only:** Every box represents an independently deployable container, cluster service, or external UI (`platform-data-generator`, `platform-kafka`, `credit_risk_airflow`, `credit_risk_postgres`, `platform-datahub-gms`, etc.). Embedded libraries/SDKs (Feast SDK, Pydantic, Great Expectations) execute inside the container processes and are not separate deployable boxes.
2. **Data Flow & Arrow Directions:** Arrows strictly follow data movement and carry descriptive payload labels.
3. **Numbered Multi-Flow Lineage:** Distinct colors and sequence numbers demarcate operational workflows:
   - **Flow 1 (Blue — 1.1 to 1.4):** Real-time Kafka event streaming, Flink 60s tumbling window aggregation, risk classification, and stateful checkpointing.
   - **Flow 2 (Green — 2.1 to 2.4):** Airflow-orchestrated PySpark batch ingestion, Medallion Lakehouse transformations (Bronze/Silver/Gold), Delta Compaction & Z-Order, and PostgreSQL DW syncing.
   - **Flow 3 (Purple — 3.1 to 3.2):** Automated emission of data lineage graphs, schema contracts, and quality assertions to DataHub.
   - **Flow 4 (Orange/Yellow — 4.1 to 4.2):** Risk Analyst SQL queries on Gold 360 tables via DBeaver and Data Engineer operational dashboard monitoring.
4. **Solid Lines for Primary Flow:** Solid arrows represent primary data paths; a dashed line is strictly reserved for internal Zookeeper-Kafka metadata synchronization.

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
│   ├── Online_Data_Pipeline_Index.md  # [Doc] Online streaming pipeline specs
│   ├── schema_design.md            # [Doc] Multi-Zone DBeaver Schema & ER diagram evidence
│   ├── streaming_pipeline_report.md# [Doc] Flink baseline vs optimized streaming report
│   ├── images/                     # Screenshot assets & architecture diagrams
│   ├── sql/
│   │   └── schema_setup.sql        # PostgreSQL DDL script for Gold Zone DW schema
│   └── study-purpose/
│       ├── Docker_Optimization.md  # Docker multi-stage build reference guide
│       └── storage.md              # Baseline Parquet vs Delta Lake Lakehouse comparison
│
├── generators/                     # Data generator scripts
│   ├── credit_events.py            # Simulated credit risk event generator
│   └── send_stream_data.py         # Streaming Kafka producer application
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

# Launch services (Kafka, Zookeeper, Postgres, Airflow)
docker-compose up -d

# Verify container status
docker ps
```

### Step 2: Running Real-Time Streaming Pipeline
To run the PyFlink streaming jobs and stream live transaction events:
```bash
# Terminal 1 — Launch Flink Baseline Stream Job (Port 8081)
.venv\Scripts\python.exe streaming/flink_baseline_job.py

# Terminal 2 — Launch Flink Optimized Stream Job (Port 8082 with Checkpointing & Risk Classification)
.venv\Scripts\python.exe streaming/flink_optimzed_job.py

# Terminal 3 — Produce 100 streaming credit risk events to Kafka
.venv\Scripts\python.exe generators/send_stream_data.py
```
*Access Flink Dashboard:* `http://localhost:8081` or `http://localhost:8082`

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
| **System Architecture Diagram** | [architecture-diagram.png](./docs/images/architecture-diagram.png) | High-res 300 DPI deployable unit architecture diagram image. |
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
