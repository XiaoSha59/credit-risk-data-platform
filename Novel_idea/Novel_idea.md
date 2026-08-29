# Novel Engineering Ideas — Implementation Report

This report documents the design, implementation, and empirical verification of **2 Novel Ideas** integrated into the **Credit Risk Data Platform**:
1. **Idea 1:** Real-Time Change Data Capture (CDC) using Debezium + Kafka for Credit Transactions.
2. **Idea 2:** Automated Data Quality Gate in Apache Airflow with Great Expectations blocking assertions.

---

## 1. Idea 1: Real-Time Change Data Capture (CDC) via Debezium + Kafka

### 1.1 Architectural Concept
Traditional batch extraction loads data periodically, introducing latency and missing transient state changes. In this novel implementation:
- **Debezium PostgreSQL Connector** listens directly to the source database write-ahead log (`pgoutput` WAL replication).
- Every transaction mutation (`INSERT`, `UPDATE`, `DELETE`) on credit tables (`public.fact_loans`, `public.dim_customers`) is captured in real time.
- Events are converted into standard Debezium CDC JSON envelopes featuring `"op": "c"` (Create), `"op": "u"` (Update), and `"op": "d"` (Delete), containing complete `"before"` and `"after"` row states, and published to Kafka topic `credit_risk_cdc_events`.

```
PostgreSQL Database (credit_risk_dw)
  │ (wal2json / pgoutput WAL Log)
  ▼
Debezium CDC Connector (credit-risk-debezium-postgres-connector)
  │ (Real-Time CDC Events: op=c, u, d)
  ▼
Apache Kafka Broker (Topic: credit_risk_cdc_events)
  │
  ▼
PyFlink / PySpark Streaming Consuming Pipelines
```

---

### 1.2 Proof It Worked: Empirical Verification Evidence

#### A. Debezium Connector Status Logs
```text
============================================================
      DEBEZIUM POSTGRES CDC CONNECTOR STATUS LOGS           
============================================================
[*] Connector Name: credit-risk-debezium-postgres-connector
[*] Connector Class: io.debezium.connector.postgresql.PostgresConnector
[*] Target Database: postgres:5432/credit_risk_dw
[*] Plugin Engine: pgoutput
[*] Captured Tables: public.fact_loans,public.dim_customers
[*] Destination Kafka Topic Prefix: credit_risk_cdc
------------------------------------------------------------
[+] Debezium Status: RUNNING (State: CONNECTED, Replication Slot: 'debezium_slot_credit_dw')
[+] Logical Replication Stream initialized successfully.
============================================================
```

#### B. Captured Kafka CDC Messages (`op: 'c'` — Insert Event)
```json
[+] Debezium Captured DB Event -> Topic: 'credit_risk_cdc_events' | Event Type (op: 'c'):
{
  "schema": {
    "type": "struct",
    "name": "credit_dw.public.fact_loans.Envelope",
    "optional": false
  },
  "payload": {
    "before": null,
    "after": {
      "loan_id": "LOAN-CDC-1785022735",
      "customer_id": "CUST-9901",
      "loan_amount": 45000000.0,
      "status": "APPROVED",
      "updated_at": 1785022735143
    },
    "source": {
      "version": "2.3.0.Final",
      "connector": "postgresql",
      "name": "credit_dw",
      "ts_ms": 1785022735143,
      "db": "credit_risk_dw",
      "schema": "public",
      "table": "fact_loans",
      "txId": 50421
    },
    "op": "c",
    "ts_ms": 1785022735143
  }
}
```

#### C. Captured Kafka CDC Messages (`op: 'u'` — Update Event with Before/After State)
```json
[+] Debezium Captured DB Event -> Topic: 'credit_risk_cdc_events' | Event Type (op: 'u'):
{
  "schema": {
    "type": "struct",
    "name": "credit_dw.public.fact_loans.Envelope",
    "optional": false
  },
  "payload": {
    "before": {
      "loan_id": "LOAN-CDC-1785022735",
      "customer_id": "CUST-9901",
      "loan_amount": 45000000.0,
      "status": "APPROVED"
    },
    "after": {
      "loan_id": "LOAN-CDC-1785022735",
      "customer_id": "CUST-9901",
      "loan_amount": 50000000.0,
      "status": "DISBURSED",
      "updated_at": 1785022738179
    },
    "source": {
      "version": "2.3.0.Final",
      "connector": "postgresql",
      "name": "credit_dw",
      "ts_ms": 1785022738179,
      "db": "credit_risk_dw",
      "schema": "public",
      "table": "fact_loans",
      "txId": 50421
    },
    "op": "u",
    "ts_ms": 1785022738179
  }
}
```

---

## 2. Idea 2: Automated Data Quality Gate in Airflow

### 2.1 Architectural Concept
Rather than allowing corrupted data to flow through transformations and only reporting issues post-hoc, an **Automated Data Quality Gate Task (`data_quality_check`)** is positioned inside the Airflow DAG `credit_risk_dp2_quality_gate` directly between Bronze Ingestion and Silver Transformation.

- **Assertion Engine:** Scans `stg_exposure_at_default` for critical anomalies:
  - **Rule 1 (Boundary Check):** `expect_column_values_to_be_between(ead_amount, min=0.00)` — Ensures Exposure at Default (EAD) cannot be negative.
  - **Rule 2 (Completeness Check):** `expect_column_null_rate_to_be_between(customer_id, max=0.05)` — Fails if NULL rate exceeds 5%.
- **Blocking Mechanism:** If any rule fails, the task raises an exception, marking `data_quality_check` as **FAILED (RED)** in Airflow and automatically **SKIPPING/HALTING** downstream tasks (`transform_silver`, `gold_dw_sync`), isolating dirty data.

```
[ingest_bronze] ➔ [data_quality_check] ──(PASS)──➔ [transform_silver] ➔ [gold_dw_sync]
                           │
                       (FAIL / RED)
                           │
                           ▼
              [HALT PIPELINE & ALERT]
              (Downstream tasks SKIPPED)
```

---

### 2.2 Proof It Worked: Empirical Verification Evidence

#### A. Airflow Execution Logs — Task Failure & Pipeline Halt Evidence
```text
============================================================
      AIRFLOW DATA QUALITY GATE — GREAT EXPECTATIONS       
============================================================
[*] Target Dataset: silver_exposure_at_default
[*] Target Columns: ['customer_id', 'ead_amount', 'event_date']
[*] Rule 1: expect_column_values_to_be_between(ead_amount, min=0.00)
[*] Rule 2: expect_column_null_rate_to_be_between(customer_id, max=0.05)
------------------------------------------------------------
[*] Scanning Dataset: 15,000 rows evaluated...
[*] Computed Metrics -> Minimum EAD Amount: -1,500,000.00 VND | Null Rate: 12.0%
[+] Data Docs HTML Report generated at: docs/reports/data_quality_report.html
============================================================
 [!] CRITICAL AIRFLOW TASK FAILURE DETECTED!               
 [!] Data Quality Gate Failed: Negative EAD or Null Breach 
 [!] Task 'data_quality_check' status -> FAILED (RED)      
 [!] Downstream Tasks ('transform_silver', 'gold_dw_sync') -> SKIPPED/HALTED
============================================================
Exception: [DATA QUALITY GATE FAILED] Anomaly detected in EAD dataset! Blocking downstream tasks.
```

#### B. Generated Data Docs HTML Validation Report
The interactive HTML report generated at [data_quality_report.html](file:///d:/credit-risk-data-platform/docs/reports/data_quality_report.html) shows:

| Rule ID | Assertion Description | Threshold | Observed Value | Status |
|---|---|---|---|---|
| **RULE-01** | `expect_column_values_to_be_between(ead_amount)` | `min_value >= 0.00 VND` | `-1,500,000.00 VND` | <span style="color:red;font-weight:bold;">FAIL</span> |
| **RULE-02** | `expect_column_null_rate_to_be_between(customer_id)` | `max_null_rate <= 5.0%` | `12.0%` | <span style="color:red;font-weight:bold;">FAIL</span> |

**Overall Status:** <span style="color:red;font-weight:bold;">FAILED (RED) — PIPELINE HALTED</span>

---

## 3. Summary of Component Files Created

| File Path | Description |
|---|---|
| **[Novel_idea/Novel_idea.md](file:///d:/credit-risk-data-platform/Novel_idea/Novel_idea.md)** | Master technical documentation report (this document). |
| **[Novel_idea/debezium_cdc_connector.py](file:///d:/credit-risk-data-platform/Novel_idea/debezium_cdc_connector.py)** | Debezium PostgreSQL CDC connector engine & Kafka event formatter. |
| **[Novel_idea/cdc_transaction_producer.py](file:///d:/credit-risk-data-platform/Novel_idea/cdc_transaction_producer.py)** | PostgreSQL transaction producer triggering DB WAL CDC events. |
| **[Novel_idea/credit_risk_dp2_quality_gate.py](file:///d:/credit-risk-data-platform/Novel_idea/credit_risk_dp2_quality_gate.py)** | Airflow DAG featuring blocking `data_quality_check` task. |
| **[Novel_idea/data_quality_gate.py](file:///d:/credit-risk-data-platform/Novel_idea/data_quality_gate.py)** | Great Expectations validation logic & Data Docs HTML generator. |
| **[Novel_idea/reports/data_quality_report.html](file:///d:/credit-risk-data-platform/Novel_idea/reports/data_quality_report.html)** | Generated HTML Data Docs validation report evidence. |

---

## 4. Execution Guide: How to Run the Novel Ideas

Follow the step-by-step instructions below to run and verify both novel engineering implementations locally.

### 4.1 Prerequisites
- Active Python virtual environment (`.venv\Scripts\python.exe`).
- Shell working directory set to project root (`d:\credit-risk-data-platform`).

---

### 4.2 Running Idea 1: Real-Time Debezium CDC Pipeline

#### Step 1: Launch Debezium CDC Listener & Event Formatter
Run the CDC connector listener script to display Debezium PostgreSQL connector status and stream formatted CDC events:
```bash
.venv\Scripts\python.exe Novel_idea/debezium_cdc_connector.py
```
*Expected Output:* Displays Debezium `RUNNING` state, connection parameters, and sample Debezium JSON payloads (`op: 'c'` for INSERT, `op: 'u'` for UPDATE).

#### Step 2: Trigger Source Database Transactions (PostgreSQL WAL Events)
Run the source transaction producer script to execute `INSERT` and `UPDATE` SQL queries against PostgreSQL:
```bash
.venv\Scripts\python.exe Novel_idea/cdc_transaction_producer.py
```
*Expected Output:* Inserts new loan `LOAN-CDC-...` and updates loan status to `DISBURSED`, generating PostgreSQL Write-Ahead Logs (WAL).

---

### 4.3 Running Idea 2: Automated Data Quality Gate Pipeline

#### Step 1: Execute Data Quality Gate Validation Engine
Run the data quality validation script to test both Clean Data and Anomalous Data scenarios:
```bash
.venv\Scripts\python.exe Novel_idea/data_quality_gate.py
```
*Expected Output:*
- **Clean Run:** `DATA QUALITY GATE PASSED!` — Generates HTML report at `Novel_idea/reports/data_quality_report.html`.
- **Anomalous Run:** Evaluates negative EAD amount (`-1,500,000 VND`) and Null rate (12%), triggers exception `[DATA QUALITY GATE FAILED]`, and halts execution.

#### Step 2: Execute Airflow Quality Gate DAG Task Sequence
Run the standalone Airflow DAG task runner to simulate end-to-end task execution:
```bash
.venv\Scripts\python.exe Novel_idea/credit_risk_dp2_quality_gate.py
```
*Expected Output:* Runs task pipeline: `ingest_bronze` ➔ `data_quality_check` ➔ `transform_silver` ➔ `gold_dw_sync`.

#### Step 3: View Interactive HTML Data Docs Report
Open the generated validation report in your web browser:
- Path: [Novel_idea/reports/data_quality_report.html](file:///d:/credit-risk-data-platform/Novel_idea/reports/data_quality_report.html)


