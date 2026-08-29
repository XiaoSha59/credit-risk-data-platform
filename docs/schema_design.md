# Data Warehouse & Schema Design Report

**System:** Credit Risk Data Platform  
**Database Technology:** PostgreSQL 15  
**SQL Definition File:** [schema_setup.sql](file:///d:/credit-risk-data-platform/docs/sql/schema_setup.sql)

This document details the multi-zone Data Warehouse schema design for the Credit Risk Data Platform, adhering strictly to enterprise architecture standards, explicit naming conventions, Slowly Changing Dimensions (SCD Type 2), and machine learning feature storage patterns.

---

## 1. Overview of Multi-Zone Architecture

The data warehouse is structured across three isolated logical database schemas representing progressive data refinement zones:

| Zone (Schema) | Layer Purpose | Naming Convention | Description |
|---|---|---|---|
| **`bronze`** | Ingestion / Raw | `raw_` | Immutable, semi-structured raw payloads ingested directly from streaming/batch producers. |
| **`silver`** | Staging / Cleansed | `stg_` | Standardized, cleansed, and validated data prepared for intermediate business calculations. |
| **`gold`** | Business / Analytical | `dim_`, `fact_`, `feat_`, `obt_` | Production-ready dimensional modeling, feature stores, and denormalized views for BI & ML. |

---

## 2. Detailed Schema & Table Specifications

### 2.1. Bronze Zone (`bronze`)

- **`bronze.raw_loan_events`**: Stores raw semi-structured JSON payloads received from real-time Kafka events.
  - `event_id` (VARCHAR(50), Primary Key)
  - `payload` (JSON)
  - `ingested_at` (TIMESTAMP)

### 2.2. Silver Zone (`silver`)

- **`silver.stg_exposure_at_default`**: Staging layer for cleansed loan records with initial Exposure at Default (EAD) computations.
  - `loan_id` (VARCHAR(50), Primary Key)
  - `customer_id` (VARCHAR(50))
  - `ead_amount` (DECIMAL(15, 2))
  - `processed_at` (TIMESTAMP)

### 2.3. Gold Zone (`gold`)

- **`gold.dim_customer`** *(SCD Type 2 Dimension Table)*: Maintains full historical audit trails of customer risk attributes over time.
  - `customer_sk` (SERIAL, Primary Key)
  - `customer_id` (VARCHAR(50))
  - `customer_name` (VARCHAR(100))
  - `credit_score` (INT)
  - `valid_from_ts` (TIMESTAMP, Mandatory) — Start timestamp of record validity.
  - `valid_to_ts` (TIMESTAMP) — Expiration timestamp of record validity.
  - `is_current` (BOOLEAN, Mandatory) — Active record indicator (`TRUE`/`FALSE`).

- **`gold.fact_loan_transactions`** *(Fact Table)*: Core transactional fact table linked via surrogate key.
  - `transaction_id` (VARCHAR(50), Primary Key)
  - `customer_sk` (INT, Foreign Key referencing `gold.dim_customer.customer_sk`)
  - `loan_amount` (DECIMAL(15, 2))
  - `transaction_date` (TIMESTAMP)

- **`gold.feat_pd_matrix`** *(Feature Table)*: Optimized for Probability of Default (PD) quantitative risk models.
  - `feature_id` (SERIAL, Primary Key)
  - `customer_sk` (INT, Foreign Key referencing `gold.dim_customer.customer_sk`)
  - `pd_probability` (DECIMAL(5, 4))
  - `event_timestamp` (TIMESTAMP, Mandatory) — Timestamp when the risk event occurred.
  - `created` (TIMESTAMP, Mandatory) — Record insertion/generation timestamp.

- **`gold.obt_customer_risk_profile`** *(One Big Table / OBT)*: Denormalized table optimized for executive dashboards and high-speed reporting.
  - `customer_sk` (INT)
  - `total_exposure` (DECIMAL(15, 2))
  - `default_probability` (DECIMAL(5, 4))
  - `snapshot_date` (DATE)

---

## 3. Compliance & Architectural Checklist

| Requirement | Implementation Verification | Status |
|---|---|---|
| **Visualize tables on all zones** | All 3 zones (`bronze`, `silver`, `gold`) contain 6 defined tables in total. | ✅ Pass |
| **Dim table with SCD Type 2** | `gold.dim_customer` contains `valid_from_ts`, `valid_to_ts`, and `is_current`. | ✅ Pass |
| **Feature table requirements** | `gold.feat_pd_matrix` contains both mandatory timestamps: `event_timestamp` & `created`. | ✅ Pass |
| **Relationship between Dim & Fact** | Foreign Keys enforced from `fact_loan_transactions.customer_sk` and `feat_pd_matrix.customer_sk` to `dim_customer.customer_sk`. | ✅ Pass |
| **Gold Naming Convention** | Prefixes `dim_`, `fact_`, `feat_`, `obt_` used consistently. | ✅ Pass |
| **Bronze & Silver Naming Convention** | Prefixes `raw_` and `stg_` used consistently. | ✅ Pass |

---

## 4. DBeaver Schema & ER Diagram Evidence

> **Screenshot Evidence:**
> 
> Below are the screenshots captured directly from DBeaver displaying the Database Navigator structure across all zones and the relational Entity-Relationship (ER) diagram for the Gold zone.

### 4.1. Multi-Zone Table Navigation (All Zones)
![DBeaver All Zones Schema](docs/images/tables.png)
![DBeaver All Zones Schema](docs/images/data_dbeaver.png)
### 4.2. Gold Zone ER Diagram (Relationships & SCD Type 2)
![DBeaver Gold ER Diagram](docs/images/gold_table.png)
