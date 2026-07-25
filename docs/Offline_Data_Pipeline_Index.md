# 📂 Offline Credit Data Generation & Ingestion - Master Index

This document serves as the master navigation index for the **Offline Data Pipeline**, providing direct links to configuration files, source codes, generated datasets, external storage simulation guides, and execution evidence.

---

## 📑 Table of Contents & File Links

### 1. Configuration & Environment
* ⚙️ **Generator Configuration (YAML):** [`../configs/offline_config.yaml`](../configs/offline_config.yaml)
* 📦 **Dependencies Requirements:** [`../requirements.txt`](../requirements.txt) *(includes database connectors like `sqlalchemy` and `psycopg2-binary`)*

---

### 2. Source Code Implementation
* 🐍 **Data Generator Script:** [`../generators/offline_data_gen.py`](../generators/offline_data_gen.py)
  * *Description:* Generates baseline records, injects statistical skewness, simulates schema evolution, and introduces duplicates.
* 🐍 **External Department DB Loader:** [`../generators/offline_db_loader.py`](../generators/offline_db_loader.py)
  * *Description:* Simulates an external department database by pushing generated offline data directly into a PostgreSQL instance.
* 🐍 **Bronze Zone Ingestion Pipeline:** [`../pipelines/ingest_offline_bronze.py`](../pipelines/ingest_offline_bronze.py)
  * *Description:* Pulls raw offline credit data from the external PostgreSQL database and stores it into the Bronze Zone.
* 🐍 **Data Quality Summary Utility:** [`../generators/summarize_offline_data.py`](../generators/summarize_offline_data.py)
  * *Description:* Inspects and validates data quality metrics for the offline portfolio.

---

### 3. Generated Datasets & Storage Layers
* 📊 **Local Raw Parquet File:** [`../data/raw/offline/offline_loan_portfolio.parquet`](../data/raw/offline/offline_loan_portfolio.parquet)
* 🗄️ **Bronze Zone Parquet Lake Storage:** [`../data/bronze/offline/raw_loan_portfolio.parquet`](../data/bronze/offline/raw_loan_portfolio.parquet)

---

### 4. Data Quality Validation & Metrics Evidence

Terminal execution output verifying data quality anomaly injections:

![Offline Data Quality Summary](images/offline_data_quality_summary.png)

#### Summary of Achieved Metrics:
* **Duplicate Rate:** ~2.0% pre-cleaning (simulating upstream storage overlap).
* **Cardinality:** High distinct count verified on `loan_id` and `customer_id`.
* **Skew Distribution:** Proportions aligned with configured YAML probabilities.
* **Schema Evolution:** Partitions older than 180 days correctly return `NULL` for `credit_bureau_score`.

---

## 🚀 Quick Start Guide

```bash
# 1. Activate virtual environment and install dependencies
.venv\Scripts\Activate  # On Windows PowerShell
pip install -r ../requirements.txt

# 2. Spin up external department database (PostgreSQL via Docker)
docker run --name external_core_banking_db -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=core_banking -p 5432:5432 -d postgres:15

# 3. Simulate external data storage (Push data to external DB)
python ../generators/offline_db_loader.py

# 4. Ingest data from external source into Bronze Zone
python ../pipelines/ingest_offline_bronze.py

# 5. Run the data quality inspection utility
python ../generators/summarize_offline_data.py