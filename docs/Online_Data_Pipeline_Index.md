# 📂 Online Streaming Data Generation & Ingestion - Master Index

This document serves as the master navigation index for the **Online Streaming Data Pipeline**, providing direct links to streaming configurations, source codes, MinIO external storage setup, Bronze ingestion pipelines, and execution evidence.

---

## 📑 Table of Contents & File Links

### 1. Configuration & Environment
* ⚙️ **Streaming Generator Configuration (YAML):** [`../configs/online_config.yaml`](../configs/online_config.yaml)
* 📦 **Dependencies Requirements:** [`../requirements-db.txt`](../requirements-db.txt) *(includes `boto3` for S3/MinIO connectivity)*

---

### 2. Source Code Implementation
* 🐍 **Online Stream Loader Script:** [`../generators/online_db_loader.py`](../generators/online_stream_loader.py)
  * *Description:* Simulates real-time credit transactions, embeds data anomalies (burst traffic, late arrivals, API retry duplicates at ~1.5%), and pushes batches directly to an external MinIO S3 bucket.
* 🐍 **Online Bronze Ingestion Pipeline:** [`../pipelines/ingest_online_bronze.py`](../pipelines/ingest_online_bronze.py)
  * *Description:* Connects to the external MinIO S3 storage, pulls streaming batches, and ingests them into the Bronze Zone (`data/bronze/online/`).
* 🐍 **Online Data Quality Summary Utility:** [`../generators/summarize_online_data.py`](../generators/summarize_online_data.py)
  * *Description:* Inspects streaming batch files and reports duplicate rates, late arrival lag counts, and temporal ranges.

---

### 3. Generated Datasets & Storage Layers
* 🗄️ **External S3 Source Bucket:** MinIO Bucket `external-streaming-source` (`http://localhost:9000`)
* 📊 **Bronze Zone Stream Lake Storage:** [`../data/bronze/online/`](../data/bronze/online/)

---

### 4. Data Quality Validation & Metrics Evidence

Terminal execution output verifying online data stream anomaly injections:

![Online Data Quality Summary](images/online_data_quality_summary.png)

#### Summary of Achieved Metrics:
* **Burst Traffic:** Automatically triggers traffic spikes based on configured probability and multipliers.
* **Late Arrivals:** Captures time lags between event timestamps and real-time ingestion timestamps.
* **Duplicate Rate:** Verified at ~1.5% due to API retry simulations.

---

## 🚀 Quick Start Guide

```bash
# 1. Activate virtual environment and install dependencies
.venv\Scripts\Activate  # On Windows PowerShell
pip install -r ../requirements.txt

# 2. Spin up MinIO container via Docker (Simulating external department S3 object storage)
docker run -d --name minio_streaming_source -p 9000:9000 -p 9001:9001 -e "MINIO_ROOT_USER=minioadmin" -e "MINIO_ROOT_PASSWORD=minioadmin" minio/minio server /data --console-address ":9001"

# 3. Run the online stream loader (Simulates external system generating and pushing batches to MinIO S3)
python ../generators/online_db_loader.py

# 4. Ingest streaming batches from the external MinIO S3 bucket into the Bronze Zone
python ../pipelines/ingest_online_bronze.py

# 5. Run the online data quality summary inspection utility
python ../generators/summarize_online_data.py