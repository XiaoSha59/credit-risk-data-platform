# 📂 Online Streaming Data Generation - Master Index

This document serves as the master navigation index for the **Online / Streaming Data Generation** module, providing direct links to the streaming configuration, source codes, generated batch files, and execution evidence.

---

## 📑 Table of Contents & File Links

### 1. Configuration & Environment
* ⚙️ **Streaming Generator Configuration (YAML):** [`../configs/online_config.yaml`](../configs/online_config.yaml)
* 📦 **Dependencies Requirements:** [`../requirements-gen.txt`](../requirements-gen.txt)

---

### 2. Source Code Implementation
* 🐍 **Online Stream Generator Script:** [`../generators/online_stream_gen.py`](../generators/online_stream_gen.py)
  * *Description:* Simulates real-time credit transactions, embedding online data quality anomalies such as burst traffic, late-arriving records, and API retry duplicates (1.5%).
* 🐍 **Online Data Quality Summary Utility:** [`../generators/summarize_online_data.py`](../generators/summarize_online_data.py)
  * *Description:* Inspects the latest streaming batch files and reports metrics including duplicate rates, late arrival lag counts, and temporal ranges.

---

### 3. Generated Dataset Output
* 📊 **Streaming Parquet Batch Files:** [`../data/raw/online/`](../data/raw/online/)

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

# 2. Run the online stream data generator batch
python ../generators/online_stream_gen.py

# 3. Run the online data quality summary inspection utility
python ../generators/summarize_online_data.py