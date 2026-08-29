## SECTION 3: STORAGE BASELINE VS OPTIMIZED LAKEHOUSE COMPARISON

### 3.1 Baseline Storage (`storage/baseline_storage.py`)

- **Storage Format:** Raw Parquet (`data/raw/credit_risk_parquet/`).
- **Write Method:** `append`.
- **Characteristics & Limitations:**
  1. **Creates Small Files (Small File Problem):** Each micro-batch or stream append writes a small Parquet file. Over time, the rapidly growing number of small files severely degrades query performance (Small File Problem).
  2. **Unpartitioned:** Data is stored in a single directory. Date-filtered queries must perform a Full Table Scan.
  3. **No ACID Transaction Support:** Concurrent reads/writes can lead to inconsistent or incomplete data state.

---

### 3.2 Optimized Lakehouse (`storage/optimized_lakehouse.py`)

- **Storage Format:** Delta Lake (`data/lakehouse/credit_risk_delta/`).
- **Optimization Methods & Techniques:**
  1. **Partitioning by `event_date` (`.partitionBy("event_date")`):**
     - Enables Spark to trigger **Partition Pruning** — reading only the target date directory and pruning 90%+ of redundant data.
  2. **Compaction (`deltaTable.optimize().executeCompaction()`):**
     - Automatically compacts thousands of small Parquet files into standardized larger files (typically 128MB - 1GB), completely resolving the Small File Problem.
  3. **Z-Order Clustering (`deltaTable.optimize().executeZOrderBy("customer_id")`):**
     - Re-organizes data using multi-dimensional clustering based on `customer_id`.
     - Enables fine-grained **Data Skipping** (file min/max statistics), significantly accelerating query speed when filtering by customer.
  4. **ACID & Time Travel Support:** Ensures data integrity during concurrent read/write operations.

---

### 3.3 Comparison Table: Baseline Storage vs Optimized Storage

| Metric / Feature | Baseline Storage (`baseline_storage.py`) | Optimized Storage (`optimized_lakehouse.py`) |
|---|---|---|
| **Format** | Raw Parquet | Delta Lake |
| **Partitioning** | ❌ No (Unpartitioned) | ✅ Yes (`partitionBy("event_date")`) |
| **Small File Management** | ❌ Suffers from Small File Problem | ✅ Compaction (`executeCompaction()`) |
| **Customer Query Speed** | ❌ Full Table Scan | ✅ Z-Order Clustering (`customer_id`) |
| **Data Integrity (ACID)** | ❌ Unsupported | ✅ ACID Transactions & Time Travel |
