# Technical Report: Optimizing Batch Data Pipelines & Orchestration for Credit Risk Analytics

## 1. Overview & Architecture Context
In credit risk data engineering, processing large-scale loan portfolios efficiently is critical for downstream quantitative models, such as Probability of Default (PD), Loss Given Default (LGD), and Exposure at Default (EAD) frameworks. This report outlines the transformation of our offline batch pipeline from an unoptimized baseline prone to severe performance bottlenecks into an enterprise-grade, resilient architecture. This is achieved by leveraging Apache Spark Adaptive Query Execution (AQE), statistical data quality imputation, and robust pipeline orchestration.

---

## 2. Baseline Performance Bottlenecks & Analysis

The baseline pipeline simulates legacy or naive big data processing without modern query optimization[cite: 1]. Running heavy aggregations over millions of loan records reveals three critical structural issues:

### A. Data Skew (The HCMC Regional Bottleneck)
* **The Issue:** In our regional distribution, approximately 80% of loan portfolios and customer transactions are heavily concentrated in Ho Chi Minh City (`branch_city`). 
* **Spark UI Evidence:** When examining the **Stages** tab and expanding the **Event Timeline**, certain tasks take significantly longer to complete, exhibiting long green execution bars, while others finish almost instantaneously. This occurs because Spark assigns default data partitions blindly based on file block sizes, forcing a single worker node to process the overwhelming majority of HCMC data while other cluster workers sit idle.
* **Visual Verification Asset:** `docs/images/...`

### B. High Cardinality Memory Pressure
* **The Issue:** Utilizing exact counting methods, such as standard exact distinct counts on millions of high-cardinality customer identifiers, forces Spark to shuffle massive volumes of string and ID data across the cluster network.
* **Spark UI Evidence:** In the **SQL / DataFrame** DAG execution plan, aggregation nodes display extreme memory consumption, frequently triggering memory and disk spilling when RAM limits are exceeded, severely degrading overall job throughput.

---

## 3. Optimization Strategy & Spark AQE Implementation

To resolve these processing hurdles, the optimized pipeline introduces robust data engineering strategies:

### A. Adaptive Query Execution (AQE) Mechanism
Rather than relying on static execution plans, the pipeline enables runtime query optimization via Spark AQE configurations. 
* **Runtime Statistics Gathering:** AQE dynamically re-optimizes the physical query plan *during* runtime based on precise statistics collected from completed shuffle stages.
* **Skew Join & Aggregation Remediation:** When processing grouped keys with disproportionate volume, AQE detects partitions whose size significantly exceeds the median threshold. It automatically splits these colossal partitions into smaller sub-blocks.
* **Workload Distribution:** Instead of overwhelming a single worker node, the split tasks are distributed evenly across all available cluster resources, eliminating long-tail execution delays.
* **Spark UI Verification:** In the **SQL / DataFrame** execution plan, standard shuffle nodes are replaced by specialized runtime operators such as custom shuffle readers. Furthermore, the **Event Timeline** shows balanced, uniform task execution bars.
* **Visual Verification Asset:** `docs/images` .

### B. High Cardinality Approximation (HyperLogLog)
To eliminate memory spilling during customer aggregation, exact counting is replaced with the HyperLogLog approximation algorithm. This technique reduces memory overhead drastically by utilizing matrix mathematics to estimate unique counts instead of storing raw string identifiers, while maintaining a strict statistical accuracy threshold within a low error rate.

### C. Schema Evolution & Missing Data Imputation
Historical loan portfolios frequently suffer from missing credit bureau scores due to schema evolution and legacy data gaps over time. Rather than failing the processing job or dropping valuable risk records, the pipeline applies a median statistical imputation strategy alongside an audit tracking flag. This preserves portfolio size for downstream machine learning models while clearly flagging interpolated values.

---

## 4. Pipeline Orchestration & Spark Integration Techniques

To transition from isolated script executions to an automated production workflow, specific architectural integration techniques were implemented to bind the Spark job into a unified pipeline.

### Process-Level Encapsulation & Native Subprocess Orchestration
For modular local orchestration, the system utilizes a master orchestrator script implementing **process-level encapsulation via Python native subprocess execution**. 
* **Technique:** Rather than mixing execution contexts, the master script calls the PySpark processing job as an independent child process using safe interpreter bindings. 
* **Engineering Benefits:** This ensures complete memory isolation between orchestration logic and the heavy JVM-based Spark driver. It also provides deterministic exit-code validation, allowing the master pipeline to immediately capture failure states, log execution durations per step, and halt execution cleanly if an anomaly occurs in preceding data ingestion layers.
