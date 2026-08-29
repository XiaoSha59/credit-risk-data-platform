# Data Governance Implementation Report
**System:** Credit Risk Data Platform  
**Platform:** Acryl DataHub  

This report outlines the implementation of Data Governance artifacts—specifically Data Lineage, Data Validation, and Data Contracts—across three core data pipelines (DP1, DP2, and DP3) within the Credit Risk Data Platform.

---

## 1. Pipeline DP1: Raw to Bronze Zone
**Objective:** Ingest raw loan events from Kafka into the Bronze storage zone (`bronze_loan_data`).

### 1.1. Lineage Between Pipeline and Tables
The lineage graph illustrates the data flow from the source Kafka topic (`raw_loan_events`) directly into the Hive dataset (`bronze_loan_data`).
> **Screenshot Evidence:**
> ![DP1 Lineage](docs/images/DP1_lineage.png)

### 1.2. Data Validation
Ensures the ingested data passes fundamental quality checks (e.g., schema validation, null checks) before landing in the Bronze zone. The badge indicates a `SUCCESS` status for the assertions.
> **Screenshot Evidence:**
> ![DP1 Validation](docs/images/DP1_validation.png)

### 1.3. Data Contract
An `ACTIVE` contract is established to guarantee the structural integrity of the raw data between producers and consumers.
> **Screenshot Inspect:**
> ![DP1 Data Contract](docs/images/DP1_Datacontract.png)

---

## 2. Pipeline DP2: Bronze to Silver Zone
**Objective:** Cleanse, transform, and standardize Bronze data into the Silver zone (`silver_exposure_at_default`) for operational risk metrics calculation (e.g., Exposure at Default).

### 2.1. Lineage Between Pipeline and Tables
The lineage graph traces the transformation mapping from `bronze_loan_data` to `silver_exposure_at_default`.
> **Screenshot Evidence:**
> ![DP2 Lineage](docs/images/DP2_lineage.png)

### 2.2. Data Validation
Confirms that the transformed data meets specific risk business rules (e.g., EAD numeric range validation).
> **Screenshot Evidence:**
> ![DP2 Validation](docs/images/DP2_validation.png)

### 2.3. Data Contract
An `ACTIVE` contract is enforced to bind the data producer and consumer to the agreed schema for Silver zone analytics.
> **Screenshot Evidence:**
> ![DP2 Data Contract](docs/images/DP2_Datacontract.png)

---

## 3. Pipeline DP3: Silver to Gold Zone (Offline Features)
**Objective:** Aggregate and compute offline features from the Silver zone to generate the final analytical dataset in the Gold zone (`gold_pd_feature_matrix`) used for quantitative risk modeling (e.g., Probability of Default).

### 3.1. Lineage Between Pipeline and Tables
The lineage graph demonstrates the final dependency, linking `silver_exposure_at_default` to the output feature matrix `gold_pd_feature_matrix`.
> **Screenshot Evidence:**
> ![DP3 Lineage](docs/images/DP3_lineage.png)

### 3.2. Data Validation
Asserts the readiness of the analytical variables, guaranteeing zero missing values for critical predictive modeling inputs.
> **Screenshot Evidence:**
> ![DP3 Validation](docs/images/DP3_validation.png)

### 3.3. Data Contract
An `ACTIVE` contract serves as a strict Service Level Agreement (SLA) for data scientists consuming the quantitative feature matrix.
> **Screenshot Evidence:**
> ![DP3 Data Contract](docs/images/DP3_Datacontract.png)