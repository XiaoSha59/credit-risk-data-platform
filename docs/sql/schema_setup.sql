/**
 * ==============================================================================
 * File Name: schema_setup.sql
 * File Location: sql/schema_setup.sql
 * Description: Complete data warehouse schema design script for the Credit Risk 
 *              Data Platform across Bronze, Silver, and Gold zones. Implements 
 *              strict naming conventions, SCD Type 2 patterns, feature tables, 
 *              and relational integrity constraints for quantitative risk models (PD, EAD).
 * ==============================================================================
 */

-- Create isolated database schemas for layered data architecture
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ==============================================================================
-- 1. BRONZE & SILVER LAYERS (Naming convention: raw_, stg_)
-- ==============================================================================

/**
 * Table: bronze.raw_loan_events
 * Description: Raw ingestion layer capturing unstructured or semi-structured 
 *              loan event payloads directly from real-time streaming sources (Kafka).
 */
CREATE TABLE bronze.raw_loan_events (
    event_id VARCHAR(50) PRIMARY KEY,
    payload JSON,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

/**
 * Table: silver.stg_exposure_at_default
 * Description: Staging and transformed layer used for data cleansing, standardization, 
 *              and preparing core metrics required for Exposure at Default (EAD) computations.
 */
CREATE TABLE silver.stg_exposure_at_default (
    loan_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    ead_amount DECIMAL(15, 2),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==============================================================================
-- 2. GOLD LAYER (Naming convention: dim_, fact_, feat_, obt_)
-- ==============================================================================

/**
 * Table: gold.dim_customer
 * Description: Customer dimension table implementing Slowing Changing Dimension 
 *              (SCD) Type 2 patterns to maintain complete historical audit trails 
 *              of customer risk attributes over time.
 */
CREATE TABLE gold.dim_customer (
    customer_sk SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(100),
    credit_score INT,
    valid_from_ts TIMESTAMP NOT NULL,
    valid_to_ts TIMESTAMP,
    is_current BOOLEAN NOT NULL
);

/**
 * Table: gold.fact_loan_transactions
 * Description: Core transactional fact table linked via surrogate keys to the 
 *              customer dimension table to support multidimensional analysis.
 */
CREATE TABLE gold.fact_loan_transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    customer_sk INT REFERENCES gold.dim_customer(customer_sk),
    loan_amount DECIMAL(15, 2),
    transaction_date TIMESTAMP
);

/**
 * Table: gold.feat_pd_matrix
 * Description: Analytical feature table optimized for machine learning models 
 *              calculating Probability of Default (PD), containing mandatory event 
 *              and record creation timestamp columns.
 */
CREATE TABLE gold.feat_pd_matrix (
    feature_id SERIAL PRIMARY KEY,
    customer_sk INT REFERENCES gold.dim_customer(customer_sk),
    pd_probability DECIMAL(5, 4),
    event_timestamp TIMESTAMP NOT NULL,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

/**
 * Table: gold.obt_customer_risk_profile
 * Description: Denormalized One Big Table (OBT) combining multi-domain indicators 
 *              to streamline business intelligence dashboards and executive reporting.
 */
CREATE TABLE gold.obt_customer_risk_profile (
    customer_sk INT,
    total_exposure DECIMAL(15,2),
    default_probability DECIMAL(5,4),
    snapshot_date DATE
);