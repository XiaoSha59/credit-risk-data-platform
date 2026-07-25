"""
Module: offline_db_loader.py
Description: Simulates storing offline loan portfolio data into an external 
             department's database (PostgreSQL) for subsequent Bronze ingestion.
"""

import pandas as pd
import numpy as np
import yaml
import os
from sqlalchemy import create_engine

def load_config(config_path="configs/offline_config.yaml") -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def push_to_external_department_db():
    config = load_config()
    n = config["n_loans"]
    print(f"[*] Generating offline loan portfolio for external department simulation ({n:,} records)...")

    # Generate base data (same logic as offline_data_gen)
    loan_ids = np.array([f"LN-{val}" for val in np.random.randint(10000000, 99999999, size=n)])
    customer_ids = np.array([f"CUST-{val}" for val in np.random.randint(100000, 999999, size=n)])

    rng = np.random.default_rng()
    start_ts = pd.Timestamp(config["start_date"]).value
    end_ts = pd.Timestamp(config["end_date"]).value
    application_dates = pd.to_datetime(rng.integers(start_ts, end_ts, size=n))

    loan_amounts = np.random.exponential(scale=60000000, size=n).astype(int) + 10000000
    interest_rates = np.round(np.random.uniform(7.5, 24.0, size=n), 2)

    skew_cfg = config["skew_distributions"]
    loan_purposes = np.random.choice(skew_cfg["loan_purpose"]["categories"], size=n, p=skew_cfg["loan_purpose"]["probabilities"])
    branch_cities = np.random.choice(skew_cfg["branch_city"]["categories"], size=n, p=skew_cfg["branch_city"]["probabilities"])

    cutoff_date = pd.Timestamp.today() - pd.Timedelta(days=config["schema_evolution_cutoff_days"])
    scores = np.clip(np.random.normal(680, 75, size=n), 300, 850).round(0)
    scores[application_dates < cutoff_date] = np.nan

    df = pd.DataFrame({
        "loan_id": loan_ids,
        "customer_id": customer_ids,
        "application_date": application_dates,
        "loan_amount": loan_amounts,
        "interest_rate": interest_rates,
        "loan_purpose": loan_purposes,
        "branch_city": branch_cities,
        "credit_bureau_score": scores
    })

    # Inject duplicates (2%)
    dup_rate = config["duplicate_rate"]
    dups = df.sample(n=int(n * dup_rate), replace=True)
    df = pd.concat([df, dups], ignore_index=True).sample(frac=1.0).reset_index(drop=True)

    # Connect to PostgreSQL (External Department DB)
    print("[*] Storing data into external PostgreSQL database (Department: Core Banking)...")
    db_url = "postgresql://postgres:postgres@localhost:5432/core_banking"
    engine = create_engine(db_url)

    # Write to external source table
    df.to_sql("external_loan_portfolio", engine, if_exists="replace", index=False)
    print(f"[+] Success! Stored {len(df):,} records into PostgreSQL table 'external_loan_portfolio'.")

if __name__ == "__main__":
    push_to_external_department_db()