"""
Module: ingest_offline_bronze.py
Description: Ingests offline credit data from the external department's 
             PostgreSQL database into the Bronze Zone (Data Lake).
"""

import os
import pandas as pd
from sqlalchemy import create_engine

def ingest_to_bronze():
    print("[*] Connecting to external department database to pull raw offline data...")
    db_url = "postgresql://postgres:postgres@localhost:5432/core_banking"
    engine = create_engine(db_url)

    # Pull data (Ingestion process)
    query = "SELECT * FROM external_loan_portfolio"
    df = pd.read_sql(query, engine)
    print(f"[+] Successfully pulled {len(df):,} records from external source.")

    # Save to Bronze Zone
    bronze_dir = "data/bronze/offline"
    os.makedirs(bronze_dir, exist_ok=True)
    
    bronze_path = os.path.join(bronze_dir, "raw_loan_portfolio.parquet")
    df.to_parquet(bronze_path, index=False)
    print(f"[+] Ingestion complete! Raw data stored in Bronze Zone at: {bronze_path}")

if __name__ == "__main__":
    ingest_to_bronze()