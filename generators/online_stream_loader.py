"""
Module: online_stream_loader.py
Description: Simulates streaming credit transactions and stores them into 
             an external object storage (MinIO S3 bucket) representing an 
             external system/department for subsequent Bronze ingestion.
             (Aligned with offline_db_loader nomenclature).
"""

import pandas as pd
import numpy as np
import yaml
import os
import boto3
from datetime import datetime, timedelta

def load_config(config_path="configs/online_config.yaml") -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

class OnlineStreamLoader:
    def __init__(self, config: dict):
        self.config = config
        # Connect to local MinIO (S3-compatible object storage simulating external department source)
        self.s3_client = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin'
        )
        self.bucket_name = "external-streaming-source"
        self._ensure_bucket()

    def _ensure_bucket(self):
        try:
            self.s3_client.create_bucket(Bucket=self.bucket_name)
        except Exception:
            pass

    def generate_and_push_stream(self):
        base_n = self.config["n_transactions_per_batch"]
        is_burst = np.random.rand() < self.config["burst_probability"]
        n = int(base_n * self.config["burst_multiplier"]) if is_burst else base_n
        
        if is_burst:
            print(f"[!] BURST TRAFFIC DETECTED! Multiplying batch size to {n:,} transactions.")
        else:
            print(f"[*] Generating standard streaming batch of {n:,} transactions...")

        # Vectorized generation
        txn_ids = np.array([f"TXN-{val}" for val in np.random.randint(100000000, 999999999, size=n)])
        customer_ids = np.array([f"CUST-{val}" for val in np.random.randint(100000, 999999, size=n)])
        amounts = np.random.exponential(scale=2000000, size=n).astype(int) + 100000

        now = datetime.now()
        ingestion_times = [now - timedelta(seconds=np.random.randint(0, 60)) for _ in range(n)]
        
        event_times = []
        for ing_time in ingestion_times:
            if np.random.rand() < self.config["late_arrival_rate"]:
                event_times.append(ing_time - timedelta(seconds=np.random.randint(3600, 86400)))
            else:
                event_times.append(ing_time - timedelta(seconds=np.random.randint(0, 10)))

        df = pd.DataFrame({
            "transaction_id": txn_ids,
            "customer_id": customer_ids,
            "amount": amounts,
            "event_time": pd.to_datetime(event_times).astype("datetime64[us]"),
            "ingestion_time": pd.to_datetime(ingestion_times).astype("datetime64[us]")
        })

        # Inject Duplicates (e.g., 1.5% API retry duplicate rate)
        dup_rate = self.config["duplicate_rate"]
        n_dups = int(n * dup_rate)
        if n_dups > 0:
            print(f"[*] Injecting {n_dups} duplicate records ({dup_rate*100}%)...")
            dups = df.sample(n=n_dups, replace=True)
            df = pd.concat([df, dups], ignore_index=True)

        df = df.sample(frac=1.0).reset_index(drop=True)

        # Temporary local save before pushing to MinIO S3 bucket
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"stream_batch_{timestamp_str}.parquet"
        local_path = os.path.join("data", "raw", "online")
        os.makedirs(local_path, exist_ok=True)
        local_file_path = os.path.join(local_path, file_name)
        
        df.to_parquet(local_file_path, index=False)

        print(f"[*] Pushing streaming batch to external MinIO bucket ({self.bucket_name})...")
        self.s3_client.upload_file(local_file_path, self.bucket_name, file_name)
        print(f"[+] Success! Batch stored in external MinIO source: {file_name}")

if __name__ == "__main__":
    config = load_config("configs/online_config.yaml")
    loader = OnlineStreamLoader(config)
    loader.generate_and_push_stream()