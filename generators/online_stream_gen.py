"""
Module: online_stream_gen.py
Description: 
    Simulates streaming credit transactions with online data problems:
    - Burst traffic (sudden traffic spikes)
    - Late arriving data (event time vs ingestion time lag)
    - Duplicate records (due to API retries / network overlaps at 1.5%)
    - Externalized configuration via YAML.
"""

import pandas as pd
import numpy as np
import yaml
import os
from datetime import datetime, timedelta

def load_config(config_path="configs/online_config.yaml") -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

class OnlineStreamDataGenerator:
    def __init__(self, config: dict):
        self.config = config

    def generate_stream_batch(self) -> pd.DataFrame:
        base_n = self.config["n_transactions_per_batch"]
        
        # 1. Simulate Burst Traffic
        is_burst = np.random.rand() < self.config["burst_probability"]
        n = int(base_n * self.config["burst_multiplier"]) if is_burst else base_n
        
        if is_burst:
            print(f"[!] BURST TRAFFIC DETECTED! Multiplying batch size to {n:,} transactions.")
        else:
            print(f"[*] Generating standard streaming batch of {n:,} transactions...")

        # Vectorized IDs & amounts
        txn_ids = np.array([f"TXN-{val}" for val in np.random.randint(100000000, 999999999, size=n)])
        customer_ids = np.array([f"CUST-{val}" for val in np.random.randint(100000, 999999, size=n)])
        amounts = np.random.exponential(scale=2000000, size=n).astype(int) + 100000

        # Current ingestion time (simulating real-time arrival)
        now = datetime.now()
        ingestion_times = [now - timedelta(seconds=np.random.randint(0, 60)) for _ in range(n)]

        # 2. Simulate Late Arrivals
        event_times = []
        late_rate = self.config["late_arrival_rate"]
        for ing_time in ingestion_times:
            if np.random.rand() < late_rate:
                # Late arrival: event happened hours or days ago
                lag_seconds = np.random.randint(3600, 86400 * 3) 
                event_times.append(ing_time - timedelta(seconds=lag_seconds))
            else:
                # On-time arrival
                lag_seconds = np.random.randint(0, 10)
                event_times.append(ing_time - timedelta(seconds=lag_seconds))

        df = pd.DataFrame({
            "transaction_id": txn_ids,
            "customer_id": customer_ids,
            "amount": amounts,
            "event_time": pd.to_datetime(event_times).astype("datetime64[us]"),
            "ingestion_time": pd.to_datetime(ingestion_times).astype("datetime64[us]")
        })

        # 3. Simulate Duplicates (e.g., 1.5% duplicate rate from API retries)
        dup_rate = self.config["duplicate_rate"]
        n_dups = int(n * dup_rate)
        if n_dups > 0:
            print(f"[*] Injecting {n_dups} duplicate records ({dup_rate*100}%)...")
            dups = df.sample(n=n_dups, replace=True)
            df = pd.concat([df, dups], ignore_index=True)

        return df.sample(frac=1.0).reset_index(drop=True)

    def run_stream_simulation(self, output_dir: str):
        os.makedirs(output_dir, exist_ok=True)
        df_stream = self.generate_stream_batch()
        
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(output_dir, f"online_stream_batch_{timestamp_str}.parquet")
        
        df_stream.to_parquet(output_path, index=False)
        print(f"[+] Success! Streaming batch saved to: {output_path}")
        print(f"[+] Total batch records (including duplicates): {len(df_stream):,}")

if __name__ == "__main__":
    config = load_config("configs/online_config.yaml")
    generator = OnlineStreamDataGenerator(config)
    generator.run_stream_simulation("data/raw/online")