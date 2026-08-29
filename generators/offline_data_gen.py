"""
Module: offline_data_gen.py
Description: 
    This module simulates historical credit loan portfolios for the offline data pipeline.
    It injects data quality issues required by the coursework specifications, including:
    - High cardinality (unique loan and customer IDs)
    - Skewed distributions (loan purposes and branch cities)
    - Schema evolution (nulls in older data partitions)
    - Duplicate records (simulating upstream storage/ingestion overlap)
    All generation behaviors are fully controlled via an external YAML configuration file.
"""

import pandas as pd
import numpy as np
import yaml
import os
from faker import Faker

def load_config(config_path="configs/offline_config.yaml") -> dict:
    """
    Loads the generator configuration from a YAML file.
    """
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

class OfflineCreditDataGenerator:
    """
    A high-performance vectorised generator for synthetic offline credit datasets.
    """

    def __init__(self, config: dict):
        """
        Initializes the generator with the provided configuration dictionary.
        
        Args:
            config (dict): Configuration parameters for data generation.
        """
        self.config = config
        self.fake = Faker('vi_VN')

    def generate_base_data(self) -> pd.DataFrame:
        """
        Generates base records featuring high cardinality IDs and vectorized attributes.
        
        Returns:
            pd.DataFrame: Dataframe containing baseline credit records.
        """
        n = self.config["n_loans"]
        print(f"[*] Generating high-cardinality base data for {n:,} loans...")

        # Vectorized high-cardinality IDs
        loan_ids = np.array([f"LN-{val}" for val in np.random.randint(10000000, 99999999, size=n)])
        customer_ids = np.array([f"CUST-{val}" for val in np.random.randint(100000, 999999, size=n)])

        # Vectorized random dates generation using modern generator to prevent int32 overflow
        rng = np.random.default_rng()
        start_ts = pd.Timestamp(self.config["start_date"]).value
        end_ts = pd.Timestamp(self.config["end_date"]).value
        random_timestamps = rng.integers(start_ts, end_ts, size=n)
        application_dates = pd.to_datetime(random_timestamps)

        # Vectorized numerical features (Loan amounts and interest rates)
        loan_amounts = np.random.exponential(scale=60000000, size=n).astype(int) + 10000000
        interest_rates = np.round(np.random.uniform(7.5, 24.0, size=n), 2)

        df = pd.DataFrame({
            "loan_id": loan_ids,
            "customer_id": customer_ids,
            "application_date": application_dates,
            "loan_amount": loan_amounts,
            "interest_rate": interest_rates
        })
        return df

    def inject_skewness(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Injects skewness into categorical variables based on configuration probabilities.
        """
        print("[*] Injecting data skewness...")
        n = len(df)
        skew_cfg = self.config["skew_distributions"]

        # Skew loan purpose
        p_purposes = skew_cfg["loan_purpose"]
        df["loan_purpose"] = np.random.choice(
            p_purposes["categories"], size=n, p=p_purposes["probabilities"]
        )

        # Skew branch city
        p_cities = skew_cfg["branch_city"]
        df["branch_city"] = np.random.choice(
            p_cities["categories"], size=n, p=p_cities["probabilities"]
        )

        return df

    def inject_schema_evolution(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Simulates schema evolution by setting column values to NULL for older partitions.
        """
        print("[*] Simulating schema evolution (introducing partition-based nulls)...")
        cutoff_date = pd.Timestamp.today() - pd.Timedelta(days=self.config["schema_evolution_cutoff_days"])

        scores = np.random.normal(680, 75, size=len(df)).astype(float)
        scores = np.clip(scores, 300, 850).round(0)
        
        # Mask older dates as NaN to simulate schema evolution
        mask_old = df["application_date"] < cutoff_date
        scores[mask_old] = np.nan

        df["credit_bureau_score"] = scores
        return df

    def inject_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Injects duplicate rows based on the configured duplicate rate.
        """
        dup_rate = self.config["duplicate_rate"]
        print(f"[*] Injecting duplicates at a rate of {dup_rate * 100}%...")
        
        n_dups = int(len(df) * dup_rate)
        dups = df.sample(n=n_dups, replace=True)
        
        df_combined = pd.concat([df, dups], ignore_index=True)
        return df_combined.sample(frac=1.0).reset_index(drop=True)

    def run_pipeline(self, output_dir: str):
        """
        Executes the end-to-end offline generation pipeline and exports to parquet.
        """
        os.makedirs(output_dir, exist_ok=True)
        
        df = self.generate_base_data()
        df = self.inject_skewness(df)
        df = self.inject_schema_evolution(df)
        df = self.inject_duplicates(df)

        # Ép kiểu cột datetime về microsecond để tương thích tuyệt đối với pyarrow khi lưu parquet
        df["application_date"] = pd.to_datetime(df["application_date"]).astype("datetime64[us]")

        output_path = os.path.join(output_dir, "offline_loan_portfolio.parquet")
        df.to_parquet(output_path, index=False)
        
        print(f"[+] Success! Offline dataset stored at: {output_path}")
        print(f"[+] Final row count (including duplicates): {len(df):,}")

if __name__ == "__main__":
    config = load_config("configs/offline_config.yaml")
    generator = OfflineCreditDataGenerator(config)
    generator.run_pipeline("data/raw/offline")