"""
Module: summarize_online_data.py
Description: 
    Reads the latest generated streaming batch from the online raw layer 
    and prints data quality metrics (duplicate rates, late arrivals count, 
    burst presence) for reporting and coursework documentation.
"""

import pandas as pd
import os
import glob

def summarize_online_data(input_dir: str = "data/raw/online"):
    """
    Finds the latest online stream batch parquet file and outputs metrics.
    """
    if not os.path.exists(input_dir):
        print(f"[-] Error: Directory not found at {input_dir}. Please run online_stream_gen.py first.")
        return

    # Find all parquet files in the online raw folder
    files = glob.glob(os.path.join(input_dir, "*.parquet"))
    if not files:
        print(f"[-] Error: No streaming parquet files found in {input_dir}.")
        return

    # Pick the latest file based on modification time
    latest_file = max(files, key=os.path.getmtime)
    print(f"[*] Reading latest streaming batch from: {latest_file}")
    
    df = pd.read_parquet(latest_file)
    total_rows = len(df)
    
    # 1. Duplicate analysis
    exact_dups = df.duplicated().sum()
    dup_rate = (exact_dups / total_rows) * 100 if total_rows > 0 else 0
    df_clean = df.drop_duplicates()
    
    # 2. Late arrivals analysis (event_time < ingestion_time significantly)
    # Let's define late arrival as a lag of more than 1 minute (60 seconds)
    time_lag = (df_clean['ingestion_time'] - df_clean['event_time']).dt.total_seconds()
    late_arrivals_count = (time_lag > 60).sum()
    late_arrivals_pct = (late_arrivals_count / len(df_clean)) * 100 if len(df_clean) > 0 else 0

    print("\n================ ONLINE STREAM DATA QUALITY REPORT ================")
    print(f"1. STREAM BATCH SCALE:")
    print(f"   - Target File:                      {os.path.basename(latest_file)}")
    print(f"   - Total rows (Including duplicates): {total_rows:,}")
    print(f"   - Total rows (After deduplication):  {len(df_clean):,}")
    
    print(f"\n2. ONLINE DATA QUALITY PROBLEMS:")
    print(f"   - Duplicate Records Count:          {exact_dups:,} ({dup_rate:.2f}%)")
    print(f"   - Late Arriving Records (> 1 min):  {late_arrivals_count:,} ({late_arrivals_pct:.2f}%)")
    
    print(f"\n3. TEMPORAL METRICS:")
    print(f"   - Earliest Event Time:              {df_clean['event_time'].min()}")
    print(f"   - Latest Event Time:                {df_clean['event_time'].max()}")
    print("===================================================================")

if __name__ == "__main__":
    summarize_online_data()