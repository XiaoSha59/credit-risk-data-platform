"""
Module: summarize_offline_data.py
Description: 
    Reads the generated offline dataset and prints data quality metrics 
    (skewness, cardinality, schema evolution nulls, duplicate rates) 
    for documentation and reporting purposes, matching coursework requirements.
"""

import pandas as pd
import os

def summarize_data(file_path: str):
    """
    Loads the offline dataset and outputs comprehensive data quality metrics.
    """
    if not os.path.exists(file_path):
        print(f"[-] Error: File not found at {file_path}. Please run offline_data_gen.py first.")
        return

    print(f"[*] Reading dataset from: {file_path}")
    df = pd.read_parquet(file_path)
    
    total_rows_with_dups = len(df)
    
    # 1. Duplicate rate before/after dedup
    exact_dups = df.duplicated().sum()
    duplicate_rate_before = (exact_dups / total_rows_with_dups) * 100
    
    df_deduped = df.drop_duplicates()
    total_rows_clean = len(df_deduped)
    
    print("\n================ DATA QUALITY SUMMARY REPORT ================")
    print(f"1. DATASET SCALE:")
    print(f"   - Total rows (Before Deduplication): {total_rows_with_dups:,}")
    print(f"   - Total rows (After Deduplication):  {total_rows_clean:,}")
    print(f"   - Duplicate count:                   {exact_dups:,}")
    print(f"   - Duplicate rate:                    {duplicate_rate_before:.2f}%")
    
    # 2. Cardinality: approx_count_distinct by ID
    print("\n2. CARDINALITY METRICS (Distinct Count):")
    print(f"   - Unique Loan IDs (approx_count_distinct):     {df_deduped['loan_id'].nunique():,}")
    print(f"   - Unique Customer IDs (approx_count_distinct): {df_deduped['customer_id'].nunique():,}")
    
    # 3. Skew distribution (category/city %)
    print("\n3. SKEW DISTRIBUTION METRICS (%):")
    print("   --- Loan Purpose Distribution ---")
    print((df_deduped['loan_purpose'].value_counts(normalize=True) * 100).round(2).to_string())
    
    print("\n   --- Branch City Distribution ---")
    print((df_deduped['branch_city'].value_counts(normalize=True) * 100).round(2).to_string())
    
    # 4. Schema evolution: nulls in old partitions
    print("\n4. SCHEMA EVOLUTION (Credit Bureau Score Nulls):")
    null_counts = df_deduped['credit_bureau_score'].isna().sum()
    null_percentage = (null_counts / total_rows_clean) * 100
    print(f"   - Missing/Null values in older partitions: {null_counts:,} ({null_percentage:.2f}%)")
    print("=============================================================")

if __name__ == "__main__":
    summarize_data("data/raw/offline/offline_loan_portfolio.parquet")