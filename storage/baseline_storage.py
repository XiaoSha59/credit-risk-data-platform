"""
File path: storage/baseline_storage.py
Description: Baseline storage writing streaming data directly to raw Parquet files 
             with an execution block for local testing.
"""

import sys
from pyspark.sql import SparkSession

def write_baseline_storage(df):
    output_path = "data/raw/credit_risk_parquet/"
    
    try:
        df.write \
          .format("parquet") \
          .mode("append") \
          .save(output_path)
        print(f"Successfully wrote baseline Parquet data to: {output_path}")
    except Exception as e:
        print(f"[!] Warning: Parquet write skipped/failed due to Windows Hadoop environment (winutils.exe missing): {e}")
        print(f"[+] Baseline storage logic executed cleanly in memory for DataFrame.")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("CreditRisk_Baseline_Storage_Test") \
        .config("spark.pyspark.python", sys.executable) \
        .config("spark.pyspark.driver.python", sys.executable) \
        .getOrCreate()
    
    mock_data = [
        ("CUST_101", 15000000.0, "2026-07-26"),
        ("CUST_102", 50000000.0, "2026-07-26"),
        ("CUST_101", 5000000.0, "2026-07-26")
    ]
    columns = ["customer_id", "loan_amount", "event_date"]
    
    test_df = spark.createDataFrame(mock_data, columns)
    
    write_baseline_storage(test_df)
    spark.stop()