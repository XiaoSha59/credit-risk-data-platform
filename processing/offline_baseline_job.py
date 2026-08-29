"""
Module: offline_baseline_job.py
Description: Baseline PySpark job WITHOUT optimizations. 
             Used to demonstrate performance bottlenecks, task skew, 
             and shuffle issues in Spark UI.
"""

import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def run_baseline():
    # Explicitly disable AQE and performance configs to simulate unoptimized baseline
    spark = SparkSession.builder \
        .appName("OfflineBaselineNoOptimization") \
        .config("spark.ui.enabled", "true") \
        .config("spark.ui.port", "4040") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.driver.host", "localhost") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

    ui_url = spark.sparkContext.uiWebUrl or "http://localhost:4040"
    print("[*] Running Baseline Job (WITHOUT optimization)...")
    print(f"[*] Spark UI URL: {ui_url}")
    bronze_path = "data/bronze/offline/raw_loan_portfolio.parquet"
    
    df = spark.read.parquet(bronze_path)
    start_time = time.time()

    # Naive heavy aggregation on skewed columns (branch_city, loan_purpose) 
    # and high-cardinality analysis without broadcast or AQE skew join.
    baseline_agg = df.groupBy("branch_city", "loan_purpose") \
                     .agg(
                         F.count("loan_id").alias("total_loans"),
                         F.sum("loan_amount").alias("total_amount"),
                         F.avg("interest_rate").alias("avg_interest"),
                         F.count("customer_id").alias("exact_customer_count") # High cardinality bottleneck
                     )

    # Force execution action
    row_count = baseline_agg.count()
    duration = time.time() - start_time

    print(f"[!] Baseline execution completed in: {duration:.2f} seconds. Rows: {row_count:,}")

    # --- KEEP SYSTEM ALIVE FOR SPARK UI SCREENSHOTS ---
    print("\n" + "="*60)
    print(f"[!] SPARK UI ACTIVE AT: {ui_url}")
    print("[!] System will keep Spark UI alive.")
    print("[!] Open your browser and capture 'Stages' and 'SQL/DataFrame' tabs.")
    print("[!] Press Ctrl + C when finished to exit.")
    print("="*60)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[!] Ctrl + C received. Stopping SparkSession...")
        spark.stop()

if __name__ == "__main__":
    run_baseline()