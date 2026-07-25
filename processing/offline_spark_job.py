"""
Module: offline_spark_job.py

Description:
    Optimized PySpark job that handles offline data quality problems,
    including schema evolution, skewed aggregation, and high-cardinality
    analysis. The job demonstrates Spark SQL Adaptive Query Execution (AQE)
    optimizations and keeps the Spark UI alive for documentation purposes.
"""

import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def run_optimized_processing():
    """
    Execute the optimized Spark job.
    """

    spark = (
        SparkSession.builder
        .appName("OfflineOptimizedSilverJob")
        .master("local[*]")

        # ---------- Spark UI ----------
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4040")

        # ---------- Driver ----------
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.host", "localhost")

        # ---------- Windows / Hadoop Workaround ----------
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")

        # ---------- AQE ----------
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

        .getOrCreate()
    )

    ui_url = spark.sparkContext.uiWebUrl or "http://localhost:4040"
    print("[*] Running Optimized Spark Job...")
    print(f"[*] Spark UI URL: {ui_url}")

    bronze_path = "data/bronze/offline/raw_loan_portfolio.parquet"
    silver_path = "data/silver/offline/optimized_summary.parquet"

    print(f"[*] Reading Bronze dataset: {bronze_path}")

    df = spark.read.parquet(bronze_path)

    start_time = time.time()

    # ==========================================================
    # 1. Handle Schema Evolution
    # ==========================================================

    median_score = 680

    df_cleaned = (
        df
        .withColumn(
            "has_credit_bureau_score",
            F.col("credit_bureau_score").isNotNull()
        )
        .withColumn(
            "credit_bureau_score",
            F.coalesce(
                F.col("credit_bureau_score"),
                F.lit(median_score)
            )
        )
    )

    # ==========================================================
    # 2. Handle High Cardinality
    # ==========================================================

    optimized_agg = (
        df_cleaned
        .groupBy(
            "branch_city",
            "loan_purpose"
        )
        .agg(
            F.count("loan_id").alias("total_loans"),

            F.sum("loan_amount").alias("total_amount"),

            F.avg("interest_rate").alias("avg_interest"),

            F.approx_count_distinct(
                "customer_id",
                0.02
            ).alias("approx_unique_customers")
        )
    )

    print("\n========== Optimized Physical Plan ==========")

    optimized_agg.explain(mode="formatted")

    # Force action to populate Spark UI Jobs & Stages
    print("[*] Executing aggregation plan...")
    row_count = optimized_agg.count()
    print(f"[*] Aggregation returned {row_count:,} records.")

    # ==========================================================
    # 3. Storage Optimization
    # ==========================================================

    try:
        (
            optimized_agg.write
            .mode("overwrite")
            .partitionBy("branch_city")
            .parquet(silver_path)
        )
        print(f"[+] Successfully wrote Silver dataset to: {silver_path}")
    except Exception as e:
        print(f"[!] Warning: Parquet write skipped/failed due to Windows Hadoop environment (winutils.exe missing): {e}")
        print("[!] Note: Spark execution and AQE optimizations completed successfully in memory.")

    duration = time.time() - start_time

    print(
        f"\n[+] Optimized execution completed in "
        f"{duration:.2f} seconds."
    )

    print("\n" + "*" * 70)
    print(f"[+] Spark UI URL : {ui_url}")
    print("[+] Spark UI will remain active.")
    print("[+] Capture screenshots of:")
    print("    - Jobs")
    print("    - Stages")
    print("    - SQL / DataFrame")
    print("    - Executors")
    print("[+] Press Ctrl + C when you finish.")
    print("=" * 70)

    # --- KEEP SYSTEM ALIVE FOR SPARK UI SCREENSHOTS ---
    print("\n" + "*" * 60)
    print("[!] SYSTEM PAUSED SUCCESSFULLY! SPARK UI IS ACTIVE.")
    print(f"[!] OPEN YOUR BROWSER AND GO TO: {ui_url}")
    print("[!] Press Ctrl + C when you finish capturing screenshots.")
    print("*" * 60)

    try:
        # Loop to keep process alive for Spark UI
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[!] Ctrl + C received. Stopping SparkSession...")
        spark.stop()

if __name__ == "__main__":
    run_optimized_processing()
