"""
Module: run_offline_pipeline.py
Description: Master orchestrator integrating the entire Offline data pipeline.
             Executes sequentially: Ingestion (Bronze) -> Optimized Processing (Silver).
"""

import subprocess
import sys
import time

def run_step(script_path: str, step_name: str):
    """
    Executes a Python script as a subprocess and manages its success/failure state.
    """
    print(f"\n" + "-"*50)
    print(f"[>] STARTING: {step_name}")
    print("-" * 50)
    
    start_time = time.time()
    try:
        # Call the script using the current Python interpreter
        subprocess.run(
            [sys.executable, script_path], 
            check=True,
            capture_output=False # Allow child script logs to print directly to the Terminal
        )
        elapsed = time.time() - start_time
        print(f"[v] COMPLETED: {step_name} (Time: {elapsed:.2f}s)")
        
    except subprocess.CalledProcessError as e:
        print(f"[x] FAILED: {step_name} encountered an error!")
        print("[!] Offline Pipeline has been forcefully stopped to ensure data integrity.")
        sys.exit(1)

def main():
    print("="*60)
    print("[*] INITIATING DATA PIPELINE (OFFLINE BATCH) [*]")
    print("="*60)
    
    total_start = time.time()

    # --- STEP 1: Ingest raw data to Data Lake (Bronze Zone) ---
    run_step(
        script_path="pipelines/ingest_offline_bronze.py", 
        step_name="STEP 1: Ingest Data to Bronze Zone"
    )
    
    # --- STEP 2: Clean, optimize with AQE, and push to Silver Zone ---
    run_step(
        script_path="processing/offline_spark_job.py", 
        step_name="STEP 2: Spark AQE Processing to Silver Zone"
    )

    total_elapsed = time.time() - total_start
    print("\n" + "="*60)
    print(f"[*] FULL PIPELINE COMPLETED IN {total_elapsed:.2f}s [*]")
    print("Silver data is now ready as input for risk measurement models.")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()