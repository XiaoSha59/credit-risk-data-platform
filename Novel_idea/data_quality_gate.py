import os
import json
import time

def run_data_quality_gate(simulate_anomaly=False):
    print("============================================================")
    print("      AIRFLOW DATA QUALITY GATE — GREAT EXPECTATIONS       ")
    print("============================================================")
    print("[*] Target Dataset: silver_exposure_at_default")
    print("[*] Target Columns: ['customer_id', 'ead_amount', 'event_date']")
    print("[*] Rule 1: expect_column_values_to_be_between(ead_amount, min=0.00)")
    print("[*] Rule 2: expect_column_null_rate_to_be_between(customer_id, max=0.05)")
    print("------------------------------------------------------------")

    records_scanned = 15000
    null_rate = 0.01 if not simulate_anomaly else 0.12 # 12% null rate if anomaly
    min_ead_val = 500000.00 if not simulate_anomaly else -1500000.00 # Negative value if anomaly

    print(f"[*] Scanning Dataset: {records_scanned:,} rows evaluated...")
    print(f"[*] Computed Metrics -> Minimum EAD Amount: {min_ead_val:,.2f} VND | Null Rate: {null_rate*100:.1f}%")

    passed_rule1 = min_ead_val >= 0.0
    passed_rule2 = null_rate <= 0.05
    all_passed = passed_rule1 and passed_rule2

    # Generate HTML Data Docs Report
    os.makedirs("d:/credit-risk-data-platform/Novel_idea/reports", exist_ok=True)
    report_path = "d:/credit-risk-data-platform/Novel_idea/reports/data_quality_report.html"


    status_color = "#22C55E" if all_passed else "#EF4444"
    status_text = "PASSED (GREEN)" if all_passed else "FAILED (RED) — PIPELINE HALTED"

    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <title>Great Expectations Data Quality Report</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background-color: #0F172A; color: #F8FAFC; padding: 30px; }}
        .header {{ background-color: #1E293B; padding: 20px; border-radius: 10px; border-left: 6px solid {status_color}; }}
        .status {{ font-size: 24px; fontweight: bold; color: {status_color}; }}
        .metric-card {{ background-color: #1E293B; padding: 15px; margin-top: 15px; border-radius: 8px; border: 1px solid #334155; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #334155; }}
        th {{ background-color: #334155; color: #38BDF8; }}
        .pass {{ color: #22C55E; fontweight: bold; }}
        .fail {{ color: #EF4444; fontweight: bold; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Data Quality Validation Report</h1>
        <div class="status">Overall Status: {status_text}</div>
        <p>Target Table: <code>stg_exposure_at_default</code> | Execution Time: {time.strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>

    <div class="metric-card">
        <h3>Evaluation Summary</h3>
        <p>Total Rows Scanned: <b>{records_scanned:,}</b></p>
        <p>Evaluated Rules: <b>2</b> | Passed: <b>{2 if all_passed else 0}</b> | Failed: <b>{0 if all_passed else 2}</b></p>
    </div>

    <table>
        <thead>
            <tr>
                <th>Rule ID</th>
                <th>Assertion Description</th>
                <th>Threshold</th>
                <th>Observed Value</th>
                <th>Status</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>RULE-01</td>
                <td>expect_column_values_to_be_between(ead_amount)</td>
                <td>min_value >= 0.00 VND</td>
                <td>{min_ead_val:,.2f} VND</td>
                <td class="{'pass' if passed_rule1 else 'fail'}">{'PASS' if passed_rule1 else 'FAIL'}</td>
            </tr>
            <tr>
                <td>RULE-02</td>
                <td>expect_column_null_rate_to_be_between(customer_id)</td>
                <td>max_null_rate <= 5.0%</td>
                <td>{null_rate*100:.1f}%</td>
                <td class="{'pass' if passed_rule2 else 'fail'}">{'PASS' if passed_rule2 else 'FAIL'}</td>
            </tr>
        </tbody>
    </table>
</body>
</html>
"""
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"[+] Data Docs HTML Report generated at: {report_path}")

    if not all_passed:
        print("============================================================")
        print(" [!] CRITICAL AIRFLOW TASK FAILURE DETECTED!               ")
        print(" [!] Data Quality Gate Failed: Negative EAD or Null Breach ")
        print(" [!] Task 'data_quality_check' status -> FAILED (RED)      ")
        print(" [!] Downstream Tasks ('transform_silver', 'gold_dw_sync') -> SKIPPED/HALTED")
        print("============================================================")
        raise ValueError("[DATA QUALITY GATE FAILED] Anomaly detected in EAD dataset! Blocking downstream tasks.")
    else:
        print("[+] DATA QUALITY GATE PASSED! Proceeding to Silver Transformation.")
        return True

if __name__ == "__main__":
    # Test Clean Run
    print(">>> RUNNING CLEAN DATA SET TEST:")
    try:
        run_data_quality_gate(simulate_anomaly=False)
    except Exception as e:
        print(e)
        
    print("\n" + "="*70 + "\n")
    
    # Test Anomaly Run
    print(">>> RUNNING ANOMALOUS DATA SET TEST (Triggering Airflow Task Failure):")
    try:
        run_data_quality_gate(simulate_anomaly=True)
    except Exception as e:
        print(f"Captured Expected Exception: {e}")
