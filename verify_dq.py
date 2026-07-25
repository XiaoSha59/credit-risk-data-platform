"""
Script: verify_dq.py
Mục đích: In ra các minh chứng Data Quality để chụp ảnh làm báo cáo.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def run_profiling():
    spark = SparkSession.builder \
        .appName("DataQualityProfiler") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
        
    df = spark.read.parquet("data/bronze/offline/raw_loan_portfolio.parquet")

    print("\n" + "="*60)
    print(" 🔍 BÁO CÁO KIỂM ĐỊNH CHẤT LƯỢNG DỮ LIỆU (BRONZE ZONE) 🔍")
    print("="*60)

    total_rows = df.count()

    # --- 1. MINH CHỨNG TRÙNG LẶP (DUPLICATES) ---
    distinct_rows = df.dropDuplicates().count()
    duplicates_count = total_rows - distinct_rows
    print(f"\n[1] LỖI TRÙNG LẶP DỮ LIỆU (DUPLICATES):")
    print(f"    - Tổng số dòng hiện có : {total_rows:,}")
    print(f"    - Số dòng độc nhất     : {distinct_rows:,}")
    print(f"    => Phát hiện {duplicates_count:,} dòng bị trùng lặp cần loại bỏ.")

    # --- 2. MINH CHỨNG SCHEMA EVOLUTION (MISSING SCORES) ---
    null_scores = df.filter(F.col("credit_bureau_score").isNull()).count()
    null_pct = (null_scores / total_rows) * 100
    print(f"\n[2] LỖI SCHEMA EVOLUTION (THIẾU ĐIỂM TÍN DỤNG):")
    print(f"    - Số khoản vay bị khuyết điểm tín dụng: {null_scores:,} dòng")
    print(f"    => Chiếm {null_pct:.2f}% toàn bộ danh mục. Cần nội suy (Imputation) ")
    print(f"       để không làm gián đoạn mô hình rủi ro tín dụng.")

    # --- 3. MINH CHỨNG HIGH CARDINALITY ---
    # Dùng thuật toán xấp xỉ HyperLogLog để đếm cực nhanh
    unique_customers = df.select(F.approx_count_distinct("customer_id")).collect()[0][0]
    print(f"\n[3] VẤN ĐỀ HIGH CARDINALITY (ĐỘ PHÂN TÁN CAO):")
    print(f"    - Ước lượng số lượng khách hàng độc nhất: ~{unique_customers:,}")
    print(f"    => Cột customer_id có số lượng giá trị duy nhất quá lớn,")
    print(f"       sẽ gây tràn RAM (OOM) nếu GroupBy theo cách truyền thống.")

    print("\n" + "="*60 + "\n")
    spark.stop()

if __name__ == "__main__":
    import logging
    logging.getLogger("py4j").setLevel(logging.ERROR) # Tắt bớt log rác của Spark
    run_profiling()