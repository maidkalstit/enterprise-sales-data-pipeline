import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr, current_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from dotenv import load_dotenv

# ==============================================================================
# 1. NẠP CẤU HÌNH VÀ KIỂM TRA PHÒNG THỦ (DEFENSIVE CHECK)
# ==============================================================================
load_dotenv(dotenv_path="/opt/spark/.env")

db_url = os.getenv("DB_URL")
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASS")
db_driver = os.getenv("DB_DRIVER")

if not all([db_url, db_user, db_pass, db_driver]):
    print("💥 Lỗi hệ thống: Không thể nạp đầy đủ các biến môi trường cấu hình kết nối.")
    sys.exit(1)

# Khởi tạo Spark Session tích hợp tham số tối ưu hóa phân vùng tính toán
spark = SparkSession.builder \
    .appName("Sales_Batch_ETL_With_Parquet_Optimization") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

props = {"user": db_user, "password": db_pass, "driver": db_driver}

# Định nghĩa cấu trúc Schema chuẩn hóa 5 trường thông tin
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("order_date", StringType(), True)
])

csv_source_path = "/opt/spark/data/sales_data.csv"
parquet_destination_path = "/opt/spark/data/sales_data.parquet"

try:
    # ==============================================================================
    # 2. CÔNG ĐOẠN TỐI ƯU LƯU TRỮ (STORAGE FORMAT OPTIMIZATION)
    # ==============================================================================
    print("📥 Tiến hành đọc dữ liệu thô đầu vào từ tệp tĩnh CSV...")
    raw_csv_df = spark.read.format("csv") \
        .option("header", "true") \
        .schema(schema) \
        .load(csv_source_path)

    print("⚡ Thực thi chuyển đổi định dạng: CSV -> Parquet Columnar (Nén Snappy)...")
    raw_csv_df.write.format("parquet") \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .save(parquet_destination_path)
    print(f"📊 Tệp Parquet tối ưu định dạng cột đã được lưu kho tại: {parquet_destination_path}")

    # ==============================================================================
    # 3. TRÍCH XUẤT VÀ ÁP DỤNG QUY TẮC KIỂM THỬ CHẤT LƯỢNG (TRANSFORMATION)
    # ==============================================================================
    print("🚀 Nạp dữ liệu từ nguồn Parquet đã tối ưu hóa để phân tách chất lượng...")
    optimized_df = spark.read.format("parquet").load(parquet_destination_path)

    # TẦNG SILVER: Lọc các bản ghi sạch đạt chuẩn doanh nghiệp
    clean_batch_df = optimized_df.filter(
        col("customer_id").isNotNull() & (col("amount") > 0) & (col("order_id") != "")
    ).withColumn("order_date", to_timestamp(col("order_date")))

    # TẦNG DEAD LETTER QUEUE (DLQ): Cô lập các bản ghi rác, gắn nhãn nguồn phát 'Batch'
    error_batch_df = optimized_df.filter(
        col("customer_id").isNull() | (col("amount") <= 0) | (col("order_id") == "")
    ).withColumn("error_reason", expr("""
        CASE 
            WHEN customer_id IS NULL THEN 'Missing Customer ID'
            WHEN amount <= 0 THEN 'Invalid Amount'
            WHEN order_id = '' THEN 'Missing Order ID'
            ELSE 'Schema Mismatch Exception'
        END
    """)).withColumn("source_type", lit("Batch")) \
       .withColumn("order_date", to_timestamp(col("order_date"))) \
       .withColumn("created_at", current_timestamp())

    # ==============================================================================
    # 4. GHI DỮ LIỆU ĐÍCH (LOADING LAYER - POSTGRESQL JDBC WRITE)
    # ==============================================================================
    print("💾 Đang đồng bộ và thực thi tác vụ nạp dữ liệu xuống cơ sở dữ liệu...")

    # 🔥 CHIẾN LƯỢC TỐI ƯU IDEMPOTENCY: Sử dụng mode("overwrite") cho bảng doanh thu Batch
    # Giúp hệ thống thoải mái chạy 10 phút một lần mà không bị nhân đôi hoặc sai lệch số liệu
    if clean_batch_df.count() > 0:
        clean_batch_df.write.jdbc(url=db_url, table="gold_batch_revenue", mode="overwrite", properties=props)
        print(f"✅ Thành công: Đã ghi đè cập nhật snapshot {clean_batch_df.count()} bản ghi sạch vào Gold Layer.")

    # Sử dụng mode("append") cho error_logs để tích lũy dữ liệu rác phục vụ hậu kiểm
    if error_batch_df.count() > 0:
        error_batch_df.write.jdbc(url=db_url, table="error_logs", mode="append", properties=props)
        print(f"⚠️ Cảnh báo DLQ: Đã tích lũy thêm {error_batch_df.count()} bản ghi dị thường vào error_logs.")

except Exception as e:
    print(f"💥 Lỗi nghiêm trọng phát sinh trong tiến trình Batch ETL Pipeline: {e}")

finally:
    spark.stop()