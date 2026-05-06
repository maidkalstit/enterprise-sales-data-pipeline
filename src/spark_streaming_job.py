import os
import requests
import logging
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum, to_timestamp, expr, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# ==============================================================================
# 1. THIẾT LẬP MÔI TRƯỜNG VÀ HỆ THỐNG GHI LOG
# ==============================================================================
load_dotenv(dotenv_path="/opt/spark/.env")

for folder in ["/opt/spark/logs", "/opt/spark/checkpoints"]:
    os.makedirs(folder, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/opt/spark/logs/spark_streaming.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("RealTime-Streaming-Engine")

# ==============================================================================
# 2. KHỞI TẠO PHIÊN LÀM VIỆC SPARK (SPARK SESSION OPTIMIZATION)
# ==============================================================================
spark = SparkSession.builder \
    .appName("Sales_RealTime_Processor_V12") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.2") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# ==============================================================================
# 3. ĐỊNH NGHĨA CẤU TRÚC SCHEMA VÀ TIẾP NHẬN DỮ LIỆU THÔ (BRONZE LAYER)
# ==============================================================================
schema = StructType([
    StructField("order_id", StringType(), True), 
    StructField("customer_id", StringType(), True), 
    StructField("product_id", StringType(), True), 
    StructField("amount", DoubleType(), True),
    StructField("order_date", StringType(), True)
])

kafka_server = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
topic = os.getenv("KAFKA_TOPIC", "sales_topic")

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

parsed_df = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*")

# ==============================================================================
# 4. KIỂM SOÁT CHẤT LƯỢNG VÀ PHÂN TÁCH LUỒNG DỮ LIỆU (SILVER LAYER & DLQ)
# ==============================================================================
# Tầng Silver: Dữ liệu sạch vượt qua các điều kiện kiểm thử logic
clean_stream = parsed_df.filter(
    col("customer_id").isNotNull() & (col("amount") > 0) & (col("order_id") != "")
).withColumn("event_time", to_timestamp(col("order_date"))) \
 .withWatermark("event_time", "1 minute")

# Tầng Dead Letter Queue (DLQ): Cô lập dữ liệu lỗi và cấu trúc trường dữ liệu
error_stream = parsed_df.filter(
    col("customer_id").isNull() | (col("amount") <= 0) | (col("order_id") == "")
).withColumn("error_reason", expr("""
    CASE 
        WHEN customer_id IS NULL THEN 'Missing Customer ID'
        WHEN amount <= 0 THEN 'Invalid Amount'
        WHEN order_id = '' THEN 'Missing Order ID'
        ELSE 'Schema Mismatch Exception'
    END
""")).withColumn("source_type", lit("Streaming")) \
   .withColumn("order_date", to_timestamp(col("order_date"))) \
   .withColumn("created_at", current_timestamp())

# ==============================================================================
# 5. KỸ THUẬT GỘP LÔ VÀ PHÁT TIN CẢNH BÁO TỐI ƯU (BATCHING ALERTS)
# ==============================================================================
def send_batched_telegram_alert(batch_df, batch_id):
    if batch_df.isEmpty(): 
        return
        
    token = str(os.getenv("STREAMING_BOT_TOKEN")).strip()
    chat_id = str(os.getenv("STREAMING_CHAT_ID")).strip()
    tz_vn = timezone(timedelta(hours=7))
    now_vn = datetime.now(tz_vn).strftime('%Y-%m-%d %H:%M:%S')
    
    records = batch_df.collect()
    
    message_lines = [
        f"[SYSTEM ALERT - HIGH VALUE TRANSACTIONS]",
        f"----------------------------------------",
        f"Micro-batch ID : {batch_id}",
        f"Thời gian      : {now_vn} (ICT)",
        f"Số lượng đơn   : {len(records)} giao dịch",
        f"----------------------------------------"
    ]
    
    for row in records[:10]:
        message_lines.append(f"• Mã đơn: {row['order_id']} | SP: {row['product_id']} | Giá trị: ${row['amount']:,}")
    
    if len(records) > 10:
        message_lines.append(f"• ... và {len(records) - 10} giao dịch giá trị lớn khác được nén trong lô.")
        
    message_lines.append("----------------------------------------")
    payload_message = "\n".join(message_lines)
    
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        res = requests.post(url, json={"chat_id": chat_id, "text": payload_message}, timeout=10)
        if res.status_code == 200:
            logger.info(f"✅ Đã phát thành công thông báo tổng hợp lô cho Micro-batch ID: {batch_id}")
        else:
            logger.error(f"❌ Telegram API từ chối Payload: {res.status_code} - {res.text}")
    except Exception as e:
        logger.error(f"💥 Lỗi mạng hệ thống khi truyền dữ liệu sang cổng API Telegram: {e}")

# ==============================================================================
# 6. ĐỒNG BỘ CÁC PHƯƠNG THỨC GHI DỮ LIỆU XUỐNG POSTGRESQL (GOLD & DLQ)
# ==============================================================================
def save_gold_to_db(batch_df, batch_id):
    if batch_df.isEmpty(): return
    db_url = os.getenv("DB_URL")
    props = {"user": os.getenv("DB_USER"), "password": os.getenv("DB_PASS"), "driver": os.getenv("DB_DRIVER")}
    batch_df.select("window.start", "window.end", "total_revenue") \
            .write.jdbc(url=db_url, table="gold_minute_revenue", mode="append", properties=props)

def save_errors_to_db(batch_df, batch_id):
    if batch_df.isEmpty(): return
    db_url = os.getenv("DB_URL")
    props = {"user": os.getenv("DB_USER"), "password": os.getenv("DB_PASS"), "driver": os.getenv("DB_DRIVER")}
    
    # 🔥 ĐỒNG BỘ TRƯỜNG DỮ LIỆU: Chỉ định tường minh các cột khớp 100% cấu hình bảng Postgres mới
    batch_df.select("order_id", "customer_id", "product_id", "amount", "order_date", "error_reason", "source_type", "created_at") \
            .write.jdbc(url=db_url, table="error_logs", mode="append", properties=props)
    logger.info(f"💾 Đã thực thi lưu trữ thành công {batch_df.count()} bản ghi dị thường vào bảng error_logs (DLQ).")

# ==============================================================================
# 7. KHỞI CHẠY ĐỒNG THỜI CÁC TRUY VẤN STREAMING (STREAMING QUERIES EXECUTION)
# ==============================================================================
# Mạch 1: Giám sát giao dịch giá trị cao phát tin Telegram Alert
alert_query = clean_stream.filter(col("amount") > 1000) \
    .writeStream \
    .foreachBatch(send_batched_telegram_alert) \
    .option("checkpointLocation", "/opt/spark/checkpoints/vip_alerts_v12") \
    .start()

# Mạch 2: Tổng hợp dữ liệu chỉ số doanh thu thời gian thực đưa vào Postgres (Gold Layer)
gold_analytics = clean_stream.groupBy(window(col("event_time"), "1 minute")) \
    .agg(sum("amount").alias("total_revenue"))

db_query = gold_analytics.writeStream \
    .foreachBatch(save_gold_to_db) \
    .option("checkpointLocation", "/opt/spark/checkpoints/gold_db_v12") \
    .start()

# Mạch 3: Trích xuất và cô lập rác dữ liệu đưa vào kho lưu trữ lỗi tập trung (DLQ Storage)
error_query = error_stream.writeStream \
    .foreachBatch(save_errors_to_db) \
    .option("checkpointLocation", "/opt/spark/checkpoints/error_logging_v12") \
    .start()

spark.streams.awaitAnyTermination()