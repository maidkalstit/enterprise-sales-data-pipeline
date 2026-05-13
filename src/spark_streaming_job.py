import os
import requests
import logging
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum, to_timestamp, expr, lit, current_timestamp, trim
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
    .appName("Sales_RealTime_Processor_V13") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.2") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# ==============================================================================
# 3. ĐỊNH NGHĨA SCHEMA VÀ TIẾP NHẬN DỮ LIỆU THÔ (BRONZE LAYER)
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
# 4. KIỂM SOÁT CHẤT LƯỢNG VÀ BẮT LỖI (DLQ ROUTING)
# ==============================================================================
# Tầng Dead Letter Queue (DLQ): Cô lập dữ liệu lỗi ngay lập tức
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

# Tầng Silver thô: Các bản ghi sạch chuẩn bị mang đi Enrich
silver_raw_stream = parsed_df.filter(
    col("customer_id").isNotNull() & (col("amount") > 0) & (col("order_id") != "")
).withColumn("event_time", to_timestamp(col("order_date"))) \
 .withWatermark("event_time", "1 minute") \
 .withColumn("product_id", trim(col("product_id")))
# ==============================================================================
# 5. TÍCH HỢP BẢNG METADATA TĨNH (STREAM-STATIC JOIN ENRICHMENT)
# ==============================================================================
# 🔥 Nạp danh mục sản phẩm tĩnh từ Local Disk làm Bảng Lookup
product_metadata_path = "/opt/spark/data/product_info.csv"

# Khai báo schema chuẩn cho Metadata tránh Spark suy luận tốn thời gian
product_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True)
])

try:
    product_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(product_schema) \
        .load(product_metadata_path) \
        .withColumn("product_id", trim(col("product_id")))  # 🔥 Gọt sạch khoảng trắng file CSV
    logger.info("📦 Đã nạp và chuẩn hóa thành công Static Metadata.")
except Exception as e:
    logger.error(f"💥 Lỗi đọc tệp product_info.csv: {e}")
    raise e
# 🔥 Thực thi Lookup Enrichment: Nối Stream động với Bảng tĩnh
# Lưu ý: Trong Stream-Static Join, Stream bắt buộc phải nằm bên trái (Left)
clean_enriched_stream = silver_raw_stream.join(
    product_df, 
    on="product_id", 
    how="left"
)

# ==============================================================================
# 6. KỸ THUẬT GỘP LÔ VÀ PHÁT TIN CẢNH BÁO TỐI ƯU (BATCHING ALERTS)
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
    
    # 🔥 Bản tin Telegram xịn hơn vì đã có product_name từ bước Join
    for row in records[:15]:
        p_name = row['product_name'] if row['product_name'] else "Unknown Product"
        message_lines.append(f"• Đơn: {row['order_id']} | SP: {p_name} | Tiền: ${row['amount']:,}")
    
    if len(records) > 15:
        message_lines.append(f"• ... và {len(records) - 15} giao dịch VIP khác trong lô.")
        
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
# 7. ĐỒNG BỘ CÁC PHƯƠNG THỨC GHI DỮ LIỆU XUỐNG POSTGRESQL (GOLD & DLQ)
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
    
    batch_df.select("order_id", "customer_id", "product_id", "amount", "order_date", "error_reason", "source_type", "created_at") \
            .write.jdbc(url=db_url, table="error_logs", mode="append", properties=props)
    logger.info(f"💾 Đã thực thi lưu trữ thành công {batch_df.count()} bản ghi dị thường vào bảng error_logs (DLQ).")

# ==============================================================================
# 8. KHỞI CHẠY ĐỒNG THỜI CÁC TRUY VẤN STREAMING (STREAMING QUERIES EXECUTION)
# ==============================================================================
# Mạch 1: Giám sát giao dịch giá trị cao phát tin Telegram Alert (Sử dụng stream đã Enrich)
alert_query = clean_enriched_stream.filter(col("amount") > 1000) \
    .writeStream \
    .foreachBatch(send_batched_telegram_alert) \
    .option("checkpointLocation", "/opt/spark/checkpoints/vip_alerts_v13") \
    .start()

# Mạch 2: Tổng hợp doanh thu thời gian thực đưa vào Postgres (Sử dụng stream đã Enrich)
gold_analytics = clean_enriched_stream.groupBy(window(col("event_time"), "1 minute")) \
    .agg(sum("amount").alias("total_revenue"))

db_query = gold_analytics.writeStream \
    .foreachBatch(save_gold_to_db) \
    .option("checkpointLocation", "/opt/spark/checkpoints/gold_db_v13") \
    .start()

# Mạch 3: Trích xuất và cô lập rác dữ liệu đưa vào kho lưu trữ lỗi tập trung (DLQ Storage)
error_query = error_stream.writeStream \
    .foreachBatch(save_errors_to_db) \
    .option("checkpointLocation", "/opt/spark/checkpoints/error_logging_v13") \
    .start()

spark.streams.awaitAnyTermination()