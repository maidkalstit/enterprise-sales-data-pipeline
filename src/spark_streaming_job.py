"""
Speed Layer — Spark Structured Streaming, thực thi trọn vẹn Medallion:

  Mạch 0  BRONZE : raw JSON từ Kafka được persist nguyên vẹn vào raw_sales_events
                   (append-only) — trước đây mạch này KHÔNG tồn tại trong code dù
                   README tuyên bố, khiến Bronze là bảng chết.
  Mạch 1  SILVER : bản ghi sạch được UPSERT vào clean_sales_events theo order_id
                   (idempotent — replay offset không nhân đôi dữ liệu).
  Mạch 2  GOLD   : tổng hợp (cửa sổ 1 phút × product_id) UPSERT vào
                   gold_minute_revenue theo đúng PK của bảng.
  Mạch 3  ALERT  : đơn > $1000 gộp tin Telegram theo micro-batch (chống 429).
  Mạch 4  DLQ    : dòng lỗi cô lập vào error_logs kèm lý do.

Checkpoint nằm trên mounted volume; mọi ghi đều idempotent nên mất checkpoint
(replay từ earliest) chỉ tốn công tính lại, không sai số liệu.
"""
import logging
import os
from datetime import datetime, timedelta, timezone

import pyspark.sql.functions as F
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, StringType, StructField, StructType

from db_utils import load_db_config, upsert_gold_minute_rows, upsert_silver_rows
from notify import send_telegram
from transforms import aggregate_gold_minute, split_quality

# ==============================================================================
# 1. MÔI TRƯỜNG VÀ GHI LOG
# ==============================================================================
load_dotenv("/opt/spark/.env")

for folder in ["/opt/spark/logs", "/opt/spark/checkpoints"]:
    os.makedirs(folder, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/opt/spark/logs/spark_streaming.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("RealTime-Streaming-Engine")

# ==============================================================================
# 2. SPARK SESSION
# ==============================================================================
spark = SparkSession.builder \
    .appName("Sales_RealTime_Processor_V14_Medallion") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.2") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

cfg = load_db_config()
db_url = cfg["DB_URL"]
jdbc_props = {"user": cfg["DB_USER"], "password": cfg["DB_PASS"], "driver": cfg["DB_DRIVER"]}

CHECKPOINT_ROOT = "/opt/spark/checkpoints"
TRIGGER = "10 seconds"  # micro-batch 10s — khớp với độ trễ end-to-end ghi trong README

# ==============================================================================
# 3. SCHEMA VÀ TIẾP NHẬN TỪ KAFKA
# ==============================================================================
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("amount", DecimalType(12, 2), True),  # tiền parse thẳng DECIMAL, không qua float
    StructField("order_date", StringType(), True),
])

kafka_server = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
topic = os.getenv("KAFKA_TOPIC", "sales_topic")

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

payload_stream = raw_stream.selectExpr("CAST(value AS STRING) AS payload")
parsed_df = payload_stream.select(F.from_json(F.col("payload"), schema).alias("data")).select("data.*")

# ==============================================================================
# 4. TÁCH CHẤT LƯỢNG DÙNG CHUNG VỚI BATCH LAYER (transforms.split_quality)
# ==============================================================================
clean_stream, error_stream = split_quality(parsed_df, source_type="Streaming")

# ==============================================================================
# 5. STREAM-STATIC JOIN VỚI DANH MỤC SẢN PHẨM
# ==============================================================================
product_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
])

product_metadata_path = "/opt/spark/data/product_info.csv"
try:
    product_df = (
        spark.read.format("csv").option("header", "true").schema(product_schema)
        .load(product_metadata_path)
        .withColumn("product_id", F.trim(F.col("product_id")))
    )
    logger.info("📦 Đã nạp và chuẩn hóa Static Metadata (danh mục idempotent, không trôi tên).")
except Exception as exc:
    logger.error(f"💥 Lỗi đọc tệp product_info.csv: {exc}")
    raise exc

clean_enriched = (
    clean_stream.withWatermark("order_date", "5 minutes")
    .join(product_df, on="product_id", how="left")
)

# ==============================================================================
# 6. CÁC SINK (mỗi micro-batch chạy trên driver, ghi qua db_utils)
# ==============================================================================
SILVER_COLUMNS = ["order_id", "customer_id", "product_id", "amount", "order_date"]


def save_bronze_to_db(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    batch_df.select("payload").write.jdbc(
        url=db_url, table="raw_sales_events", mode="append", properties=jdbc_props
    )
    logger.info(f"🥇 Bronze: đã persist nguyên vẹn micro-batch {batch_id}.")


def save_silver_to_db(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    rows = [row.asDict() for row in batch_df.select(*SILVER_COLUMNS).collect()]
    saved = upsert_silver_rows(cfg, rows)
    logger.info(f"🥈 Silver: UPSERT {saved} dòng sạch (khóa order_id) — batch {batch_id}.")


def save_gold_to_db(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    rows = [row.asDict() for row in batch_df.collect()]
    saved = upsert_gold_minute_rows(cfg, rows)
    logger.info(f"🥇 Gold: UPSERT {saved} dòng (cửa sổ, product) — batch {batch_id}.")


def save_errors_to_db(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    out = (
        batch_df.withColumn("created_at", F.current_timestamp())
        .select(
            "order_id", "customer_id", "product_id", "amount",
            "order_date", "error_reason", "source_type", "created_at",
        )
    )
    count = out.count()
    out.write.jdbc(url=db_url, table="error_logs", mode="append", properties=jdbc_props)
    logger.info(f"💾 DLQ: ghi {count} dòng dị thường vào error_logs — batch {batch_id}.")


def send_batched_telegram_alert(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    token = str(os.getenv("STREAMING_BOT_TOKEN") or "").strip()
    chat_id = str(os.getenv("STREAMING_CHAT_ID") or "").strip()
    tz_vn = timezone(timedelta(hours=7))
    now_vn = datetime.now(tz_vn).strftime("%Y-%m-%d %H:%M:%S")

    records = batch_df.collect()
    message_lines = [
        "[SYSTEM ALERT - HIGH VALUE TRANSACTIONS]",
        "----------------------------------------",
        f"Micro-batch ID : {batch_id}",
        f"Thời gian      : {now_vn} (ICT)",
        f"Số lượng đơn   : {len(records)} giao dịch",
        "----------------------------------------",
    ]
    for row in records[:15]:
        p_name = row["product_name"] if row["product_name"] else "Unknown Product"
        message_lines.append(f"• Đơn: {row['order_id']} | SP: {p_name} | Tiền: ${row['amount']:,}")
    if len(records) > 15:
        message_lines.append(f"• ... và {len(records) - 15} giao dịch VIP khác trong lô.")
    message_lines.append("----------------------------------------")

    if send_telegram(token, chat_id, "\n".join(message_lines)):
        logger.info(f"✅ Đã phát thông báo tổng hợp lô cho micro-batch {batch_id}.")
    else:
        logger.error(f"❌ Không gửi được cảnh báo Telegram cho micro-batch {batch_id}.")


# ==============================================================================
# 7. KHỞI CHẠY 5 MẠCH STREAMING
# ==============================================================================
bronze_query = (
    payload_stream.writeStream
    .foreachBatch(save_bronze_to_db)
    .trigger(processingTime=TRIGGER)
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/bronze_v14")
    .start()
)

silver_query = (
    clean_enriched.writeStream
    .foreachBatch(save_silver_to_db)
    .trigger(processingTime=TRIGGER)
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/silver_v14")
    .start()
)

gold_query = (
    aggregate_gold_minute(clean_enriched).writeStream
    .foreachBatch(save_gold_to_db)
    .trigger(processingTime=TRIGGER)
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/gold_v14")
    .start()
)

alert_query = (
    clean_enriched.filter(F.col("amount") > 1000).writeStream
    .foreachBatch(send_batched_telegram_alert)
    .trigger(processingTime=TRIGGER)
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/vip_alerts_v14")
    .start()
)

error_query = (
    error_stream.writeStream
    .foreachBatch(save_errors_to_db)
    .trigger(processingTime=TRIGGER)
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/dlq_v14")
    .start()
)

spark.streams.awaitAnyTermination()
