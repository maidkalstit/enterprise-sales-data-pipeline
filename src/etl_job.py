"""
Batch ETL — thực thi Medallion đúng nghĩa: Bronze → Silver → Gold.

Khác bản cũ (đọc CSV → ghi thẳng gold bằng mode="overwrite"):
  1. Đọc dữ liệu ĐÃ TÍCH LŨY từ raw_sales_events (Bronze) — điểm hội tụ của cả
     stream lẫn CSV landing, hết cảnh "Gold chỉ còn snapshot 10 phút".
  2. Ghi bản ghi sạch vào Silver clean_sales_events bằng append-các-dòng-mới
     (left_anti-join theo order_id) — idempotent, chạy lại không nhân đôi.
  3. Gold được TÍNH LẠI từ Silver, ghi vào bảng staging rồi atomic swap trong
     một transaction — Primary Key được bảo toàn, Metabase không bao giờ thấy
     bảng rỗng lửng (mode="overwrite" cũ từng DROP table và xé mất PK).
"""
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from db_utils import atomic_swap_gold, load_db_config
from notify import send_env_bot
from transforms import aggregate_gold_daily, split_quality

cfg = load_db_config()
db_url = cfg["DB_URL"]
jdbc_props = {"user": cfg["DB_USER"], "password": cfg["DB_PASS"], "driver": cfg["DB_DRIVER"]}

spark = SparkSession.builder \
    .appName("Sales_Batch_ETL_Medallion") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Schema thống nhất với payload JSON đang persist ở Bronze
event_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("order_date", StringType(), True),
])

bronze_count = new_count = err_count = gold_rows = 0

try:
    # ------------------------------------------------------------------
    # 1. BRONZE — nguồn tích lũy, bất biến
    # ------------------------------------------------------------------
    print("📥 [1/5] Đọc dữ liệu thô tích lũy từ Bronze (raw_sales_events)...")
    bronze_df = spark.read.jdbc(url=db_url, table="raw_sales_events", properties=jdbc_props)
    bronze_count = bronze_df.count()

    if bronze_count == 0:
        print("ℹ️ Bronze rỗng (chưa bơm dữ liệu) — giữ nguyên Gold, kết thúc lượt chạy.")
    else:
        parsed_df = (
            bronze_df.select(F.from_json(F.col("payload"), event_schema).alias("data"))
            .select("data.*")
        )
        clean_df, error_df = split_quality(parsed_df, source_type="Batch")
        clean_df.cache()
        error_df.cache()

        # --------------------------------------------------------------
        # 2. SILVER — chỉ append những order_id chưa tồn tại (idempotent)
        # --------------------------------------------------------------
        print("🧹 [2/5] Silver: tách bản ghi sạch và ghi bổ sung các đơn chưa có...")
        existing_ids = (
            spark.read.jdbc(url=db_url, table="clean_sales_events", properties=jdbc_props)
            .select("order_id")
        )
        new_clean = (
            clean_df.dropDuplicates(["order_id"])
            .join(existing_ids, on="order_id", how="left_anti")
        )
        new_count = new_clean.count()
        if new_count > 0:
            (
                new_clean.select("order_id", "customer_id", "product_id", "amount", "order_date")
                .write.jdbc(url=db_url, table="clean_sales_events", mode="append", properties=jdbc_props)
            )
        print(f"🥈 Silver: đã append {new_count} dòng sạch mới (khóa order_id).")

        # --------------------------------------------------------------
        # 3. DLQ — cô lập và tích lũy dòng lỗi phục vụ hậu kiểm
        # --------------------------------------------------------------
        err_count = error_df.count()
        if err_count > 0:
            (
                error_df.withColumn("created_at", F.current_timestamp())
                .select(
                    "order_id", "customer_id", "product_id", "amount",
                    "order_date", "error_reason", "source_type", "created_at",
                )
                .write.jdbc(url=db_url, table="error_logs", mode="append", properties=jdbc_props)
            )
        print(f"⚠️ DLQ: đã ghi {err_count} dòng dị thường vào error_logs.")

        # --------------------------------------------------------------
        # 4. GOLD — tính lại từ Silver (bao gồm cả dòng đã recovery), atomic swap
        # --------------------------------------------------------------
        print("📊 [4/5] Gold: tính lại (report_date, product) từ Silver rồi atomic swap...")
        silver_df = spark.read.jdbc(url=db_url, table="clean_sales_events", properties=jdbc_props)
        gold_agg = aggregate_gold_daily(silver_df)
        gold_rows = gold_agg.count()
        if gold_rows > 0:
            gold_agg.write.jdbc(
                url=db_url, table="gold_batch_revenue_staging",
                mode="overwrite", properties=jdbc_props,
            )
            atomic_swap_gold(cfg)
            print(f"✅ Gold: atomic swap hoàn tất với {gold_rows} dòng (ngày, sản phẩm) — PK nguyên vẹn.")
        else:
            print("ℹ️ Silver rỗng sau lọc — bỏ qua swap để không xóa trắng Gold.")

        # --------------------------------------------------------------
        # 5. BÁO CÁO — post-commit notification (trước đây báo trước ETL, sai story)
        # --------------------------------------------------------------
        report = (
            "🚀 *[BATCH ETL REPORT]*\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🥇 Bronze: `{bronze_count}` dòng thô tích lũy\n"
            f"🥈 Silver: `+{new_count}` dòng sạch mới\n"
            f"⚠️ DLQ: `+{err_count}` dị thường\n"
            f"📊 Gold: `{gold_rows}` dòng (ngày, sản phẩm) — atomic swap OK\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "👤 Engineer: `Tung Dang`"
        )
        send_env_bot("BATCH", report)
        print("📨 Đã gửi báo cáo Telegram (BATCH bot).")

except Exception as exc:
    print(f"💥 Lỗi nghiêm trọng phát sinh trong tiến trình Batch ETL: {exc}")

finally:
    spark.stop()
