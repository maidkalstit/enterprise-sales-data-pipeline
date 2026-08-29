"""
DLQ Recovery Job — quét error_logs theo VÒNG ĐỜI (cột status), hết thời DELETE mù
quáng theo error_reason (cách cũ có race: dòng lỗi mới lọt giữa lúc đọc và lúc
xóa sẽ biến mất mà không được khôi phục).

Quy trình:
  1. Đọc các dòng status = 'unprocessed' AND error_reason = 'Invalid Amount'.
  2. Amount GIỮ NGUYÊN DẤU (âm = refund/hoàn tiền — nghiệp vụ hợp lệ), UPSERT vào
     Silver clean_sales_events. (Bản cũ ép về 0 gọi là "đơn khuyến mãi" — làm
     méo doanh thu thực.)
  3. Tính lại cửa sổ 1 phút × product và UPSERT vào gold_minute_revenue.
  4. Đánh dấu 'processed' cho đúng bộ id đã đọc — lần chạy sau không đụng lại.

Dòng 'Missing Customer ID' / 'Missing Order ID' ở lại 'unprocessed' chờ con người
— DLQ là hàng đợi công việc, không phải bãi rác vô định.
"""
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from db_utils import load_db_config, mark_errors_processed, upsert_gold_minute_rows, upsert_silver_rows
from transforms import aggregate_gold_minute

cfg = load_db_config()
db_url = cfg["DB_URL"]
jdbc_props = {"user": cfg["DB_USER"], "password": cfg["DB_PASS"], "driver": cfg["DB_DRIVER"]}

spark = SparkSession.builder \
    .appName("DLQ_Recovery_Job") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

try:
    print("🔍 Quét DLQ: status = 'unprocessed' AND error_reason = 'Invalid Amount'...")
    recovered = (
        spark.read.jdbc(url=db_url, table="error_logs", properties=jdbc_props)
        .filter(
            (F.col("status") == "unprocessed")
            & (F.col("error_reason") == "Invalid Amount")
            & F.col("order_id").isNotNull()
            & (F.col("order_id") != "")
        )
        .withColumn("order_date", F.to_timestamp(F.col("order_date")))
        .select("id", "order_id", "customer_id", "product_id", "amount", "order_date")
    )
    rows = [row.asDict() for row in recovered.collect()]

    if not rows:
        print("✨ Không có gì để khôi phục — DLQ sạch hoặc chỉ còn lỗi cần con người duyệt.")
    else:
        # 2. SILVER — amount giữ nguyên dấu: âm tiền là hoàn tiền, cộng vào tổng đúng ngữ nghĩa
        silver_rows = [{k: v for k, v in row.items() if k != "id"} for row in rows]
        upsert_silver_rows(cfg, silver_rows)
        print(f"🥈 Silver: UPSERT {len(silver_rows)} dòng đã khôi phục (giữ dấu gốc = refund).")

        # 3. GOLD-MINUTE — cập nhật view vận tốc cho Metabase nhìn thấy ngay
        gold_rows = [row.asDict() for row in aggregate_gold_minute(recovered).collect()]
        upsert_gold_minute_rows(cfg, gold_rows)
        print(f"🥇 Gold-minute: UPSERT {len(gold_rows)} dòng (cửa sổ, product).")

        # 4. ĐÓNG VÒNG ĐỜI — chỉ đúng bộ id đã đọc, hết race condition
        ids = [row["id"] for row in rows]
        mark_errors_processed(cfg, ids)
        print(f"🗑️ Vòng đời hoàn tất: đã đánh dấu 'processed' cho {len(ids)} dòng DLQ.")

except Exception as exc:
    print(f"💥 Lỗi phát sinh trong quá trình vận hành Recovery Job: {exc}")

finally:
    spark.stop()
