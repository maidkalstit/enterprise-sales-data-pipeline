"""
transforms — logic biến đổi dùng CHUNG giữa Speed Layer (streaming) và
Batch Layer. Đưa ra module riêng để pytest bắn được từng nhánh mà không cần
dựng cả hệ Docker.

Quy tắc chất lượng (thống nhất 2 layer, hết cảnh streaming một rules batch
một rules):
  - customer_id NULL            → 'Missing Customer ID'
  - amount NULL hoặc amount<=0  → 'Invalid Amount'  (NULL được bóc riêng thay vì
                                   biến mất lặng lẽ ở cả 2 tầng như code cũ)
  - order_id NULL hoặc rỗng     → 'Missing Order ID'
  - product_id NULL/rỗng        → 'Missing Product ID' (đảm bảo bản ghi vào
                                   gold_minute_revenue không vi phạm NOT NULL)

Mọi bản ghi ra khỏi hàm này có amount kiểu DECIMAL(12,2): tiền tệ không bao giờ
lưu bằng float — DOUBLE PRECISION làm lệch doanh thu từng xu khi cộng dồn.
"""
import pyspark.sql.functions as F

ERROR_REASON_CASE = """
CASE
    WHEN customer_id IS NULL THEN 'Missing Customer ID'
    WHEN amount <= 0 THEN 'Invalid Amount'
    WHEN order_id = '' THEN 'Missing Order ID'
    WHEN product_id IS NULL OR product_id = '' THEN 'Missing Product ID'
    ELSE 'Schema Mismatch Exception'
END
"""


def split_quality(parsed_df, source_type):
    """
    Tách DataFrame sự kiện đã parse thành (clean_df, error_df):
      - error_df: gắn error_reason + source_type ('Streaming'/'Batch'), ép
        order_date sang TIMESTAMP sẵn để ghi thẳng error_logs.
      - clean_df: chỉ dòng sạch, order_date đã là TIMESTAMP.
    Hàm is_bad viết null-safe: thiếu kiểm tra isNull() trước đây khiến bản ghi
    order_id/amount NULL bị rơi khỏi CẢ hai nhánh (mất dữ liệu không dấu vết).
    """
    normalized = parsed_df.withColumn("product_id", F.trim(F.col("product_id")))
    is_bad = (
        F.col("customer_id").isNull()
        | F.col("amount").isNull()
        | (F.col("amount") <= 0)
        | F.col("order_id").isNull()
        | (F.col("order_id") == "")
        | F.col("product_id").isNull()
        | (F.col("product_id") == "")
    )
    error_df = (
        normalized.filter(is_bad)
        .withColumn("error_reason", F.expr(ERROR_REASON_CASE))
        .withColumn("source_type", F.lit(source_type))
        .withColumn("order_date", F.to_timestamp(F.col("order_date")))
        .withColumn("amount", F.col("amount").cast("decimal(12,2)"))
    )
    clean_df = (
        normalized.filter(~is_bad)
        .withColumn("order_date", F.to_timestamp(F.col("order_date")))
        .withColumn("amount", F.col("amount").cast("decimal(12,2)"))
    )
    return clean_df, error_df


def aggregate_gold_minute(clean_df):
    """
    Tổng hợp doanh thu theo cửa sổ 1 phút × product_id — đúng grain của
    gold_minute_revenue (PK: window_start, product_id). Code cũ gộp thiếu
    product_id nên ghi vào bảng luôn vi phạm NOT NULL / trùng PK.
    """
    return (
        clean_df.groupBy(F.window(F.col("order_date"), "1 minute"), F.col("product_id"))
        .agg(F.sum("amount").alias("total_revenue"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("product_id"),
            F.col("total_revenue").cast("decimal(12,2)"),
        )
    )


def aggregate_gold_daily(clean_df):
    """Tổng hợp doanh thu theo (ngày, sản phẩm) — grain của gold_batch_revenue."""
    return (
        clean_df.groupBy(F.to_date(F.col("order_date")).alias("report_date"), F.col("product_id"))
        .agg(F.sum("amount").cast("decimal(12,2)").alias("total_revenue"))
    )
