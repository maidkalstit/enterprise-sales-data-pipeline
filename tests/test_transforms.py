"""
Unit test cho transforms — chạy Spark local session (local[2]), không cần Docker.
Máy dev thiếu JVM thì pytest tự skip; CI (GitHub Actions có Java 17) chạy đủ.
"""
import pytest

pytest.importorskip("pyspark")

import pyspark.sql.functions as F  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    DoubleType, StringType, StructField, StructType,
)

from transforms import aggregate_gold_daily, aggregate_gold_minute, split_quality  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    try:
        session = (
            SparkSession.builder.master("local[2]")
            .appName("transforms-tests")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        session.range(1).count()  # ép khởi động JVM gateway ngay tại đây
    except Exception as exc:
        # Lỗi môi trường cục bộ (vd: đường dẫn Windows chứa dấu ngoặc đơn làm gãy
        # batch script spark-class, hoặc thiếu JVM) — skip thay vì báo test sai.
        # CI (Linux + Java 17 + Python 3.11) vẫn chạy đủ bộ test này.
        pytest.skip(f"PySpark/JVM không khả dụng trên môi trường này: {exc}")
    yield session
    session.stop()


EVENT_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("order_date", StringType(), True),
])


def make_df(spark, rows):
    return spark.createDataFrame(rows, EVENT_SCHEMA)


# ==============================================================================
# split_quality — routing sạch / lỗi
# ==============================================================================
def test_clean_rows_route_to_silver_side(spark):
    clean, error = split_quality(
        make_df(spark, [("ORD-1", "CUST-1", "1", 120.5, "2026-08-29 10:00:00")]),
        source_type="Streaming",
    )
    assert clean.count() == 1
    assert error.count() == 0


def test_amount_is_cast_to_decimal_for_money(spark):
    """Tiền tệ phải là DECIMAL(12,2) — cấm float chạm vào cột amount."""
    clean, error = split_quality(
        make_df(spark, [("ORD-1", "CUST-1", "1", 120.5, "2026-08-29 10:00:00")]),
        source_type="Streaming",
    )
    assert clean.schema["amount"].dataType.simpleString() == "decimal(12,2)"
    assert error.schema["amount"].dataType.simpleString() == "decimal(12,2)"


def test_missing_customer_id_classified(spark):
    _, error = split_quality(
        make_df(spark, [("ORD-1", None, "1", 120.5, "2026-08-29 10:00:00")]),
        source_type="Streaming",
    )
    assert error.select("error_reason").first()[0] == "Missing Customer ID"


def test_negative_amount_classified_with_source_label(spark):
    _, error = split_quality(
        make_df(spark, [("ORD-1", "CUST-1", "1", -99.99, "2026-08-29 10:00:00")]),
        source_type="Batch",
    )
    row = error.select("error_reason", "source_type").first()
    assert row["error_reason"] == "Invalid Amount"
    assert row["source_type"] == "Batch"


def test_empty_order_id_classified(spark):
    _, error = split_quality(
        make_df(spark, [("", "CUST-1", "1", 120.5, "2026-08-29 10:00:00")]),
        source_type="Streaming",
    )
    assert error.select("error_reason").first()[0] == "Missing Order ID"


def test_null_amount_is_captured_not_lost(spark):
    """Regression: bản cũ để amount NULL rơi khỏi CẢ hai nhánh — mất dữ liệu không dấu vết."""
    clean, error = split_quality(
        make_df(spark, [("ORD-1", "CUST-1", "1", None, "2026-08-29 10:00:00")]),
        source_type="Streaming",
    )
    assert clean.count() == 0
    assert error.count() == 1  # phải nằm trong DLQ, không biến mất


def test_null_order_id_is_captured(spark):
    """Regression tương tự cho order_id NULL (toán tử so sánh trả NULL khiến routing rơi rớt)."""
    clean, error = split_quality(
        make_df(spark, [(None, "CUST-1", "1", 120.5, "2026-08-29 10:00:00")]),
        source_type="Streaming",
    )
    assert clean.count() == 0
    assert error.count() == 1


def test_missing_product_id_classified(spark):
    """product_id rỗng phải vào DLQ — nếu lọt vào clean, gold_minute_revenue sẽ vi phạm NOT NULL."""
    _, error = split_quality(
        make_df(spark, [("ORD-1", "CUST-1", "  ", 120.5, "2026-08-29 10:00:00")]),
        source_type="Streaming",
    )
    assert error.select("error_reason").first()[0] == "Missing Product ID"


def test_product_id_trimmed_on_clean_side(spark):
    clean, _ = split_quality(
        make_df(spark, [("ORD-1", "CUST-1", " 7 ", 120.5, "2026-08-29 10:00:00")]),
        source_type="Streaming",
    )
    assert clean.select("product_id").first()[0] == "7"


def test_clean_and_error_are_disjoint(spark):
    rows = [
        ("ORD-1", "CUST-1", "1", 120.5, "2026-08-29 10:00:00"),
        ("ORD-2", None, "1", 120.5, "2026-08-29 10:00:01"),
        ("ORD-3", "CUST-2", "2", -5.0, "2026-08-29 10:00:02"),
    ]
    clean, error = split_quality(make_df(spark, rows), source_type="Streaming")
    assert clean.count() == 1
    assert error.count() == 2
    assert clean.select("order_id").first()[0] == "ORD-1"


# ==============================================================================
# Gold aggregations — đúng grain của bảng đích
# ==============================================================================
def test_gold_daily_aggregation_sums_per_date_product(spark):
    rows = [
        ("ORD-1", "CUST-1", "1", 100.0, "2026-08-29 10:00:00"),
        ("ORD-2", "CUST-2", "1", 50.0, "2026-08-29 10:05:00"),
        ("ORD-3", "CUST-3", "2", 30.0, "2026-08-29 10:07:00"),
        ("ORD-4", "CUST-4", "1", 20.0, "2026-08-30 09:00:00"),
    ]
    clean, _ = split_quality(make_df(spark, rows), source_type="Batch")
    gold = aggregate_gold_daily(clean)
    collected = {
        (r["report_date"].isoformat(), r["product_id"]): float(r["total_revenue"])
        for r in gold.collect()
    }
    assert collected[("2026-08-29", "1")] == pytest.approx(150.0)
    assert collected[("2026-08-29", "2")] == pytest.approx(30.0)
    assert collected[("2026-08-30", "1")] == pytest.approx(20.0)


def test_gold_minute_aggregation_includes_product_grain(spark):
    """PK của gold_minute_revenue là (window_start, product_id) — bản cũ gộp thiếu product_id."""
    rows = [
        ("ORD-1", "CUST-1", "1", 100.0, "2026-08-29 10:00:10"),
        ("ORD-2", "CUST-2", "1", 50.0, "2026-08-29 10:00:40"),
        ("ORD-3", "CUST-3", "2", 30.0, "2026-08-29 10:00:20"),
    ]
    clean, _ = split_quality(make_df(spark, rows), source_type="Streaming")
    gold = aggregate_gold_minute(clean)
    collected = {(r["window_start"], r["product_id"]): float(r["total_revenue"]) for r in gold.collect()}

    assert len(collected) == 2  # 2 sản phẩm trong cùng cửa sổ 1 phút
    assert sum(collected.values()) == pytest.approx(180.0)
    for (window_start, _product_id) in collected:
        assert window_start.strftime("%Y-%m-%d %H:%M:%S") == "2026-08-29 10:00:00"
