"""
db_utils — lớp dùng chung cho mọi job Spark khi đụng tới PostgreSQL.

An toàn SQL: mỗi câu lệnh là chuỗi literal viết ngay tại chỗ gọi, dữ liệu
luôn truyền qua placeholder %s — không ghép chuỗi, không SQL động.
"""
import os
import sys

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# Đường dẫn .env bên trong Spark container (docker-compose mount ./.env vào đây)
DEFAULT_ENV_PATH = "/opt/spark/.env"

ENV_KEYS = ("DB_URL", "DB_USER", "DB_PASS", "DB_DRIVER")


def load_db_config(env_path=DEFAULT_ENV_PATH):
    """Nạp và kiểm tra phòng thủ cấu hình DB — thiếu biến nào là dừng ngay."""
    load_dotenv(env_path)
    cfg = {key: os.getenv(key) for key in ENV_KEYS}
    missing = [key for key, value in cfg.items() if not value]
    if missing:
        print(f"💥 Lỗi hệ thống: thiếu biến cấu hình {missing} (env: {env_path}).")
        sys.exit(1)
    return cfg


def jdbc_to_psycopg_params(jdbc_url):
    """
    Phân giải 'jdbc:postgresql://host:port/dbname' thành dict psycopg2.
    Tách hàm riêng (thay cho hardcode host='postgres-db' cũ) để mọi nơi
    đổi DB_URL một chỗ là chạy — và để unit test được.
    """
    try:
        rest = jdbc_url.split("jdbc:postgresql://", 1)[1]
        hostport, dbname = rest.split("/", 1)
        host, port = hostport.split(":", 1)
        return {"host": host, "port": int(port), "dbname": dbname}
    except Exception as exc:
        raise ValueError(f"JDBC URL không hợp lệ: {jdbc_url!r}") from exc


def get_conn(cfg):
    params = jdbc_to_psycopg_params(cfg["DB_URL"])
    return psycopg2.connect(
        host=params["host"],
        port=params["port"],
        dbname=params["dbname"],
        user=cfg["DB_USER"],
        password=cfg["DB_PASS"],
        connect_timeout=10,
    )


def upsert_silver_rows(cfg, rows):
    """
    UPSERT list[dict] vào clean_sales_events theo khóa order_id.
    Idempotent: chạy lại bao nhiêu lần cũng không nhân đôi, không trùng PK.
    Trả về số dòng đã xử lý. Dùng cho dữ liệu nhỏ theo micro-batch.
    """
    if not rows:
        return 0
    cols = ("order_id", "customer_id", "product_id", "amount", "order_date")
    tuples = [tuple(row[c] for c in cols) for row in rows]
    conn = get_conn(cfg)
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(
                cur,
                """
                INSERT INTO clean_sales_events (order_id, customer_id, product_id, amount, order_date)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO UPDATE SET
                    customer_id = EXCLUDED.customer_id,
                    product_id  = EXCLUDED.product_id,
                    amount      = EXCLUDED.amount,
                    order_date  = EXCLUDED.order_date
                """,
                tuples,
            )
        conn.commit()
    finally:
        conn.close()
    return len(rows)


def upsert_gold_minute_rows(cfg, rows):
    """
    UPSERT list[dict] vào gold_minute_revenue theo khóa (window_start, product_id)
    — đúng Primary Key khai báo trong init-db.sql, hết cảnh trùng khóa khi có
    dữ liệu muộn hoặc replay offset.
    """
    if not rows:
        return 0
    cols = ("window_start", "window_end", "product_id", "total_revenue")
    tuples = [tuple(row[c] for c in cols) for row in rows]
    conn = get_conn(cfg)
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(
                cur,
                """
                INSERT INTO gold_minute_revenue (window_start, window_end, product_id, total_revenue)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (window_start, product_id) DO UPDATE SET
                    window_end    = EXCLUDED.window_end,
                    total_revenue = EXCLUDED.total_revenue
                """,
                tuples,
            )
        conn.commit()
    finally:
        conn.close()
    return len(rows)


def mark_errors_processed(cfg, ids):
    """
    Đóng dấu 'processed' cho các dòng error_logs đã khôi phục thành công
    (dùng đúng cột status — trước đây cột này tồn tại mà không ai update,
    thay cho DELETE mù quáng từng gây mất dữ liệu do race condition).
    """
    if not ids:
        return 0
    id_list = list(ids)
    conn = get_conn(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE error_logs SET status = 'processed' WHERE id = ANY(%s)",
                (id_list,),
            )
        conn.commit()
    finally:
        conn.close()
    return len(id_list)


def atomic_swap_gold(cfg):
    """
    Refresh gold_batch_revenue từ gold_batch_revenue_staging trong MỘT
    transaction (DELETE + INSERT cùng lúc commit/rollback) — Metabase không bao
    giờ thấy bảng rỗng lửng giữa chừng, và Primary Key của bảng Gold được
    bảo toàn (thay cho mode("overwrite") — thứ đã DROP table và xé mất PK).
    """
    conn = get_conn(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM gold_batch_revenue")
            cur.execute(
                """
                INSERT INTO gold_batch_revenue (report_date, product_id, total_revenue)
                SELECT report_date, product_id, total_revenue
                FROM gold_batch_revenue_staging
                """
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
