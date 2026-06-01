-- ==============================================================================
-- KỊCH BẢN KHỞI TẠO CẤU TRÚC CƠ SỞ DỮ LIỆU CHUẨN MEDALLION (DATABASE SCHEMA)
-- ==============================================================================

-- 1. TẦNG BRONZE: Lưu trữ dữ liệu thô (Schema-on-Read)
CREATE TABLE IF NOT EXISTS raw_sales_events (
    id SERIAL PRIMARY KEY,
    payload TEXT NOT NULL, -- Chứa chuỗi JSON gốc từ Kafka
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. TẦNG SILVER: Dữ liệu đã làm sạch và ép kiểu (Schema-on-Write)
CREATE TABLE IF NOT EXISTS clean_sales_events (
    order_id VARCHAR(255) PRIMARY KEY,
    customer_id VARCHAR(255),
    product_id VARCHAR(255),
    amount DOUBLE PRECISION,
    order_date TIMESTAMP,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. TẦNG GOLD (SPEED LAYER): Doanh thu thời gian thực từ Spark Streaming
-- Cấu trúc Khóa chính tổ hợp để hỗ trợ UPSERT (Idempotency)
CREATE TABLE IF NOT EXISTS gold_minute_revenue (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    product_id VARCHAR(255) NOT NULL,
    total_revenue DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, product_id)
);

-- 4. TẦNG GOLD (BATCH LAYER): Bảng đệm (Staging) phục vụ luồng ghi nguyên tử
CREATE TABLE IF NOT EXISTS gold_batch_revenue_staging (
    report_date DATE NOT NULL,
    product_id VARCHAR(255) NOT NULL,
    total_revenue DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. TẦNG GOLD (BATCH LAYER): Nguồn chân lý (Source of Truth)
CREATE TABLE IF NOT EXISTS gold_batch_revenue (
    report_date DATE NOT NULL,
    product_id VARCHAR(255) NOT NULL,
    total_revenue DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (report_date, product_id)
);

-- 6. HÀNG ĐỢI DLQ (Dead Letter Queue): Lưu trữ & Điều phối lỗi
-- Có cột status để Airflow theo dõi vòng đời sửa lỗi
CREATE TABLE IF NOT EXISTS error_logs (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(255),
    customer_id VARCHAR(255),
    product_id VARCHAR(255),
    amount DOUBLE PRECISION,
    order_date TIMESTAMP,
    error_reason VARCHAR(255),
    source_type VARCHAR(50), 
    status VARCHAR(20) DEFAULT 'unprocessed', -- 'unprocessed' hoặc 'processed'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);