-- ==============================================================================
-- KỊCH BẢN KHỞI TẠO CẤU TRÚC CƠ SỞ DỮ LIỆU (DATABASE SCHEMA)
-- ==============================================================================

-- 1. Bảng chứa dữ liệu tính toán doanh thu thời gian thực (Speed Layer - Gold Layer)
CREATE TABLE IF NOT EXISTS gold_minute_revenue (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    total_revenue DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Bảng chứa dữ liệu sạch từ luồng xử lý lô tĩnh (Batch Layer - Gold Layer)
-- Thiết kế cấu trúc lưu trữ dạng Snapshot phục vụ chu kỳ chạy ngắn 10 phút/lần
CREATE TABLE IF NOT EXISTS gold_batch_revenue (
    order_id VARCHAR(255),
    customer_id VARCHAR(255),
    product_id VARCHAR(255),
    amount DOUBLE PRECISION,
    order_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Bảng Dead Letter Queue (DLQ) tích hợp lưu trữ lỗi hệ thống tập trung
-- Đồng bộ cấu trúc gồm cả trường product_id để tránh hiện tượng Data Loss
CREATE TABLE IF NOT EXISTS error_logs (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(255),
    customer_id VARCHAR(255),
    product_id VARCHAR(255),
    amount DOUBLE PRECISION,
    order_date TIMESTAMP,
    error_reason VARCHAR(255),
    source_type VARCHAR(50), -- Định danh nguồn gốc phát sinh lỗi: 'Streaming' hoặc 'Batch'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);