-- ==============================================================================
-- Migration 001: tiền tệ NEVER float — DOUBLE PRECISION → NUMERIC(12,2)
-- Áp dụng cho DB ĐANG CHẠY (không cần wipe volume).
-- Nếu đã `docker compose down -v` SAU commit này thì khỏi chạy: init-db.sql
-- đã áp dụng sẵn NUMERIC(12,2) từ đầu.
--
-- Cách chạy (PowerShell):
--   Get-Content migrations\001_amount_to_numeric.sql -Raw | docker exec -i newfolder2-postgres-db-1 psql -U admin -d sales_db
-- ==============================================================================

BEGIN;

ALTER TABLE clean_sales_events
    ALTER COLUMN amount TYPE NUMERIC(12,2) USING amount::numeric(12,2);

ALTER TABLE error_logs
    ALTER COLUMN amount TYPE NUMERIC(12,2) USING amount::numeric(12,2);

ALTER TABLE gold_minute_revenue
    ALTER COLUMN total_revenue TYPE NUMERIC(12,2) USING total_revenue::numeric(12,2);

ALTER TABLE gold_batch_revenue
    ALTER COLUMN total_revenue TYPE NUMERIC(12,2) USING total_revenue::numeric(12,2);

-- Bảng staging sẽ được Spark JDBC ghi đè theo kiểu dữ liệu của DataFrame ở
-- chu kỳ batch kế tiếp; ALTER ở đây chỉ để schema khớp ngay lập tức.
ALTER TABLE gold_batch_revenue_staging
    ALTER COLUMN total_revenue TYPE NUMERIC(12,2) USING total_revenue::numeric(12,2);

COMMIT;

-- Xác minh:
-- SELECT table_name, column_name, data_type FROM information_schema.columns
-- WHERE column_name IN ('amount','total_revenue') ORDER BY table_name;
