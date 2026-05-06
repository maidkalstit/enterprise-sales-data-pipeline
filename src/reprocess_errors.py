import os
import sys

# Tự động nạp thư viện điều khiển cơ sở dữ liệu gốc nếu môi trường Docker chưa tích hợp
try:
    import psycopg2
except ImportError:
    os.system("pip install psycopg2-binary --quiet")
    import psycopg2

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, to_timestamp, window, sum
from dotenv import load_dotenv

# ==============================================================================
# 1. NẠP CẤU HÌNH VÀ KIỂM TRA PHÒNG THỦ (DEFENSIVE CHECK)
# ==============================================================================
load_dotenv(dotenv_path="/opt/spark/.env")

db_url = os.getenv("DB_URL")
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASS")
db_driver = os.getenv("DB_DRIVER")

if not all([db_url, db_user, db_pass, db_driver]):
    print("💥 Lỗi nghiêm trọng: Các biến cấu hình kết nối từ tệp .env đang bị trống.")
    sys.exit(1)

# Khởi tạo Spark Session độc lập cho tiến trình xử lý khôi phục định kỳ
spark = SparkSession.builder \
    .appName("Data_Recovery_Batch_Job") \
    .getOrCreate()

props = {"user": db_user, "password": db_pass, "driver": db_driver}
print("🔍 Đã thiết lập kết nối an toàn. Bắt đầu quét Metadata từ bảng chứa lỗi tập trung...")

try:
    # --------------------------------------------------------------------------
    # BƯỚC 1: TRÍCH XUẤT (EXTRACT) DỮ LIỆU TỪ BẢNG CHỨA LỖI (DLQ)
    # --------------------------------------------------------------------------
    errors_df = spark.read.jdbc(url=db_url, table="error_logs", properties=props)
    record_count = errors_df.count()

    if record_count > 0:
        # --------------------------------------------------------------------------
        # BƯỚC 2: BIẾN ĐỔI VÀ HOÀN THIỆN CẤU TRÚC (TRANSFORM)
        # --------------------------------------------------------------------------
        # Sửa đổi logic: Lọc đơn hàng âm tiền (Invalid Amount), đưa giá trị về 0 và chuyển sang dạng Timestamp
        recovered_df = errors_df.filter(col("error_reason") == 'Invalid Amount') \
            .withColumn("amount", when(col("amount") < 0, lit(0)).otherwise(col("amount"))) \
            .withColumn("event_time", to_timestamp(col("order_date")))

        target_recovery_count = recovered_df.count()
        
        if target_recovery_count > 0:
            # Tái tính toán tích lũy doanh thu bổ sung theo cửa sổ thời gian 1 phút
            gold_recovery = recovered_df.groupBy(window(col("event_time"), "1 minute")) \
                .agg(sum("amount").alias("total_revenue")) \
                .select("window.start", "window.end", "total_revenue")

            # --------------------------------------------------------------------------
            # BƯỚC 3: ĐỔ DỮ LIỆU (LOAD) VÀO TẦNG GOLD ĐÍCH
            # --------------------------------------------------------------------------
            gold_recovery.write.jdbc(url=db_url, table="gold_minute_revenue", mode="append", properties=props)
            print(f"✅ TÁC VỤ SPARK HOÀN THÀNH: Quy trình xử lý lỗi kết thúc thành công. Đã tái cấu trúc: {target_recovery_count} bản ghi.")

            # --------------------------------------------------------------------------
            # BƯỚC 4: TIẾN TRÌNH DATA PURGE (XÓA RÁC CŨ ĐỂ ĐẢM BẢO TÍNH IDEMPOTENCY)
            # --------------------------------------------------------------------------
            print("🗑️ Đang tiến hành dọn dẹp sạch sẽ kho chứa rác cũ tại bảng error_logs...")
            
            # Khởi tạo kết nối native vào mạng nội bộ của container Postgres để thực thi lệnh DELETE
            conn = psycopg2.connect(
                host="postgres-db",
                database="sales_db",
                user=db_user,
                password=db_pass,
                port="5432"
            )
            cursor = conn.cursor()
            
            # Xóa các bản ghi lỗi 'Invalid Amount' đã được Spark xử lý khôi phục thành công ở trên
            delete_query = "DELETE FROM error_logs WHERE error_reason = 'Invalid Amount';"
            cursor.execute(delete_query)
            conn.commit()
            
            print(f"🧹 DATA PURGE HOÀN THÀNH: Bảng error_logs đã được làm sạch tuyệt đối.")
            cursor.close()
            conn.close()

        else:
            print("ℹ️ Kết thúc tác vụ: Không tìm thấy thực thể dữ liệu lỗi nào phù hợp tiêu chí làm sạch.")
    else:
        print("✨ Trạng thái: Kho dữ liệu lỗi trống. Hệ thống Ingestion chính vận hành đạt độ toàn vẹn tối ưu.")

except Exception as e:
    print(f"💥 Lỗi phát sinh trong quá trình vận hành Batch Job: {e}")

finally:
    # Giải phóng tài nguyên bộ nhớ
    spark.stop()