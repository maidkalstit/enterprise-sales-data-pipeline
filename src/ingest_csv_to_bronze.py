"""
Landing → Bronze: đưa log giao dịch tĩnh (sales_data.csv) vào CÙNG tầng Bronze
raw_sales_events với stream, ở dạng JSON payload thống nhất.

"""
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, StringType, StructField, StructType

from db_utils import load_db_config

cfg = load_db_config()
db_url = cfg["DB_URL"]
jdbc_props = {"user": cfg["DB_USER"], "password": cfg["DB_PASS"], "driver": cfg["DB_DRIVER"]}

spark = SparkSession.builder \
    .appName("CSV_Landing_To_Bronze") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

CSV_PATH = "/opt/spark/data/sales_data.csv"

# Schema khớp ĐÚNG tên cột của file Landing (hết cảnh lệch vị trí cột âm thầm
# — bản cũ khai 'order_date' cho cột 'timestamp' và sống nhờ khớp theo vị trí)
# amount đọc thẳng DECIMAL(12,2) — CSV landing cũng không cho float chạm tiền
landing_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DecimalType(12, 2), True),
    StructField("timestamp", StringType(), True),
])

try:
    landing_df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(landing_schema)
        .load(CSV_PATH)
    )
    events_df = landing_df.withColumnRenamed("timestamp", "order_date")

    # Gói về đúng hình dạng payload như event của producer để parse một chuẩn ở ETL
    payload_df = events_df.select(
        F.to_json(F.struct("order_id", "customer_id", "product_id", "amount", "order_date")).alias("payload")
    )

    count = payload_df.count()
    if count > 0:
        payload_df.write.jdbc(url=db_url, table="raw_sales_events", mode="append", properties=jdbc_props)
    print(f"✅ Đã ingest {count} dòng từ Landing CSV vào Bronze (raw_sales_events).")

except Exception as exc:
    print(f"💥 Lỗi khi ingest Landing CSV vào Bronze: {exc}")

finally:
    spark.stop()
