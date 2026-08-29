from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# ==============================================================================
# 1. CẤU HÌNH THAM SỐ MẶC ĐỊNH (DEFAULT ARGUMENTS)
# ==============================================================================
default_args = {
    'owner': 'Tung_Dang_Dai_Nam',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# spark-submit qua --packages (Maven tự phân giải, hết cảnh chỉ chạy được khi
# ivy cache tình cờ có sẵn jar như lệnh --jars /root/.ivy2/... cũ)
SPARK_SUBMIT = (
    'docker exec -t newfolder2-spark-master-1 '
    '/opt/spark/bin/spark-submit '
    '--packages org.postgresql:postgresql:42.7.2 '
)

# ==============================================================================
# 2. KHỞI TẠO DAG
# ==============================================================================
with DAG(
    'sales_batch_optimization_v1',
    default_args=default_args,
    description='Luồng Batch Medallion: Landing CSV → Bronze → Silver → Gold (atomic swap)',
    schedule_interval='*/10 * * * *',  # chu kỳ 10 phút
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['batch', 'medallion'],
) as dag:

    # Tác vụ 1: Khởi tạo danh mục sản phẩm (idempotent — có sẵn thì giữ nguyên)
    generate_product_metadata = BashOperator(
        task_id='generate_product_metadata',
        bash_command='docker exec -t newfolder2-spark-master-1 python3 /opt/spark/src/gen_product_metadata.py'
    )

    # Tác vụ 2: Sinh log giao dịch tĩnh (CSV Landing)
    generate_raw_sales_data = BashOperator(
        task_id='generate_raw_sales_data',
        bash_command='docker exec -t newfolder2-spark-master-1 python3 /opt/spark/src/gen_data.py'
    )

    # Tác vụ 3: Ingest CSV Landing vào tầng Bronze dùng chung với stream
    # (đây là mảnh ghép khiến batch và stream hội tụ đúng nghĩa Lambda)
    ingest_landing_to_bronze = BashOperator(
        task_id='ingest_landing_to_bronze',
        bash_command=SPARK_SUBMIT + '/opt/spark/src/ingest_csv_to_bronze.py'
    )

    # Tác vụ 4: Health check Spark Master (ps thay jps — image Spark không có JDK jps)
    check_spark_master_health = BashOperator(
        task_id='check_spark_master_health',
        bash_command='docker exec -t newfolder2-spark-master-1 '
                     'sh -c "ps -ef | grep org.apache.spark.deploy.master.Master | grep -v grep"'
    )

    # Tác vụ 5: Batch ETL — đọc Bronze, ghi Silver, tính lại Gold bằng atomic swap
    run_spark_batch_etl = BashOperator(
        task_id='run_spark_batch_etl',
        bash_command=SPARK_SUBMIT + '/opt/spark/src/etl_job.py'
    )

    # ==============================================================================
    # 3. LUỒNG PHỤ THUỘC
    # ==============================================================================
    generate_product_metadata >> generate_raw_sales_data >> ingest_landing_to_bronze \
        >> check_spark_master_health >> run_spark_batch_etl
