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

# ==============================================================================
# 2. KHỞI TẠO ĐỊNH TUYẾN TIẾN TRÌNH (DAG INITIALIZATION)
# ==============================================================================
with DAG(
    'sales_batch_optimization_v1',
    default_args=default_args,
    description='Luồng xử lý lô định kỳ chu kỳ 10 phút, tối ưu lưu trữ Parquet và bóc tách DLQ',
    schedule_interval='*/10 * * * *',  # Chu kỳ tự động quét hệ thống mỗi 10 phút
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['batch', 'parquet', 'medallion'],
) as dag:

    # ==============================================================================
    # 3. ĐỊNH NGHĨA CÁC TÁC VỤ VẬN HÀNH (TASKS DEFINITION)
    # ==============================================================================

    # Tác vụ 1: Khởi tạo tệp danh mục sản phẩm (Metadata)
    generate_product_metadata = BashOperator(
        task_id='generate_product_metadata',
        bash_command='docker exec -t newfolder2-spark-master-1 python3 /opt/spark/src/gen_product_metadata.py'
    )

    # Tác vụ 2: Sinh dữ liệu giao dịch bán hàng thô dạng tĩnh (CSV)
    generate_raw_sales_data = BashOperator(
        task_id='generate_raw_sales_data',
        bash_command='docker exec -t newfolder2-spark-master-1 python3 /opt/spark/src/gen_data.py'
    )

    # Tác vụ 3: Tối ưu hóa câu lệnh kiểm tra sức khỏe (Health Check) hệ thống Spark Master
    check_spark_master_health = BashOperator(
        task_id='check_spark_master_health',
        bash_command='docker exec -t newfolder2-spark-master-1 sh -c "ps -ef | grep org.apache.spark.deploy.master.Master | grep -v grep"'
    )

    # Tác vụ 4: Thực thi ứng dụng Spark Batch ETL chuyển đổi định dạng lưu trữ cột Parquet
    run_spark_batch_etl = BashOperator(
        task_id='run_spark_batch_etl',
        bash_command=(
            'docker exec -t newfolder2-spark-master-1 '
            '/opt/spark/bin/spark-submit '
            '--jars /root/.ivy2/jars/org.postgresql_postgresql-42.7.2.jar '
            '/opt/spark/src/etl_job.py'
        )
    )

    # ==============================================================================
    # 4. THIẾT LẬP LUỒNG PHỤ THUỘC (TASK DEPENDENCIES)
    # ==============================================================================
    generate_product_metadata >> generate_raw_sales_data >> check_spark_master_health >> run_spark_batch_etl