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
    'automated_data_recovery_job',
    default_args=default_args,
    description='Luồng tự động quét kho DLQ, khôi phục đơn hàng âm tiền và thực thi Data Purge',
    schedule_interval='*/15 * * * *',  
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['batch', 'recovery', 'data_purge'],
) as dag:

    # ==============================================================================
    # 3. ĐỊNH NGHĨA TÁC VỤ KHÔI PHỤC DỮ LIỆU (RECOVERY TASK MANDATE)
    # ==============================================================================
    # Gọi công cụ spark-submit nạp trực tiếp Driver Postgres cục bộ để xử lý bảng lỗi tập trung
    execute_data_recovery_batch = BashOperator(
        task_id='trigger_spark_recovery_script',
        bash_command=(
            'docker exec -t newfolder2-spark-master-1 '
            '/opt/spark/bin/spark-submit '
            '--jars /root/.ivy2/jars/org.postgresql_postgresql-42.7.2.jar '
            '/opt/spark/src/reprocess_errors.py'
        )
    )

    execute_data_recovery_batch