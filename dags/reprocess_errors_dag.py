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

SPARK_SUBMIT = (
    'docker exec -t newfolder2-spark-master-1 '
    '/opt/spark/bin/spark-submit '
    '--packages org.postgresql:postgresql:42.7.2 '
)

# ==============================================================================
# 2. KHỞI TẠO DAG — tự động khôi phục lỗi từ DLQ mỗi 15 phút
# ==============================================================================
with DAG(
    'automated_data_recovery_job',
    default_args=default_args,
    description='Quét DLQ theo vòng đời status, khôi phục đơn hoàn tiền và đóng dấu processed',
    schedule_interval='*/15 * * * *',
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['batch', 'recovery', 'dlq_lifecycle'],
) as dag:

    execute_data_recovery_batch = BashOperator(
        task_id='trigger_spark_recovery_script',
        bash_command=SPARK_SUBMIT + '/opt/spark/src/reprocess_errors.py'
    )

    execute_data_recovery_batch
