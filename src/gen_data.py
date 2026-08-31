"""
Sinh log giao dịch tĩnh CSV (Landing) cho Batch layer — chạy mỗi chu kỳ của
sales_pipeline_dag, sau đó ingest_csv_to_bronze.py đưa về cùng tầng Bronze
với stream. Gửi tin thông báo qua Batch bot (dùng chung notify.py).

Ghi chú: random/faker -  đây là dữ liệu MÔ PHỎNG cho demo,

"""
import os
import random
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from faker import Faker

from notify import send_env_bot

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(ENV_PATH)

fake = Faker()
DATA_DIR = os.path.join(BASE_DIR, "data")


def generate_realtime_sales(n=1000):
    data = []
    product_ids = list(range(1, 51))

    for _ in range(n):
        data.append({
            "order_id": f"ORD-{fake.uuid4()[:8].upper()}",
            "product_id": random.choice(product_ids),
            "customer_id": f"CUST-{fake.uuid4()[:8].upper()}",
            "amount": round(random.uniform(50.0, 5000.0), 2),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })

    df = pd.DataFrame(data)
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    file_path = os.path.join(DATA_DIR, "sales_data.csv")
    df.to_csv(file_path, index=False)

    msg = (
        "🚀 *[BATCH PIPELINE]*\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📦 Trạng thái: *Sinh dữ liệu Landing Xong*\n"
        f"📊 Quy mô: `{n}` đơn hàng mới\n"
        "👤 Engineer: `Tung Dang`"
    )
    send_env_bot("BATCH", msg, env_path=ENV_PATH)


if __name__ == "__main__":
    generate_realtime_sales(n=random.randint(800, 1500))
