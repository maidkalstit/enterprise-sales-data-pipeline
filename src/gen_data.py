import pandas as pd
import random
from faker import Faker
import os
import requests
from datetime import datetime
from dotenv import load_dotenv

# Xác định đường dẫn file .env trong Docker (/opt/spark/.env)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(ENV_PATH)

fake = Faker()
DATA_DIR = os.path.join(BASE_DIR, "data")

# 🔑 Đã sửa: Gọi đúng Token của Bot 1 (Batch)
TELEGRAM_TOKEN = os.getenv("BATCH_BOT_TOKEN") 
TELEGRAM_CHAT_ID = os.getenv("BATCH_CHAT_ID")

def send_telegram_msg(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"⚠️ Cảnh báo: Không tìm thấy BATCH_BOT_TOKEN tại {ENV_PATH}")
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"❌ Lỗi kết nối Telegram: {e}")

def generate_realtime_sales(n=1000):
    data = []
    product_ids = list(range(1, 51))
    
    for _ in range(n):
        data.append({
            "order_id": f"ORD-{fake.uuid4()[:8].upper()}",
            "product_id": random.choice(product_ids),
            # 🚀 NÂNG CẤP: Mã khách hàng UUID đồng nhất
            "customer_id": f"CUST-{fake.uuid4()[:8].upper()}",
            "amount": round(random.uniform(50.0, 5000.0), 2),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    df = pd.DataFrame(data)
    if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)
    
    file_path = os.path.join(DATA_DIR, 'sales_data.csv')
    df.to_csv(file_path, index=False)

    msg = (
        f"🚀 *[BATCH PIPELINE]*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 Trạng thái: *Sinh dữ liệu Xong*\n"
        f"📊 Quy mô: `{n}` đơn hàng mới\n"
        f"👤 Engineer: `Tung Dang`"
    )
    send_telegram_msg(msg)

if __name__ == "__main__":
    generate_realtime_sales(n=random.randint(800, 1500))