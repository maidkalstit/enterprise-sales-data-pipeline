"""
Kafka Producer — mô phỏng giao dịch bán hàng theo thời gian thực.

Nâng cấp so với bản cũ:
  - Đọc cấu hình từ .env (KAFKA_BOOTSTRAP_SERVERS / KAFKA_TOPIC) thay vì
    hardcode localhost:9092. Chạy được cả trong container lẫn trên host —
    chỉ cần .env trỏ đúng listener tương ứng.
  - Message có KEY = customer_id (cùng khách hàng về cùng partition → đúng
    thứ tự giao dịch của từng khách) và acks='all' (độ bền gửi).
  - Sửa bug injection: trước đây nhánh lỗi order_id dùng lại ngưỡng
    error_chance < 0.02 sau nhánh < 0.03 → không bao giờ chạy.

Ghi chú: random module dùng CỐ Ý — dữ liệu mô phỏng, không phải ngữ cảnh mật mã.
"""
import json
import os
import random
import time
import uuid
from datetime import datetime

from dotenv import load_dotenv
from kafka import KafkaProducer

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, ".env"))

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(","),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",
    retries=3,
    api_version=(3, 7, 0),
)

TOPIC = os.getenv("KAFKA_TOPIC", "sales_topic")

# Bể khách hàng ban đầu (50 người) — mô phỏng tỷ lệ khách quay lại
customer_pool = [str(uuid.uuid4()) for _ in range(50)]


def generate_data():
    global customer_pool
    error_chance = random.random()

    # --- LOGIC KHÁCH HÀNG (RETENTION SIMULATION): 80% khách cũ, 20% khách mới ---
    if random.random() > 0.2:
        customer_id = random.choice(customer_pool)
    else:
        new_cust = str(uuid.uuid4())
        customer_pool.append(new_cust)
        customer_id = new_cust

    order_id = str(uuid.uuid4())
    product_id = str(random.randint(1, 50))  # khớp danh mục product_info.csv
    amount = round(random.uniform(10.5, 1500.0), 2)
    order_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # --- CƠ CHẾ BƠM LỖI (ERROR INJECTION) — 3 nhánh ngưỡng rời nhau, đều chạy được ---
    if error_chance < 0.02:
        customer_id = None   # Lỗi thiếu ID khách
    elif error_chance < 0.04:
        amount = -99.99      # Hoàn tiền — ghi âm, DLQ sẽ recovery giữ dấu gốc
    elif error_chance < 0.06:
        order_id = ""        # Lỗi thiếu mã đơn hàng

    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "product_id": product_id,
        "amount": amount,
        "order_date": order_date,
    }


print(f"🚀 Bơm dữ liệu vào topic '{TOPIC}' (key theo customer_id, acks=all)...")
try:
    while True:
        data = generate_data()
        producer.send(TOPIC, key=(data["customer_id"] or data["order_id"]), value=data)

        status = "❌ ERROR" if not data["customer_id"] or data["amount"] < 0 else "✅ OK"
        cust_str = str(data["customer_id"])[:8] if data["customer_id"] else "None"
        print(f"📡 {status} | Cust: {cust_str}... | Prod: {data['product_id']} | Amount: ${data['amount']}", end="\r")

        time.sleep(0.05)  # ~15–20 events/s (số liệu thật, không bơm lên như README cũ)
except KeyboardInterrupt:
    print("\n🛑 Đã dừng Producer.")
finally:
    producer.close()
