import json, time, random, uuid
from kafka import KafkaProducer
from datetime import datetime

# Kết nối Kafka Broker
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(3, 7, 0)
)

# 1. Khởi tạo bể khách hàng ban đầu (50 người)
customer_pool = [str(uuid.uuid4()) for _ in range(50)]

def generate_data():
    global customer_pool
    error_chance = random.random()
    
    # --- LOGIC KHÁCH HÀNG (RETENTION SIMULATION) ---
    # 80% chọn khách cũ từ pool, 20% tạo khách mới
    if random.random() > 0.2:
        customer_id = random.choice(customer_pool)
    else:
        new_cust = str(uuid.uuid4())
        customer_pool.append(new_cust) # Thêm khách mới vào bể để mô phỏng họ quay lại
        customer_id = new_cust

    # --- LOGIC ĐƠN HÀNG ---
    order_id = str(uuid.uuid4())
    product_id = str(random.randint(1, 50))  # 🔥 Chuỗi ID khớp với product_info.csv
    amount = round(random.uniform(10.5, 1500.0), 2)
    order_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # --- CƠ CHẾ BƠM LỖI (ERROR INJECTION) ---
    if error_chance < 0.02:
        customer_id = None  # Lỗi thiếu ID khách
    elif error_chance < 0.03:
        amount = -99.99     # Lỗi giá trị âm vô lý
    elif error_chance < 0.02:
        order_id = ""       # Lỗi thiếu mã đơn hàng

    return {
        'order_id': order_id,
        'customer_id': customer_id,
        'product_id': product_id,  # 🔥 Đã bổ sung trường dữ liệu móc nối
        'amount': amount,
        'order_date': order_date
    }

print("🚀 Hệ thống bắt đầu bơm dữ liệu: Khách cũ + Khách mới + Dữ liệu lỗi...")
try:
    while True:
        data = generate_data()
        producer.send('sales_topic', value=data)
        
        # Hiển thị trạng thái giám sát trực tiếp trên console
        status = "❌ ERROR" if not data['customer_id'] or data['amount'] < 0 else "✅ OK"
        cust_str = str(data['customer_id'])[:8] if data['customer_id'] else "None"
        print(f"📡 {status} | Cust: {cust_str}... | Prod: {data['product_id']} | Amount: ${data['amount']}", end='\r')
        
        time.sleep(0.05)
except KeyboardInterrupt:
    print("\n🛑 Đã dừng Producer.")
finally:
    producer.close()