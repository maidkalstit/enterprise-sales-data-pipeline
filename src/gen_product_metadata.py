import pandas as pd
from faker import Faker
import os

fake = Faker()
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

def generate_products(n=50):
    categories = ['Electronics', 'Fashion', 'Home & Kitchen', 'Beauty', 'Sports', 'Books']
    data = []
    for i in range(1, n + 1):
        data.append({
            "product_id": i,
            "product_name": f"{fake.color_name()} {fake.word().capitalize()} {fake.random_element(['Pro', 'Max', 'Ultra', 'Edition'])}",
            "category": fake.random_element(categories)
        })
    
    df = pd.DataFrame(data)
    file_path = os.path.join(DATA_DIR, 'product_info.csv')
    df.to_csv(file_path, index=False)
    print(f"✅ Đã tạo {n} sản phẩm thực tế tại: {file_path}")

if __name__ == "__main__":
    generate_products()