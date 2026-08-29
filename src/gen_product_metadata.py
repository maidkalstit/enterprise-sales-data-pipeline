import os

import pandas as pd
from faker import Faker

fake = Faker()
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)


def generate_products(n=50):
    file_path = os.path.join(DATA_DIR, "product_info.csv")

    # IDEMPOTENT: danh mục là bảng lookup tĩnh cho stream-static join — nếu sinh
    # lại ngẫu nhiên mỗi lần chạy DAG thì tên sản phẩm trôi theo thời gian và lệch
    # với bản cache của streaming job (bug của bản cũ).
    if os.path.exists(file_path):
        print(f"ℹ️ {file_path} đã tồn tại — giữ nguyên danh mục (chế độ idempotent).")
        return

    categories = ["Electronics", "Fashion", "Home & Kitchen", "Beauty", "Sports", "Books"]
    data = []
    for i in range(1, n + 1):
        data.append({
            "product_id": i,
            "product_name": f"{fake.color_name()} {fake.word().capitalize()} "
                            f"{fake.random_element(['Pro', 'Max', 'Ultra', 'Edition'])}",
            "category": fake.random_element(categories),
        })

    df = pd.DataFrame(data)
    df.to_csv(file_path, index=False)
    print(f"✅ Đã khởi tạo {n} sản phẩm (chạy một lần duy nhất) tại: {file_path}")


if __name__ == "__main__":
    generate_products()
