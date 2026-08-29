"""
notify — gọn một chỗ cho mọi gửi tin Telegram (Batch bot / Streaming bot).
Trước đây 3 file mỗi nơi một hàm send_telegram_msg riêng — giờ về một cửa.
"""
import os

import requests
from dotenv import load_dotenv

DEFAULT_ENV_PATH = "/opt/spark/.env"


def load_env(env_path=DEFAULT_ENV_PATH):
    load_dotenv(env_path)


def send_telegram(token, chat_id, text, timeout=10):
    """Gửi 1 tin nhắn; trả về True/False thay vì âm thầm nuốt lỗi."""
    if not token or not chat_id:
        print("⚠️ Thiếu TELEGRAM token/chat_id — bỏ qua gửi thông báo.")
        return False
    try:
        res = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=timeout,
        )
        if res.status_code != 200:
            print(f"❌ Telegram API từ chối: {res.status_code} - {res.text}")
        return res.status_code == 200
    except Exception as exc:
        print(f"❌ Lỗi mạng khi gọi Telegram API: {exc}")
        return False


def send_env_bot(prefix, text, env_path=DEFAULT_ENV_PATH):
    """
    Gửi bằng bot khai báo trong .env: prefix='BATCH'  → BATCH_BOT_TOKEN/BATCH_CHAT_ID,
    prefix='STREAMING' → STREAMING_BOT_TOKEN/STREAMING_CHAT_ID.
    """
    load_env(env_path)
    token = str(os.getenv(f"{prefix}_BOT_TOKEN") or "").strip()
    chat_id = str(os.getenv(f"{prefix}_CHAT_ID") or "").strip()
    return send_telegram(token, chat_id, text)
