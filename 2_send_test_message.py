# скрипт для проверки ID ТГ группы и отправки сообщения
# chat_id = "-100id из вебприложения" бота нужно добавить в группу и сделать админом
import os
import sys
import logging
from dotenv import load_dotenv
import requests


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def send_message(chat_id: str, text: str) -> None:
    load_dotenv()
    token = os.getenv("TELEGRAM_BOT_TOKEN_ASSISTANT", "")
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN_ASSISTANT не найден в .env")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    resp = requests.post(url, json=payload, timeout=15)
    resp.raise_for_status()
    logger.info("Сообщение отправлено: %s", resp.json())


if __name__ == "__main__":
    chat_id = "-100264...."
    text = "Тестовое сообщение"
    # Позволим передать текст через аргумент
    if len(sys.argv) > 1:
        text = " ".join(sys.argv[1:])
    send_message(chat_id, text)