#  скрипт, который слушает обновления бота и печатает chat.id групп/каналов, куда прилетело событие.
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


API_URL = "https://api.telegram.org/bot{token}/{method}"


def get_updates(token: str, offset: Optional[int] = None, timeout: int = 50) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {"timeout": timeout}
    if offset is not None:
        params["offset"] = offset
    # Сфокусируемся на событиях, дающих chat.id
    params["allowed_updates"] = ["message", "channel_post", "my_chat_member", "chat_member"]

    resp = requests.get(API_URL.format(token=token, method="getUpdates"), params=params, timeout=timeout + 5)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram API error: {data}")
    return data.get("result", [])


def extract_and_log_chat(update: Dict[str, Any]) -> Optional[int]:
    # Возможные контейнеры чата
    containers = [
        update.get("message"),
        update.get("channel_post"),
        update.get("my_chat_member"),
        update.get("chat_member"),
        update.get("edited_message"),
        update.get("edited_channel_post"),
    ]
    for item in containers:
        if not item:
            continue
        # У my_chat_member/chat_member объект чата по ключу 'chat'
        chat = item.get("chat") if "chat" in item else item.get("message", {}).get("chat")
        if not chat:
            chat = item.get("chat") or item.get("from")
        if not chat:
            continue

        chat_id = chat.get("id")
        chat_type = chat.get("type")
        title = chat.get("title") or chat.get("username") or ""

        if chat_id is not None:
            logger.info("Получен chat: id=%s type=%s title=%s", chat_id, chat_type, title)
            print(f"chat.id: {chat_id} | type: {chat_type} | title: {title}")
            return int(chat_id)
    return None


def main() -> None:
    load_dotenv()
    token = os.getenv("TELEGRAM_BOT_TOKEN_ASSISTANT", "")
    if not token:
        print("❌ TELEGRAM_BOT_TOKEN_ASSISTANT не найден в .env")
        sys.exit(1)

    print("Откройте нужную группу, добавьте бота и отправьте любое сообщение/команду. Скрипт покажет chat.id.")

    offset: Optional[int] = None
    try:
        while True:
            try:
                updates = get_updates(token, offset=offset, timeout=50)
            except Exception as e:
                logger.error("Ошибка getUpdates: %s", e)
                time.sleep(2)
                continue

            if not updates:
                continue

            for upd in updates:
                offset = upd["update_id"] + 1
                chat_id = extract_and_log_chat(upd)
                # Продолжаем слушать, чтобы можно было поймать несколько групп при необходимости
            # Небольшая пауза, чтобы не грузить API
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n⏹️ Остановлено пользователем")


if __name__ == "__main__":
    main()


