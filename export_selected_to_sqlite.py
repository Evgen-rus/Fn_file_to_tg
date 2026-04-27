"""
Экспорт выбранных столбцов из Google Sheets в SQLite.

Берет ID таблиц из .env:
- SPREADSHEET_ID_128 -> direction = "оборудование"
- SPREADSHEET_ID_149 -> direction = "транспорт"

Из листа 'Данные' извлекает столбцы: 'ID', 'Номера', 'Источник', 'Дата'
и сохраняет в таблицу SQLite 'leads' со структурой:
  - id INTEGER
  - phone TEXT
  - utm_campaign TEXT
  - event_at TEXT  (нормализованная дата-время 'YYYY-MM-DD HH:MM:SS')
  - direction TEXT NOT NULL
  - created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  - updated_at DATETIME DEFAULT CURRENT_TIMESTAMP

Уникальность обеспечивается составным PRIMARY KEY(direction, id). При повторном запуске
используется UPSERT: обновляются phone, utm_campaign, event_at, updated_at.
"""

import os
import sys
import logging
from datetime import datetime, timedelta
import sqlite3
import re
from typing import Any, Dict, List, Tuple, Optional
import pytz
import time
import random

from dotenv import load_dotenv
from logging_setup import configure_logging
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.errors import HttpError


SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SHEET_NAME = 'Данные'
DB_FILENAME = 'baltlease_data.db'

logger = configure_logging('export_selected_to_sqlite')


UTM_PHONE_SUFFIX_RE = re.compile(r'^(.*)_\d{11}$')


def create_sheets_service():
    """
    Создает и возвращает сервис Google Sheets API.
    """
    credentials_file = os.getenv('GOOGLE_CREDENTIALS_FILE')
    if not credentials_file or not os.path.exists(credentials_file):
        raise FileNotFoundError(f"Файл credentials не найден: {credentials_file}")

    credentials = service_account.Credentials.from_service_account_file(
        credentials_file, scopes=SCOPES
    )
    service = build('sheets', 'v4', credentials=credentials)
    logger.info("Сервис Google Sheets API успешно создан")
    return service


def get_sheet_values(service, spreadsheet_id: str, sheet_name: str) -> List[List[str]]:
    """
    Возвращает все значения листа как список строк.
    """
    def _do_call():
        return service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=sheet_name
        ).execute()

    result = _execute_with_retries(_do_call, f"получение данных листа '{sheet_name}'")
    return result.get('values', [])


def _should_retry_http_error(err: HttpError) -> bool:
    status = getattr(err, 'resp', None).status if getattr(err, 'resp', None) else None
    if status in (429, 500, 502, 503, 504):
        return True
    # Некоторые 403 бывают квотными
    text = ''
    try:
        text = err.content.decode('utf-8') if hasattr(err, 'content') and isinstance(err.content, (bytes, bytearray)) else str(err)
    except Exception:
        text = str(err)
    if status == 403 and any(key in text for key in ('rateLimitExceeded', 'userRateLimitExceeded', 'quotaExceeded')):
        return True
    return False


def _execute_with_retries(call_fn, description: str, max_attempts: int = 5, base_delay: float = 1.0) -> Any:
    attempt = 0
    while True:
        try:
            return call_fn()
        except HttpError as e:
            if attempt + 1 >= max_attempts or not _should_retry_http_error(e):
                logger.error(f"API ошибка без повторов: {description}: {e}")
                raise
            delay = base_delay * (2 ** attempt) + random.uniform(0, 0.5)
            logger.warning(f"API ошибка, повтор через {delay:.1f}с ({description}, попытка {attempt+1}/{max_attempts}): {e}")
            time.sleep(delay)
            attempt += 1
        except Exception as e:
            # Сетевые временные ошибки
            if attempt + 1 >= max_attempts:
                logger.error(f"Ошибка без повторов: {description}: {e}")
                raise
            delay = base_delay * (2 ** attempt) + random.uniform(0, 0.5)
            logger.warning(f"Ошибка, повтор через {delay:.1f}с ({description}, попытка {attempt+1}/{max_attempts}): {e}")
            time.sleep(delay)
            attempt += 1


def find_header_indexes(headers: List[str], required: List[str]) -> Dict[str, int]:
    """
    Находит индексы обязательных заголовков.
    Возвращает словарь {header_name: index}. Бросает ValueError, если чего-то не хватает.
    """
    name_to_idx: Dict[str, int] = {}
    for i, name in enumerate(headers):
        name_to_idx[name] = i

    missing = [h for h in required if h not in name_to_idx]
    if missing:
        raise ValueError(f"Не найдены столбцы: {', '.join(missing)}")

    return {h: name_to_idx[h] for h in required}


def ensure_db_schema(conn: sqlite3.Connection) -> None:
    """
    Создает таблицу, если её нет, и выполняет простую миграцию структуры.
    Ключ уникальности: (direction, source_id).
    Добавлен столбец sent_at для даты/времени отправки лида клиенту.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS leads (
            row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id INTEGER NOT NULL,
            phone TEXT,
            utm_campaign TEXT,
            event_at TEXT,
            direction TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'Пасивный',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            sent_at TEXT,
            UNIQUE (direction, source_id)
        )
        """
    )
    conn.commit()

    # Простейшая миграция для уже существующей БД:
    # если столбца sent_at нет, добавляем его.
    cur = conn.execute("PRAGMA table_info(leads)")
    columns = {row[1] for row in cur.fetchall()}
    if "sent_at" not in columns:
        conn.execute("ALTER TABLE leads ADD COLUMN sent_at TEXT")
        conn.commit()


def upsert_rows(conn: sqlite3.Connection, rows: List[Tuple[int, Optional[str], Optional[str], Optional[str], str, str, str, str]]) -> int:
    """
    Вставляет или обновляет строки. Возвращает число обработанных строк.
    rows: список кортежей (source_id, phone, utm_campaign, event_at, direction, status, created_at, updated_at)
    """
    sql = (
        """
        INSERT INTO leads (source_id, phone, utm_campaign, event_at, direction, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(direction, source_id) DO UPDATE SET
            phone=excluded.phone,
            utm_campaign=excluded.utm_campaign,
            event_at=excluded.event_at,
            status=excluded.status,
            updated_at=excluded.updated_at
        """
    )
    conn.executemany(sql, rows)
    conn.commit()
    return len(rows)


def normalize_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip()
    if text == '':
        return None
    try:
        return int(text)
    except ValueError:
        return None


def normalize_datetime(value: Optional[str]) -> Optional[str]:
    """
    Принимает строку даты/времени из Google Sheets и приводит к 'YYYY-MM-DD HH:MM:SS'.
    Поддерживаемые входы: 'YYYY-MM-DD HH:MM:SS', 'DD.MM.YYYY HH:MM:SS', 'YYYY-MM-DD', 'DD.MM.YYYY'.
    Возвращает None, если распарсить не удалось или строка пустая.
    """
    if value is None:
        return None
    text = str(value).strip()
    if text == '':
        return None

    fmts = [
        '%Y-%m-%d %H:%M:%S',
        '%d.%m.%Y %H:%M:%S',
        '%Y-%m-%d',
        '%d.%m.%Y',
    ]
    for fmt in fmts:
        try:
            dt = datetime.strptime(text, fmt)
            # Если это была только дата, добавим время 00:00:00
            if fmt in ('%Y-%m-%d', '%d.%m.%Y'):
                dt = dt.replace(hour=0, minute=0, second=0)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            continue
    return None


def normalize_utm_campaign(value: Optional[str]) -> Optional[str]:
    """
    Нормализует значение источника перед записью в БД.

    Если строка оканчивается на `_<11 цифр>`, отбрасывает этот хвост.
    Примеры:
    - 'skl.ru_73433111211' -> 'skl.ru'
    - 'abc_def_12345678901' -> 'abc_def'
    """
    if value is None:
        return None

    text = str(value).strip()
    if text == '':
        return None

    match = UTM_PHONE_SUFFIX_RE.match(text)
    if match:
        normalized = match.group(1).strip()
        return normalized or None

    return text


def process_spreadsheet(service, spreadsheet_id: str, direction: str) -> int:
    """
    Загружает лист, извлекает нужные столбцы и пишет в БД.
    Возвращает количество записей, отправленных в БД.
    """
    logger.info(f"Обработка таблицы: {spreadsheet_id} (direction='{direction}')")
    values = get_sheet_values(service, spreadsheet_id, SHEET_NAME)
    if not values:
        logger.warning("Данные не получены или лист пустой")
        return 0

    headers = values[0]
    data_rows = values[1:] if len(values) > 1 else []

    idx = find_header_indexes(headers, ['ID', 'Номера', 'Источник', 'Дата'])

    rows_to_upsert: List[Tuple[int, Optional[str], Optional[str], Optional[str], str, str, str, str]] = []
    msk = pytz.timezone('Europe/Moscow')
    now_str = datetime.now(msk).strftime('%Y-%m-%d %H:%M:%S')
    # Порог для фильтрации — 5 суток назад по МСК (для фильтрации старых записей).
    threshold = datetime.now(msk) - timedelta(days=5)
    filtered_by_date = 0
    for row in data_rows:
        # Безопасно читаем значения по индексам
        id_val = row[idx['ID']] if idx['ID'] < len(row) else None
        phone_val = row[idx['Номера']] if idx['Номера'] < len(row) else None
        utm_val = row[idx['Источник']] if idx['Источник'] < len(row) else None
        date_val = row[idx['Дата']] if idx['Дата'] < len(row) else None

        id_int = normalize_int(id_val)
        phone_str = str(phone_val).strip() if phone_val is not None and str(phone_val).strip() else None
        utm_str = normalize_utm_campaign(utm_val)
        event_at = normalize_datetime(date_val)

        # Фильтр: берем только записи за последние 2 суток по Москве
        if event_at:
            try:
                event_dt_naive = datetime.strptime(event_at, '%Y-%m-%d %H:%M:%S')
                event_dt_msk = msk.localize(event_dt_naive)
                if event_dt_msk < threshold:
                    filtered_by_date += 1
                    continue
            except Exception:
                # Если дату не удалось распарсить, пропускаем
                filtered_by_date += 1
                continue
        else:
            filtered_by_date += 1
            continue

        # Требуем исходный ID (source_id) для уникальности
        if id_int is None:
            continue

        rows_to_upsert.append((id_int, phone_str, utm_str, event_at, direction, 'Пасивный', now_str, now_str))

    if not rows_to_upsert:
        logger.info("Нет валидных строк для сохранения")
        return 0

    with sqlite3.connect(DB_FILENAME) as conn:
        ensure_db_schema(conn)
        count = upsert_rows(conn, rows_to_upsert)

    logger.info(f"Сохранено/обновлено записей: {count}; пропущено по дате (>3 суток): {filtered_by_date}")
    return count


def main():
    load_dotenv()

    spreadsheet_id_128 = os.getenv('SPREADSHEET_ID_128')
    spreadsheet_id_149 = os.getenv('SPREADSHEET_ID_149')

    mapping: List[Tuple[str, str]] = []
    if spreadsheet_id_128:
        mapping.append((spreadsheet_id_128, 'оборудование'))
    if spreadsheet_id_149:
        mapping.append((spreadsheet_id_149, 'транспорт'))

    if not mapping:
        logger.error("В .env не найдены SPREADSHEET_ID_128 и/или SPREADSHEET_ID_149")
        sys.exit(1)

    service = create_sheets_service()

    total = 0
    for sid, direction in mapping:
        try:
            total += process_spreadsheet(service, sid, direction)
        except Exception as e:
            logger.error(f"Ошибка при обработке {sid}: {e}")

    logger.info(f"Готово. Всего записей обработано: {total}. База: {DB_FILENAME}")


if __name__ == '__main__':
    main()


