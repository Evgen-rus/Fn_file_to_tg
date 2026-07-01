"""
Тестовая выгрузка лидов за сегодняшний день из одной Google-таблицы.

Скрипт не затрагивает рабочую базу baltlease_data.db и основной пайплайн:
- читает одну таблицу Google Sheets;
- сохраняет данные только в отдельную тестовую SQLite-базу;
- создаёт CSV в корне проекта только для ещё не выгруженных строк;
- отправляет CSV в Telegram и на почту;
- помечает строки выгруженными только после успешной отправки email.
"""

import csv
import os
import sqlite3
import sys
from datetime import datetime
from typing import List, Optional, Tuple

import pytz
from dotenv import load_dotenv

from export_selected_to_sqlite import (
    SHEET_NAME,
    create_sheets_service,
    ensure_db_schema,
    find_header_indexes,
    get_sheet_values,
    normalize_datetime,
    normalize_int,
    normalize_utm_campaign,
    upsert_rows,
)
from email_sender import send_email_with_attachment_with_retries
from export_to_excel import send_document_with_retries, send_text_message


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEST_DB_FILENAME = "baltlease_test_today.db"
TEST_DB_PATH = os.path.join(BASE_DIR, TEST_DB_FILENAME)
DEFAULT_DIRECTION = "транспорт"
MOSCOW_TZ = pytz.timezone("Europe/Moscow")

CSV_HEADERS = [
    "Стадия сделки",
    "Телефон (мобильный)",
    "Тип источника (FN)",
    "Источник (FN)",
    "Ответственный",
    "Ответственный (Телемаркетинг)",
    "Комментарий",
]
DEAL_STAGE = "Разобрать/Новая"
SOURCE_TYPE_FN = "Телемаркетинг"
SOURCE_FN = "FNG Лиды от LeadRecord"
RESPONSIBLE = "bitrixbot"
TELEMARKETING_RESPONSIBLE = "bitrixbot"


DbRow = Tuple[int, Optional[str], Optional[str], Optional[str], str, str, str, str]
CsvRow = Tuple[str, str]


def get_spreadsheet_id() -> str:
    """
    Берёт ту же таблицу, которая в основном пайплайне используется
    для направления "транспорт".
    """
    spreadsheet_id = os.getenv("SPREADSHEET_ID_149")
    if not spreadsheet_id:
        raise ValueError("Не задана переменная SPREADSHEET_ID_149.")
    return spreadsheet_id


def is_today_by_sheet_date(event_at: Optional[str], today_iso: str) -> bool:
    """
    Проверяет, что дата из Google-таблицы относится к сегодняшнему дню.
    Важно: фильтруем именно по столбцу 'Дата', а не по времени загрузки в БД.
    """
    if not event_at:
        return False

    try:
        event_dt = datetime.strptime(event_at, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return False

    return event_dt.date().isoformat() == today_iso


def collect_today_rows(
    values: List[List[str]],
    direction: str,
    today_iso: str,
) -> Tuple[List[DbRow], int, int]:
    """
    Преобразует строки Google Sheets в формат БД.

    Returns:
        rows_to_upsert: валидные строки за сегодня;
        found_today_count: сколько строк в таблице имеют дату сегодняшнего дня;
        skipped_invalid_count: сколько сегодняшних строк пропущено из-за некорректного ID.
    """
    if not values:
        return [], 0, 0

    headers = values[0]
    data_rows = values[1:] if len(values) > 1 else []
    indexes = find_header_indexes(headers, ["ID", "Номера", "Источник", "Дата"])

    now_str = datetime.now(MOSCOW_TZ).strftime("%Y-%m-%d %H:%M:%S")
    rows_to_upsert: List[DbRow] = []
    found_today_count = 0
    skipped_invalid_count = 0

    for row in data_rows:
        id_val = row[indexes["ID"]] if indexes["ID"] < len(row) else None
        phone_val = row[indexes["Номера"]] if indexes["Номера"] < len(row) else None
        utm_val = row[indexes["Источник"]] if indexes["Источник"] < len(row) else None
        date_val = row[indexes["Дата"]] if indexes["Дата"] < len(row) else None

        event_at = normalize_datetime(date_val)
        if not is_today_by_sheet_date(event_at, today_iso):
            continue

        found_today_count += 1
        source_id = normalize_int(id_val)
        if source_id is None:
            skipped_invalid_count += 1
            continue

        phone = str(phone_val).strip() if phone_val is not None and str(phone_val).strip() else None
        utm_campaign = normalize_utm_campaign(utm_val)
        rows_to_upsert.append(
            (source_id, phone, utm_campaign, event_at, direction, "Пасивный", now_str, now_str)
        )

    return rows_to_upsert, found_today_count, skipped_invalid_count


def fetch_unexported_today_rows(
    conn: sqlite3.Connection,
    today_iso: str,
) -> Tuple[List[CsvRow], List[int]]:
    """
    Возвращает строки за сегодня, которые ещё не попадали в тестовый CSV.
    Признак уже выгруженной строки — заполненное поле sent_at в тестовой БД.
    """
    cur = conn.execute(
        """
        SELECT row_id, phone, utm_campaign
        FROM leads
        WHERE DATE(event_at) = ?
          AND (sent_at IS NULL OR sent_at = '')
        ORDER BY row_id
        """,
        (today_iso,),
    )

    rows: List[CsvRow] = []
    row_ids: List[int] = []
    for row_id, phone, utm_campaign in cur.fetchall():
        row_ids.append(int(row_id))
        rows.append((phone or "", utm_campaign or ""))

    return rows, row_ids


def mark_rows_as_exported(conn: sqlite3.Connection, row_ids: List[int], sent_at: str) -> None:
    """
    Помечает строки как уже выгруженные в тестовый файл.
    """
    if not row_ids:
        return

    placeholders = ",".join("?" for _ in row_ids)
    conn.execute(
        f"""
        UPDATE leads
        SET sent_at = ?
        WHERE row_id IN ({placeholders})
          AND (sent_at IS NULL OR sent_at = '')
        """,
        (sent_at, *row_ids),
    )
    conn.commit()


def save_csv(rows: List[CsvRow]) -> str:
    """
    Создаёт CSV в формате клиентского шаблона.
    utf-8-sig помогает Excel корректно открыть кириллицу.
    """
    timestamp = datetime.now(MOSCOW_TZ).strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"LeadRecord_FNG_{timestamp}.csv"
    file_path = os.path.join(BASE_DIR, filename)

    with open(file_path, "w", encoding="utf-8-sig", newline="") as file:
        writer = csv.writer(file, delimiter=";", lineterminator="\n")
        writer.writerow(CSV_HEADERS)
        for phone, comment in rows:
            writer.writerow(
                [
                    DEAL_STAGE,
                    phone,
                    SOURCE_TYPE_FN,
                    SOURCE_FN,
                    RESPONSIBLE,
                    TELEMARKETING_RESPONSIBLE,
                    comment,
                ]
            )

    return file_path


def send_to_telegram(file_path: str, rows_count: int) -> bool:
    """
    Отправляет тестовый CSV в Telegram.
    Telegram не является обязательным каналом: ошибка не блокирует выгрузку.
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN_ASSISTANT")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("Telegram не отправлен: не заданы TELEGRAM_BOT_TOKEN_ASSISTANT или TELEGRAM_CHAT_ID.")
        return False

    caption = f"Загружено новых идентификаторов: {rows_count}"
    return send_document_with_retries(
        token=token,
        chat_id=chat_id,
        file_path=file_path,
        caption=caption,
        max_retries=3,
    )


def send_to_email(file_path: str, rows_count: int) -> Tuple[bool, str]:
    """
    Отправляет тестовый CSV на почту через настройки из .env.
    Главный критерий успешной выгрузки — именно успешная отправка email.
    """
    filename = os.path.basename(file_path)
    subject = f"Новые идентификаторы LeadRecord_FNG: {rows_count}"
    body = (
        f"Загружено новых идентификаторов: {rows_count}\n"
        f"Во вложении файл: {filename}"
    )
    return send_email_with_attachment_with_retries(
        subject=subject,
        body=body,
        attachment_path=file_path,
        max_retries=5,
    )


def send_email_status_to_telegram(email_ok: bool, email_status_text: str) -> bool:
    """
    Отправляет отдельное сообщение со статусом email, как в основном экспорте.
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN_ASSISTANT")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return False

    text = (
        email_status_text
        if email_ok
        else f"Письмо на почту не отправлено. Причина: {email_status_text}"
    )
    return send_text_message(token, chat_id, text)


def remove_file_after_success(file_path: str) -> None:
    """
    Удаляет CSV после успешной отправки email.
    """
    try:
        os.remove(file_path)
        print(f"CSV-файл удалён после успешной отправки email: {file_path}")
    except OSError as exc:
        print(f"Предупреждение: не удалось удалить CSV-файл {file_path}: {exc}")


def main() -> None:
    os.chdir(BASE_DIR)
    load_dotenv(os.path.join(BASE_DIR, ".env"))

    try:
        spreadsheet_id = get_spreadsheet_id()
    except ValueError as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)

    direction = os.getenv("TEST_DIRECTION", DEFAULT_DIRECTION)
    today_iso = datetime.now(MOSCOW_TZ).date().isoformat()

    print("Тестовая выгрузка за сегодня")
    print(f"Дата фильтрации: {today_iso}")
    print(f"Лист Google Sheets: {SHEET_NAME}")
    print(f"Тестовая база: {TEST_DB_PATH}")

    service = create_sheets_service()
    values = get_sheet_values(service, spreadsheet_id, SHEET_NAME)
    rows_to_upsert, found_today_count, skipped_invalid_count = collect_today_rows(
        values=values,
        direction=direction,
        today_iso=today_iso,
    )

    with sqlite3.connect(TEST_DB_PATH) as conn:
        ensure_db_schema(conn)
        saved_count = upsert_rows(conn, rows_to_upsert) if rows_to_upsert else 0
        csv_rows, exported_row_ids = fetch_unexported_today_rows(conn, today_iso)

        print(f"Строк найдено за сегодня в Google Sheets: {found_today_count}")
        print(f"Строк сохранено/обновлено в тестовой БД: {saved_count}")
        if skipped_invalid_count:
            print(f"Строк пропущено из-за некорректного ID: {skipped_invalid_count}")

        if not csv_rows:
            print("Новых строк для CSV нет. Файл не создан.")
            return

        file_path = save_csv(csv_rows)

    print(f"CSV-файл создан: {file_path}")
    print(f"Строк в CSV: {len(csv_rows)}")

    telegram_ok = send_to_telegram(file_path, len(csv_rows))
    print(f"Telegram: {'отправлен' if telegram_ok else 'не отправлен'}")

    email_ok, email_status_text = send_to_email(file_path, len(csv_rows))
    print(f"Email: {email_status_text}")

    if telegram_ok:
        status_message_ok = send_email_status_to_telegram(email_ok, email_status_text)
        print(f"Статус email в Telegram: {'отправлен' if status_message_ok else 'не отправлен'}")

    if not email_ok:
        print("Email не отправлен. Строки не помечены выгруженными, CSV-файл оставлен для проверки.")
        sys.exit(1)

    sent_at = datetime.now(MOSCOW_TZ).strftime("%Y-%m-%d %H:%M:%S")
    with sqlite3.connect(TEST_DB_PATH) as conn:
        mark_rows_as_exported(conn, exported_row_ids, sent_at)

    print("Строки помечены как выгруженные в тестовой БД после успешного email.")
    remove_file_after_success(file_path)


if __name__ == "__main__":
    main()
