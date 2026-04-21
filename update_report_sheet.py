"""
Обновление отчётной Google-таблицы по отправленным лидам.

Что делает скрипт:
- Берёт данные из SQLite БД baltlease_data.db (таблица leads, поле sent_at).
- Считает по каждому UTM_CAMPAIGN и соответствующему направлению
  (с той же логикой мед/агро по доменам, что и в export_to_excel.py)
  количество лидов за каждый день.
- В отчётной таблице (ID берётся из .env -> SPREADSHEET_REPORT,
  лист "Общий отчет"):
    * строка 1: в колонках, начиная с D, стоят даты;
    * строки, начиная со 2-й: A — UTM_CAMPAIGN, B — Направление.
- Скрипт находит в шапке последние 2 календарных дня (сегодня и вчера по МСК),
  если они есть в строке 1, и пересчитывает по ним значения:
    * для каждой пары (UTM, Направление) заполняет/обновляет ячейки с количеством лидов;
    * если UTM ещё нет — создаёт новую строку сразу после последней
      заполненной по UTM (последняя непустая строка в колонке A/B).

Колонку "Выдано итого" (C) скрипт не трогает — её можно задать формулой вручную.
"""

import logging
import os
import sqlite3
from datetime import datetime, timedelta, date
from typing import Dict, List, Tuple, Optional

import pytz
from dotenv import load_dotenv
from googleapiclient.discovery import build
from google.oauth2 import service_account

from logging_setup import configure_logging
from export_to_excel import _utm_matches_med_domain, _utm_matches_agro_domain


logger = configure_logging("update_report_sheet")

DB_FILENAME = "baltlease_data.db"
# Имя листа
SHEET_NAME = "Общий отчет"


def create_sheets_service():
    """
    Создаёт сервис Google Sheets API с правами на чтение/запись.
    """
    credentials_file = os.getenv("GOOGLE_CREDENTIALS_FILE")
    if not credentials_file or not os.path.exists(credentials_file):
        raise FileNotFoundError(f"Файл credentials не найден: {credentials_file}")

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = service_account.Credentials.from_service_account_file(
        credentials_file, scopes=scopes
    )
    service = build("sheets", "v4", credentials=credentials)
    logger.info("Сервис Google Sheets API для отчёта успешно создан")
    return service


def column_index_to_letter(index: int) -> str:
    """
    Конвертирует 0-based индекс колонки в букву (0 -> A, 1 -> B, ...).
    """
    result = ""
    idx = index
    while idx >= 0:
        idx, remainder = divmod(idx, 26)
        result = chr(ord("A") + remainder) + result
        idx -= 1
    return result


def format_date_for_header(d: date) -> str:
    """
    Форматирует дату для заголовка столбца.
    Используем формат, как в отчётной таблице: DD.MM.YY.
    """
    return d.strftime("%d.%m.%y")


def ensure_header_dates_exist(
    service,
    spreadsheet_id: str,
    sheet_name: str,
    header_row: List[str],
    target_dates: List[date],
) -> Tuple[List[str], Dict[date, int]]:
    """
    Гарантирует, что в первой строке есть столбцы для всех target_dates.
    Если даты нет — добавляет новый заголовок в конец строки.

    Возвращает:
        (обновлённый header_row, словарь date_to_col).
    """
    # Текущее распределение дат по колонкам
    date_to_col = parse_header_dates(header_row)

    modified = False
    for d in target_dates:
        if d in date_to_col:
            continue
        # Дата отсутствует — добавляем новый столбец
        header_row.append(format_date_for_header(d))
        modified = True

    if modified:
        # Обновляем шапку (строка 1) ровно в нужном диапазоне.
        # Важно: не используем range "1:1", чтобы случайно не затронуть
        #/не очистить ячейки правее конца нашей шапки.
        end_col_letter = column_index_to_letter(len(header_row) - 1)
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'!A1:{end_col_letter}1",
            valueInputOption="USER_ENTERED",
            body={"values": [header_row]},
        ).execute()
        # Пересчитаем словарь дат с учётом добавленных столбцов
        date_to_col = parse_header_dates(header_row)

    return header_row, date_to_col


def get_sheet_row_count(service, spreadsheet_id: str, sheet_name: str) -> Tuple[int, int]:
    """
    Возвращает (sheet_id, текущее_количество_строк) для указанного листа.
    """
    meta = (
        service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id)
        .execute()
    )
    for sheet in meta.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == sheet_name:
            sheet_id = int(props.get("sheetId"))
            grid = props.get("gridProperties", {}) or {}
            row_count = int(grid.get("rowCount", 1000))
            return sheet_id, row_count
    raise ValueError(f"Лист '{sheet_name}' не найден в таблице")


def ensure_row_capacity(
    service, spreadsheet_id: str, sheet_name: str, needed_rows: int
) -> None:
    """
    Обеспечивает наличие не менее needed_rows строк на листе.
    Если строк меньше — увеличивает rowCount через batchUpdate.
    """
    if needed_rows <= 0:
        return

    sheet_id, current_rows = get_sheet_row_count(service, spreadsheet_id, sheet_name)
    if current_rows >= needed_rows:
        return

    body = {
        "requests": [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "gridProperties": {
                            "rowCount": needed_rows,
                        },
                    },
                    "fields": "gridProperties.rowCount",
                }
            }
        ]
    }
    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=body,
    ).execute()


def parse_header_dates(header_row: List[str]) -> Dict[date, int]:
    """
    Разбирает даты из первой строки отчётного листа, начиная с колонки D.

    Returns:
        dict: {date: column_index}
    """
    date_to_col: Dict[date, int] = {}
    # Начиная с D (индекс 3)
    for col_idx in range(3, len(header_row)):
        cell = header_row[col_idx]
        if not cell:
            continue
        text = str(cell).strip()
        parsed: Optional[date] = None
        # Поддерживаем несколько форматов, основной — DD.MM.YY
        for fmt in ("%d.%m.%y", "%d.%m.%Y", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(text, fmt).date()
                break
            except ValueError:
                continue
        if parsed is None:
            continue
        date_to_col[parsed] = col_idx
    return date_to_col


def load_existing_rows(
    service, spreadsheet_id: str
) -> Tuple[Dict[str, int], int]:
    """
    Загружает существующие строки с UTM/Направлением.

    Returns:
        utm_to_row: dict UTM -> номер строки (1-based)
        next_free_row: первая строка (1-based) после последней непустой по A/B.

    Примечание: данные начинаются с 3-й строки (A3, B3), 2-я строка
    зарезервирована под формулы.
    """
    # Читаем существующие UTM/Направление, начиная с 3-й строки
    range_a_b = f"'{SHEET_NAME}'!A3:B"
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_a_b)
        .execute()
    )
    values: List[List[str]] = result.get("values", [])

    utm_to_row: Dict[str, int] = {}
    last_non_empty_idx: int = -1

    # idx = 0 соответствует строке 3, idx = 1 -> строка 4 и т.д.
    for idx, row in enumerate(values):
        utm = row[0].strip() if len(row) > 0 and row[0] else ""
        direction = row[1].strip() if len(row) > 1 and row[1] else ""
        if utm or direction:
            last_non_empty_idx = idx
        if utm:
            # +3, т.к. диапазон начинается с A3
            utm_to_row[utm] = idx + 3

    # Строка после последней непустой (учитывая, что idx 0 -> строка 3)
    next_free_row = (last_non_empty_idx + 3) + 1 if last_non_empty_idx >= 0 else 3
    return utm_to_row, next_free_row


def determine_effective_direction(utm_campaign: str, base_direction: str) -> str:
    """
    Определяет направление так же, как при выгрузке в Excel.
    """
    utm = utm_campaign or ""
    if _utm_matches_med_domain(utm):
        return "Мед оборудование"
    if _utm_matches_agro_domain(utm):
        return "Сельхозтехника"
    return base_direction or ""


def load_counts_from_db(
    target_dates: List[date],
) -> Dict[Tuple[str, str], Dict[date, int]]:
    """
    Загружает из БД количество лидов по (UTM, Направление, Дата отправки).

    target_dates: список дат (обычно [вчера, сегодня]) по МСК.

    Returns:
        counts[(utm, direction)][date] = count
    """
    if not os.path.exists(DB_FILENAME):
        raise FileNotFoundError(f"База не найдена: {DB_FILENAME}")

    if not target_dates:
        return {}

    min_date = min(target_dates)
    max_date = max(target_dates)

    counts: Dict[Tuple[str, str], Dict[date, int]] = {}

    with sqlite3.connect(DB_FILENAME) as conn:
        cur = conn.execute(
            """
            SELECT utm_campaign, direction, sent_at
            FROM leads
            WHERE sent_at IS NOT NULL
              AND DATE(sent_at) BETWEEN ? AND ?
            """,
            (min_date.isoformat(), max_date.isoformat()),
        )
        rows = cur.fetchall()

    for utm_campaign, base_direction, sent_at in rows:
        if not utm_campaign:
            continue  # отчёт строим только по непустым UTM
        try:
            dt = datetime.strptime(str(sent_at), "%Y-%m-%d %H:%M:%S").date()
        except Exception:
            continue
        if dt not in target_dates:
            continue

        effective_direction = determine_effective_direction(
            str(utm_campaign).strip(), str(base_direction or "").strip()
        )
        key = (str(utm_campaign).strip(), effective_direction)
        if key not in counts:
            counts[key] = {}
        counts[key][dt] = counts[key].get(dt, 0) + 1

    return counts


def build_updates_for_sheet(
    counts: Dict[Tuple[str, str], Dict[date, int]],
    date_to_col: Dict[date, int],
    target_dates: List[date],
    utm_to_row: Dict[str, int],
    next_free_row: int,
) -> Tuple[List[Dict[str, object]], int]:
    """
    Формирует набор обновлений для batchUpdate.

    Returns:
        updates: список объектов для data в values.batchUpdate
        new_next_free_row: новая следующая свободная строка
    """
    updates: List[Dict[str, object]] = []
    new_row = next_free_row

    for (utm, direction), per_date in counts.items():
        if not utm:
            continue

        # Найдём или создадим строку
        row_num = utm_to_row.get(utm)
        if row_num is None:
            row_num = new_row
            new_row += 1
            utm_to_row[utm] = row_num
            # Записываем UTM и направление
            updates.append(
                {
                    "range": f"'{SHEET_NAME}'!A{row_num}",
                    "values": [[utm]],
                }
            )
            updates.append(
                {
                    "range": f"'{SHEET_NAME}'!B{row_num}",
                    "values": [[direction]],
                }
            )

        # Обновляем значения по датам
        for d in target_dates:
            col_idx = date_to_col.get(d)
            if col_idx is None:
                continue
            value = per_date.get(d, 0)
            col_letter = column_index_to_letter(col_idx)
            updates.append(
                {
                    "range": f"'{SHEET_NAME}'!{col_letter}{row_num}",
                    "values": [[value]],
                }
            )

    return updates, new_row


def main() -> None:
    load_dotenv()

    spreadsheet_id = os.getenv("SPREADSHEET_REPORT")
    if not spreadsheet_id:
        logger.error("Переменная окружения SPREADSHEET_REPORT не задана.")
        return

    # Даты для отчёта: последние 3 дня (включая сегодня) по Москве
    msk = pytz.timezone("Europe/Moscow")
    today_msk = datetime.now(msk).date()
    target_dates = [today_msk - timedelta(days=i) for i in range(2, -1, -1)]

    # Загружаем шапку и существующие строки из Google Sheets
    service = create_sheets_service()

    header_result = (
        service.spreadsheets()
        .values()
        # Берём всю строку заголовков (строка 1). Не ограничиваемся A1:Z,
        # чтобы корректно работать, когда даты уходят правее колонки Z (AA, AB, ...).
        .get(
            spreadsheetId=spreadsheet_id,
            range=f"'{SHEET_NAME}'!1:1",
        )
        .execute()
    )
    header_values: List[List[str]] = header_result.get("values", [])
    if not header_values:
        logger.error("Первая строка отчётного листа пуста — нет заголовков с датами.")
        return
    header_row = header_values[0]

    # Обеспечиваем наличие столбцов для нужных дат (если их нет — добавляем)
    header_row, date_to_col = ensure_header_dates_exist(
        service=service,
        spreadsheet_id=spreadsheet_id,
        sheet_name=SHEET_NAME,
        header_row=header_row,
        target_dates=target_dates,
    )
    if not date_to_col:
        logger.error("В строке заголовков не найдено ни одной корректной даты.")
        return

    # Оставляем только те целевые даты, которые реально есть в шапке
    effective_dates = [d for d in target_dates if d in date_to_col]
    if not effective_dates:
        logger.info(
            "Ни одной из дат (вчера/сегодня) нет в шапке отчёта — обновлять нечего."
        )
        return

    utm_to_row, next_free_row = load_existing_rows(service, spreadsheet_id)

    # Считаем данные из БД
    counts = load_counts_from_db(effective_dates)
    if not counts:
        logger.info("В БД нет данных по отправленным лидам за выбранные даты.")
        return

    # Строим обновления для таблицы
    updates, new_next_free_row = build_updates_for_sheet(
        counts=counts,
        date_to_col=date_to_col,
        target_dates=effective_dates,
        utm_to_row=utm_to_row,
        next_free_row=next_free_row,
    )

    if not updates:
        logger.info("Нет обновлений для отчётной таблицы.")
        return

    # Максимальная строка, до которой мы будем писать данные
    max_row_used = max(utm_to_row.values() or [1])
    max_row_used = max(max_row_used, new_next_free_row - 1)

    # Расширяем лист при необходимости, чтобы не было ошибки "exceeds grid limits"
    ensure_row_capacity(service, spreadsheet_id, SHEET_NAME, max_row_used)

    body = {
        "valueInputOption": "RAW",
        "data": updates,
    }
    service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body
    ).execute()

    logger.info(
        "Отчётная таблица обновлена для дат: %s",
        ", ".join(d.isoformat() for d in effective_dates),
    )


if __name__ == "__main__":
    main()


