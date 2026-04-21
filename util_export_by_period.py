"""
Утилита: выгрузка лидов из БД в Excel за указанный период (только чтение из БД).

- Читает из таблицы leads по полю created_at за последние N дней.
- Создаёт файл в том же формате, что и export_to_excel (те же колонки и логика направлений).
- В БД ничего не записывает и не изменяет (ни export_state, ни sent_at, ни тариф).
"""

import os
import sys
import sqlite3
from datetime import datetime
from typing import List, Tuple

import pytz

# Общая конфигурация и построение книги — из основного скрипта экспорта
from export_to_excel import HEADERS, build_workbook


DB_FILENAME = 'baltlease_data.db'

# Период выгрузки: за сколько последних дней брать лиды из БД (можно менять)
PERIOD_DAYS = 4


def fetch_rows_for_period(conn: sqlite3.Connection, days: int) -> List[Tuple[str, str, str, str]]:
    """
    Возвращает строки для Excel за последние `days` дней по полю created_at.
    Строка: (phone, utm_campaign, direction, status).
    """
    cur = conn.execute(
        """
        SELECT phone, utm_campaign, direction, status
        FROM leads
        WHERE created_at >= datetime('now', '-' || ? || ' days')
        ORDER BY row_id
        """,
        (days,),
    )
    return cur.fetchall()


def main() -> None:
    if not os.path.exists(DB_FILENAME):
        print(f"Ошибка: база {DB_FILENAME} не найдена.", file=sys.stderr)
        sys.exit(1)

    days = PERIOD_DAYS
    if days < 1:
        print("Ошибка: PERIOD_DAYS должен быть >= 1.", file=sys.stderr)
        sys.exit(1)

    with sqlite3.connect(DB_FILENAME) as conn:
        rows = fetch_rows_for_period(conn, days)

    if not rows:
        print(f"Нет записей в БД за последние {days} дн. Файл не создаётся.")
        return

    wb = build_workbook(rows)
    ts = datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y%m%d_%H%M%S')
    filename = f"LeadRecord_FNG_period_{days}d_{ts}.xlsx"
    wb.save(filename)
    print(f"Создан файл: {filename} (записей: {len(rows)}, период: последние {days} дн.)")


if __name__ == '__main__':
    main()
