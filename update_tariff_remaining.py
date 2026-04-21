"""
Простой скрипт для ручного обновления остатка по тарифу в SQLite базе.

Как использовать:
1) Поменяйте значение переменной NEW_TARIFF_REMAINING ниже
2) Запустите: python update_tariff_remaining.py

Скрипт создаст таблицу tariff_state при необходимости и запишет новое значение.
"""

import sqlite3


DB_FILENAME = 'baltlease_data.db'
TARIFF_TABLE = 'tariff_state'

# Задайте здесь нужный остаток по тарифу (целое число)
NEW_TARIFF_REMAINING = 6940


def ensure_tariff_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TARIFF_TABLE} (
            id INTEGER PRIMARY KEY CHECK(id = 1),
            remaining INTEGER NOT NULL,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()


def get_current_remaining(conn: sqlite3.Connection) -> int | None:
    cur = conn.execute(f"SELECT remaining FROM {TARIFF_TABLE} WHERE id = 1")
    row = cur.fetchone()
    return int(row[0]) if row else None


def set_tariff_remaining(conn: sqlite3.Connection, value: int) -> None:
    safe_value = int(value)
    conn.execute(
        f"""
        INSERT INTO {TARIFF_TABLE}(id, remaining, updated_at)
        VALUES (1, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(id) DO UPDATE SET remaining=excluded.remaining, updated_at=CURRENT_TIMESTAMP
        """,
        (safe_value,),
    )
    conn.commit()


def main() -> None:
    with sqlite3.connect(DB_FILENAME) as conn:
        ensure_tariff_table(conn)
        before = get_current_remaining(conn)
        set_tariff_remaining(conn, NEW_TARIFF_REMAINING)
        after = get_current_remaining(conn)

    print(f"Остаток по тарифу обновлён: {before if before is not None else '—'} -> {after}")


if __name__ == '__main__':
    main()


