"""
Оркестратор запуска:
1) export_selected_to_sqlite.py — загрузка данных в SQLite из Google Sheets
2) export_to_excel.py — формирование Excel и отправка в Telegram
3) update_report_sheet.py — обновление отчётной Google-таблицы по отправленным лидам
"""

import os
import sys
import subprocess

from logging_setup import configure_logging


def run_script(script_path: str) -> int:
    """
    Запускает указанный Python-скрипт отдельным процессом.
    Возвращает код возврата процесса (0 при успехе).
    """
    logger.info(f"Запуск скрипта: {script_path}")
    result = subprocess.run([sys.executable, script_path], cwd=os.path.dirname(script_path))
    if result.returncode != 0:
        logger.error(f"Скрипт завершился с ошибкой (код {result.returncode}): {script_path}")
    else:
        logger.info(f"Скрипт успешно завершён: {script_path}")
    return result.returncode


def main() -> None:
    global logger
    logger = configure_logging('main')

    base_dir = os.path.dirname(os.path.abspath(__file__))
    sqlite_script = os.path.join(base_dir, 'export_selected_to_sqlite.py')
    excel_script = os.path.join(base_dir, 'export_to_excel.py')
    report_script = os.path.join(base_dir, 'update_report_sheet.py')

    # Проверим наличие файлов
    missing = [p for p in (sqlite_script, excel_script, report_script) if not os.path.isfile(p)]
    if missing:
        for p in missing:
            logger.error(f"Не найден файл: {p}")
        sys.exit(1)

    # 1) Экспорт в SQLite
    code = run_script(sqlite_script)
    if code != 0:
        sys.exit(code)

    # 2) Экспорт в Excel и отправка в Telegram
    code = run_script(excel_script)
    if code != 0:
        sys.exit(code)

    # 3) Обновление отчётной Google-таблицы
    code = run_script(report_script)
    sys.exit(code)


if __name__ == '__main__':
    main()


