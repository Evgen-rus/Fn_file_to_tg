"""
Единая настройка логирования:
- Общий лог для всех скриптов: logs/all.log
Ротация раз в день в 00:00 по локальному времени, хранение нескольких копий.
"""

import logging
import os
from logging.handlers import TimedRotatingFileHandler
from datetime import time as daytime


def _ensure_logs_dir() -> str:
    logs_dir = os.path.join(os.getcwd(), 'logs')
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir, exist_ok=True)
    return logs_dir


def _build_handler(filepath: str) -> TimedRotatingFileHandler:
    # Ротация ежедневно в 00:00, храним 14 архивов
    handler = TimedRotatingFileHandler(
        filename=filepath,
        when='midnight',
        interval=1,
        backupCount=14,
        encoding='utf-8',
        utc=False,
        atTime=daytime(hour=0, minute=0),
    )
    # Формируем имена архивов с датой: file.log.YYYY-MM-DD
    handler.suffix = "%Y-%m-%d"
    formatter = logging.Formatter(
        fmt='%(asctime)s %(levelname)s [%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    handler.setFormatter(formatter)
    return handler


def configure_logging(script_name: str) -> logging.Logger:
    logs_dir = _ensure_logs_dir()

    logger_name = f"app.{script_name}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    # Избежать дублирования при повторной инициализации
    if logger.handlers:
        return logger

    # Общий лог всех скриптов
    common_path = os.path.join(logs_dir, 'all.log')
    common_handler = _build_handler(common_path)

    # Консоль
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter('%(asctime)s %(levelname)s [%(name)s] %(message)s', '%H:%M:%S'))

    logger.addHandler(common_handler)
    logger.addHandler(console)

    return logger


