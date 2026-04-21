# Baltlease File to Telegram

Проект для автоматизации экспорта данных из Google Sheets в Excel-файл и отправки в Telegram.

## Что делает проект

1. **Экспорт из Google Sheets в SQLite**: `export_selected_to_sqlite.py` загружает данные из таблиц Google Sheets (ID из `.env`), фильтрует по дате (последние 2 суток), сохраняет в локальную базу `baltlease_data.db`.

2. **Формирование Excel и отправка в Telegram и на почту**: `export_to_excel.py` создаёт Excel-файл с новыми данными, отправляет его в Telegram (основной канал) и на email (UniSender SMTP). В чат дополнительно приходит сообщение об успехе или ошибке почты с указанием адресов получателей. Файл удаляется после успешной отправки в Telegram.

3. **Обновление отчётной таблицы**: `update_report_sheet.py` обновляет отчётную Google-таблицу (ID из `.env` -> SPREADSHEET_REPORT) с количеством отправленных лидов по UTM-кампаниям и направлениям за последние N дня. Google-таблица заполняется **отдельно за каждый день**. Вот как это работает:

## Логика заполнения по дням на примере интервала 15 дней (range(14, -1, -1))

1. **Создание списка дат:**
   ```python
   target_dates = [today_msk - timedelta(days=i) for i in range(14, -1, -1)]
   ```
   Это даёт 15 дат: от `(сегодня - 14 дней)` до `сегодня` включительно.
   ```python
   range(14, -1, -1)
   ```
   Это функция range(), которая создаёт последовательность чисел. Параметры:
   14 — начало (стартовое число)
   -1 — конец (до какого числа, но не включая его)
   -1 — шаг (каждый раз вычитаем 1)
   Результат: числа 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0

2. **Загрузка данных из БД:**
   Функция `load_counts_from_db()` выбирает все лиды за эти даты и группирует их:
   - По UTM-кампаниям
   - По направлениям  
   - **По датам отправки** (отдельно для каждого дня)

3. **Заполнение таблицы:**
   В функции `build_updates_for_sheet()` для каждой UTM-кампании и каждого дня из списка `target_dates` записывается отдельное значение в соответствующую ячейку таблицы.

## Структура таблицы
Таблица выглядит примерно так:
```
A          B           C          D          E          ...
UTM        Направление  15.12.2025  16.12.2025  17.12.2025  ...
campaign1  direction1   5           3           7           ...
campaign2  direction2   2           8           1           ...
```

**Каждый столбец соответствует отдельному дню**, и данные за этот день записываются независимо от других дней. Если за какой-то день данных нет — ставится 0.

Если нужно изменить период (например, не 15 дней, а меньше/больше) — достаточно поменять параметры в `range()`. 😊

4. **Логи**: Все логи пишутся в `logs/all.log` с ротацией в 00:00 локального времени.

## Развертывание на сервере (Ubuntu)

### 1. Клонирование репозитория
```bash
git clone <ваш-репозиторий>
cd Fn_file_to_tg
```

### 2. Установка Python и зависимостей
```bash
# Установите Python 3.13 (или совместимую версию)
sudo apt update
sudo apt install python3 python3-pip python3-venv

# Создайте виртуальное окружение
python3 -m venv venv
source venv/bin/activate

# Установите зависимости
pip install -r requirements.txt
```

### 3. Настройка переменных окружения
Создайте `.env` файл на основе примера:
```bash
# Google Sheets credentials
GOOGLE_CREDENTIALS_FILE=credentials/sheets-data-bot-b8f4cc6634fc.json

SPREADSHEET_ID_128=<YOUR_SPREADSHEET_ID_128>
SPREADSHEET_ID_149=<YOUR_SPREADSHEET_ID_149>
SPREADSHEET_REPORT=<YOUR_REPORT_SPREADSHEET_ID>

# Telegram Bot
TELEGRAM_BOT_TOKEN_ASSISTANT=<YOUR_BOT_TOKEN>
TELEGRAM_CHAT_ID=<YOUR_CHAT_ID>

# Email: переключатель провайдера (unisender или yandex)
EMAIL_PROVIDER=unisender

# UniSender (при EMAIL_PROVIDER=unisender)
UNIS_SMTP_HOST=smtp.go2.unisender.ru
UNIS_SMTP_PORT=587
UNIS_SMTP_USERNAME=<YOUR_SMTP_USERNAME>
UNIS_SMTP_PASSWORD=<YOUR_SMTP_PASSWORD>
UNIS_FROM_EMAIL=<YOUR_FROM_EMAIL>

# Yandex (при EMAIL_PROVIDER=yandex)
SMTP_SERVER=smtp.yandex.com
SMTP_PORT=465
YANDEX_EMAIL=<YOUR_YANDEX_EMAIL>
YANDEX_APP_PASSWORD=<YOUR_APP_PASSWORD>

# Получатели (общие для обоих провайдеров)
UNIS_TO_EMAIL=email1@example.com,email2@example.com
```

- Поместите `sheets-data-bot-b8f4cc6634fc.json` в папку `credentials/`.
- `EMAIL_PROVIDER` — `unisender` или `yandex`. От этого зависит, какой SMTP используется.
- `UNIS_TO_EMAIL` — один или несколько адресов через запятую (например: `a@mail.ru,b@mail.ru`).
- Убедитесь, что `.env` в `.gitignore` и не коммитится.

### 4. Первичный запуск (проверка)
```bash
source venv/bin/activate
python export_selected_to_sqlite.py  # Загрузка данных из Sheets
python export_to_excel.py           # Формирование Excel, отправка в TG и на почту
```

## Запуск по расписанию (cron на Ubuntu)

### Настройка cron
1. Откройте crontab: `crontab -e`

2. Добавьте задачи (примеры). Время указано по МСК (UTC+3), скорректируйте для вашего сервера:
строка запустит main.py по МСК в пн–пт в 09:42, 11:42, 13:42, 15:42 и выведет логи в файл.
```bash
# ==== FN_FILE_TO_TG SCHEDULE ====
CRON_TZ=Europe/Moscow
42 9,11,13,15 * * 1-5 cd /opt/Fn_file_to_tg && /opt/Fn_file_to_tg/venv/bin/python /opt/Fn_file_to_tg/main.py >> /opt/Fn_file_to_tg/logs/cron.log 2>&1
```

- Замените `/opt/Fn_file_to_tg` на реальный путь к проекту.
- Если сервер в UTC, используйте указанные времена; для другого часового пояса скорректируйте.
- Логи проверяйте в `logs/all.log`.

### Проверка cron
- Просмотр задач: `crontab -l`
- Журнал cron: `grep CRON /var/log/syslog`

## Структура проекта
- `export_selected_to_sqlite.py`: Экспорт из Sheets в DB.
- `export_to_excel.py`: Excel + Telegram + email.
- `email_sender.py`: Отправка писем с вложением через UniSender SMTP.
- `update_report_sheet.py`: Обновление отчётной таблицы Google Sheets.
- `logging_setup.py`: Настройка логов.
- `send_test_email_unisender.py`: Тест UniSender (при `EMAIL_PROVIDER=unisender`, нужен `TEST_EMAIL_ATTACHMENT_PATH`).
- `send_test_email_yandex.py`: Тест Yandex (при `EMAIL_PROVIDER=yandex`, нужен `TEST_EMAIL_ATTACHMENT_PATH`).
- `requirements.txt`: Зависимости.
- `logs/`: Логи (создаётся автоматически).
- `credentials/`: Google credentials.
- `.env`: Переменные окружения.