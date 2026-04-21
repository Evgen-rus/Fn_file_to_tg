"""
Скрипт для изучения структуры данных в Google Sheets.
Анализирует таблицу, ID/URL которой вводится в консоли, и готовит схему для базы данных SQLite.
"""

import os
import sys
import logging
import datetime
import re
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from googleapiclient.discovery import build
from google.oauth2 import service_account

# Класс для дублирования вывода в файл и консоль
class TeeOutput:
    def __init__(self, *files):
        self.files = files
    
    def write(self, text):
        for file in self.files:
            file.write(text)
            file.flush()
    
    def flush(self):
        for file in self.files:
            file.flush()

# Настройка файла для логов и вывода
log_filename = f"table_explorer_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),  # Вывод в консоль
        logging.FileHandler(log_filename, encoding='utf-8')  # Запись в файл
    ]
)
logger = logging.getLogger(__name__)

# Перенаправляем stdout для записи всех print() в файл
original_stdout = sys.stdout
log_file = open(log_filename, 'a', encoding='utf-8')
sys.stdout = TeeOutput(original_stdout, log_file)

# Константы для Google Sheets API
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

def extract_spreadsheet_id(user_input: str) -> str:
    """
    Извлекает spreadsheetId из ввода пользователя.

    Поддерживает форматы:
    - Чистый ID (буквы/цифры/"-"/"_"), длиной от 20 символов
    - Полный URL вида https://docs.google.com/spreadsheets/d/<ID>/...

    Returns:
        Строка с ID или пустая строка, если распознать не удалось
    """
    if not user_input:
        return ""
    text = user_input.strip()

    # Пробуем извлечь ID из URL
    match = re.search(r"/spreadsheets/d/([A-Za-z0-9-_]+)", text)
    if match:
        return match.group(1)

    # Если это похоже на чистый ID
    if re.fullmatch(r"[A-Za-z0-9-_]{20,}", text):
        return text

    return ""

def create_sheets_service():
    """
    Создает и возвращает сервис Google Sheets API.
    
    Returns:
        Объект сервиса Google Sheets API
    """
    try:
        credentials_file = os.getenv('GOOGLE_CREDENTIALS_FILE')
        if not credentials_file or not os.path.exists(credentials_file):
            raise FileNotFoundError(f"Файл credentials не найден: {credentials_file}")
            
        credentials = service_account.Credentials.from_service_account_file(
            credentials_file, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=credentials)
        logger.info("Сервис Google Sheets API успешно создан")
        return service
    except Exception as e:
        logger.error(f"Ошибка при создании сервиса Sheets API: {e}")
        raise

def get_spreadsheet_info(service, spreadsheet_id: str) -> Dict[str, Any]:
    """
    Получает общую информацию о таблице.
    
    Args:
        service: Сервис Google Sheets API
        spreadsheet_id: ID таблицы
        
    Returns:
        Словарь с информацией о таблице
    """
    try:
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        
        info = {
            'title': spreadsheet.get('properties', {}).get('title', 'Без названия'),
            'spreadsheet_id': spreadsheet_id,
            'sheets': []
        }
        
        for sheet in spreadsheet['sheets']:
            sheet_props = sheet['properties']
            sheet_info = {
                'sheet_id': sheet_props.get('sheetId'),
                'title': sheet_props.get('title'),
                'index': sheet_props.get('index'),
                'sheet_type': sheet_props.get('sheetType', 'GRID'),
                'grid_properties': sheet_props.get('gridProperties', {})
            }
            info['sheets'].append(sheet_info)
            
        logger.info(f"Получена информация о таблице: '{info['title']}' с {len(info['sheets'])} вкладками")
        return info
        
    except Exception as e:
        logger.error(f"Ошибка при получении информации о таблице: {e}")
        raise

def get_sheet_data(service, spreadsheet_id: str, sheet_name: str) -> List[List]:
    """
    Получает все данные с указанной вкладки.
    
    Args:
        service: Сервис Google Sheets API
        spreadsheet_id: ID таблицы
        sheet_name: Название вкладки
        
    Returns:
        Список строк с данными
    """
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=sheet_name
        ).execute()
        
        data = result.get('values', [])
        logger.info(f"Получено {len(data)} строк данных с вкладки '{sheet_name}'")
        return data
        
    except Exception as e:
        logger.error(f"Ошибка при получении данных с вкладки '{sheet_name}': {e}")
        return []

def analyze_data_types(column_data: List[Any]) -> Dict[str, Any]:
    """
    Анализирует типы данных в столбце.
    
    Args:
        column_data: Список значений из столбца
        
    Returns:
        Словарь с информацией о типах данных
    """
    if not column_data:
        return {
            'suggested_type': 'TEXT',
            'total_values': 0,
            'empty_values': 0,
            'unique_values': 0,
            'type_distribution': {},
            'sample_values': []
        }
    
    # Фильтруем пустые значения для анализа
    non_empty_values = [v for v in column_data if v is not None and str(v).strip()]
    
    type_counts = {
        'integer': 0,
        'float': 0,
        'date': 0,
        'datetime': 0,
        'url': 0,
        'email': 0,
        'text': 0
    }
    
    # Паттерны для определения типов
    date_patterns = [
        r'^\d{4}-\d{2}-\d{2}$',  # YYYY-MM-DD
        r'^\d{2}\.\d{2}\.\d{4}$',  # DD.MM.YYYY
        r'^\d{2}/\d{2}/\d{4}$'   # DD/MM/YYYY
    ]
    
    datetime_patterns = [
        r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$',  # YYYY-MM-DD HH:MM:SS
        r'^\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2}$'  # DD.MM.YYYY HH:MM:SS
    ]
    
    url_pattern = r'^https?://'
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    for value in non_empty_values:
        str_value = str(value).strip()
        
        # Проверка на integer
        try:
            int(str_value)
            type_counts['integer'] += 1
            continue
        except ValueError:
            pass
            
        # Проверка на float
        try:
            float(str_value)
            type_counts['float'] += 1
            continue
        except ValueError:
            pass
            
        # Проверка на datetime
        is_datetime = any(re.match(pattern, str_value) for pattern in datetime_patterns)
        if is_datetime:
            type_counts['datetime'] += 1
            continue
            
        # Проверка на date
        is_date = any(re.match(pattern, str_value) for pattern in date_patterns)
        if is_date:
            type_counts['date'] += 1
            continue
            
        # Проверка на URL
        if re.match(url_pattern, str_value, re.IGNORECASE):
            type_counts['url'] += 1
            continue
            
        # Проверка на email
        if re.match(email_pattern, str_value):
            type_counts['email'] += 1
            continue
            
        # Остальное считаем текстом
        type_counts['text'] += 1
    
    # Определяем преобладающий тип
    if not non_empty_values:
        suggested_type = 'TEXT'
    else:
        max_type = max(type_counts, key=type_counts.get)
        max_count = type_counts[max_type]
        total_non_empty = len(non_empty_values)
        
        # Если преобладающий тип составляет более 80% значений
        if max_count / total_non_empty > 0.8:
            if max_type == 'integer':
                suggested_type = 'INTEGER'
            elif max_type == 'float':
                suggested_type = 'REAL'
            elif max_type in ['date', 'datetime']:
                suggested_type = 'DATETIME'
            else:
                suggested_type = 'TEXT'
        else:
            suggested_type = 'TEXT'
    
    # Получаем примеры значений
    sample_values = list(set(non_empty_values[:10])) if non_empty_values else []
    
    return {
        'suggested_type': suggested_type,
        'total_values': len(column_data),
        'empty_values': len(column_data) - len(non_empty_values),
        'unique_values': len(set(non_empty_values)) if non_empty_values else 0,
        'type_distribution': type_counts,
        'sample_values': sample_values
    }

def analyze_sheet_structure(data: List[List]) -> Dict[str, Any]:
    """
    Анализирует структуру данных на вкладке.
    
    Args:
        data: Данные с вкладки (список строк)
        
    Returns:
        Словарь с анализом структуры
    """
    if not data:
        return {
            'has_headers': False,
            'columns': [],
            'total_rows': 0,
            'max_columns': 0
        }
    
    # Определяем максимальное количество столбцов
    max_columns = max(len(row) for row in data) if data else 0
    
    # Предполагаем, что первая строка - это заголовки
    headers = data[0] if data else []
    data_rows = data[1:] if len(data) > 1 else []
    
    # Анализируем каждый столбец
    columns_analysis = []
    
    for col_index in range(max_columns):
        # Получаем название столбца
        column_name = headers[col_index] if col_index < len(headers) else f"Column_{col_index + 1}"
        
        # Собираем данные из этого столбца
        column_data = []
        for row in data_rows:
            if col_index < len(row):
                column_data.append(row[col_index])
            else:
                column_data.append(None)
        
        # Анализируем типы данных
        type_analysis = analyze_data_types(column_data)
        
        column_info = {
            'index': col_index,
            'name': column_name,
            'sql_name': sanitize_column_name(column_name),
            **type_analysis
        }
        
        columns_analysis.append(column_info)
        
        logger.info(f"Столбец {col_index}: '{column_name}' -> {type_analysis['suggested_type']} "
                   f"({type_analysis['unique_values']} уникальных из {type_analysis['total_values']})")
    
    return {
        'has_headers': len(headers) > 0,
        'headers': headers,
        'columns': columns_analysis,
        'total_rows': len(data),
        'data_rows': len(data_rows),
        'max_columns': max_columns
    }

def sanitize_column_name(name: str) -> str:
    """
    Преобразует название столбца в подходящее для SQL имя.
    
    Args:
        name: Исходное название столбца
        
    Returns:
        Очищенное название для SQL
    """
    if not name or not isinstance(name, str):
        return "unknown_column"
    
    # Удаляем или заменяем недопустимые символы
    clean_name = re.sub(r'[^\w\s]', '', name)  # Удаляем спецсимволы
    clean_name = re.sub(r'\s+', '_', clean_name)  # Пробелы на подчеркивания
    clean_name = clean_name.lower().strip('_')  # В нижний регистр и убираем крайние _
    
    # Если пустое или начинается с цифры, добавляем префикс
    if not clean_name or clean_name[0].isdigit():
        clean_name = f"col_{clean_name}" if clean_name else "unknown_column"
    
    return clean_name

def generate_sql_schema(sheet_name: str, analysis: Dict[str, Any]) -> str:
    """
    Генерирует SQL схему для создания таблицы на основе анализа.
    
    Args:
        sheet_name: Название вкладки
        analysis: Результат анализа структуры
        
    Returns:
        SQL код для создания таблицы
    """
    table_name = sanitize_column_name(sheet_name)
    
    sql_lines = [f"CREATE TABLE IF NOT EXISTS {table_name} ("]
    
    # Добавляем ID как первичный ключ
    sql_lines.append("    id INTEGER PRIMARY KEY AUTOINCREMENT,")
    
    # Добавляем столбцы на основе анализа
    for column in analysis['columns']:
        col_name = column['sql_name']
        col_type = column['suggested_type']
        
        # Определяем NOT NULL для столбцов с малым количеством пустых значений
        empty_ratio = column['empty_values'] / max(column['total_values'], 1)
        not_null = " NOT NULL" if empty_ratio < 0.1 and column['total_values'] > 0 else ""
        
        sql_lines.append(f"    {col_name} {col_type}{not_null},")
    
    # Добавляем метаданные
    sql_lines.extend([
        "    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,",
        "    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP"
    ])
    
    sql_lines.append(");")
    
    return "\n".join(sql_lines)

def explore_spreadsheet(spreadsheet_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Основная функция для полного анализа таблицы.
    
    Args:
        spreadsheet_id: ID таблицы 
        
    Returns:
        Полный анализ таблицы
    """
    # Загружаем переменные окружения
    load_dotenv()
    
    # Используем ID из параметра 
    if not spreadsheet_id:
        spreadsheet_id = "1sm0I_OBy1pQxgyoOWZjS....."
    
    logger.info(f"Начинаем анализ таблицы: {spreadsheet_id}")
    
    # Создаем сервис
    service = create_sheets_service()
    
    # Получаем общую информацию о таблице
    spreadsheet_info = get_spreadsheet_info(service, spreadsheet_id)
    
    # Анализируем каждую вкладку
    sheets_analysis = []
    
    for sheet_info in spreadsheet_info['sheets']:
        sheet_name = sheet_info['title']
        logger.info(f"\nАнализируем вкладку: '{sheet_name}'")
        
        # Получаем данные
        sheet_data = get_sheet_data(service, spreadsheet_id, sheet_name)
        
        # Анализируем структуру
        structure_analysis = analyze_sheet_structure(sheet_data)
        
        # Генерируем SQL схему
        sql_schema = generate_sql_schema(sheet_name, structure_analysis)
        
        sheet_analysis = {
            'sheet_info': sheet_info,
            'structure': structure_analysis,
            'sql_schema': sql_schema
        }
        
        sheets_analysis.append(sheet_analysis)
        
        # Выводим краткую сводку
        logger.info(f"Вкладка '{sheet_name}': {structure_analysis['data_rows']} строк данных, "
                   f"{structure_analysis['max_columns']} столбцов")
    
    # Формируем итоговый отчет
    full_analysis = {
        'spreadsheet_info': spreadsheet_info,
        'sheets_analysis': sheets_analysis,
        'summary': {
            'total_sheets': len(sheets_analysis),
            'analyzed_at': datetime.datetime.now().isoformat()
        }
    }
    
    return full_analysis

def print_analysis_report(analysis: Dict[str, Any]):
    """
    Выводит подробный отчет об анализе таблицы.
    
    Args:
        analysis: Результат анализа таблицы
    """
    print("\n" + "="*80)
    print("ОТЧЕТ АНАЛИЗА GOOGLE SHEETS ТАБЛИЦЫ")
    print("="*80)
    
    # Общая информация
    info = analysis['spreadsheet_info']
    print(f"\nТаблица: {info['title']}")
    print(f"ID: {info['spreadsheet_id']}")
    print(f"Количество вкладок: {len(info['sheets'])}")
    print(f"Время анализа: {analysis['summary']['analyzed_at']}")
    
    # Анализ каждой вкладки
    for sheet_analysis in analysis['sheets_analysis']:
        sheet_info = sheet_analysis['sheet_info']
        structure = sheet_analysis['structure']
        
        print(f"\n{'-'*60}")
        print(f"ВКЛАДКА: {sheet_info['title']}")
        print(f"{'-'*60}")
        print(f"Всего строк: {structure['total_rows']}")
        print(f"Строк данных: {structure['data_rows']}")
        print(f"Столбцов: {structure['max_columns']}")
        print(f"Есть заголовки: {'Да' if structure['has_headers'] else 'Нет'}")
        
        if structure['has_headers']:
            print(f"Заголовки: {', '.join(structure['headers'])}")
        
        print("\nАНАЛИЗ СТОЛБЦОВ:")
        for col in structure['columns']:
            print(f"  {col['index']:2d}. {col['name']} -> {col['suggested_type']}")
            print(f"      SQL имя: {col['sql_name']}")
            print(f"      Значений: {col['total_values']}, Пустых: {col['empty_values']}, "
                  f"Уникальных: {col['unique_values']}")
            if col['sample_values']:
                samples = [str(v)[:30] + "..." if len(str(v)) > 30 else str(v) 
                          for v in col['sample_values'][:3]]
                print(f"      Примеры: {', '.join(samples)}")
        
        print(f"\nПРЕДЛАГАЕМАЯ SQL СХЕМА:")
        print(sheet_analysis['sql_schema'])

def main():
    """
    Главная функция для запуска анализа.
    """
    try:
        logger.info("Запуск анализа структуры Google Sheets таблицы")
        
        # Запрашиваем у пользователя ID или URL
        user_input = input("Введите ID или URL таблицы: ").strip()
        parsed_id = extract_spreadsheet_id(user_input)
        if parsed_id:
            spreadsheet_id = parsed_id
        else:
            if user_input:
                logger.error("Не удалось распознать ID таблицы. Проверьте ввод и попробуйте снова.")
                sys.exit(1)
            # Пустой ввод — используем значение по умолчанию (как раньше)
            spreadsheet_id = None
        
        # Выполняем анализ
        analysis = explore_spreadsheet(spreadsheet_id)
        
        # Выводим отчет
        print_analysis_report(analysis)
        
        logger.info("Анализ завершен успешно!")
        
    except Exception as e:
        logger.error(f"Ошибка при анализе таблицы: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Закрываем файл и восстанавливаем stdout
        if 'log_file' in globals():
            log_file.close()
        sys.stdout = original_stdout

if __name__ == "__main__":
    main()