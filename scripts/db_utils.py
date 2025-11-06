"""
Общий модуль для работы с PostgreSQL БД
Используется всеми скриптами управления системой
"""

import asyncpg
from pathlib import Path

# Конфигурация соединения зафиксирована, чтобы скрипты не зависели от окружения
DB_CONFIG = {
    'host': '81.30.105.134',
    'port': 5417,
    'database': 'system_avito_zamer',
    'user': 'admin',
    'password': 'Password123',
}


def get_db_config() -> dict:
    """
    Возвращает параметры подключения к PostgreSQL

    Returns:
        dict: Параметры подключения (host, port, database, user, password)
    """
    return dict(DB_CONFIG)


async def connect_db() -> asyncpg.Connection:
    """
    Создает подключение к PostgreSQL БД

    Returns:
        asyncpg.Connection: Объект подключения

    Raises:
        Exception: Если не удалось подключиться к БД
    """
    config = get_db_config()

    try:
        conn = await asyncpg.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password'],
            timeout=10,  # Таймаут подключения 10 секунд
        )
        return conn
    except Exception as e:
        raise Exception(f"Ошибка подключения к БД {config['host']}:{config['port']}: {e}")


async def ensure_tables_exist(conn: asyncpg.Connection) -> None:
    """
    Проверяет наличие всех необходимых таблиц в БД
    Если таблицы отсутствуют - создает их из schema.sql

    Args:
        conn: Подключение к БД
    """
    # Список обязательных таблиц
    required_tables = {'tasks', 'proxies', 'parsed_cards', 'validation_results', 'processed_articles'}

    # Получаем список существующих таблиц
    existing_tables = await conn.fetch("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
    """)

    existing = {row['table_name'] for row in existing_tables}
    missing = required_tables - existing

    # Если все таблицы есть - выходим
    if not missing:
        return

    # Создаем отсутствующие таблицы из schema.sql
    print(f"⚠️  Отсутствующие таблицы: {', '.join(missing)}")
    print("Создание таблиц из schema.sql...")

    schema_path = Path(__file__).parent / 'schema.sql'

    if not schema_path.exists():
        raise FileNotFoundError(f"Файл schema.sql не найден: {schema_path}")

    # Читаем и выполняем SQL скрипт
    with open(schema_path, 'r', encoding='utf-8') as f:
        schema_sql = f.read()

    await conn.execute(schema_sql)
    print("✓ Таблицы успешно созданы")


async def close_connection(conn: asyncpg.Connection) -> None:
    """
    Корректно закрывает подключение к БД

    Args:
        conn: Подключение к БД
    """
    if conn and not conn.is_closed():
        await conn.close()
