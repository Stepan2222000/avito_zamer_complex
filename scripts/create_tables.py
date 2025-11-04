#!/usr/bin/env python3
"""
Скрипт для создания таблиц в PostgreSQL из schema.sql
Простая реализация по принципу KISS - без ORM, без миграций
"""

import asyncio
import os
from pathlib import Path
import asyncpg
from dotenv import load_dotenv


async def create_tables():
    """Создает таблицы в БД из schema.sql"""
    # Загрузка переменных окружения
    load_dotenv()

    # Параметры подключения к БД
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = int(os.getenv('DB_PORT', '5432'))
    db_name = os.getenv('DB_NAME', 'avito_parser')
    db_user = os.getenv('DB_USER', 'parser')
    db_password = os.getenv('DB_PASSWORD')

    if not db_password:
        raise ValueError('DB_PASSWORD не задан в переменных окружения')

    print(f'Подключение к БД: {db_host}:{db_port}/{db_name}')

    # Подключение к БД
    conn = await asyncpg.connect(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password
    )

    try:
        # Чтение schema.sql из папки scripts
        schema_path = Path(__file__).parent / 'schema.sql'

        if not schema_path.exists():
            raise FileNotFoundError(f'Файл schema.sql не найден: {schema_path}')

        with open(schema_path, 'r', encoding='utf-8') as f:
            schema_sql = f.read()

        print('Выполнение SQL скрипта...')

        # Выполнение SQL скрипта
        await conn.execute(schema_sql)

        print('✓ Таблицы успешно созданы')

        # Проверка созданных таблиц
        tables = await conn.fetch("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)

        print(f'\nСозданные таблицы ({len(tables)}):')
        for table in tables:
            print(f'  - {table["table_name"]}')

    finally:
        await conn.close()
        print('\nПодключение к БД закрыто')


if __name__ == '__main__':
    asyncio.run(create_tables())
