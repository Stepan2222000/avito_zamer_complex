#!/usr/bin/env python3
"""
Скрипт для создания таблиц в PostgreSQL из schema.sql
Простая реализация по принципу KISS - без ORM, без миграций
"""

import asyncio
from pathlib import Path
import asyncpg
from db_utils import DB_CONFIG


async def create_tables():
    """Создает таблицы в БД из schema.sql"""
    # Параметры подключения к БД
    db_host = DB_CONFIG['host']
    db_port = DB_CONFIG['port']
    db_name = DB_CONFIG['database']
    db_user = DB_CONFIG['user']
    db_password = DB_CONFIG['password']

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
