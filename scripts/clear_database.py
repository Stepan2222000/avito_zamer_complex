"""
Скрипт для очистки таблиц БД (Шаг 2.3)
ВНИМАНИЕ: Операция необратима!

Использование:
    python clear_database.py
"""

import asyncio
import sys
from pathlib import Path
from typing import List

# Добавляем путь к модулю db_utils
sys.path.append(str(Path(__file__).parent))
import db_utils


# Список всех таблиц системы
ALL_TABLES = ['tasks', 'proxies', 'parsed_cards', 'validation_results', 'processed_articles']


async def get_all_tables(conn) -> List[str]:
    """
    Получает список всех таблиц в схеме public

    Args:
        conn: Подключение к БД

    Returns:
        List[str]: Список названий таблиц
    """
    rows = await conn.fetch("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name
    """)

    return [row['table_name'] for row in rows]


async def clear_tables(conn, tables: List[str]) -> None:
    """
    Очищает указанные таблицы через TRUNCATE

    Args:
        conn: Подключение к БД
        tables: Список названий таблиц для очистки
    """
    for table in tables:
        # Явная проверка на принадлежность к разрешенным таблицам (защита от SQL injection)
        if table not in ALL_TABLES:
            raise ValueError(f"Попытка очистки неразрешенной таблицы: {table}")

        # Используем двойные кавычки для безопасного экранирования идентификатора
        await conn.execute(f'TRUNCATE TABLE "{table}" CASCADE')
        print(f"   ✓ Очищена таблица: {table}")


def get_user_choice(prompt: str, choices: dict) -> str:
    """
    Запрашивает выбор пользователя из списка вариантов

    Args:
        prompt: Текст вопроса
        choices: Словарь {номер: значение}

    Returns:
        str: Выбранное значение
    """
    while True:
        print(f"\n{prompt}")
        for key, value in choices.items():
            print(f"  {key}) {value}")

        choice = input("\nВаш выбор: ").strip()

        if choice in choices:
            return choices[choice]

        print("❌ Неверный выбор, попробуйте снова")


def select_tables_interactively(available_tables: List[str]) -> List[str]:
    """
    Интерактивный выбор таблиц для очистки

    Args:
        available_tables: Список доступных таблиц

    Returns:
        List[str]: Список выбранных таблиц
    """
    print("\nДоступные таблицы:")
    for i, table in enumerate(available_tables, 1):
        print(f"  {i}) {table}")

    while True:
        choices = input("\nВведите номера таблиц через запятую (например: 1,2,3): ").strip()

        if not choices:
            print("❌ Необходимо выбрать хотя бы одну таблицу")
            continue

        try:
            # Парсим выбор пользователя
            indices = [int(x.strip()) for x in choices.split(',')]

            # Проверяем, что все индексы валидны
            if all(1 <= i <= len(available_tables) for i in indices):
                selected = [available_tables[i - 1] for i in indices]
                return selected
            else:
                print(f"❌ Номера должны быть от 1 до {len(available_tables)}")

        except ValueError:
            print("❌ Неверный формат. Введите номера через запятую")


def confirm_action(message: str) -> bool:
    """
    Запрашивает подтверждение критичной операции

    Args:
        message: Сообщение для пользователя

    Returns:
        bool: True если подтверждено
    """
    print(f"\n⚠️  {message}")
    confirmation = input("Для подтверждения введите 'yes': ").strip()

    return confirmation == 'yes'


async def main():
    """Основная функция скрипта"""
    print("=" * 60)
    print("ОЧИСТКА ТАБЛИЦ БД")
    print("⚠️  ВНИМАНИЕ: Операция необратима!")
    print("=" * 60)

    conn = None

    try:
        # Подключение к БД
        print("\n1. Подключение к БД...")
        conn = await db_utils.connect_db()
        config = db_utils.get_db_config()
        print(f"   ✓ Подключено к {config['host']}:{config['port']}/{config['database']}")

        # Получаем список существующих таблиц
        print("\n2. Получение списка таблиц...")
        existing_tables = await get_all_tables(conn)

        # Фильтруем только таблицы нашей системы
        system_tables = [t for t in existing_tables if t in ALL_TABLES]

        if not system_tables:
            print("   ⚠️  Таблицы системы не найдены")
            return

        print(f"   ✓ Найдено таблиц: {len(system_tables)}")

        # Интерактивный выбор режима
        mode = get_user_choice(
            "\n3. Выберите режим очистки:",
            {
                '1': 'all',
                '2': 'select'
            }
        )

        tables_to_clear = []

        if mode == 'all':
            # Очистка всех таблиц - требуем явное подтверждение
            if not confirm_action("Будут очищены ВСЕ таблицы системы!"):
                print("\n❌ Операция отменена пользователем")
                return

            tables_to_clear = system_tables

        else:
            # Выбор конкретных таблиц
            tables_to_clear = select_tables_interactively(system_tables)

            # Показываем что будет очищено и просим подтверждение
            print(f"\nБудут очищены следующие таблицы:")
            for table in tables_to_clear:
                print(f"  - {table}")

            if not confirm_action("Продолжить?"):
                print("\n❌ Операция отменена пользователем")
                return

        # Очистка таблиц
        print(f"\n4. Очистка таблиц...")
        await clear_tables(conn, tables_to_clear)

        print("\n" + "=" * 60)
        print(f"ОЧИЩЕНО ТАБЛИЦ: {len(tables_to_clear)}")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        # Закрытие соединения
        if conn:
            await db_utils.close_connection(conn)


if __name__ == '__main__':
    asyncio.run(main())
