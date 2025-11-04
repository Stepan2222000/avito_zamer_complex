"""
Скрипт для управления пулом прокси (Шаг 2.2)
Прокси читаются из файла: scripts/data/proxies.txt
Формат: host:port:username:password

Использование:
    python manage_proxies.py
"""

import asyncio
import sys
import re
from pathlib import Path
from typing import List, Tuple

# Добавляем путь к модулю db_utils
sys.path.append(str(Path(__file__).parent))
import db_utils


# Фиксированный путь к файлу с прокси
DATA_FILE = Path(__file__).parent / 'data' / 'proxies.txt'


def validate_proxy_format(proxy_string: str) -> Tuple[bool, str]:
    """
    Валидирует формат прокси: host:port:username:password

    Args:
        proxy_string: Строка с прокси

    Returns:
        Tuple[bool, str]: (валидность, нормализованная строка или пустая)
    """
    proxy_string = proxy_string.strip()

    # Формат: host:port:username:password
    # host - домен или IP, port - число, username и password - любые символы
    pattern = r'^[\w\.\-]+:\d+:[\w\-]+:.+$'

    if re.match(pattern, proxy_string):
        return True, proxy_string

    return False, ""


def read_proxies_from_file(file_path: Path) -> Tuple[List[str], int]:
    """
    Читает прокси из текстового файла и валидирует их

    Args:
        file_path: Путь к файлу с прокси

    Returns:
        Tuple[List[str], int]: (список валидных прокси, количество невалидных)

    Raises:
        FileNotFoundError: Если файл не найден
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Файл не найден: {file_path}")

    proxies = []
    invalid_count = 0

    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()

            # Игнорируем пустые строки и комментарии
            if not line or line.startswith('#'):
                continue

            # Валидируем формат
            is_valid, normalized = validate_proxy_format(line)

            if is_valid:
                proxies.append(normalized)
            else:
                invalid_count += 1
                print(f"   ⚠️  Строка {line_num}: невалидный формат '{line}'")

    return proxies, invalid_count


async def insert_proxies_batch(conn, proxies: List[str], batch_size: int = 1000) -> int:
    """
    Вставляет прокси в БД батчами для производительности

    Args:
        conn: Подключение к БД
        proxies: Список прокси для вставки
        batch_size: Размер батча (по умолчанию 1000)

    Returns:
        int: Количество вставленных прокси
    """
    total_inserted = 0

    for i in range(0, len(proxies), batch_size):
        batch = proxies[i:i + batch_size]

        # Формируем данные для вставки: (proxy_address, status)
        values = [(proxy, 'свободен') for proxy in batch]

        # Вставляем батч с ON CONFLICT DO NOTHING для идемпотентности
        await conn.executemany(
            "INSERT INTO proxies (proxy_address, status) VALUES ($1, $2) ON CONFLICT (proxy_address) DO NOTHING",
            values
        )

        total_inserted += len(batch)
        print(f"  Обработано {total_inserted}/{len(proxies)} прокси...")

    return total_inserted


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


async def main():
    """Основная функция скрипта"""
    print("=" * 60)
    print("УПРАВЛЕНИЕ ПУЛОМ ПРОКСИ")
    print("=" * 60)

    # Интерактивный выбор режима
    mode = get_user_choice(
        "Выберите режим работы:",
        {
            '1': 'add',
            '2': 'replace'
        }
    )

    # Подтверждение для режима replace
    if mode == 'replace':
        print("\n⚠️  ВНИМАНИЕ: Режим replace удалит ВСЕ существующие прокси из БД!")
        answer = input("Для подтверждения введите 'yes': ").strip()
        if answer != 'yes':
            print("\n❌ Операция отменена пользователем")
            return

    print(f"\nВыбранные настройки:")
    print(f"  Режим: {mode}")
    print(f"  Файл: {DATA_FILE}")

    conn = None

    try:
        # Подключение к БД
        print("\n1. Подключение к БД...")
        conn = await db_utils.connect_db()
        config = db_utils.get_db_config()
        print(f"   ✓ Подключено к {config['host']}:{config['port']}/{config['database']}")

        # Проверка таблиц
        print("\n2. Проверка структуры БД...")
        await db_utils.ensure_tables_exist(conn)
        print("   ✓ Все необходимые таблицы существуют")

        # Чтение прокси из файла
        print(f"\n3. Чтение и валидация прокси из файла...")
        proxies, invalid_count = read_proxies_from_file(DATA_FILE)

        if invalid_count > 0:
            print(f"   ⚠️  Найдено невалидных записей: {invalid_count}")

        print(f"   ✓ Прочитано валидных прокси: {len(proxies)}")

        if len(proxies) == 0:
            print("   ⚠️  Нет валидных прокси для загрузки")
            return

        # Вставка прокси
        print(f"\n4. Загрузка прокси в БД (режим: {mode})...")

        if mode == 'replace':
            # Режим полной перезаписи - используем транзакцию для атомарности
            async with conn.transaction():
                # Удаляем все существующие прокси
                deleted = await conn.execute("DELETE FROM proxies")
                print(f"   ✓ Существующие прокси удалены")

                # Вставляем новые
                inserted = await insert_proxies_batch(conn, proxies)
                print(f"   ✓ Добавлено прокси: {inserted}")
        else:
            # Режим добавления к существующим - также используем транзакцию для атомарности
            async with conn.transaction():
                inserted = await insert_proxies_batch(conn, proxies)
                print(f"   ✓ Добавлено прокси: {inserted}")

        print("\n" + "=" * 60)
        print("ЗАГРУЗКА ЗАВЕРШЕНА УСПЕШНО")
        print("=" * 60)

    except FileNotFoundError as e:
        print(f"\n❌ Ошибка: {e}")
        print(f"   Убедитесь, что файл существует: {DATA_FILE}")
        sys.exit(1)

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
