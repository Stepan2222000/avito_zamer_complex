"""
Скрипт для загрузки артикулов в очередь задач (Шаг 2.1)
Артикулы читаются из файла: scripts/data/urls.txt

Использование:
    python add_tasks.py
"""

import asyncio
import sys
from pathlib import Path
from typing import List, Set

# Добавляем путь к модулю db_utils
sys.path.append(str(Path(__file__).parent))
import db_utils


# Фиксированный путь к файлу с артикулами
DATA_FILE = Path(__file__).parent / 'data' / 'urls.txt'


def read_articles_from_file(file_path: Path) -> List[str]:
    """
    Читает артикулы из текстового файла

    Args:
        file_path: Путь к файлу с артикулами (один на строку)

    Returns:
        List[str]: Список артикулов

    Raises:
        FileNotFoundError: Если файл не найден
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Файл не найден: {file_path}")

    articles = []

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()

            # Игнорируем пустые строки и комментарии
            if not line or line.startswith('#'):
                continue

            articles.append(line)

    return articles


async def get_existing_articles(conn) -> Set[str]:
    """
    Получает уже обработанные артикулы из таблицы processed_articles

    Это артикулы, которые были полностью обработаны воркерами
    и не должны повторно загружаться в очередь

    Args:
        conn: Подключение к БД

    Returns:
        Set[str]: Множество обработанных артикулов
    """
    rows = await conn.fetch("SELECT article FROM processed_articles")
    return {row['article'] for row in rows}


async def insert_tasks_batch(conn, articles: List[str], batch_size: int = 1000) -> int:
    """
    Вставляет задачи в БД батчами для производительности

    Args:
        conn: Подключение к БД
        articles: Список артикулов для вставки
        batch_size: Размер батча (по умолчанию 1000)

    Returns:
        int: Количество вставленных задач
    """
    total_inserted = 0

    for i in range(0, len(articles), batch_size):
        batch = articles[i:i + batch_size]

        # Формируем данные для вставки: (article, status)
        values = [(article, 'новая') for article in batch]

        # Вставляем батч с ON CONFLICT DO NOTHING для идемпотентности
        await conn.executemany(
            "INSERT INTO tasks (article, status) VALUES ($1, $2) ON CONFLICT (article) DO NOTHING",
            values
        )

        total_inserted += len(batch)
        print(f"  Обработано {total_inserted}/{len(articles)} артикулов...")

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


def get_yes_no(prompt: str) -> bool:
    """
    Запрашивает ответ да/нет у пользователя

    Args:
        prompt: Текст вопроса

    Returns:
        bool: True для да, False для нет
    """
    while True:
        answer = input(f"\n{prompt} (да/нет): ").strip().lower()

        if answer in ['да', 'yes', 'y', 'д']:
            return True
        elif answer in ['нет', 'no', 'n', 'н']:
            return False

        print("❌ Введите 'да' или 'нет'")


async def main():
    """Основная функция скрипта"""
    print("=" * 60)
    print("ЗАГРУЗКА АРТИКУЛОВ В ОЧЕРЕДЬ")
    print("=" * 60)

    # Интерактивный выбор режима
    mode = get_user_choice(
        "Выберите режим работы:",
        {
            '1': 'add',
            '2': 'replace'
        }
    )

    # Интерактивный выбор проверки дубликатов
    check_duplicates = get_yes_no("Проверять и пропускать дубликаты?")

    # Подтверждение для режима replace
    if mode == 'replace':
        print("\n⚠️  ВНИМАНИЕ: Режим replace удалит ВСЕ существующие задачи из БД!")
        if not get_yes_no("Вы уверены, что хотите продолжить?"):
            print("\n❌ Операция отменена пользователем")
            return

    print(f"\nВыбранные настройки:")
    print(f"  Режим: {mode}")
    print(f"  Проверка дубликатов: {'да' if check_duplicates else 'нет'}")
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

        # Чтение артикулов из файла
        print(f"\n3. Чтение артикулов из файла...")
        articles = read_articles_from_file(DATA_FILE)
        print(f"   ✓ Прочитано {len(articles)} артикулов")

        if len(articles) == 0:
            print("   ⚠️  Файл пуст, нечего загружать")
            return

        # Фильтрация дубликатов (если выбрано)
        if check_duplicates:
            print("\n4. Проверка дубликатов...")
            existing = await get_existing_articles(conn)
            original_count = len(articles)
            articles = [a for a in articles if a not in existing]
            skipped = original_count - len(articles)
            print(f"   ✓ Пропущено дубликатов: {skipped}")
            print(f"   ✓ К загрузке: {len(articles)} артикулов")

            if len(articles) == 0:
                print("   ⚠️  Все артикулы уже есть в БД")
                return

        # Вставка артикулов
        step_num = 5 if check_duplicates else 4
        print(f"\n{step_num}. Загрузка артикулов в БД (режим: {mode})...")

        if mode == 'replace':
            # Режим полной перезаписи - используем транзакцию для атомарности
            async with conn.transaction():
                # Удаляем все существующие задачи
                await conn.execute("DELETE FROM tasks")
                print("   ✓ Существующие задачи удалены")

                # Вставляем новые
                inserted = await insert_tasks_batch(conn, articles)
                print(f"   ✓ Добавлено задач: {inserted}")
        else:
            # Режим добавления к существующим - также используем транзакцию для атомарности
            async with conn.transaction():
                inserted = await insert_tasks_batch(conn, articles)
                print(f"   ✓ Добавлено задач: {inserted}")

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
