"""
Скрипт для мониторинга состояния системы парсинга (Шаг 2.4)
Показывает статистику по задачам, прокси, валидации и активным воркерам

Использование:
    python monitor.py
"""

import asyncio
import sys
import os
from pathlib import Path
from typing import Dict, List
from datetime import datetime

# Добавляем путь к модулю db_utils
sys.path.append(str(Path(__file__).parent))
import db_utils


async def get_tasks_stats(conn) -> Dict[str, int]:
    """
    Получает статистику по задачам (количество по статусам)

    Args:
        conn: Подключение к БД

    Returns:
        Dict[str, int]: Словарь {status: count}
    """
    rows = await conn.fetch("""
        SELECT status, COUNT(*) as count
        FROM tasks
        GROUP BY status
    """)

    return {row['status']: row['count'] for row in rows}


async def get_proxies_stats(conn) -> Dict[str, int]:
    """
    Получает статистику по прокси (количество по статусам)

    Args:
        conn: Подключение к БД

    Returns:
        Dict[str, int]: Словарь {status: count}
    """
    rows = await conn.fetch("""
        SELECT status, COUNT(*) as count
        FROM proxies
        GROUP BY status
    """)

    return {row['status']: row['count'] for row in rows}


async def get_validation_stats(conn) -> Dict[str, Dict[str, int]]:
    """
    Получает статистику по валидации (механическая и ИИ)

    Args:
        conn: Подключение к БД

    Returns:
        Dict[str, Dict[str, int]]: Вложенный словарь {validation_type: {passed: count}}
    """
    rows = await conn.fetch("""
        SELECT validation_type, passed, COUNT(*) as count
        FROM validation_results
        GROUP BY validation_type, passed
    """)

    stats = {}

    for row in rows:
        vtype = row['validation_type']
        passed = row['passed']
        count = row['count']

        if vtype not in stats:
            stats[vtype] = {}

        stats[vtype][passed] = count

    return stats


async def get_active_workers(conn, threshold_seconds: int = 300) -> List[Dict]:
    """
    Получает список активных воркеров (heartbeat < threshold)

    Args:
        conn: Подключение к БД
        threshold_seconds: Порог времени heartbeat в секундах (по умолчанию 300 = 5 минут)

    Returns:
        List[Dict]: Список словарей с данными о воркерах
    """
    rows = await conn.fetch("""
        SELECT
            worker_id,
            article,
            taken_at,
            last_heartbeat,
            EXTRACT(EPOCH FROM (NOW() - last_heartbeat)) as seconds_since_heartbeat
        FROM tasks
        WHERE status = 'в работе'
          AND last_heartbeat > NOW() - INTERVAL '1 second' * $1
        ORDER BY last_heartbeat DESC
    """, threshold_seconds)

    return [dict(row) for row in rows]


def display_dashboard(tasks_stats: Dict, proxies_stats: Dict, validation_stats: Dict, workers: List[Dict]):
    """
    Форматирует и выводит дашборд с статистикой системы

    Args:
        tasks_stats: Статистика по задачам
        proxies_stats: Статистика по прокси
        validation_stats: Статистика валидации
        workers: Список активных воркеров
    """
    # Очистка консоли (кроссплатформенная)
    os.system('clear' if os.name == 'posix' else 'cls')

    # Заголовок
    print("=" * 70)
    print("AVITO PARSER - Мониторинг системы".center(70))
    print(f"Обновлено: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}".center(70))
    print("=" * 70)
    print()

    # Статистика задач
    print("ЗАДАЧИ:")
    print("-" * 70)
    all_statuses = ['новая', 'в работе', 'завершена', 'ошибка']
    for status in all_statuses:
        count = tasks_stats.get(status, 0)
        print(f"  {status:20} : {count:>10}")

    total_tasks = sum(tasks_stats.values())
    print(f"  {'ВСЕГО':20} : {total_tasks:>10}")
    print()

    # Статистика прокси
    print("ПРОКСИ:")
    print("-" * 70)
    all_proxy_statuses = ['свободен', 'используется', 'заблокирован']
    for status in all_proxy_statuses:
        count = proxies_stats.get(status, 0)
        print(f"  {status:20} : {count:>10}")

    total_proxies = sum(proxies_stats.values())
    print(f"  {'ВСЕГО':20} : {total_proxies:>10}")
    print()

    # Статистика валидации
    print("ВАЛИДАЦИЯ:")
    print("-" * 70)
    if validation_stats:
        for vtype in ['механическая', 'ИИ']:
            if vtype in validation_stats:
                passed = validation_stats[vtype].get(True, 0)
                failed = validation_stats[vtype].get(False, 0)
                total = passed + failed

                if total > 0:
                    percentage = (passed / total * 100)
                    print(f"  {vtype:20} : {passed}/{total} ({percentage:.1f}% прошло)")
                else:
                    print(f"  {vtype:20} : нет данных")
    else:
        print("  Данных пока нет")
    print()

    # Активные воркеры
    print(f"АКТИВНЫЕ ВОРКЕРЫ ({len(workers)}):")
    print("-" * 70)
    if workers:
        # Показываем первые 15 воркеров
        for worker in workers[:15]:
            worker_id = worker['worker_id'][:25]  # Обрезаем длинные ID
            article = worker['article'][:15]
            seconds = int(worker['seconds_since_heartbeat'])

            # Форматируем время
            if seconds < 60:
                time_str = f"{seconds}с назад"
            else:
                minutes = seconds // 60
                time_str = f"{minutes}м назад"

            print(f"  {worker_id:27} | {article:17} | {time_str:>12}")

        if len(workers) > 15:
            print(f"  ... и ещё {len(workers) - 15} воркеров")
    else:
        print("  Нет активных воркеров")

    print()
    print("=" * 70)


async def fetch_all_stats(conn):
    """
    Получает всю статистику параллельно через asyncio.gather

    Args:
        conn: Подключение к БД

    Returns:
        Tuple: (tasks_stats, proxies_stats, validation_stats, workers)
    """
    # Выполняем все запросы параллельно
    tasks_stats, proxies_stats, validation_stats, workers = await asyncio.gather(
        get_tasks_stats(conn),
        get_proxies_stats(conn),
        get_validation_stats(conn),
        get_active_workers(conn),
    )

    return tasks_stats, proxies_stats, validation_stats, workers


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


def get_interval() -> int:
    """
    Запрашивает интервал обновления у пользователя

    Returns:
        int: Интервал в секундах
    """
    while True:
        try:
            interval = input("\nВведите интервал обновления в секундах (по умолчанию 10): ").strip()

            if not interval:
                return 10

            interval = int(interval)

            if interval < 1:
                print("❌ Интервал должен быть >= 1 секунды")
                continue

            return interval

        except ValueError:
            print("❌ Введите целое число")


async def main():
    """Основная функция скрипта"""
    print("=" * 60)
    print("МОНИТОРИНГ СИСТЕМЫ ПАРСИНГА")
    print("=" * 60)

    # Интерактивный выбор режима
    mode = get_user_choice(
        "Выберите режим работы:",
        {
            '1': 'once',
            '2': 'watch'
        }
    )

    interval = None
    if mode == 'watch':
        interval = get_interval()

    conn = None

    try:
        # Подключение к БД
        print("\nПодключение к БД...")
        conn = await db_utils.connect_db()
        config = db_utils.get_db_config()
        print(f"✓ Подключено к {config['host']}:{config['port']}/{config['database']}")

        if mode == 'once':
            # Однократный вывод статистики
            print("Получение статистики...\n")
            await asyncio.sleep(1)  # Небольшая пауза для читаемости

            tasks_stats, proxies_stats, validation_stats, workers = await fetch_all_stats(conn)
            display_dashboard(tasks_stats, proxies_stats, validation_stats, workers)

        else:
            # Циклический режим с автообновлением
            print(f"Запуск мониторинга с интервалом {interval} сек...")
            print("Нажмите Ctrl+C для выхода\n")
            await asyncio.sleep(2)

            try:
                while True:
                    tasks_stats, proxies_stats, validation_stats, workers = await fetch_all_stats(conn)
                    display_dashboard(tasks_stats, proxies_stats, validation_stats, workers)

                    # Ожидаем интервал
                    await asyncio.sleep(interval)

            except KeyboardInterrupt:
                print("\n\n✓ Мониторинг остановлен пользователем")

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
