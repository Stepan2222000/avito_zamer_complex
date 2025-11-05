#!/usr/bin/env python3
"""
Быстрая проверка состояния очереди задач
"""
import asyncio
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))
import db_utils


async def check_queue():
    """Проверка состояния очереди"""
    conn = await db_utils.connect_db()

    try:
        print("=" * 60)
        print("СОСТОЯНИЕ ОЧЕРЕДИ ЗАДАЧ")
        print("=" * 60)

        # Статистика по статусам
        stats = await conn.fetch("""
            SELECT status, COUNT(*) as count
            FROM tasks
            GROUP BY status
            ORDER BY status
        """)

        total = 0
        print("\n📊 Статистика по статусам:")
        print("-" * 60)
        for row in stats:
            status = row['status']
            count = row['count']
            total += count

            # Эмодзи для статусов
            emoji = {
                'новая': '⏳',
                'в работе': '⚙️',
                'завершена': '✅',
                'ошибка': '❌'
            }.get(status, '📋')

            print(f"{emoji} {status:15} | {count:6} задач")

        print("-" * 60)
        print(f"📦 ВСЕГО           | {total:6} задач")

        # Активные воркеры
        active_workers = await conn.fetch("""
            SELECT DISTINCT worker_id
            FROM tasks
            WHERE status = 'в работе' AND worker_id IS NOT NULL
            ORDER BY worker_id
        """)

        print(f"\n👷 Активных воркеров: {len(active_workers)}")
        if active_workers:
            worker_ids = [row['worker_id'] for row in active_workers]
            print(f"   Воркеры: {', '.join(worker_ids)}")

        # Статистика по прокси
        proxies = await conn.fetch("""
            SELECT status, COUNT(*) as count
            FROM proxies
            GROUP BY status
            ORDER BY status
        """)

        print(f"\n🔌 Прокси:")
        for row in proxies:
            status = row['status']
            count = row['count']
            emoji = '✅' if status == 'свободен' else '🔴' if status == 'заблокирован' else '⚙️'
            print(f"   {emoji} {status:15} | {count:4} шт")

        # Последние 3 задачи
        recent = await conn.fetch("""
            SELECT id, article, status, worker_id, created_at
            FROM tasks
            ORDER BY created_at DESC
            LIMIT 3
        """)

        if recent:
            print(f"\n📋 Последние 3 задачи:")
            print("-" * 60)
            for row in recent:
                worker = row['worker_id'] or 'не назначен'
                print(f"ID: {row['id']:4} | Артикул: {row['article'][:25]:25} | {row['status']:10} | {worker}")

        print("=" * 60)

    finally:
        await db_utils.close_connection(conn)


if __name__ == '__main__':
    asyncio.run(check_queue())
