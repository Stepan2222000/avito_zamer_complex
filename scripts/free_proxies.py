#!/usr/bin/env python3
"""
Скрипт для освобождения всех прокси, находящихся в статусе "используется"
"""
import asyncio
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))
import db_utils


async def free_all_proxies():
    """Освобождает все используемые прокси"""
    conn = await db_utils.connect_db()

    try:
        print("=" * 60)
        print("ОСВОБОЖДЕНИЕ ПРОКСИ")
        print("=" * 60)

        # Проверяем текущее состояние
        before_stats = await conn.fetch("""
            SELECT status, COUNT(*) as count
            FROM proxies
            GROUP BY status
            ORDER BY status
        """)

        print("\n📊 Состояние до освобождения:")
        for row in before_stats:
            print(f"   {row['status']:15} | {row['count']:4} шт")

        # Освобождаем все используемые прокси
        result = await conn.execute("""
            UPDATE proxies
            SET status = 'свободен',
                worker_id = NULL,
                taken_at = NULL
            WHERE status = 'используется'
        """)

        freed_count = int(result.split()[-1]) if result and 'UPDATE' in result else 0

        # Проверяем состояние после
        after_stats = await conn.fetch("""
            SELECT status, COUNT(*) as count
            FROM proxies
            GROUP BY status
            ORDER BY status
        """)

        print(f"\n✅ Освобождено прокси: {freed_count}")

        print("\n📊 Состояние после освобождения:")
        for row in after_stats:
            emoji = '✅' if row['status'] == 'свободен' else '🔴' if row['status'] == 'заблокирован' else '⚙️'
            print(f"   {emoji} {row['status']:15} | {row['count']:4} шт")

        print("=" * 60)

    finally:
        await db_utils.close_connection(conn)


if __name__ == '__main__':
    asyncio.run(free_all_proxies())
