#!/bin/bash
# Healthcheck скрипт для проверки подключения к PostgreSQL
# Использует переменные окружения вместо hardcoded значений
# Это предотвращает утечку пароля в docker ps

set -e

# Проверка наличия необходимых переменных окружения
if [ -z "$DB_HOST" ] || [ -z "$DB_PORT" ] || [ -z "$DB_NAME" ] || [ -z "$DB_USER" ] || [ -z "$DB_PASSWORD" ]; then
    echo "ERROR: Required environment variables not set"
    exit 1
fi

# Попытка подключения к БД с timeout
python -c "
import asyncio
import asyncpg
import os
import sys

async def check_db():
    try:
        # Подключаемся к БД с timeout
        conn = await asyncpg.connect(
            host=os.environ['DB_HOST'],
            port=int(os.environ['DB_PORT']),
            database=os.environ['DB_NAME'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD'],
            timeout=5
        )

        # Проверяем что подключение работает
        await conn.fetchval('SELECT 1')

        # Проверяем heartbeat последнего активного воркера
        last_heartbeat = await conn.fetchval('''
            SELECT MAX(last_heartbeat)
            FROM tasks
            WHERE status = ''в работе''
        ''')

        if last_heartbeat:
            from datetime import datetime, timezone
            # Вычисляем возраст последнего heartbeat
            age_seconds = (datetime.now(timezone.utc) - last_heartbeat).total_seconds()

            # Если нет heartbeat больше 5 минут - воркеры могут быть мертвы
            if age_seconds > 300:  # 5 минут
                print(f'WARNING: No worker heartbeat for {age_seconds:.0f}s', file=sys.stderr)
                await conn.close()
                return False

        await conn.close()
        return True
    except Exception as e:
        print(f'Database check failed: {e}', file=sys.stderr)
        return False

# Запускаем проверку
result = asyncio.run(check_db())
sys.exit(0 if result else 1)
"

exit $?
