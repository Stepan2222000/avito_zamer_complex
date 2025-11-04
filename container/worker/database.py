"""
Работа с PostgreSQL через asyncpg
Простая реализация по принципу KISS
"""

import asyncpg
from typing import Optional
import structlog

from . import config

logger = structlog.get_logger()

# Глобальный connection pool
_pool: Optional[asyncpg.Pool] = None


async def init_db_pool():
    """Инициализирует connection pool к PostgreSQL"""
    global _pool

    logger.info(
        "Инициализация connection pool",
        host=config.DB_HOST,
        port=config.DB_PORT,
        database=config.DB_NAME,
        min_size=config.DB_POOL_MIN_SIZE,
        max_size=config.DB_POOL_MAX_SIZE,
    )

    _pool = await asyncpg.create_pool(
        host=config.DB_HOST,
        port=config.DB_PORT,
        database=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        min_size=config.DB_POOL_MIN_SIZE,
        max_size=config.DB_POOL_MAX_SIZE,
        command_timeout=config.DB_COMMAND_TIMEOUT,
        timeout=config.DB_CONNECTION_TIMEOUT,
    )

    logger.info("Connection pool инициализирован успешно")


async def close_db_pool():
    """Закрывает connection pool"""
    global _pool

    if _pool:
        logger.info("Закрытие connection pool")
        await _pool.close()
        _pool = None


def get_pool() -> asyncpg.Pool:
    """Возвращает connection pool"""
    if _pool is None:
        raise RuntimeError("Connection pool не инициализирован. Вызовите init_db_pool() сначала.")
    return _pool


async def return_stuck_tasks():
    """
    Возвращает зависшие задачи в очередь
    Задачи со статусом 'в работе' и heartbeat > STUCK_TASK_TIMEOUT секунд назад
    """
    pool = get_pool()

    result = await pool.execute(
        """
        UPDATE tasks
        SET status = 'новая',
            worker_id = NULL,
            taken_at = NULL
        WHERE status = 'в работе'
          AND last_heartbeat < NOW() - INTERVAL '1 second' * $1
        """,
        config.STUCK_TASK_TIMEOUT,
    )

    # Извлекаем количество обновленных строк из результата
    updated_count = int(result.split()[-1]) if result and 'UPDATE' in result else 0

    if updated_count > 0:
        logger.warning(
            "Возвращены зависшие задачи в очередь",
            count=updated_count,
            timeout_seconds=config.STUCK_TASK_TIMEOUT,
        )

    return updated_count
