"""
Работа с PostgreSQL через asyncpg
12 функций для работы с БД с атомарными операциями
"""

import asyncpg
from typing import Optional, Tuple, Dict, Any
import logging

from . import config
from .errors import NoTasksAvailableError, NoProxiesAvailableError

logger = logging.getLogger(__name__)


async def create_pool() -> asyncpg.Pool:
    """
    Создает пул соединений к PostgreSQL

    Returns:
        asyncpg.Pool: Пул соединений
    """
    logger.info(
        f"Создание пула соединений к БД: {config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"
    )

    pool = await asyncpg.create_pool(
        host=config.DB_HOST,
        port=config.DB_PORT,
        database=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        min_size=config.POOL_MIN_SIZE,
        max_size=config.POOL_MAX_SIZE,
    )

    logger.info("Пул соединений создан успешно")
    return pool


async def close_pool(pool: asyncpg.Pool) -> None:
    """
    Закрывает пул соединений

    Args:
        pool: Пул соединений asyncpg
    """
    logger.info("Закрытие пула соединений к БД")
    await pool.close()


async def return_stuck_tasks(pool: asyncpg.Pool) -> int:
    """
    Возвращает зависшие задачи в очередь

    Ищет задачи со статусом 'в работе', у которых last_heartbeat
    был более STUCK_TASK_TIMEOUT секунд назад, и возвращает их статус в 'новая'

    Args:
        pool: Пул соединений asyncpg

    Returns:
        int: Количество возвращенных задач
    """
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

    # Извлекаем количество обновленных строк из результата "UPDATE N"
    updated_count = int(result.split()[-1]) if result and 'UPDATE' in result else 0

    if updated_count > 0:
        logger.warning(
            f"Возвращено зависших задач в очередь: {updated_count} "
            f"(timeout: {config.STUCK_TASK_TIMEOUT}s)"
        )

    return updated_count


async def take_next_task(pool: asyncpg.Pool, worker_id: str) -> Optional[Tuple[int, str]]:
    """
    Атомарно берет следующую задачу из очереди

    Использует FOR UPDATE SKIP LOCKED для предотвращения race conditions

    Args:
        pool: Пул соединений asyncpg
        worker_id: Идентификатор воркера

    Returns:
        Optional[Tuple[int, str]]: (task_id, article) или None если задач нет
    """
    row = await pool.fetchrow(
        """
        UPDATE tasks
        SET status = 'в работе',
            worker_id = $1,
            taken_at = NOW(),
            last_heartbeat = NOW()
        WHERE id = (
            SELECT id FROM tasks
            WHERE status = 'новая'
            ORDER BY created_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id, article
        """,
        worker_id,
    )

    if row:
        return (row['id'], row['article'])
    else:
        return None


async def take_free_proxy(pool: asyncpg.Pool, worker_id: str) -> Optional[Tuple[int, str]]:
    """
    Атомарно берет случайный свободный прокси

    Использует FOR UPDATE SKIP LOCKED для атомарности
    Выбирает случайный прокси (ORDER BY RANDOM()) для равномерной нагрузки

    Args:
        pool: Пул соединений asyncpg
        worker_id: Идентификатор воркера

    Returns:
        Optional[Tuple[int, str]]: (proxy_id, proxy_address) или None если прокси нет
    """
    row = await pool.fetchrow(
        """
        UPDATE proxies
        SET status = 'используется',
            worker_id = $1,
            taken_at = NOW()
        WHERE id = (
            SELECT id FROM proxies
            WHERE status = 'свободен'
            ORDER BY RANDOM()
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id, proxy_address
        """,
        worker_id,
    )

    if row:
        return (row['id'], row['proxy_address'])
    else:
        return None


async def block_proxy(pool: asyncpg.Pool, proxy_id: int, reason: str) -> None:
    """
    Помечает прокси как заблокированный навсегда

    Args:
        pool: Пул соединений asyncpg
        proxy_id: ID прокси
        reason: Причина блокировки (403/407)
    """
    await pool.execute(
        """
        UPDATE proxies
        SET status = 'заблокирован',
            worker_id = NULL,
            taken_at = NULL,
            blocked_at = NOW(),
            blocked_reason = $2
        WHERE id = $1
        """,
        proxy_id,
        reason,
    )

    logger.warning(f"Прокси #{proxy_id} заблокирован: {reason}")


async def release_proxy(pool: asyncpg.Pool, proxy_id: int) -> None:
    """
    Освобождает прокси (возвращает в статус 'свободен')

    Используется при graceful shutdown

    Args:
        pool: Пул соединений asyncpg
        proxy_id: ID прокси
    """
    await pool.execute(
        """
        UPDATE proxies
        SET status = 'свободен',
            worker_id = NULL,
            taken_at = NULL
        WHERE id = $1
        """,
        proxy_id,
    )


async def update_heartbeat(pool: asyncpg.Pool, task_id: int) -> None:
    """
    Обновляет last_heartbeat для задачи

    Вызывается фоновой задачей каждые HEARTBEAT_INTERVAL секунд

    Args:
        pool: Пул соединений asyncpg
        task_id: ID задачи
    """
    await pool.execute(
        """
        UPDATE tasks
        SET last_heartbeat = NOW()
        WHERE id = $1 AND status = 'в работе'
        """,
        task_id,
    )


async def complete_task(
    pool: asyncpg.Pool,
    task_id: int,
    article: str,
    worker_id: str,
    processing_status: str = 'success',
    items_found: Optional[int] = None,
    items_passed: Optional[int] = None
) -> None:
    """
    Завершает задачу и записывает артикул в processed_articles

    Args:
        pool: Пул соединений asyncpg
        task_id: ID задачи
        article: Артикул
        worker_id: ID воркера
        processing_status: Статус обработки ('success', 'error', 'no_results')
        items_found: Количество найденных объявлений
        items_passed: Количество прошедших валидацию
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Получаем время начала обработки из задачи
            task_row = await conn.fetchrow(
                """
                SELECT taken_at FROM tasks WHERE id = $1
                """,
                task_id,
            )
            started_at = task_row['taken_at'] if task_row else None

            # Обновляем статус задачи
            await conn.execute(
                """
                UPDATE tasks
                SET status = 'завершена',
                    completed_at = NOW()
                WHERE id = $1
                """,
                task_id,
            )

            # Записываем в processed_articles
            await conn.execute(
                """
                INSERT INTO processed_articles (
                    article,
                    processed_at,
                    processing_status,
                    items_found,
                    items_passed,
                    started_at,
                    worker_id
                )
                VALUES ($1, NOW(), $2, $3, $4, $5, $6)
                ON CONFLICT (article) DO UPDATE SET
                    processed_at = EXCLUDED.processed_at,
                    processing_status = EXCLUDED.processing_status,
                    items_found = EXCLUDED.items_found,
                    items_passed = EXCLUDED.items_passed,
                    started_at = EXCLUDED.started_at,
                    worker_id = EXCLUDED.worker_id
                """,
                article,
                processing_status,
                items_found,
                items_passed,
                started_at,
                worker_id,
            )

            logger.info(
                f"Задача #{task_id} успешно завершена | Статус: {processing_status} | "
                f"Найдено: {items_found} | Прошло: {items_passed}"
            )


async def return_task_to_queue(
    pool: asyncpg.Pool, task_id: int, error_message: str, increment_retry: bool = True
) -> None:
    """
    Возвращает задачу в очередь

    Args:
        pool: Пул соединений asyncpg
        task_id: ID задачи
        error_message: Сообщение об ошибке
        increment_retry: Увеличивать ли счетчик попыток
    """
    if increment_retry:
        await pool.execute(
            """
            UPDATE tasks
            SET status = 'новая',
                worker_id = NULL,
                taken_at = NULL,
                retry_count = retry_count + 1,
                error_message = $2
            WHERE id = $1
            """,
            task_id,
            error_message,
        )
    else:
        await pool.execute(
            """
            UPDATE tasks
            SET status = 'новая',
                worker_id = NULL,
                taken_at = NULL,
                error_message = $2
            WHERE id = $1
            """,
            task_id,
            error_message,
        )


async def mark_task_as_error(pool: asyncpg.Pool, task_id: int, error_message: str) -> None:
    """
    Помечает задачу как ошибку (после исчерпания попыток)

    Args:
        pool: Пул соединений asyncpg
        task_id: ID задачи
        error_message: Сообщение об ошибке
    """
    await pool.execute(
        """
        UPDATE tasks
        SET status = 'ошибка',
            error_message = $2
        WHERE id = $1
        """,
        task_id,
        error_message,
    )

    logger.error(f"Задача #{task_id} помечена как ошибка: {error_message}")


async def get_task_retry_count(pool: asyncpg.Pool, task_id: int) -> int:
    """
    Получает текущий счетчик попыток задачи

    Args:
        pool: Пул соединений asyncpg
        task_id: ID задачи

    Returns:
        int: Количество попыток
    """
    row = await pool.fetchrow(
        """
        SELECT retry_count FROM tasks WHERE id = $1
        """,
        task_id,
    )

    return row['retry_count'] if row else 0
