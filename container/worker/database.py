"""
Работа с PostgreSQL через asyncpg
12 функций для работы с БД с атомарными операциями
"""

import asyncpg
import asyncio
from functools import wraps
from typing import Optional, Tuple, Dict, Any
import logging

from . import config
from .errors import NoTasksAvailableError, NoProxiesAvailableError

logger = logging.getLogger(__name__)


def db_retry(max_attempts: int = 3, initial_delay: float = 2.0):
    """
    Декоратор для повторных попыток при ошибках соединения с БД

    Использует exponential backoff: 2s -> 4s -> 8s
    Обрабатывает временные ошибки соединения с PostgreSQL

    Args:
        max_attempts: Максимальное количество попыток (по умолчанию 3)
        initial_delay: Начальная задержка в секундах (по умолчанию 2.0)

    Raises:
        asyncpg.exceptions.*: После исчерпания попыток пробрасывает исходное исключение
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            delay = initial_delay

            for attempt in range(1, max_attempts + 1):
                try:
                    return await func(*args, **kwargs)
                except (
                    asyncpg.exceptions.PostgresConnectionError,
                    asyncpg.exceptions.InterfaceError,
                    asyncpg.exceptions.CannotConnectNowError,
                    asyncpg.exceptions.ConnectionDoesNotExistError,
                ) as e:
                    last_exception = e

                    if attempt == max_attempts:
                        logger.error(
                            f"{func.__name__} failed after {max_attempts} attempts: {e}"
                        )
                        raise

                    logger.warning(
                        f"{func.__name__} attempt {attempt}/{max_attempts} failed: {e}. "
                        f"Retrying in {delay}s..."
                    )

                    await asyncio.sleep(delay)
                    delay *= 2  # Exponential backoff: 2s -> 4s -> 8s

            raise last_exception

        return wrapper
    return decorator


async def create_pool() -> asyncpg.Pool:
    """
    Создает пул соединений к PostgreSQL с retry механизмом

    Использует 3 попытки с exponential backoff (2s -> 4s -> 8s)

    Returns:
        asyncpg.Pool: Пул соединений

    Raises:
        asyncpg.exceptions.*: После исчерпания попыток пробрасывает исходное исключение
    """
    logger.info(
        f"Создание пула соединений к БД: {config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"
    )

    max_attempts = 3
    delay = 2.0
    last_exception = None

    for attempt in range(1, max_attempts + 1):
        try:
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

        except (
            asyncpg.exceptions.PostgresConnectionError,
            asyncpg.exceptions.InterfaceError,
            asyncpg.exceptions.CannotConnectNowError,
            asyncpg.exceptions.ConnectionDoesNotExistError,
            OSError,  # Сетевые ошибки (connection refused, timeout)
        ) as e:
            last_exception = e

            if attempt == max_attempts:
                logger.error(
                    f"Не удалось создать пул соединений после {max_attempts} попыток: {e}"
                )
                raise

            logger.warning(
                f"create_pool attempt {attempt}/{max_attempts} failed: {e}. "
                f"Retrying in {delay}s..."
            )

            await asyncio.sleep(delay)
            delay *= 2  # Exponential backoff: 2s -> 4s -> 8s

    raise last_exception


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
    Возвращает зависшие задачи в очередь или помечает как ошибку

    Ищет задачи со статусом 'в работе', у которых last_heartbeat
    был более STUCK_TASK_TIMEOUT секунд назад.

    - Если retry_count < MAX_RETRY_ATTEMPTS: возврат в очередь с increment retry_count
    - Если retry_count >= MAX_RETRY_ATTEMPTS: пометка как 'ошибка'

    Args:
        pool: Пул соединений asyncpg

    Returns:
        int: Количество обработанных задач (возвращено + помечено как ошибка)
    """
    # Возвращаем задачи с непревышенным лимитом попыток
    returned = await pool.execute(
        """
        UPDATE tasks
        SET status = 'новая',
            worker_id = NULL,
            taken_at = NULL,
            retry_count = retry_count + 1
        WHERE status = 'в работе'
          AND last_heartbeat < NOW() - INTERVAL '1 second' * $1
          AND retry_count < $2
        """,
        config.STUCK_TASK_TIMEOUT,
        config.MAX_RETRY_ATTEMPTS,
    )

    # Помечаем как ошибку задачи с превышенным лимитом
    marked = await pool.execute(
        """
        UPDATE tasks
        SET status = 'ошибка',
            worker_id = NULL,
            taken_at = NULL,
            error_message = 'Превышено максимальное количество попыток (stuck timeout)'
        WHERE status = 'в работе'
          AND last_heartbeat < NOW() - INTERVAL '1 second' * $1
          AND retry_count >= $2
        """,
        config.STUCK_TASK_TIMEOUT,
        config.MAX_RETRY_ATTEMPTS,
    )

    returned_count = int(returned.split()[-1]) if returned and 'UPDATE' in returned else 0
    marked_count = int(marked.split()[-1]) if marked and 'UPDATE' in marked else 0

    if returned_count > 0:
        logger.warning(
            f"Возвращено зависших задач в очередь: {returned_count} "
            f"(timeout: {config.STUCK_TASK_TIMEOUT}s)"
        )

    if marked_count > 0:
        logger.error(
            f"Помечено зависших задач как ошибка (превышен лимит попыток): {marked_count}"
        )

    return returned_count + marked_count


@db_retry(max_attempts=3, initial_delay=2.0)
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


@db_retry(max_attempts=3, initial_delay=2.0)
async def update_heartbeat(pool: asyncpg.Pool, task_id: int) -> None:
    """
    Обновляет last_heartbeat для задачи

    Вызывается фоновой задачей каждые HEARTBEAT_INTERVAL секунд
    Gracefully обрабатывает PoolClosedError (нормально при shutdown)

    Args:
        pool: Пул соединений asyncpg
        task_id: ID задачи
    """
    try:
        await pool.execute(
            """
            UPDATE tasks
            SET last_heartbeat = NOW()
            WHERE id = $1 AND status = 'в работе'
            """,
            task_id,
        )
    except asyncpg.exceptions.PoolClosedError:
        # Это нормально при graceful shutdown - пул уже закрыт
        logger.debug(f"Пул соединений закрыт при обновлении heartbeat для задачи #{task_id}")


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


@db_retry(max_attempts=3, initial_delay=2.0)
async def save_parsed_card(
    pool: asyncpg.Pool,
    avito_item_id: int,
    article: str,
    title: str,
    description: str,
    price: float,
    seller_name: str,
    parsed_data: dict
) -> None:
    """
    Сохраняет спаршенную карточку в БД

    При конфликте по avito_item_id обновляет связь с артикулом

    Args:
        pool: Пул соединений asyncpg
        avito_item_id: ID объявления с Авито
        article: Артикул
        title: Название объявления
        description: Описание
        price: Цена
        seller_name: Ник продавца
        parsed_data: Все спаршенные поля в JSON
    """
    await pool.execute(
        """
        INSERT INTO parsed_cards (
            avito_item_id, article, title, description, price, seller_name, parsed_data
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (avito_item_id) DO UPDATE SET
            article = EXCLUDED.article,
            parsed_at = NOW()
        """,
        avito_item_id,
        article,
        title,
        description,
        price,
        seller_name,
        parsed_data,
    )


async def check_existing_cards(pool: asyncpg.Pool, avito_ids: list) -> set:
    """
    Проверяет какие карточки уже существуют в БД

    Args:
        pool: Пул соединений asyncpg
        avito_ids: Список ID объявлений для проверки

    Returns:
        set[int]: Множество существующих ID
    """
    if not avito_ids:
        return set()

    rows = await pool.fetch(
        """
        SELECT avito_item_id FROM parsed_cards
        WHERE avito_item_id = ANY($1)
        """,
        avito_ids,
    )

    return {row['avito_item_id'] for row in rows}


@db_retry(max_attempts=3, initial_delay=2.0)
async def save_validation_result(
    pool: asyncpg.Pool,
    avito_item_id: int,
    validation_type: str,
    passed: bool,
    rejection_reason: Optional[str],
    validation_details: Optional[dict]
) -> None:
    """
    Сохраняет результат валидации

    Args:
        pool: Пул соединений asyncpg
        avito_item_id: ID объявления
        validation_type: 'механическая' или 'ИИ'
        passed: Прошло валидацию или нет
        rejection_reason: Причина отклонения (текст)
        validation_details: Детали валидации (JSONB) - может быть None
    """
    await pool.execute(
        """
        INSERT INTO validation_results (
            avito_item_id, validation_type, passed, rejection_reason, validation_details
        )
        VALUES ($1, $2, $3, $4, $5)
        """,
        avito_item_id,
        validation_type,
        passed,
        rejection_reason,
        validation_details,
    )


async def get_cards_for_ai_validation(pool: asyncpg.Pool, article: str) -> list:
    """
    Получает карточки, прошедшие механическую валидацию

    Выбирает только те карточки по артикулу, где последняя валидация
    была механической и успешной

    Args:
        pool: Пул соединений asyncpg
        article: Артикул

    Returns:
        list[dict]: Список карточек с полями: avito_item_id, title, description, price
    """
    rows = await pool.fetch(
        """
        SELECT DISTINCT c.avito_item_id, c.title, c.description, c.price
        FROM parsed_cards c
        INNER JOIN validation_results v ON c.avito_item_id = v.avito_item_id
        WHERE c.article = $1
          AND v.validation_type = 'механическая'
          AND v.passed = TRUE
          AND NOT EXISTS (
              SELECT 1 FROM validation_results v2
              WHERE v2.avito_item_id = c.avito_item_id
                AND v2.validation_type = 'ИИ'
          )
        """,
        article,
    )

    return [dict(row) for row in rows]


@db_retry(max_attempts=3, initial_delay=2.0)
async def get_cards_for_detailed_parsing(pool: asyncpg.Pool, article: str) -> list:
    """
    Получает карточки, прошедшие обе валидации и нуждающиеся в детальном парсинге

    Выбирает карточки которые:
    1. Прошли механическую валидацию
    2. Прошли ИИ-валидацию
    3. Не имеют детальных данных (published_at IS NULL)

    Args:
        pool: Пул соединений asyncpg
        article: Артикул

    Returns:
        list[dict]: Список карточек с полями: avito_item_id, title
    """
    rows = await pool.fetch(
        """
        SELECT DISTINCT c.avito_item_id, c.title
        FROM parsed_cards c
        WHERE c.article = $1
          AND c.published_at IS NULL
          AND EXISTS (
              SELECT 1 FROM validation_results v1
              WHERE v1.avito_item_id = c.avito_item_id
                AND v1.validation_type = 'механическая'
                AND v1.passed = TRUE
          )
          AND EXISTS (
              SELECT 1 FROM validation_results v2
              WHERE v2.avito_item_id = c.avito_item_id
                AND v2.validation_type = 'ИИ'
                AND v2.passed = TRUE
          )
        ORDER BY c.avito_item_id
        """,
        article,
    )

    return [dict(row) for row in rows]


@db_retry(max_attempts=3, initial_delay=2.0)
async def update_card_detailed_data(
    pool: asyncpg.Pool,
    avito_item_id: int,
    detailed_data: dict
) -> None:
    """
    Обновляет карточку детальными данными после парсинга страницы объявления

    Args:
        pool: Пул соединений asyncpg
        avito_item_id: ID объявления
        detailed_data: Детальные данные из парсинга (все поля из avito-library)

    Raises:
        ValueError: Если карточка не найдена в БД
    """
    result = await pool.execute(
        """
        UPDATE parsed_cards
        SET
            published_at = $2,
            location = $3,
            views_count = $4,
            characteristics = $5,
            parsed_data = parsed_data || $6::jsonb,
            parsed_at = NOW()
        WHERE avito_item_id = $1
        """,
        avito_item_id,
        detailed_data.get('published_at'),
        detailed_data.get('location'),
        detailed_data.get('views_count'),
        detailed_data.get('characteristics'),
        detailed_data,
    )

    # Проверка что строка была обновлена (критично для отлова ошибок)
    updated_count = int(result.split()[-1]) if result and 'UPDATE' in result else 0
    if updated_count == 0:
        logger.error(f"Карточка {avito_item_id} не найдена в БД при обновлении детальных данных")
        raise ValueError(f"Card {avito_item_id} not found in database")
