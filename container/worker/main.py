"""
Точка входа воркера для парсинга Авито
Реализация Шага 1.1-1.2: инициализация и подготовка инфраструктуры
"""

import asyncio
import structlog

from . import config
from . import database

# Настройка структурированного логирования
structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)

logger = structlog.get_logger()


async def main():
    """Основная функция воркера"""
    logger.info(
        "Запуск воркера",
        worker_id=config.WORKER_ID,
        db_host=config.DB_HOST,
        db_port=config.DB_PORT,
        db_name=config.DB_NAME,
    )

    try:
        # Инициализация connection pool
        await database.init_db_pool()

        # Возврат зависших задач в очередь (Шаг 3.1)
        stuck_count = await database.return_stuck_tasks()
        if stuck_count > 0:
            logger.info("Зависшие задачи возвращены", count=stuck_count)

        logger.info(
            "Воркер инициализирован и готов к работе",
            worker_id=config.WORKER_ID,
        )

        # TODO: Здесь будет основной цикл воркера (Шаг 3.2-3.5 и далее)
        # Пока просто держим воркер запущенным
        logger.info("Ожидание задач... (основная логика парсинга будет реализована в следующих шагах)")
        await asyncio.sleep(60)  # Временно: спим 60 секунд

    except Exception as e:
        logger.error("Критическая ошибка воркера", error=str(e), exc_info=True)
        raise
    finally:
        # Закрытие connection pool
        await database.close_db_pool()
        logger.info("Воркер остановлен", worker_id=config.WORKER_ID)


if __name__ == '__main__':
    asyncio.run(main())
