#!/usr/bin/env python
"""
Supervisor для управления множественными воркерами через multiprocessing.

Запускает N независимых процессов-воркеров, мониторит их состояние
и автоматически перезапускает упавшие процессы.
"""
import os
import sys
import time
import signal
import logging
import multiprocessing
from typing import List, Dict

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Event для graceful shutdown (безопаснее чем глобальная переменная)
shutdown_event = multiprocessing.Event()


def sigterm_handler(signum, frame):
    """Обработчик SIGTERM для graceful shutdown"""
    logger.info("Получен сигнал SIGTERM, начинаем graceful shutdown всех воркеров")
    shutdown_event.set()


def worker_wrapper(worker_id: int):
    """
    Обертка для запуска воркера в отдельном процессе.

    Args:
        worker_id: Уникальный идентификатор воркера (1, 2, ..., N)
    """
    # Сбрасываем обработчики сигналов от parent процесса
    # Child процессы НЕ должны наследовать signal handlers от supervisor
    signal.signal(signal.SIGTERM, signal.SIG_DFL)  # Используем default handler
    signal.signal(signal.SIGINT, signal.SIG_DFL)   # Используем default handler

    # Устанавливаем переменную окружения DISPLAY для этого процесса
    display_num = 99 + worker_id - 1  # Worker 1 → :99, Worker 2 → :100, и т.д.
    os.environ['DISPLAY'] = f':{display_num}'

    # Устанавливаем WORKER_ID
    os.environ['WORKER_ID'] = f'worker_{worker_id}'

    logger.info(f"Запуск воркера worker_{worker_id} на DISPLAY=:{display_num}")

    try:
        # Импортируем и запускаем main из worker.main
        from worker.main import main
        import asyncio

        # Запускаем асинхронный main воркера
        asyncio.run(main())

    except KeyboardInterrupt:
        logger.info(f"Worker {worker_id} получил KeyboardInterrupt")
    except Exception as e:
        logger.error(f"Worker {worker_id} упал с ошибкой: {e}", exc_info=True)
        # Даем время для завершения cleanup
        time.sleep(1)
        sys.exit(1)


def start_worker_process(worker_id: int) -> multiprocessing.Process:
    """
    Создает и запускает новый процесс воркера.

    Args:
        worker_id: Уникальный идентификатор воркера

    Returns:
        Объект Process
    """
    process = multiprocessing.Process(
        target=worker_wrapper,
        args=(worker_id,),
        name=f'worker_{worker_id}'
    )
    process.start()
    logger.info(f"✓ Worker {worker_id} запущен (PID: {process.pid})")
    return process


def monitor_and_restart_workers(num_workers: int):
    """
    Главный цикл мониторинга и перезапуска воркеров.

    Args:
        num_workers: Количество воркеров для управления
    """
    # Словарь {worker_id: Process}
    workers: Dict[int, multiprocessing.Process] = {}

    # Запускаем начальные воркеры
    logger.info(f"Запуск {num_workers} воркеров...")
    for worker_id in range(1, num_workers + 1):
        workers[worker_id] = start_worker_process(worker_id)

    logger.info(f"✓ Все {num_workers} воркеров запущены")
    logger.info("Supervisor готов к мониторингу")

    # Счетчик перезапусков для логирования
    restart_counts: Dict[int, int] = {i: 0 for i in range(1, num_workers + 1)}

    # Главный цикл мониторинга
    while not shutdown_event.is_set():
        try:
            # Проверяем каждого воркера
            for worker_id in range(1, num_workers + 1):
                process = workers[worker_id]

                if not process.is_alive():
                    restart_counts[worker_id] += 1
                    exit_code = process.exitcode

                    logger.warning(
                        f"⚠ Worker {worker_id} завершился (exit code: {exit_code}, "
                        f"перезапусков: {restart_counts[worker_id]})"
                    )

                    # Даем немного времени перед перезапуском
                    time.sleep(2)

                    # Перезапускаем воркер
                    logger.info(f"Перезапуск worker {worker_id}...")
                    workers[worker_id] = start_worker_process(worker_id)

            # Небольшая пауза перед следующей проверкой (используем wait для быстрой реакции на shutdown)
            # Timeout 1 сек для быстрого обнаружения упавших воркеров
            shutdown_event.wait(timeout=1)

        except KeyboardInterrupt:
            logger.info("Получен Ctrl+C, начинаем graceful shutdown")
            shutdown_event.set()
            break
        except Exception as e:
            logger.error(f"Ошибка в цикле мониторинга: {e}", exc_info=True)
            time.sleep(5)

    # Graceful shutdown всех воркеров
    logger.info("Останавливаем все воркеры...")

    # Сначала отправляем SIGTERM всем живым процессам
    for worker_id, process in workers.items():
        if process.is_alive():
            logger.info(f"Отправка SIGTERM worker {worker_id} (PID: {process.pid})")
            try:
                process.terminate()
            except Exception as e:
                logger.error(f"Ошибка при terminate worker {worker_id}: {e}")

    # Ждем завершения с таймаутом
    logger.info("Ожидание завершения воркеров (таймаут 30 сек)...")
    for worker_id, process in workers.items():
        if process.is_alive():
            process.join(timeout=30)

            # Если не завершился - принудительное завершение
            if process.is_alive():
                logger.warning(f"Worker {worker_id} не завершился, принудительное kill")
                process.kill()
                process.join(timeout=5)  # Timeout для предотвращения бесконечного ожидания

                # Проверка на zombie процесс
                if process.is_alive():
                    logger.error(
                        f"Worker {worker_id} (PID: {process.pid}) не завершился даже после SIGKILL. "
                        f"Возможен zombie процесс."
                    )

    logger.info("✓ Все воркеры остановлены")


def main():
    """Главная функция supervisor"""
    logger.info("=" * 60)
    logger.info("Запуск Supervisor для управления воркерами")
    logger.info("=" * 60)

    # Читаем количество воркеров из переменной окружения
    num_workers = int(os.getenv('NUM_WORKERS', '15'))
    logger.info(f"Количество воркеров: {num_workers}")

    # Проверка разумных границ
    if num_workers < 1:
        logger.error("NUM_WORKERS должно быть >= 1")
        sys.exit(1)

    if num_workers > 50:
        logger.warning(f"NUM_WORKERS={num_workers} очень большое, это может вызвать проблемы с ресурсами")

    # Устанавливаем обработчик SIGTERM
    signal.signal(signal.SIGTERM, sigterm_handler)
    signal.signal(signal.SIGINT, sigterm_handler)

    try:
        # Запускаем мониторинг и управление воркерами
        monitor_and_restart_workers(num_workers)

    except Exception as e:
        logger.error(f"Критическая ошибка в supervisor: {e}", exc_info=True)
        sys.exit(1)

    logger.info("Supervisor завершен")
    sys.exit(0)


if __name__ == '__main__':
    # Для multiprocessing обязательно нужен этот guard
    multiprocessing.set_start_method('spawn', force=True)
    main()
