"""
Главный цикл воркера для парсинга Авито

Реализует Этапы 3-4: работа воркеров + парсинг каталога
"""

import asyncio
import signal
import logging
from typing import Optional, Tuple
from playwright.async_api import Page

# Импорты из avito-library (будут доступны после установки)
try:
    from avito_library import (
        parse_catalog_until_complete,
        wait_for_page_request,
        supply_page,
        resolve_captcha_flow,
        detect_page_state,
        PageRequest
    )
    # Детекторы состояний страницы
    from avito_library.detector_ids import (
        CAPTCHA_DETECTOR_ID,
        CONTINUE_BUTTON_DETECTOR_ID,
        PROXY_BLOCK_429_DETECTOR_ID,
        PROXY_BLOCK_403_DETECTOR_ID,
        PROXY_AUTH_DETECTOR_ID,
        NOT_DETECTED_STATE_ID,
        SUCCESS_DETECTOR_ID
    )
    AVITO_LIBRARY_AVAILABLE = True
except ImportError:
    AVITO_LIBRARY_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.warning("avito-library не установлена - работа в режиме заглушки")

from . import config
from . import database
from . import browser
from .errors import ProxyBlockedError, CaptchaNotSolvedError, NoProxiesAvailableError

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Глобальные переменные для graceful shutdown
running = True
pool = None
current_task_id = None
current_proxy_id = None
heartbeat_task = None
playwright_instance = None
browser_instance = None
context_instance = None
page_instance = None


async def graceful_shutdown():
    """Graceful shutdown при получении SIGTERM"""
    global running, pool, current_task_id, current_proxy_id
    global playwright_instance, browser_instance, context_instance, page_instance

    logger.info(f"Получен сигнал SIGTERM, начинаем graceful shutdown")
    running = False

    # Отменяем heartbeat task
    if heartbeat_task and not heartbeat_task.done():
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass

    # Возвращаем текущую задачу в очередь
    if pool and current_task_id:
        logger.info(f"Возврат текущей задачи #{current_task_id} в очередь")
        await database.return_task_to_queue(
            pool, current_task_id, "Worker shutdown", increment_retry=False
        )

    # Освобождаем прокси
    if pool and current_proxy_id:
        logger.info(f"Освобождение прокси #{current_proxy_id}")
        await database.release_proxy(pool, current_proxy_id)

    # Закрываем браузер
    if playwright_instance:
        await browser.cleanup_browser(
            playwright_instance, browser_instance, context_instance, page_instance
        )

    # Закрываем пул соединений БД
    if pool:
        await database.close_pool(pool)

    logger.info("Graceful shutdown завершен")


def sigterm_handler(signum, frame):
    """Обработчик сигнала SIGTERM"""
    global running
    running = False
    logger.info("Получен сигнал SIGTERM, установлен флаг running=False")


async def heartbeat_loop(task_id: int):
    """
    Фоновая задача обновления heartbeat

    Args:
        task_id: ID текущей задачи
    """
    while True:
        try:
            await asyncio.sleep(config.HEARTBEAT_INTERVAL)
            await database.update_heartbeat(pool, task_id)
            logger.debug(f"Heartbeat обновлен для задачи #{task_id}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            # По решению пользователя - продолжаем работу при ошибке heartbeat
            logger.warning(f"Ошибка обновления heartbeat: {e}")


async def check_and_solve_captcha(page: Page, state: str, context: str = "") -> bool:
    """
    Проверяет состояние страницы на капчу и пытается решить

    Вынесенная общая логика для избежания дублирования кода

    Args:
        page: Страница Playwright
        state: Результат detect_page_state
        context: Контекст для логирования (например, "при переходе")

    Returns:
        bool: True если капчи нет или успешно решена, False если не решена
    """
    if not AVITO_LIBRARY_AVAILABLE:
        return True

    # Проверяем наличие капчи
    if state not in [CAPTCHA_DETECTOR_ID, CONTINUE_BUTTON_DETECTOR_ID, PROXY_BLOCK_429_DETECTOR_ID]:
        return True  # Капчи нет

    # Капча обнаружена - пытаемся решить
    logger.info(f"Обнаружена капча {context}, решаем...")
    _, solved = await resolve_captcha_flow(page)

    if solved:
        logger.info(f"Капча {context} успешно решена")
        return True
    else:
        logger.warning(f"Капча {context} не решена")
        return False


async def process_task_stub(task_id: int, article: str) -> dict:
    """
    ЗАГЛУШКА для обработки задачи на Этапе 3-4

    На Этапах 5-6 будет заменена на реальную валидацию и детальный парсинг

    Args:
        task_id: ID задачи
        article: Артикул

    Returns:
        dict: Результаты обработки
    """
    logger.info(f"[ЗАГЛУШКА] Обработка результатов парсинга для задачи #{task_id}, артикул: {article}")
    await asyncio.sleep(2)  # Имитация обработки
    return {'processed': True}


async def orchestrator_task(page: Page, catalog_url: str) -> dict:
    """
    Задача оркестратора - вызывает parse_catalog_until_complete

    Args:
        page: Страница Playwright
        catalog_url: URL каталога

    Returns:
        dict: Результаты парсинга
    """
    if not AVITO_LIBRARY_AVAILABLE:
        logger.warning("[ЗАГЛУШКА] avito-library недоступна, имитация парсинга")
        await asyncio.sleep(5)
        return {'status': 'SUCCESS', 'listings': [], 'completed': True}

    result = await parse_catalog_until_complete(
        page=page,
        catalog_url=catalog_url,
        fields=['avito_id', 'title', 'description', 'price', 'seller'],
        max_pages=None,  # БЕЗ ОГРАНИЧЕНИЙ - парсим все страницы
        sort_by_date=True,
        start_page=1
    )

    return result


async def coordinator_task(initial_page: Page, catalog_url: str, article: str):
    """
    Задача координатора - обрабатывает запросы от оркестратора

    Args:
        initial_page: Начальная страница
        catalog_url: URL каталога
        article: Артикул
    """
    global current_proxy_id, playwright_instance, browser_instance, context_instance, page_instance

    current_page = initial_page

    if not AVITO_LIBRARY_AVAILABLE:
        logger.warning("[ЗАГЛУШКА] avito-library недоступна, координатор не активен")
        # В режиме заглушки координатор просто ждет
        await asyncio.sleep(10)
        return

    while running:
        try:
            # Ждем запрос от оркестратора с timeout (300 сек = 5 минут)
            # Если timeout истек, значит orchestrator завершился
            try:
                page_req: PageRequest = await asyncio.wait_for(
                    wait_for_page_request(),
                    timeout=300.0
                )
            except asyncio.TimeoutError:
                logger.info("Timeout ожидания запроса от оркестратора - завершаем координатор")
                break

            logger.info(
                f"Получен запрос на новую страницу: status={page_req.status}, "
                f"attempt={page_req.attempt}, next_page={page_req.next_start_page}"
            )

            # Обрабатываем в зависимости от ошибки
            if page_req.status == "PROXY_BLOCKED":
                # Прокси заблокирован - меняем прокси и браузер
                logger.warning("Прокси заблокирован, смена прокси...")

                try:
                    # Блокируем текущий прокси
                    await database.block_proxy(pool, current_proxy_id, "Blocked by Avito")

                    # Берем новый прокси
                    proxy_result = await database.take_free_proxy(pool, config.WORKER_ID)
                    if not proxy_result:
                        logger.error("Нет свободных прокси!")
                        raise NoProxiesAvailableError("Нет свободных прокси")

                    current_proxy_id, proxy_address = proxy_result
                    logger.info(f"Взят новый прокси: {proxy_address.split(':')[0]}:****")

                    # Закрываем старый браузер
                    await browser.cleanup_browser(
                        playwright_instance, browser_instance, context_instance, current_page
                    )

                    # Запускаем новый браузер с новым прокси
                    playwright_instance, browser_instance, context_instance, current_page = \
                        await browser.launch_browser(proxy_address)

                    # Переходим на нужную страницу каталога
                    url = f"{catalog_url}&p={page_req.next_start_page}"
                    await current_page.goto(url)

                    # Проверяем состояние страницы и решаем капчу если есть
                    state = await detect_page_state(current_page)
                    if not await check_and_solve_captcha(current_page, state, "в координаторе"):
                        # Капча не решена - повторная смена прокси
                        continue

                except Exception as e:
                    logger.error(f"Ошибка смены прокси в координаторе: {e}", exc_info=True)
                    # При ошибке завершаем координатор
                    break

            elif page_req.status in ["CAPTCHA_UNSOLVED", "CONTINUE_BUTTON", "RATE_LIMIT"]:
                # Капча или rate limit - пытаемся решить через общую функцию
                # Передаем любой из капча-детекторов, т.к. resolve_captcha_flow все равно сам определит тип
                if not await check_and_solve_captcha(current_page, CAPTCHA_DETECTOR_ID, f"от оркестратора ({page_req.status})"):
                    # Не решилась - блокируем прокси и закрываем браузер
                    await database.block_proxy(pool, current_proxy_id, "Captcha failed")
                    await browser.cleanup_browser(
                        playwright_instance, browser_instance, context_instance, current_page
                    )
                    current_proxy_id = None
                    playwright_instance = browser_instance = context_instance = None
                    # Продолжаем цикл - будет новый запрос (берем новый прокси)
                    continue

            elif page_req.status == "NOT_DETECTED":
                # По решению пользователя - считаем успехом, ничего не делаем
                logger.info("NOT_DETECTED - считаем успехом, продолжаем")

            # Возвращаем страницу оркестратору
            await supply_page(current_page)

        except asyncio.CancelledError:
            logger.info("Координатор отменен")
            break
        except Exception as e:
            logger.error(f"Ошибка в координаторе: {e}", exc_info=True)
            break


async def worker_main_loop():
    """Главный цикл воркера"""
    global running, pool, current_task_id, current_proxy_id, heartbeat_task
    global playwright_instance, browser_instance, context_instance, page_instance

    logger.info(f"Запуск главного цикла воркера {config.WORKER_ID}")

    while running:
        try:
            # ШАГ 1: Взять задачу из очереди
            task_result = await database.take_next_task(pool, config.WORKER_ID)

            if not task_result:
                logger.info("Нет задач в очереди, ожидание...")
                await asyncio.sleep(config.NO_TASKS_WAIT)
                continue

            current_task_id, article = task_result
            logger.info(f"Взята задача #{current_task_id}, артикул: {article}")

            # ШАГ 2: Взять прокси (если нужно)
            if not current_proxy_id:
                proxy_result = await database.take_free_proxy(pool, config.WORKER_ID)

                if not proxy_result:
                    logger.warning("Нет свободных прокси, возврат задачи в очередь")
                    await database.return_task_to_queue(
                        pool, current_task_id, "No proxies available", increment_retry=False
                    )
                    current_task_id = None
                    await asyncio.sleep(config.NO_PROXIES_WAIT)
                    continue

                current_proxy_id, proxy_address = proxy_result
                logger.info(f"Взят прокси: {proxy_address.split(':')[0]}:****")
            else:
                # Используем существующий прокси
                proxy_address = None  # Не нужно, браузер уже запущен

            # ШАГ 3: Запустить браузер (если нужно)
            if not playwright_instance and proxy_address:
                try:
                    playwright_instance, browser_instance, context_instance, page_instance = \
                        await browser.launch_browser(proxy_address)
                except Exception as e:
                    logger.error(f"Ошибка запуска браузера: {e}")
                    # Блокируем прокси
                    await database.block_proxy(pool, current_proxy_id, f"Browser launch error: {e}")
                    current_proxy_id = None
                    playwright_instance = None
                    # Возвращаем задачу
                    await database.return_task_to_queue(
                        pool, current_task_id, f"Browser error: {e}", increment_retry=True
                    )
                    current_task_id = None
                    continue

            # ШАГ 4: Переход на каталог и проверка состояния
            catalog_url = f"https://www.avito.ru/rossiya?q={article}&s=104"

            if AVITO_LIBRARY_AVAILABLE:
                await page_instance.goto(catalog_url)
                state = await detect_page_state(page_instance)

                # Проверка и решение капчи
                if not await check_and_solve_captcha(page_instance, state, "при переходе на каталог"):
                    # Капча не решена - блокируем прокси и берем новый
                    logger.warning("Капча не решена, блокируем прокси")
                    await database.block_proxy(pool, current_proxy_id, "Captcha failed")
                    await browser.cleanup_browser(
                        playwright_instance, browser_instance, context_instance, page_instance
                    )
                    current_proxy_id = None
                    playwright_instance = browser_instance = context_instance = page_instance = None
                    await database.return_task_to_queue(
                        pool, current_task_id, "Captcha not solved", increment_retry=True
                    )
                    current_task_id = None
                    continue

                # Проверка блокировки прокси
                if state in [PROXY_BLOCK_403_DETECTOR_ID, PROXY_AUTH_DETECTOR_ID]:
                    logger.warning("Прокси заблокирован при переходе")
                    await database.block_proxy(pool, current_proxy_id, "403/407")
                    await browser.cleanup_browser(
                        playwright_instance, browser_instance, context_instance, page_instance
                    )
                    current_proxy_id = None
                    playwright_instance = browser_instance = context_instance = page_instance = None
                    await database.return_task_to_queue(
                        pool, current_task_id, "Proxy blocked", increment_retry=False
                    )
                    current_task_id = None
                    continue

            # ШАГ 5: Запустить heartbeat
            heartbeat_task = asyncio.create_task(heartbeat_loop(current_task_id))

            # ШАГ 6: Запустить парсинг с двумя параллельными задачами
            logger.info(f"Запуск парсинга для артикула: {article}")

            try:
                result, _ = await asyncio.gather(
                    orchestrator_task(page_instance, catalog_url),
                    coordinator_task(page_instance, catalog_url, article)
                )
            except Exception as e:
                logger.error(f"Ошибка парсинга: {e}", exc_info=True)
                result = {'status': 'ERROR', 'details': str(e)}

            # Отменяем heartbeat
            if heartbeat_task and not heartbeat_task.done():
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass

            # ШАГ 7: Обработка результатов
            if result.get('status') != 'SUCCESS' or result.get('attempts_exhausted'):
                # Проверяем счетчик попыток
                retry_count = await database.get_task_retry_count(pool, current_task_id)

                if retry_count >= config.MAX_RETRY_ATTEMPTS:
                    # Исчерпаны попытки - помечаем как ошибку
                    await database.mark_task_as_error(
                        pool, current_task_id, result.get('details', 'Attempts exhausted')
                    )
                    logger.error(f"Задача #{current_task_id} помечена как ошибка после {retry_count} попыток")
                else:
                    # Возвращаем в очередь
                    await database.return_task_to_queue(
                        pool, current_task_id, result.get('details', 'Parse failed'), increment_retry=True
                    )
                    logger.warning(f"Задача #{current_task_id} возвращена в очередь, попытка {retry_count + 1}/{config.MAX_RETRY_ATTEMPTS}")

                current_task_id = None
                continue

            # ШАГ 8: Обработка и сохранение (в одной транзакции)
            listings = result.get('listings', [])
            logger.info(f"Парсинг завершен: {len(listings)} объявлений")

            # Обработка результатов (заглушка на Этапе 3-4)
            await process_task_stub(current_task_id, article)

            # Определяем статус обработки
            processing_status = 'no_results' if len(listings) == 0 else 'success'

            # Завершение задачи
            await database.complete_task(
                pool,
                current_task_id,
                article,
                worker_id=config.WORKER_ID,
                processing_status=processing_status,
                items_found=len(listings),
                items_passed=None  # Валидация будет на Этапе 5
            )
            logger.info(f"✅ Задача #{current_task_id} завершена успешно")

            current_task_id = None

            # ШАГ 9: Переиспользование ресурсов
            # Браузер и прокси остаются для следующей задачи

        except KeyboardInterrupt:
            logger.info("Получен Ctrl+C, завершение работы")
            break
        except Exception as e:
            logger.error(f"Ошибка в главном цикле: {e}", exc_info=True)
            # Возвращаем задачу в очередь если она есть
            if current_task_id:
                await database.return_task_to_queue(
                    pool, current_task_id, f"Worker error: {e}", increment_retry=True
                )
                current_task_id = None
            await asyncio.sleep(5)


async def main():
    """Точка входа воркера"""
    global pool, running

    logger.info(
        f"Запуск воркера {config.WORKER_ID} | "
        f"БД: {config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"
    )

    # Установка обработчика SIGTERM
    signal.signal(signal.SIGTERM, sigterm_handler)

    try:
        # Создание пула соединений
        pool = await database.create_pool()

        # Возврат зависших задач
        stuck_count = await database.return_stuck_tasks(pool)
        if stuck_count > 0:
            logger.warning(f"Возвращено зависших задач: {stuck_count}")

        logger.info(f"✓ Воркер {config.WORKER_ID} готов к работе")

        # Запуск главного цикла
        await worker_main_loop()

    except KeyboardInterrupt:
        logger.info("Получен сигнал остановки")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
        raise
    finally:
        await graceful_shutdown()


if __name__ == '__main__':
    asyncio.run(main())
