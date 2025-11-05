"""
Главный цикл воркера для парсинга Авито

Реализует Этапы 3-8:
- Этап 3: Работа воркеров (инициализация, взятие задач, heartbeat)
- Этап 4: Парсинг каталога (через оркестратор-координатор)
- Этап 5: Валидация (механическая + ИИ через Gemini)
- Этап 6: Детальный парсинг карточек (через parse_card)
- Этап 7: Завершение задачи (автоматически)
- Этап 8: Обработка сбоев (retry, graceful shutdown)
"""

import asyncio
import signal
import logging
from typing import Optional, Tuple
from playwright.async_api import Page, Error as PlaywrightError

# Импорты из avito-library (обязательная зависимость)
from avito_library import resolve_captcha_flow, detect_page_state, CardParsingError
from avito_library.parsers.card_parser import parse_card
from avito_library.parsers.catalog_parser import (
    parse_catalog_until_complete,
    wait_for_page_request,
    supply_page,
)
from avito_library.parsers.catalog_parser.steam import PageRequest
# Детекторы состояний страницы
from avito_library import (
    CAPTCHA_DETECTOR_ID,
    CONTINUE_BUTTON_DETECTOR_ID,
    PROXY_BLOCK_429_DETECTOR_ID,
    PROXY_BLOCK_403_DETECTOR_ID,
    PROXY_AUTH_DETECTOR_ID,
    NOT_DETECTED_STATE_ID,
    CARD_FOUND_DETECTOR_ID,
)
# Исключения

from . import config
from . import database
from . import browser
from .errors import ProxyBlockedError, CaptchaNotSolvedError, NoProxiesAvailableError
from .validation import validate_mechanical, validate_ai

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

# Lock для защиты от race conditions при доступе к прокси и браузеру
# Используется когда coordinator и main_loop одновременно могут менять состояние
state_lock = None  # Будет инициализирован как asyncio.Lock() в main()

# Максимум попыток смены прокси при входе в каталог прежде чем вернуть задачу
CATALOG_PROXY_ROTATION_LIMIT = 5


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
        # Проверка что pool еще существует и воркер не завершается
        if not running or pool is None:
            logger.debug("Heartbeat завершен: воркер останавливается")
            break

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


async def block_and_cleanup_current_proxy(reason: str) -> None:
    """
    Блокирует текущий прокси (если есть) и закрывает связанные браузерные ресурсы.
    """
    global current_proxy_id, playwright_instance, browser_instance, context_instance, page_instance

    old_playwright = playwright_instance
    old_browser = browser_instance
    old_context = context_instance
    old_page = page_instance

    async with state_lock:
        if current_proxy_id:
            await database.block_proxy(pool, current_proxy_id, reason)
        current_proxy_id = None
        playwright_instance = browser_instance = context_instance = page_instance = None

    try:
        await browser.cleanup_browser(old_playwright, old_browser, old_context, old_page)
    except Exception as cleanup_error:
        logger.error(f"Ошибка cleanup браузера при блокировке прокси: {cleanup_error}", exc_info=True)


async def rotate_blocked_proxy(reason: str) -> bool:
    """
    Блокирует текущий прокси, берет новый и перезапускает браузер.

    Args:
        reason: Причина блокировки (логирование в БД)

    Returns:
        bool: True если удалось взять новый прокси и запустить браузер, иначе False
    """
    global current_proxy_id, playwright_instance, browser_instance, context_instance, page_instance

    await block_and_cleanup_current_proxy(reason)

    # Берем новый прокси
    proxy_result = await database.take_free_proxy(pool, config.WORKER_ID)
    if not proxy_result:
        logger.error("Нет свободных прокси после блокировки текущего")
        return False

    new_proxy_id, proxy_address = proxy_result
    logger.info(f"Пробуем новый прокси после блокировки: {proxy_address.split(':')[0]}:****")

    # Запускаем новый браузер
    try:
        new_playwright, new_browser, new_context, new_page = await browser.launch_browser(proxy_address)
    except Exception as e:
        logger.error(f"Ошибка запуска браузера при смене прокси: {e}")
        await database.block_proxy(pool, new_proxy_id, f"Browser launch error after rotate: {e}")
        return False

    # Фиксируем новые объекты
    async with state_lock:
        current_proxy_id = new_proxy_id
        playwright_instance = new_playwright
        browser_instance = new_browser
        context_instance = new_context
        page_instance = new_page

    return True


async def process_validation_and_save(
    task_id: int,
    article: str,
    listings: list
) -> tuple:
    """
    Выполняет валидацию и сохранение объявлений

    Этапы:
    1. Проверка существующих карточек
    2. Сохранение новых карточек
    3. Механическая валидация
    4. Сохранение результатов механической валидации
    5. ИИ-валидация прошедших механическую
    6. Сохранение результатов ИИ-валидации

    Args:
        task_id: ID задачи
        article: Артикул
        listings: Список объявлений из parse_catalog_until_complete

    Returns:
        tuple[int, int]: (количество найденных, количество прошедших)
    """
    if not listings:
        logger.info(f"Задача #{task_id}: нет объявлений для валидации")
        return (0, 0)

    logger.info(f"Задача #{task_id}: обработка {len(listings)} объявлений")

    # ШАГ 1: Проверка существующих карточек
    avito_ids = [item.get('avito_item_id') for item in listings if item.get('avito_item_id')]
    existing_ids = await database.check_existing_cards(pool, avito_ids)

    logger.info(f"Задача #{task_id}: {len(existing_ids)} карточек уже существуют")

    # ШАГ 2: Сохранение карточек (новые + обновление артикула для существующих)
    for item in listings:
        if not item.get('avito_item_id'):
            continue

        await database.save_parsed_card(
            pool=pool,
            avito_item_id=item['avito_item_id'],
            article=article,
            title=item.get('title', ''),
            description=item.get('description', ''),
            price=item.get('price', 0),
            seller_name=item.get('seller', ''),
            parsed_data=item
        )

    # ШАГ 3: Механическая валидация
    mechanical_results = validate_mechanical(listings)

    # ШАГ 4: Сохранение результатов механической валидации
    for avito_id, result in mechanical_results.items():
        await database.save_validation_result(
            pool=pool,
            avito_item_id=avito_id,
            validation_type='механическая',
            passed=result['passed'],
            rejection_reason=result['rejection_reason'],
            validation_details=result['validation_details']
        )

    # Считаем прошедших механическую
    mechanical_passed_ids = [
        avito_id for avito_id, res in mechanical_results.items() if res['passed']
    ]

    logger.info(
        f"Задача #{task_id}: механическая валидация - "
        f"{len(mechanical_passed_ids)}/{len(mechanical_results)} прошло"
    )

    # ШАГ 5: ИИ-валидация прошедших механическую
    items_passed = len(mechanical_passed_ids)

    if mechanical_passed_ids and config.GEMINI_API_KEY:
        # Получаем данные только прошедших механическую
        ai_listings = [
            item for item in listings
            if item.get('avito_item_id') in mechanical_passed_ids
        ]

        try:
            ai_results = await validate_ai(
                listings=ai_listings,
                article=article,
                api_key=config.GEMINI_API_KEY
            )

            # ШАГ 6: Сохранение результатов ИИ-валидации
            for avito_id, result in ai_results.items():
                await database.save_validation_result(
                    pool=pool,
                    avito_item_id=avito_id,
                    validation_type='ИИ',
                    passed=result['passed'],
                    rejection_reason=result['rejection_reason'],
                    validation_details=result['validation_details']
                )

            # Считаем финально прошедших (механическая + ИИ)
            ai_passed_ids = [
                avito_id for avito_id, res in ai_results.items() if res['passed']
            ]

            logger.info(
                f"Задача #{task_id}: ИИ-валидация - "
                f"{len(ai_passed_ids)}/{len(ai_results)} прошло"
            )

            items_passed = len(ai_passed_ids)

        except Exception as e:
            logger.error(f"Задача #{task_id}: ошибка ИИ-валидации: {e}", exc_info=True)
            # При ошибке ИИ - не падаем, считаем что прошли механическую
            items_passed = len(mechanical_passed_ids)

    else:
        # Если ИИ не настроена или нет прошедших механическую
        if not config.GEMINI_API_KEY:
            logger.warning(f"Задача #{task_id}: GEMINI_API_KEY не установлен, пропуск ИИ-валидации")

    return (len(listings), items_passed)


async def parse_detailed_cards(
    task_id: int,
    article: str,
    page: Page
) -> int:
    """
    Выполняет детальный парсинг карточек, прошедших валидацию (ЭТАП 6)

    Для каждой карточки:
    1. Переход на страницу объявления
    2. Проверка детектора + обработка капчи
    3. Парсинг через parse_card()
    4. Сохранение детальных данных в БД

    Args:
        task_id: ID задачи
        article: Артикул
        page: Страница Playwright (переиспользуется)

    Returns:
        int: Количество успешно спаршенных карточек

    Raises:
        CaptchaNotSolvedError: Если капча не решена (возврат задачи в очередь)
        ProxyBlockedError: Если прокси заблокирован (возврат задачи в очередь)
    """
    # Получение списка карточек для парсинга
    cards = await database.get_cards_for_detailed_parsing(pool, article)

    if not cards:
        logger.info(f"Задача #{task_id}: нет карточек для детального парсинга")
        return 0

    logger.info(f"Задача #{task_id}: детальный парсинг {len(cards)} карточек")

    success_count = 0
    error_count = 0

    for card in cards:
        avito_item_id = card['avito_item_id']
        title = card.get('title', 'N/A')

        try:
            # Формирование URL карточки
            card_url = f"https://www.avito.ru/{avito_item_id}"

            logger.info(f"Парсинг карточки {avito_item_id}: {title[:50]}...")

            # Переход на страницу карточки
            try:
                # Проверка что браузер еще подключен
                if browser_instance and not browser_instance.is_connected():
                    logger.error(f"Браузер отключен перед переходом на карточку {avito_item_id}")
                    raise PlaywrightError("Browser disconnected")

                await page.goto(card_url, timeout=30000)
            except asyncio.TimeoutError:
                # Timeout перехода на страницу - логируем и пропускаем карточку
                logger.warning(f"Timeout перехода на карточку {avito_item_id}, пропуск")
                error_count += 1
                continue
            except PlaywrightError as e:
                # Ошибка браузера - логируем и пропускаем карточку
                logger.warning(f"Playwright error при переходе на карточку {avito_item_id}: {e}, пропуск")
                error_count += 1
                continue

            # Проверка состояния страницы
            state = await detect_page_state(page)

            # Обработка капчи
            if not await check_and_solve_captcha(page, state, f"на карточке {avito_item_id}"):
                # Капча не решена - это фатальная ошибка, возвращаем задачу
                logger.error(f"Капча не решена на карточке {avito_item_id}, возврат задачи")
                raise CaptchaNotSolvedError(f"Captcha not solved for card {avito_item_id}")

            # Проверка блокировки прокси
            if state in [PROXY_BLOCK_403_DETECTOR_ID, PROXY_AUTH_DETECTOR_ID]:
                logger.error(f"Прокси заблокирован на карточке {avito_item_id}, возврат задачи")
                raise ProxyBlockedError(f"Proxy blocked for card {avito_item_id}")

            # Проверка что карточка найдена
            if state == NOT_DETECTED_STATE_ID:
                logger.warning(f"Карточка {avito_item_id} не найдена (удалена?), маркируем как DELETED")
                # Маркируем удаленную карточку чтобы не пытаться парсить её снова
                await database.update_card_detailed_data(
                    pool=pool,
                    avito_item_id=avito_item_id,
                    detailed_data={
                        'published_at': '1970-01-01',  # Sentinel значение для удаленных
                        'location': 'DELETED',
                        'views_count': 0,
                        'characteristics': {}
                    }
                )
                error_count += 1
                continue

            # Явная проверка что на странице карточка (критично для parse_card)
            if state != CARD_FOUND_DETECTOR_ID:
                logger.warning(
                    f"Неожиданное состояние страницы {state} для карточки {avito_item_id}, пропуск"
                )
                error_count += 1
                continue

            # Получение HTML страницы
            html = await page.content()

            # Парсинг карточки через avito-library
            card_data = await parse_card(
                html=html,
                fields=[
                    'title', 'price', 'seller', 'item_id',
                    'published_at', 'description', 'location',
                    'characteristics', 'views_total'
                ],
                ensure_card=True,
                include_html=False
            )

            # Валидация что получили ключевое поле (критично для retry-safe механизма)
            # Используем falsy check (не is None) чтобы отловить пустые строки и другие falsy значения
            if not card_data.published_at:
                logger.warning(
                    f"Карточка {avito_item_id} спаршена, но published_at отсутствует или пусто. "
                    f"Возможно неполные данные, пропуск."
                )
                error_count += 1
                continue

            # Подготовка данных для сохранения
            detailed_data = {
                'published_at': card_data.published_at,
                'location': card_data.location,
                'views_count': card_data.views_total,  # Маппинг поля
                'characteristics': card_data.characteristics,
                # Полные данные в JSONB
                'title': card_data.title,
                'price': card_data.price,
                'seller': card_data.seller,
                'item_id': card_data.item_id,
                'description': card_data.description,
            }

            # Сохранение в БД
            await database.update_card_detailed_data(
                pool=pool,
                avito_item_id=avito_item_id,
                detailed_data=detailed_data
            )

            success_count += 1
            logger.info(f"✓ Карточка {avito_item_id} успешно спаршена ({success_count}/{len(cards)})")

        except (CaptchaNotSolvedError, ProxyBlockedError):
            # Фатальные ошибки - пробрасываем наверх для возврата задачи
            raise

        except CardParsingError as e:
            # Ошибка парсинга конкретной карточки - логируем и пропускаем
            logger.warning(f"Ошибка парсинга карточки {avito_item_id}: {e}")
            error_count += 1
            continue

        except Exception as e:
            # Любая другая ошибка - логируем и пропускаем карточку
            logger.error(f"Неожиданная ошибка при парсинге карточки {avito_item_id}: {e}", exc_info=True)
            error_count += 1
            continue

    logger.info(
        f"Задача #{task_id}: детальный парсинг завершен | "
        f"Успешно: {success_count}/{len(cards)} | Ошибок: {error_count}"
    )

    return success_count


async def orchestrator_task(page: Page, catalog_url: str) -> dict:
    """
    Задача оркестратора - вызывает parse_catalog_until_complete

    Args:
        page: Страница Playwright
        catalog_url: URL каталога

    Returns:
        dict: Результаты парсинга
    """
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
    global current_proxy_id, playwright_instance, browser_instance, context_instance, page_instance, state_lock

    current_page = initial_page

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

            # Проверка что воркер еще активен (предотвращает race condition при shutdown)
            if not running:
                logger.info("Координатор останавливается (running=False)")
                break

            # Обрабатываем в зависимости от ошибки
            if page_req.status == "PROXY_BLOCKED":
                # Прокси заблокирован - меняем прокси и браузер
                logger.warning("Прокси заблокирован, смена прокси...")

                try:
                    # Критическая секция: смена прокси (защита от race condition)
                    # Сохраняем старые переменные для cleanup вне lock
                    old_playwright = playwright_instance
                    old_browser = browser_instance
                    old_context = context_instance
                    old_page = current_page

                    async with state_lock:
                        # Блокируем текущий прокси
                        await database.block_proxy(pool, current_proxy_id, "Blocked by Avito")

                        # Берем новый прокси
                        proxy_result = await database.take_free_proxy(pool, config.WORKER_ID)
                        if not proxy_result:
                            logger.error("Нет свободных прокси!")
                            raise NoProxiesAvailableError("Нет свободных прокси")

                        current_proxy_id, proxy_address = proxy_result
                        logger.info(f"Взят новый прокси: {proxy_address.split(':')[0]}:****")

                    # Cleanup и launch ВЫНЕСЕНЫ из lock для предотвращения deadlock
                    # Закрываем старый браузер
                    await browser.cleanup_browser(
                        old_playwright, old_browser, old_context, old_page
                    )

                    # Запускаем новый браузер с новым прокси
                    playwright_instance, browser_instance, context_instance, current_page = \
                        await browser.launch_browser(proxy_address)

                    # Переходим на нужную страницу каталога (вне lock)
                    url = f"{catalog_url}&p={page_req.next_start_page}"

                    # Проверка что браузер еще подключен и переход
                    try:
                        if browser_instance and not browser_instance.is_connected():
                            logger.error("Браузер отключен в coordinator перед goto")
                            raise PlaywrightError("Browser disconnected in coordinator")

                        await current_page.goto(url, timeout=30000)  # Таймаут 30 секунд
                    except (asyncio.TimeoutError, PlaywrightError) as e:
                        logger.error(f"Ошибка перехода в coordinator: {e}")
                        # При ошибке перехода - пробрасываем для обработки выше
                        raise

                    # Проверяем состояние страницы и решаем капчу если есть
                    state = await detect_page_state(current_page)
                    if not await check_and_solve_captcha(current_page, state, "в координаторе"):
                        # Капча не решена - освобождаем прокси, закрываем браузер и пробуем другой
                        logger.warning("Капча не решена после смены прокси, освобождаем текущий прокси")

                        # Сохраняем переменные для cleanup вне lock
                        cleanup_playwright = playwright_instance
                        cleanup_browser = browser_instance
                        cleanup_context = context_instance
                        cleanup_page = current_page

                        async with state_lock:
                            if current_proxy_id:
                                await database.release_proxy(pool, current_proxy_id)
                            current_proxy_id = None
                            playwright_instance = browser_instance = context_instance = None

                        # Cleanup ВЫНЕСЕН из lock для предотвращения deadlock
                        await browser.cleanup_browser(
                            cleanup_playwright, cleanup_browser, cleanup_context, cleanup_page
                        )
                        continue

                except Exception as e:
                    logger.error(f"Ошибка смены прокси в координаторе: {e}", exc_info=True)
                    # При ошибке пробрасываем исключение чтобы gather() упал и main_loop сделал cleanup
                    raise

            elif page_req.status in ["CAPTCHA_UNSOLVED", "CONTINUE_BUTTON", "RATE_LIMIT"]:
                # Капча или rate limit - пытаемся решить через общую функцию
                # Передаем любой из капча-детекторов, т.к. resolve_captcha_flow все равно сам определит тип
                if not await check_and_solve_captcha(current_page, CAPTCHA_DETECTOR_ID, f"от оркестратора ({page_req.status})"):
                    # Не решилась - освобождаем прокси и закрываем браузер
                    # Сохраняем переменные для cleanup вне lock
                    cleanup_playwright = playwright_instance
                    cleanup_browser = browser_instance
                    cleanup_context = context_instance
                    cleanup_page = current_page

                    async with state_lock:
                        if current_proxy_id:
                            await database.release_proxy(pool, current_proxy_id)
                        # Обнуляем все переменные браузера для повторного запуска
                        current_proxy_id = None
                        playwright_instance = browser_instance = context_instance = page_instance = None

                    # Cleanup ВЫНЕСЕН из lock для предотвращения deadlock
                    await browser.cleanup_browser(
                        cleanup_playwright, cleanup_browser, cleanup_context, cleanup_page
                    )
                    # Продолжаем цикл - будет новый запрос (берем новый прокси)
                    continue

            elif page_req.status == "NOT_DETECTED":
                # По решению пользователя - считаем успехом, ничего не делаем
                logger.info("NOT_DETECTED - считаем успехом, продолжаем")

            # Возвращаем страницу оркестратору
            supply_page(current_page)

        except asyncio.CancelledError:
            logger.info("Координатор отменен")
            # При отмене пробрасываем исключение для корректной остановки gather()
            raise
        except Exception as e:
            logger.error(f"Ошибка в координаторе: {e}", exc_info=True)
            # При ошибке пробрасываем исключение чтобы gather() упал и main_loop сделал cleanup
            raise


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
                    logger.warning(f"Нет свободных прокси, возвращаем задачу #{current_task_id} и ждем {config.NO_PROXIES_WAIT} сек")
                    await database.return_task_to_queue(
                        pool, current_task_id, "No proxies available", increment_retry=False
                    )
                    current_task_id = None
                    await asyncio.sleep(config.NO_PROXIES_WAIT)
                    continue

                # Атомарное присваивание прокси (защита от race condition с coordinator)
                async with state_lock:
                    current_proxy_id, proxy_address = proxy_result
                logger.info(f"Взят прокси: {proxy_address.split(':')[0]}:****")
            else:
                # Используем существующий прокси
                proxy_address = None  # Не нужно, браузер уже запущен

            # ШАГ 3: Запустить браузер (если нужно)
            # Проверяем что браузер действительно жив (не упал ранее)
            if playwright_instance and browser_instance:
                if not browser_instance.is_connected():
                    logger.warning("Обнаружен мертвый браузер, обнуляем переменные")
                    playwright_instance = browser_instance = context_instance = page_instance = None

            if not playwright_instance and proxy_address:
                try:
                    playwright_instance, browser_instance, context_instance, page_instance = \
                        await browser.launch_browser(proxy_address)
                except Exception as e:
                    logger.error(f"Ошибка запуска браузера: {e}")
                    # Атомарная блокировка прокси и обнуление переменных (защита от race condition)
                    async with state_lock:
                        await database.block_proxy(pool, current_proxy_id, f"Browser launch error: {e}")
                        # Обнуляем все переменные браузера
                        current_proxy_id = None
                        playwright_instance = browser_instance = context_instance = page_instance = None
                    # Возвращаем задачу
                    await database.return_task_to_queue(
                        pool, current_task_id, f"Browser error: {e}", increment_retry=True
                    )
                    current_task_id = None
                    continue

            # ШАГ 4: Переход на каталог и проверка состояния
            catalog_url = f"https://www.avito.ru/rossiya?q={article}&s=104"

            rotation_attempts = 0

            while True:
                # Проверка что браузер еще подключен и переход
                try:
                    if browser_instance and not browser_instance.is_connected():
                        logger.error("Браузер отключен перед переходом на каталог")
                        raise PlaywrightError("Browser disconnected before catalog goto")

                    await page_instance.goto(catalog_url, timeout=30000)  # Таймаут 30 секунд
                except (asyncio.TimeoutError, PlaywrightError) as e:
                    logger.error(f"Ошибка перехода на каталог: {e}")
                    # Атомарная блокировка прокси, cleanup и обнуление
                    async with state_lock:
                        if current_proxy_id:
                            await database.block_proxy(pool, current_proxy_id, f"Catalog goto error: {e}")
                        if playwright_instance and browser_instance:
                            await browser.cleanup_browser(
                                playwright_instance, browser_instance, context_instance, page_instance
                            )
                        current_proxy_id = None
                        playwright_instance = browser_instance = context_instance = page_instance = None
                    # Возврат задачи в очередь
                    await database.return_task_to_queue(
                        pool, current_task_id, f"Catalog goto error: {e}", increment_retry=True
                    )
                    current_task_id = None
                    break

                state = await detect_page_state(page_instance)

                # Проверка и решение капчи
                if not await check_and_solve_captcha(page_instance, state, "при переходе на каталог"):
                    # Капча не решена - освобождаем прокси и берем новый
                    logger.warning("Капча не решена, освобождаем прокси")
                    # Атомарное освобождение прокси, cleanup и обнуление (защита от race condition)
                    async with state_lock:
                        if current_proxy_id:
                            await database.release_proxy(pool, current_proxy_id)
                        await browser.cleanup_browser(
                            playwright_instance, browser_instance, context_instance, page_instance
                        )
                        current_proxy_id = None
                        playwright_instance = browser_instance = context_instance = page_instance = None
                    await database.return_task_to_queue(
                        pool, current_task_id, "Captcha not solved", increment_retry=True
                    )
                    current_task_id = None
                    break

                # Проверка блокировки прокси
                if state in [PROXY_BLOCK_403_DETECTOR_ID, PROXY_AUTH_DETECTOR_ID]:
                    logger.warning("Прокси заблокирован при переходе, пробуем сменить без возврата задачи")
                    rotation_attempts += 1

                    if rotation_attempts > CATALOG_PROXY_ROTATION_LIMIT:
                        logger.error("Превышен лимит смены прокси при переходе на каталог")
                        await block_and_cleanup_current_proxy("Proxy blocked during catalog goto (rotation limit reached)")
                        await database.return_task_to_queue(
                            pool, current_task_id, "Proxy blocked (rotation limit)", increment_retry=False
                        )
                        current_task_id = None
                        break

                    if await rotate_blocked_proxy("Proxy blocked during catalog goto (403/407)"):
                        logger.info("Прокси сменен, повторяем переход на каталог")
                        continue

                    logger.error("Не удалось сменить прокси после блокировки")
                    await database.return_task_to_queue(
                        pool, current_task_id, "Proxy blocked (rotation failed)", increment_retry=False
                    )
                    current_task_id = None
                    break

                # Если дошли сюда - переход успешен
                break

            if current_task_id is None:
                continue

            # ШАГ 5: Запустить heartbeat
            heartbeat_task = asyncio.create_task(heartbeat_loop(current_task_id))

            try:
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

                    # Cleanup браузера и прокси при ошибке парсинга (избегаем утечку ресурсов)
                    async with state_lock:
                        if playwright_instance and browser_instance:
                            try:
                                # Добавляем timeout для cleanup чтобы он не зависал
                                await asyncio.wait_for(
                                    browser.cleanup_browser(
                                        playwright_instance, browser_instance, context_instance, page_instance
                                    ),
                                    timeout=10
                                )
                            except asyncio.TimeoutError:
                                logger.error("Timeout при cleanup браузера после ошибки парсинга")
                            except Exception as cleanup_error:
                                logger.error(f"Ошибка при cleanup браузера: {cleanup_error}", exc_info=True)
                        # Обнуляем переменные браузера и прокси
                        current_proxy_id = None
                        playwright_instance = browser_instance = context_instance = page_instance = None

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

                # ШАГ 8: Валидация и сохранение результатов
                listings = result.get('listings', [])
                logger.info(f"Парсинг завершен: {len(listings)} объявлений")

                # Валидация (механическая + ИИ) и сохранение
                items_found, items_passed = await process_validation_and_save(
                    current_task_id,
                    article,
                    listings
                )

                # ШАГ 8.5: Детальный парсинг карточек (ЭТАП 6)
                try:
                    items_detailed = await parse_detailed_cards(
                        task_id=current_task_id,
                        article=article,
                        page=page_instance
                    )
                    logger.info(f"Детально спаршено: {items_detailed} карточек")

                except (CaptchaNotSolvedError, ProxyBlockedError) as e:
                    # Фатальная ошибка детального парсинга - возврат задачи
                    logger.error(f"Фатальная ошибка детального парсинга: {e}")
                    # Cleanup браузера и прокси
                    async with state_lock:
                        if playwright_instance and browser_instance:
                            await browser.cleanup_browser(
                                playwright_instance, browser_instance, context_instance, page_instance
                            )
                        # Обнуляем переменные
                        current_proxy_id = None
                        playwright_instance = browser_instance = context_instance = page_instance = None

                    # Возврат задачи в очередь
                    await database.return_task_to_queue(
                        pool, current_task_id, f"Detailed parsing error: {e}", increment_retry=True
                    )
                    current_task_id = None
                    continue

                # Определяем статус обработки
                if items_found == 0:
                    processing_status = 'no_results'
                else:
                    processing_status = 'success'

                # Завершение задачи
                await database.complete_task(
                    pool,
                    current_task_id,
                    article,
                    worker_id=config.WORKER_ID,
                    processing_status=processing_status,
                    items_found=items_found,
                    items_passed=items_passed
                )
                logger.info(
                    f"✅ Задача #{current_task_id} завершена успешно | "
                    f"Найдено: {items_found} | Прошло: {items_passed}"
                )

                current_task_id = None

                # ШАГ 9: Переиспользование ресурсов
                # Браузер и прокси остаются для следующей задачи

            finally:
                # Отменяем heartbeat в любом случае (гарантирует остановку при ошибке)
                if heartbeat_task and not heartbeat_task.done():
                    heartbeat_task.cancel()
                    try:
                        await heartbeat_task
                    except asyncio.CancelledError:
                        pass

        except KeyboardInterrupt:
            logger.info("Получен Ctrl+C, завершение работы")
            break
        except Exception as e:
            logger.error(f"Ошибка в главном цикле: {e}", exc_info=True)

            # Cleanup ресурсов при критической ошибке
            async with state_lock:
                # Cleanup браузера
                if playwright_instance and browser_instance:
                    try:
                        await asyncio.wait_for(
                            browser.cleanup_browser(
                                playwright_instance, browser_instance, context_instance, page_instance
                            ),
                            timeout=10
                        )
                    except Exception as cleanup_error:
                        logger.error(f"Ошибка при cleanup браузера в главном цикле: {cleanup_error}")

                # Освобождение прокси
                if current_proxy_id:
                    try:
                        await database.release_proxy(pool, current_proxy_id)
                    except Exception as proxy_error:
                        logger.error(f"Ошибка при освобождении прокси: {proxy_error}")

                # Обнуляем все переменные
                current_proxy_id = None
                playwright_instance = browser_instance = context_instance = page_instance = None

            # Возвращаем задачу в очередь если она есть
            if current_task_id:
                try:
                    await database.return_task_to_queue(
                        pool, current_task_id, f"Worker error: {e}", increment_retry=True
                    )
                except Exception as db_error:
                    logger.error(f"Ошибка при возврате задачи в очередь: {db_error}")
                current_task_id = None

            await asyncio.sleep(5)


async def main():
    """Точка входа воркера"""
    global pool, running, state_lock

    logger.info(
        f"Запуск воркера {config.WORKER_ID} | "
        f"БД: {config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"
    )

    # Инициализация lock для защиты от race conditions
    state_lock = asyncio.Lock()

    # Установка обработчика SIGTERM
    signal.signal(signal.SIGTERM, sigterm_handler)

    try:
        # Логирование успешной инициализации avito-library
        logger.info("✓ avito-library успешно инициализирована")

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
