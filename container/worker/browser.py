"""
Управление Playwright браузером и страницами

Функции для запуска, настройки и закрытия браузера с прокси
"""

from typing import Tuple, Dict
from playwright.async_api import async_playwright, Playwright, Browser, BrowserContext, Page
import logging

logger = logging.getLogger(__name__)


def parse_proxy_address(proxy_address: str) -> Dict[str, str]:
    """
    Парсит адрес прокси из формата host:port:username:password

    Args:
        proxy_address: Строка формата "host:port:username:password"

    Returns:
        Dict[str, str]: Словарь с ключами server, username, password

    Raises:
        ValueError: Если формат неверный
    """
    parts = proxy_address.split(':')

    if len(parts) != 4:
        raise ValueError(
            f"Неверный формат прокси: {proxy_address}. "
            f"Ожидается: host:port:username:password"
        )

    host, port, username, password = parts

    return {
        'server': f'{host}:{port}',
        'username': username,
        'password': password
    }


async def launch_browser(
    proxy_address: str
) -> Tuple[Playwright, Browser, BrowserContext, Page]:
    """
    Запускает браузер Chromium с настройками прокси

    Args:
        proxy_address: Адрес прокси в формате "host:port:username:password"

    Returns:
        Tuple: (playwright, browser, context, page)

    Raises:
        Exception: При ошибке запуска браузера
    """
    logger.info(f"Запуск браузера с прокси: {proxy_address.split(':')[0]}:****")

    # Парсим прокси
    proxy_config = parse_proxy_address(proxy_address)

    # Запускаем Playwright
    playwright = await async_playwright().start()

    # Запускаем браузер Chromium
    browser = await playwright.chromium.launch(
        headless=False,  # ОБЯЗАТЕЛЬНО по требованиям CLAUDE.md
        proxy={
            'server': proxy_config['server'],
            'username': proxy_config['username'],
            'password': proxy_config['password']
        },
        args=[
            '--disable-blink-features=AutomationControlled',  # Скрыть автоматизацию
            '--no-sandbox'  # Для работы в контейнере
        ]
    )

    # Создаем контекст браузера
    context = await browser.new_context(
        viewport={'width': 1920, 'height': 1080}
    )

    # Создаем страницу
    page = await context.new_page()

    logger.info("Браузер успешно запущен")

    return playwright, browser, context, page


async def cleanup_browser(
    playwright: Playwright,
    browser: Browser,
    context: BrowserContext,
    page: Page
) -> None:
    """
    Безопасно закрывает все ресурсы браузера

    Args:
        playwright: Объект Playwright
        browser: Объект браузера
        context: Контекст браузера
        page: Страница браузера
    """
    logger.info("Закрытие браузера")

    try:
        # Закрываем страницу
        if page and not page.is_closed():
            await page.close()
    except Exception as e:
        logger.error(f"Ошибка закрытия страницы: {e}")

    try:
        # Закрываем контекст
        if context:
            await context.close()
    except Exception as e:
        logger.error(f"Ошибка закрытия контекста: {e}")

    try:
        # Закрываем браузер
        if browser and browser.is_connected():
            await browser.close()
    except Exception as e:
        logger.error(f"Ошибка закрытия браузера: {e}")

    try:
        # Останавливаем Playwright
        if playwright:
            await playwright.stop()
    except Exception as e:
        logger.error(f"Ошибка остановки Playwright: {e}")

    logger.info("Браузер закрыт")
