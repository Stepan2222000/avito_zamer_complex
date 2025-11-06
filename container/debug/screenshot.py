"""
Screenshot debugging utility for Playwright pages.

Provides a function to capture screenshots during debugging,
which can be enabled/disabled via the DEBUG_SCREENSHOTS environment variable.
"""

import os
import inspect
from pathlib import Path
from typing import Optional
from playwright.async_api import Page
import logging

logger = logging.getLogger(__name__)


async def debug_screenshot(page: Page, description: str) -> Optional[Path]:
    """
    Сохраняет скриншот страницы Playwright для отладки.

    Создает папку screenshots_{description} рядом с вызывающим файлом
    и сохраняет скриншот с автоинкрементным номером (screenshot_001.png, screenshot_002.png, ...).

    Args:
        page: Экземпляр Playwright Page для снятия скриншота
        description: Описание для имени папки (например, "after_catalog_parse")

    Returns:
        Path к сохраненному скриншоту или None если DEBUG_SCREENSHOTS=false

    Example:
        >>> from container.debug.screenshot import debug_screenshot
        >>> await debug_screenshot(page, "after_goto_catalog")
        # Создаст: container/worker/screenshots_after_goto_catalog/screenshot_001.png

    Environment Variables:
        DEBUG_SCREENSHOTS: "true" или "false" (по умолчанию "false")
    """
    # Проверка env переменной
    debug_enabled = os.getenv("DEBUG_SCREENSHOTS", "false").lower() == "true"
    if not debug_enabled:
        return None

    try:
        # Определение вызывающего файла (caller)
        frame = inspect.stack()[1]
        caller_file_path = Path(frame.filename).resolve()
        caller_dir = caller_file_path.parent

        # Создание папки для скриншотов
        screenshot_folder_name = f"screenshots_{description}"
        screenshot_dir = caller_dir / screenshot_folder_name
        screenshot_dir.mkdir(parents=True, exist_ok=True)

        # Поиск следующего доступного номера
        existing_screenshots = sorted(screenshot_dir.glob("screenshot_*.png"))
        if existing_screenshots:
            # Извлечь последний номер и инкрементировать
            last_file = existing_screenshots[-1]
            last_number = int(last_file.stem.split("_")[-1])
            next_number = last_number + 1
        else:
            next_number = 1

        # Формирование имени файла
        screenshot_filename = f"screenshot_{next_number:03d}.png"
        screenshot_path = screenshot_dir / screenshot_filename

        # Сохранение скриншота
        await page.screenshot(path=str(screenshot_path), full_page=True)

        logger.info(
            f"Debug screenshot saved: {screenshot_path.relative_to(caller_dir.parent)}"
        )

        return screenshot_path

    except Exception as e:
        logger.error(f"Failed to save debug screenshot '{description}': {e}")
        return None
