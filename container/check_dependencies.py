#!/usr/bin/env python
"""
Скрипт проверки зависимостей перед запуском воркера.
Проверяет наличие всех критичных компонентов системы.
"""
import sys
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def check_avito_library():
    """Проверка наличия и корректности установки avito-library"""
    logger.info("Проверка avito-library...")

    try:
        # Попытка импортировать основные компоненты библиотеки
        from avito_library import resolve_captcha_flow, detect_page_state  # noqa: F401
        from avito_library.parsers.card_parser import parse_card  # noqa: F401
        from avito_library.parsers.catalog_parser import (  # noqa: F401
            parse_catalog_until_complete,
            wait_for_page_request,
            supply_page,
        )
        from avito_library.parsers.catalog_parser.steam import PageRequest  # noqa: F401
        logger.info("✓ avito-library установлена и все необходимые функции доступны")
        return True
    except ImportError as e:
        logger.error(f"✗ ОШИБКА: avito-library не установлена или установлена некорректно")
        logger.error(f"  Детали: {e}")
        logger.error(f"  Решение: Проверьте requirements.txt и пересоберите контейнер")
        return False


def check_playwright_browser():
    """Проверка наличия установленного браузера Chromium для Playwright"""
    logger.info("Проверка браузера Chromium...")

    try:
        from playwright.sync_api import sync_playwright

        # Проверяем наличие Chromium
        with sync_playwright() as p:
            try:
                # Пытаемся получить путь к браузеру (не запуская его)
                browser_type = p.chromium
                logger.info("✓ Playwright Chromium доступен")
                return True
            except Exception as e:
                logger.error(f"✗ ОШИБКА: Chromium не установлен для Playwright")
                logger.error(f"  Детали: {e}")
                logger.error(f"  Решение: Выполните 'playwright install chromium' или пересоберите контейнер")
                return False
    except ImportError as e:
        logger.error(f"✗ ОШИБКА: Playwright не установлен")
        logger.error(f"  Детали: {e}")
        return False


def check_database_driver():
    """Проверка наличия драйвера PostgreSQL"""
    logger.info("Проверка asyncpg...")

    try:
        import asyncpg
        logger.info(f"✓ asyncpg установлен (версия {asyncpg.__version__})")
        return True
    except ImportError as e:
        logger.error(f"✗ ОШИБКА: asyncpg не установлен")
        logger.error(f"  Детали: {e}")
        return False


def check_ai_validation():
    """Проверка наличия библиотеки для ИИ-валидации"""
    logger.info("Проверка openai...")

    try:
        import openai
        logger.info(f"✓ openai установлен")
        return True
    except ImportError as e:
        logger.error(f"✗ ОШИБКА: openai не установлен")
        logger.error(f"  Детали: {e}")
        return False


def check_python_version():
    """Проверка версии Python"""
    logger.info("Проверка версии Python...")

    version_info = sys.version_info
    version_str = f"{version_info.major}.{version_info.minor}.{version_info.micro}"

    # Требуется Python 3.11+ (совместимо с avito-library)
    if (version_info.major, version_info.minor) >= (3, 11):
        logger.info(f"✓ Python {version_str} (требуется 3.11+)")
        return True
    else:
        logger.error(f"✗ ОШИБКА: Python {version_str} не соответствует требованиям")
        logger.error(f"  Требуется: Python 3.11 или выше")
        return False


def main():
    """Основная функция проверки всех зависимостей"""
    logger.info("=" * 60)
    logger.info("Запуск проверки зависимостей воркера")
    logger.info("=" * 60)

    checks = [
        ("Python версия", check_python_version),
        ("avito-library", check_avito_library),
        ("Playwright Chromium", check_playwright_browser),
        ("asyncpg", check_database_driver),
        ("openai", check_ai_validation),
    ]

    results = []
    for name, check_func in checks:
        try:
            result = check_func()
            results.append((name, result))
        except Exception as e:
            logger.error(f"Неожиданная ошибка при проверке {name}: {e}")
            results.append((name, False))
        logger.info("")  # Пустая строка для читаемости

    # Итоговый результат
    logger.info("=" * 60)
    logger.info("Результаты проверки:")
    logger.info("=" * 60)

    all_passed = True
    for name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        logger.info(f"  {status}: {name}")
        if not result:
            all_passed = False

    logger.info("=" * 60)

    if all_passed:
        logger.info("✓ Все проверки пройдены успешно! Воркер готов к запуску.")
        return 0
    else:
        logger.error("✗ Некоторые проверки не пройдены. Воркер не может быть запущен.")
        logger.error("   Пожалуйста, исправьте указанные проблемы и пересоберите контейнер.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
