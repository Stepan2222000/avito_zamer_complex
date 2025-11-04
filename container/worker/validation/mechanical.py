"""
Механическая валидация объявлений

Проверяет стоп-слова и цены
"""

import logging
from typing import Optional
from ..stopwords import STOPWORDS

logger = logging.getLogger(__name__)


def check_stopwords(text: str) -> list:
    """
    Проверяет текст на наличие стоп-слов

    Для слов с дефисами, слэшами, точками (б/у, б.у, б-у) - поиск подстрокой
    Для остальных слов - поиск с границами слов

    Args:
        text: Текст для проверки

    Returns:
        list[str]: Найденные стоп-слова
    """
    if not text:
        return []

    text_lower = text.lower()
    found = []

    for stopword in STOPWORDS:
        # Проверяем наличие спецсимволов: дефис, слэш, точка
        if any(char in stopword for char in ['-', '/', '.']):
            # Поиск подстрокой для слов со спецсимволами
            if stopword in text_lower:
                found.append(stopword)
        else:
            # Поиск как отдельного слова (с границами)
            # Добавляем пробелы вокруг текста для корректной проверки начала/конца
            if f' {stopword} ' in f' {text_lower} ':
                found.append(stopword)

    return found


def calculate_price_threshold(prices: list) -> Optional[float]:
    """
    Вычисляет ценовой порог (50% от средней топ-20%)

    Исключает выбросы: цена >3× медианы топ-20%

    Args:
        prices: Список цен объявлений

    Returns:
        Optional[float]: Пороговая цена или None если недостаточно данных
    """
    if not prices:
        return None

    # Сортируем по убыванию
    sorted_prices = sorted(prices, reverse=True)

    # Берем топ-20% (минимум 1 объявление)
    top20_count = max(1, int(len(sorted_prices) * 0.2))
    top20_prices = sorted_prices[:top20_count]

    # Вычисляем медиану топ-20%
    sorted_top20 = sorted(top20_prices)
    median_index = len(sorted_top20) // 2
    median = sorted_top20[median_index]

    # Исключаем выбросы: цена >3× медианы
    filtered_top20 = [p for p in top20_prices if p <= median * 3]

    # Если после фильтрации ничего не осталось - используем медиану
    if not filtered_top20:
        filtered_top20 = [median]

    # Средняя цена топ-20% без выбросов
    avg_top20 = sum(filtered_top20) / len(filtered_top20)

    # Порог = 50% от средней
    threshold = avg_top20 * 0.5

    logger.info(
        f"Ценовой анализ: топ-{top20_count} цен (среднее {avg_top20:.2f}), "
        f"медиана {median:.2f}, порог 50% = {threshold:.2f}"
    )

    return threshold


def validate_mechanical(listings: list) -> dict:
    """
    Выполняет механическую валидацию всех объявлений

    Args:
        listings: Список объявлений из parse_catalog_until_complete

    Returns:
        dict[int, dict]: Словарь {avito_item_id: результат_валидации}
    """
    if not listings:
        return {}

    results = {}
    prices = [item.get('price', 0) for item in listings if item.get('price')]
    price_threshold = calculate_price_threshold(prices)

    for item in listings:
        avito_id = item.get('avito_item_id')
        if not avito_id:
            continue

        title = item.get('title', '')
        description = item.get('description', '')
        seller = item.get('seller', '')
        price = item.get('price', 0)

        # Проверка стоп-слов
        stopwords_title = check_stopwords(title)
        stopwords_desc = check_stopwords(description)
        stopwords_seller = check_stopwords(seller)

        all_stopwords = stopwords_title + stopwords_desc + stopwords_seller

        # Проверка цены
        price_valid = True
        if price_threshold and price > 0:
            price_valid = price >= price_threshold

        # Определение результата
        passed = not all_stopwords and price_valid

        if all_stopwords:
            rejection_reason = 'stopwords'
        elif not price_valid:
            rejection_reason = 'price'
        else:
            rejection_reason = None

        # Минимальная структура validation_details (согласно ответу 3A)
        validation_details = None

        results[avito_id] = {
            'passed': passed,
            'rejection_reason': rejection_reason,
            'validation_details': validation_details
        }

    passed_count = sum(1 for r in results.values() if r['passed'])
    logger.info(
        f"Механическая валидация завершена: {passed_count}/{len(results)} прошло"
    )

    return results
