"""
ИИ-валидация объявлений через Gemini 2.5-flash

Использует OpenAI библиотеку с Gemini endpoint через Google AI Studio
"""

import logging
import json
import asyncio
from openai import AsyncOpenAI

logger = logging.getLogger(__name__)

# Gemini endpoint через Google AI Studio (OpenAI-compatible)
GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"
GEMINI_MODEL = "gemini-2.5-flash"

# Промпт для Gemini
SYSTEM_PROMPT = """
Ты эксперт по проверке оригинальности товаров на основе объявлений.

ЗАДАЧА:
Проанализируй объявления и определи, какие из них предлагают ОРИГИНАЛЬНЫЕ товары.

КРИТЕРИИ ОТКЛОНЕНИЯ:
1. Скрытые признаки неоригинальности в тексте (завуалированные фразы типа "как оригинал", "качественная копия", "аналог оригинала", "совместимость", "подходит для")
2. Подозрительно низкая цена (дешевле 70% от среднего топ-20%)

ВАЖНО:
- Игнорируй явные стоп-слова (б/у, аналог) - они уже отфильтрованы механической валидацией
- Ищи СКРЫТЫЕ признаки и ценовые аномалии
- Будь строгим но справедливым
- Если нет признаков подделки - включай в passed_ids

ФОРМАТ ОТВЕТА (строго JSON):
{
    "passed_ids": [123, 456],
    "rejected": [
        {"avito_item_id": 789, "reason": "краткая причина отклонения"}
    ]
}
"""


def format_listings_for_prompt(listings: list, article: str) -> str:
    """
    Форматирует список объявлений для промпта

    Args:
        listings: Список объявлений
        article: Артикул

    Returns:
        str: Форматированный текст
    """
    lines = [f"Артикул: {article}", ""]

    # Добавляем ценовой ориентир
    prices = [item.get('price', 0) for item in listings if item.get('price')]

    if prices:
        sorted_prices = sorted(prices, reverse=True)
        top20_count = max(1, int(len(sorted_prices) * 0.2))
        top20_avg = sum(sorted_prices[:top20_count]) / top20_count
        price70 = top20_avg * 0.7

        lines.append(f"ЦЕНОВОЙ ОРИЕНТИР: топ-20% среднее = {top20_avg:.2f}₽, порог 70% = {price70:.2f}₽")
        lines.append("")

    # Добавляем объявления
    for item in listings:
        lines.append(f"ID: {item.get('avito_item_id')}")
        lines.append(f"Название: {item.get('title', 'N/A')}")
        lines.append(f"Описание: {item.get('description', 'N/A')}")
        lines.append(f"Цена: {item.get('price', 0)}₽")
        lines.append("")

    return "\n".join(lines)


async def validate_ai(
    listings: list,
    article: str,
    api_key: str
) -> dict:
    """
    Выполняет ИИ-валидацию объявлений через Gemini API

    Args:
        listings: Список объявлений (прошедших механическую валидацию)
        article: Артикул (для логирования)
        api_key: API ключ Google AI Studio

    Returns:
        dict[int, dict]: Словарь {avito_item_id: результат_валидации}

    Raises:
        Exception: При ошибках API
    """
    if not listings:
        return {}

    if not api_key:
        raise ValueError("GEMINI_API_KEY не установлен")

    logger.info(f"ИИ-валидация для {len(listings)} объявлений (артикул: {article})")

    # Создание клиента OpenAI с Gemini endpoint
    client = AsyncOpenAI(
        api_key=api_key,
        base_url=GEMINI_BASE_URL
    )

    # Подготовка промпта
    user_prompt = format_listings_for_prompt(listings, article)

    try:
        # Вызов Gemini через OpenAI API с timeout 60 секунд
        response = await asyncio.wait_for(
            client.chat.completions.create(
                model=GEMINI_MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.3
            ),
            timeout=60.0
        )

        # Парсинг JSON ответа
        result_text = response.choices[0].message.content
        result = json.loads(result_text)

        # Логирование статистики
        logger.info(
            f"ИИ-валидация завершена: {len(result.get('passed_ids', []))} прошло, "
            f"{len(result.get('rejected', []))} отклонено"
        )

        # Формирование результатов
        results = {}

        # Прошедшие валидацию
        for avito_id in result.get('passed_ids', []):
            results[avito_id] = {
                'passed': True,
                'rejection_reason': None,
                'validation_details': {
                    'stage': 'ai',
                    'decision': 'passed'
                }
            }

        # Отклоненные
        for rejected in result.get('rejected', []):
            avito_id = rejected.get('avito_item_id')
            reason = rejected.get('reason', 'AI rejection')

            if avito_id:
                results[avito_id] = {
                    'passed': False,
                    'rejection_reason': reason,
                    'validation_details': {
                        'stage': 'ai',
                        'decision': 'rejected',
                        'model_reason': reason
                    }
                }

        return results

    except asyncio.TimeoutError:
        # Timeout API запроса - пробрасываем для retry
        logger.error(f"Timeout ИИ-валидации (60 сек) для артикула {article}")
        raise

    except json.JSONDecodeError as e:
        logger.warning(f"Ошибка парсинга JSON от Gemini: {e}, response: {result_text[:200]}")
        # Fallback: считаем что все прошли
        return {
            item.get('avito_item_id'): {
                'passed': True,
                'rejection_reason': None,
                'validation_details': {
                    'stage': 'ai',
                    'decision': 'passed',
                    'fallback': 'json_decode_error'
                }
            }
            for item in listings if item.get('avito_item_id')
        }

    except Exception as e:
        logger.error(f"Ошибка ИИ-валидации: {e}", exc_info=True)
        raise
