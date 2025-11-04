"""
Конфигурация воркера из переменных окружения
"""

import os


# Подключение к БД
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', '5432'))
DB_NAME = os.getenv('DB_NAME', 'avito_parser')
DB_USER = os.getenv('DB_USER', 'parser')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')

# Идентификатор воркера
WORKER_ID = os.getenv('WORKER_ID', 'unknown_worker')

# Настройки heartbeat
HEARTBEAT_INTERVAL = int(os.getenv('HEARTBEAT_INTERVAL', '120'))  # 2 минуты
STUCK_TASK_TIMEOUT = int(os.getenv('STUCK_TASK_TIMEOUT', '3600'))  # 1 час

# Максимальное количество попыток обработки задачи
MAX_RETRY_ATTEMPTS = int(os.getenv('MAX_RETRY_ATTEMPTS', '3'))

# API ключ для ИИ-валидации (OpenAI)
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')

# Параметры connection pool для asyncpg
DB_POOL_MIN_SIZE = 2  # Минимум соединений
DB_POOL_MAX_SIZE = 10  # Максимум соединений
DB_COMMAND_TIMEOUT = 60  # Таймаут команды в секундах
DB_CONNECTION_TIMEOUT = 10  # Таймаут подключения в секундах
