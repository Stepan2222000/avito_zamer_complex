"""
Конфигурация воркера из переменных окружения

Все настройки читаются из env-переменных, установленных в docker-compose.yml
"""

import os
import socket
import uuid


# Подключение к БД
DB_HOST = os.getenv('DB_HOST', '81.30.105.134')
DB_PORT = int(os.getenv('DB_PORT', '5417'))
DB_NAME = os.getenv('DB_NAME', 'system_avito_zamer')
DB_USER = os.getenv('DB_USER', 'admin')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'Password123')

# Настройки пула соединений asyncpg
POOL_MIN_SIZE = 2
POOL_MAX_SIZE = 10

# Таймауты и интервалы (в секундах)
HEARTBEAT_INTERVAL = int(os.getenv('HEARTBEAT_INTERVAL', '120'))  # 2 минуты
STUCK_TASK_TIMEOUT = int(os.getenv('STUCK_TASK_TIMEOUT', '3600'))  # 1 час
MAX_RETRY_ATTEMPTS = int(os.getenv('MAX_RETRY_ATTEMPTS', '3'))

# API ключ для ИИ-валидации (OpenAI)
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')

# Интервалы ожидания при отсутствии задач/прокси (в секундах)
NO_TASKS_WAIT = 5
NO_PROXIES_WAIT = 60

# Уникальный идентификатор воркера
# Формируется из hostname контейнера + UUID для уникальности
WORKER_ID = f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"


def get_db_dsn() -> str:
    """
    Возвращает DSN строку для подключения к PostgreSQL

    Returns:
        str: DSN в формате postgresql://user:pass@host:port/dbname
    """
    return f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
