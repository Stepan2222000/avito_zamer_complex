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

# Количество воркеров (multiprocessing)
NUM_WORKERS = int(os.getenv('NUM_WORKERS', '15'))

# Настройки пула соединений asyncpg
# ВАЖНО: NUM_WORKERS × POOL_MAX_SIZE должно быть < PostgreSQL max_connections (обычно 100)
POOL_MIN_SIZE = int(os.getenv('POOL_MIN_SIZE', '2'))
POOL_MAX_SIZE = int(os.getenv('POOL_MAX_SIZE', '5'))  # NUM_WORKERS × 5 подключений

# Таймауты и интервалы (в секундах)
HEARTBEAT_INTERVAL = int(os.getenv('HEARTBEAT_INTERVAL', '120'))  # 2 минуты
STUCK_TASK_TIMEOUT = int(os.getenv('STUCK_TASK_TIMEOUT', '3600'))  # 1 час
MAX_RETRY_ATTEMPTS = int(os.getenv('MAX_RETRY_ATTEMPTS', '3'))

# API ключ для ИИ-валидации (Gemini через Google AI Studio)
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', 'AIzaSyAjRQnMmqZjt3eJnSoLlho1gC9fv1IcCo')

# Интервалы ожидания при отсутствии задач/прокси (в секундах)
NO_TASKS_WAIT = 5
NO_PROXIES_WAIT = 20

# Уникальный идентификатор воркера
# Устанавливается supervisor'ом (например: worker_1, worker_2, ...)
# Если переменная не установлена (старый режим), используем hostname + UUID
WORKER_ID = os.getenv('WORKER_ID', f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}")


def get_db_dsn() -> str:
    """
    Возвращает DSN строку для подключения к PostgreSQL

    Returns:
        str: DSN в формате postgresql://user:pass@host:port/dbname
    """
    return f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
