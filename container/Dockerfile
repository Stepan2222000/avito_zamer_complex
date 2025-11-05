# Базовый образ с Playwright и Xvfb
FROM mcr.microsoft.com/playwright/python:v1.55.0-jammy

# Рабочая директория
WORKDIR /app

# Устанавливаем Python 3.11 и создаём виртуальное окружение (avito-library требует >=3.11)
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3.11 python3.11-venv python3.11-distutils && \
    rm -rf /var/lib/apt/lists/*

RUN python3.11 -m venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

# Обновляем pip в окружении
RUN python -m pip install --upgrade pip

# Копируем requirements.txt и устанавливаем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Устанавливаем Chromium и его зависимости (необходимо для Playwright)
RUN playwright install chromium && \
    playwright install-deps chromium

# Копируем код воркера и вспомогательные скрипты
COPY worker/ ./worker/
COPY check_dependencies.py .
COPY supervisor.py .

# Копируем entrypoint и healthcheck скрипты
COPY entrypoint.sh /entrypoint.sh
COPY healthcheck.sh /app/healthcheck.sh
RUN chmod +x /entrypoint.sh /app/healthcheck.sh

# Entrypoint для настройки Xvfb и запуска воркера
ENTRYPOINT ["/entrypoint.sh"]
