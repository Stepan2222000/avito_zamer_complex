# Базовый образ с Playwright, Python 3.13 и Xvfb
FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy

# Рабочая директория
WORKDIR /app

# Копируем requirements.txt и устанавливаем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код воркера
COPY worker/ ./worker/

# Копируем entrypoint скрипт
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Entrypoint для настройки Xvfb и запуска воркера
ENTRYPOINT ["/entrypoint.sh"]
