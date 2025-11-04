#!/bin/bash
set -e

# Очистка старых Xvfb процессов и lock файлов
echo "Cleaning up old Xvfb processes and locks..."
pkill Xvfb 2>/dev/null || true
rm -f /tmp/.X*-lock 2>/dev/null || true
rm -rf /tmp/.X11-unix/* 2>/dev/null || true

# Генерируем уникальный DISPLAY на основе hostname контейнера
# Docker Compose добавляет номер к hostname (например avito_zamer_complex-worker-1, avito_zamer_complex-worker-2)
DISPLAY_NUM=$(echo "$HOSTNAME" | grep -o '[0-9]*$')

# Если не удалось извлечь номер, используем случайное число
if [ -z "$DISPLAY_NUM" ]; then
    DISPLAY_NUM=$((99 + RANDOM % 100))
fi

export DISPLAY=:${DISPLAY_NUM}

echo "Starting Xvfb on DISPLAY=$DISPLAY for worker $HOSTNAME"

# Запускаем Xvfb на сгенерированном дисплее
Xvfb $DISPLAY -screen 0 1920x1080x24 -ac -nolisten tcp &
XVFB_PID=$!

# Ждем запуска Xvfb
sleep 3

# Генерируем уникальный WORKER_ID из hostname и PID
export WORKER_ID="${HOSTNAME}_$$"

echo "Worker ID: $WORKER_ID"
echo "Starting worker..."

# Запускаем воркер
python -m worker.main

# При завершении убиваем Xvfb и чистим lock файлы
kill $XVFB_PID 2>/dev/null || true
rm -f /tmp/.X${DISPLAY_NUM}-lock 2>/dev/null || true
