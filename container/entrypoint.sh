#!/bin/bash
set -e

# Функция для ожидания готовности Xvfb
wait_for_xvfb() {
    local display_num=$1
    local timeout=30  # 30 секунд
    local count=0

    echo "Waiting for DISPLAY=:${display_num} to be ready..."

    while [ $count -lt $((timeout * 10)) ]; do
        # Проверяем существование X11 socket файла
        if [ -S "/tmp/.X11-unix/X${display_num}" ]; then
            echo "✓ DISPLAY=:${display_num} is ready"
            return 0
        fi

        sleep 0.1
        count=$((count + 1))
    done

    echo "✗ Timeout waiting for DISPLAY=:${display_num}"
    return 1
}

# Функция cleanup для гарантированной очистки Xvfb
cleanup_xvfb() {
    echo "Cleaning up Xvfb processes..."

    for i in $(seq 1 $NUM_WORKERS); do
        pid=${XVFB_PIDS[$i]}
        DISPLAY_NUM=$((99 + i - 1))

        if [ -n "$pid" ]; then
            # Проверяем не zombie ли процесс
            if ps -p "$pid" -o stat= 2>/dev/null | grep -q Z; then
                echo "Process $pid is zombie, reaping..."
                wait "$pid" 2>/dev/null || true
            else
                # Отправляем SIGTERM
                kill -TERM "$pid" 2>/dev/null || true

                # Ждем завершения с timeout
                for retry in {1..10}; do
                    if ! kill -0 "$pid" 2>/dev/null; then
                        break
                    fi
                    sleep 0.5
                done

                # Если не завершился - принудительное завершение
                if kill -0 "$pid" 2>/dev/null; then
                    echo "Force killing Xvfb on DISPLAY=:${DISPLAY_NUM}"
                    kill -9 "$pid" 2>/dev/null || true
                    # Даем время системе для обработки
                    sleep 0.1
                    # Пробуем reap если стал zombie
                    wait "$pid" 2>/dev/null || true
                fi
            fi
        fi

        # Удаляем lock файлы и сокеты
        rm -f /tmp/.X${DISPLAY_NUM}-lock 2>/dev/null || true
        rm -f /tmp/.X11-unix/X${DISPLAY_NUM} 2>/dev/null || true
    done

    echo "✓ Xvfb cleanup complete"
}

# Установка trap для гарантированной очистки при EXIT/SIGTERM/SIGINT
trap cleanup_xvfb EXIT SIGTERM SIGINT

# Очистка старых Xvfb процессов и lock файлов
echo "Cleaning up old Xvfb processes and locks..."
pkill Xvfb 2>/dev/null || true
rm -f /tmp/.X*-lock 2>/dev/null || true
rm -rf /tmp/.X11-unix/* 2>/dev/null || true

# Получаем количество воркеров из переменной окружения
NUM_WORKERS=${NUM_WORKERS:-15}
echo "NUM_WORKERS=$NUM_WORKERS"

# Массив для хранения PID процессов Xvfb
declare -a XVFB_PIDS

# Запускаем N экземпляров Xvfb (по одному на воркер)
echo "Starting $NUM_WORKERS Xvfb instances..."
for i in $(seq 1 $NUM_WORKERS); do
    DISPLAY_NUM=$((99 + i - 1))  # :99, :100, :101, ...
    echo "Starting Xvfb on DISPLAY=:${DISPLAY_NUM}"

    Xvfb :${DISPLAY_NUM} -screen 0 1920x1080x24 -ac -nolisten tcp &
    XVFB_PIDS[$i]=$!

    # Небольшая пауза между запусками
    sleep 0.5
done

# Активная проверка готовности всех Xvfb вместо sleep 3
echo "Waiting for Xvfb instances to be ready..."
for i in $(seq 1 $NUM_WORKERS); do
    DISPLAY_NUM=$((99 + i - 1))

    if ! wait_for_xvfb $DISPLAY_NUM; then
        echo "❌ Failed to start Xvfb on DISPLAY=:${DISPLAY_NUM}"
        exit 1
    fi
done

echo "✓ All Xvfb instances are ready"

# Проверяем зависимости перед запуском воркеров
echo "Checking dependencies..."
python check_dependencies.py

# Если проверка не прошла - останавливаем запуск
if [ $? -ne 0 ]; then
    echo "❌ Dependency check failed! Workers cannot start."
    # Trap автоматически вызовет cleanup_xvfb при exit
    exit 1
fi

echo "✓ All dependencies checked successfully"
echo "Starting supervisor with $NUM_WORKERS workers..."

# Запускаем supervisor для управления воркерами
# При завершении trap автоматически вызовет cleanup_xvfb
python supervisor.py

echo "Supervisor завершен"
