-- Avito Parser System - Database Schema
-- PostgreSQL 16+

-- Таблица задач для парсинга
CREATE TABLE IF NOT EXISTS tasks (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    article TEXT NOT NULL UNIQUE,  -- Артикул для парсинга
    status TEXT NOT NULL CHECK (status IN ('новая', 'в работе', 'завершена', 'ошибка')) DEFAULT 'новая',
    worker_id TEXT,  -- Идентификатор воркера
    taken_at TIMESTAMPTZ,  -- Время взятия задачи
    last_heartbeat TIMESTAMPTZ,  -- Время последнего heartbeat
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,  -- Время завершения
    retry_count INTEGER NOT NULL DEFAULT 0,  -- Количество попыток
    error_message TEXT  -- Описание последней ошибки
);

-- Индексы для tasks
CREATE INDEX IF NOT EXISTS idx_tasks_status_created ON tasks(status, created_at);  -- Для взятия новых задач
CREATE INDEX IF NOT EXISTS idx_tasks_heartbeat ON tasks(last_heartbeat) WHERE status = 'в работе';  -- Partial index для зависших задач
CREATE INDEX IF NOT EXISTS idx_tasks_worker ON tasks(worker_id, status);  -- Для мониторинга воркеров

-- Таблица прокси
CREATE TABLE IF NOT EXISTS proxies (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    proxy_address TEXT NOT NULL UNIQUE,  -- host:port:username:password
    status TEXT NOT NULL CHECK (status IN ('свободен', 'используется', 'заблокирован')) DEFAULT 'свободен',
    worker_id TEXT,  -- Идентификатор использующего воркера
    taken_at TIMESTAMPTZ,  -- Время взятия воркером
    blocked_at TIMESTAMPTZ,  -- Время блокировки
    blocked_reason TEXT,  -- Причина блокировки (403/407)
    success_count INTEGER NOT NULL DEFAULT 0,  -- Количество успешных использований
    error_count INTEGER NOT NULL DEFAULT 0  -- Количество ошибок
);

-- Индексы для proxies
CREATE INDEX IF NOT EXISTS idx_proxies_status ON proxies(status);  -- Для выбора свободных прокси
CREATE INDEX IF NOT EXISTS idx_proxies_worker ON proxies(worker_id);  -- Для освобождения при падении воркера

-- Таблица спаршенных карточек
CREATE TABLE IF NOT EXISTS parsed_cards (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    avito_item_id BIGINT NOT NULL UNIQUE,  -- ID объявления с Авито
    article TEXT NOT NULL,  -- Артикул (связь перезаписывается)
    title TEXT,  -- Название объявления
    description TEXT,  -- Описание
    price NUMERIC(12,2),  -- Цена
    seller_name TEXT,  -- Ник продавца
    published_at TIMESTAMPTZ,  -- Дата публикации на Авито
    location TEXT,  -- Местоположение
    views_count INTEGER,  -- Количество просмотров
    characteristics JSONB,  -- Характеристики товара (отдельное поле для удобства)
    parsed_data JSONB,  -- Все спаршенные поля в структурированном виде
    parsed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()  -- Время парсинга
);

-- Индексы для parsed_cards
CREATE INDEX IF NOT EXISTS idx_parsed_cards_article ON parsed_cards(article);  -- Для поиска карточек по артикулу
CREATE INDEX IF NOT EXISTS idx_parsed_cards_parsed_at ON parsed_cards(parsed_at);  -- Для временного анализа
CREATE INDEX IF NOT EXISTS idx_parsed_cards_data_gin ON parsed_cards USING GIN (parsed_data jsonb_path_ops);  -- Для поиска внутри JSONB

-- Таблица результатов валидации
CREATE TABLE IF NOT EXISTS validation_results (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    avito_item_id BIGINT NOT NULL REFERENCES parsed_cards(avito_item_id) ON DELETE CASCADE,
    validation_type TEXT NOT NULL CHECK (validation_type IN ('механическая', 'ИИ')),
    passed BOOLEAN NOT NULL,  -- Прошло валидацию или нет
    rejection_reason TEXT,  -- Причина отклонения (текстовая)
    description TEXT,  -- Описание объявления (для валидации)
    characteristics JSONB,  -- Характеристики товара (для валидации)
    validation_details JSONB,  -- Детали валидации (стоп-слова, ценовое отклонение, анализ ИИ)
    validated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Индексы для validation_results
CREATE INDEX IF NOT EXISTS idx_validation_item ON validation_results(avito_item_id);  -- Для связи с карточками
CREATE INDEX IF NOT EXISTS idx_validation_type_result ON validation_results(validation_type, passed);  -- Для статистики
CREATE INDEX IF NOT EXISTS idx_validation_validated_at ON validation_results(validated_at);  -- Для временного анализа

-- Таблица истории обработки артикулов
CREATE TABLE IF NOT EXISTS processed_articles (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    article TEXT NOT NULL UNIQUE,  -- Артикул
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),  -- Время завершения обработки
    processing_status TEXT NOT NULL CHECK (processing_status IN ('success', 'error', 'no_results')),
    items_found INTEGER,  -- Количество найденных объявлений
    items_passed INTEGER,  -- Количество прошедших валидацию
    started_at TIMESTAMPTZ,  -- Время начала обработки
    worker_id TEXT  -- ID воркера, обработавшего задачу
);

-- Индексы для processed_articles
CREATE INDEX IF NOT EXISTS idx_processed_articles_processed_at ON processed_articles(processed_at);  -- Для временного анализа

-- Комментарии к таблицам
COMMENT ON TABLE tasks IS 'Очередь задач для парсинга артикулов';
COMMENT ON TABLE proxies IS 'Пул прокси-серверов для воркеров';
COMMENT ON TABLE parsed_cards IS 'Спаршенные карточки объявлений с Авито';
COMMENT ON TABLE validation_results IS 'Результаты механической и ИИ-валидации';
COMMENT ON TABLE processed_articles IS 'История полностью обработанных артикулов';

-- Комментарии к важным полям
COMMENT ON COLUMN tasks.last_heartbeat IS 'Обновляется каждые 2 минуты для отслеживания зависших воркеров';
COMMENT ON COLUMN proxies.blocked_reason IS 'HTTP коды: 403 (блокировка Авито), 407 (ошибка авторизации прокси)';
COMMENT ON COLUMN parsed_cards.article IS 'Связь перезаписывается при повторной находке в другом артикуле';
COMMENT ON COLUMN validation_results.description IS 'Копия описания из карточки для анализа валидации';
COMMENT ON COLUMN validation_results.characteristics IS 'Копия характеристик из карточки для анализа валидации';
COMMENT ON COLUMN validation_results.validation_details IS 'JSONB: стоп-слова, отклонение цены, анализ ИИ';
