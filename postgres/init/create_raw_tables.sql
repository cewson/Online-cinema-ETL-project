-- Инициализация RAW-слоя PostgreSQL 
-- Этот скрипт создаёт:
-- 1. Схему raw — слой сырых данных из MinIO.
-- 2. Таблицу raw.events — валидные JSON-события (загрузка: load_minio_to_raw).
-- 3. Таблицу raw.events_invalid — отклонённые при валидации Pydantic.
-- 4. Индекс для быстрого выборa необработанных событий (load_raw_to_dds).


CREATE SCHEMA IF NOT EXISTS raw;

-- Валидные события из MinIO (Data Lake)
CREATE TABLE IF NOT EXISTS raw.events (
    id SERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    event_date TEXT NOT NULL,
    data JSONB NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_processed BOOLEAN DEFAULT FALSE
);

-- Индекс для SELECT ... WHERE is_processed = FALSE
CREATE INDEX IF NOT EXISTS idx_raw_events_processed ON raw.events(is_processed) WHERE is_processed = FALSE;

-- Невалидные события (Data Quality Audit)
CREATE TABLE IF NOT EXISTS raw.events_invalid (
    id SERIAL PRIMARY KEY,
    raw_data JSONB NOT NULL,
    error_message TEXT,
    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
