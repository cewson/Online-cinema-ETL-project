CREATE SCHEMA IF NOT EXISTS raw;

-- 1. Основное хранилище валидных событий (Data Lake)
CREATE TABLE IF NOT EXISTS raw.events (
    id SERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    event_date TEXT NOT NULL,
    data JSONB NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_processed BOOLEAN DEFAULT FALSE
);

-- 4. Индекс для сырых данных (чтобы SELECT ... WHERE is_processed = FALSE работал быстро)
CREATE INDEX IF NOT EXISTS idx_raw_events_processed ON raw.events(is_processed) WHERE is_processed = FALSE;

-- 2. Хранилище «мусорных» данных (Data Quality Audit)
CREATE TABLE IF NOT EXISTS raw.events_invalid (
    id SERIAL PRIMARY KEY,
    raw_data JSONB NOT NULL,
    error_message TEXT,
    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

