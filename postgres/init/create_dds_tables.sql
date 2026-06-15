-- Инициализация DDS-слоя PostgreSQL 
--
-- Этот скрипт создаёт звёздную схему dds:
-- 1. Измерения (dim_*): жанры, устройства, локации, маркетинг, контент, пользователи.
-- 2. Факты (fact_*): сессии, события, смены подписки.
-- 3. Связь M:N контент ↔ жанры (link_content_genres).
-- 4. Индексы для JOIN-ов при построении витрин ClickHouse.


CREATE SCHEMA IF NOT EXISTS dds;

-- Справочник жанров
CREATE TABLE dds.dim_genres (
    genre_id SERIAL PRIMARY KEY,
    genre_name VARCHAR(50) UNIQUE NOT NULL
);

-- Справочник устройств
CREATE TABLE dds.dim_devices (
    device_id VARCHAR(50) PRIMARY KEY,
    type VARCHAR(50),
    os VARCHAR(50),
    model VARCHAR(100)
);

-- Справочник локаций
CREATE TABLE dds.dim_locations (
    location_id SERIAL PRIMARY KEY,
    country VARCHAR(100),
    city VARCHAR(100),
    timezone VARCHAR(50),
    UNIQUE(country, city)
);

-- Справочник маркетинговых кампаний
CREATE TABLE dds.dim_marketing (
    campaign_id VARCHAR(50) PRIMARY KEY,
    campaign_name VARCHAR(100),
    source VARCHAR(50)
);

-- Справочник контента (фильмы)
CREATE TABLE dds.dim_content (
    content_id INT PRIMARY KEY,
    title VARCHAR(255),
    director VARCHAR(255),
    release_year INT
);

-- Справочник пользователей (first-touch campaign)
CREATE TABLE dds.dim_users (
    user_id UUID PRIMARY KEY,
    email VARCHAR(255),
    name VARCHAR(255),
    birth_date DATE,
    first_touch_campaign_id VARCHAR(50) REFERENCES dds.dim_marketing(campaign_id)
);

-- Факт: сессия просмотра
CREATE TABLE dds.fact_sessions (
    session_id UUID PRIMARY KEY,
    user_id UUID REFERENCES dds.dim_users(user_id),
    device_id VARCHAR(50) REFERENCES dds.dim_devices(device_id),
    location_id INT REFERENCES dds.dim_locations(location_id),
    ip_address VARCHAR(45)
);

-- Факт: событие внутри сессии
CREATE TABLE dds.fact_events (
    event_id UUID PRIMARY KEY,
    session_id UUID REFERENCES dds.fact_sessions(session_id),
    content_id INT REFERENCES dds.dim_content(content_id),
    event_type VARCHAR(20),
    event_date VARCHAR(20),
    event_details JSONB
);

-- Факт: смена статуса подписки
CREATE TABLE dds.fact_subscription_changes (
    change_id SERIAL PRIMARY KEY,
    user_id UUID REFERENCES dds.dim_users(user_id),
    old_status VARCHAR(20),
    new_status VARCHAR(20),
    changed_at TIMESTAMP
);

-- Связь контент ↔ жанр
CREATE TABLE dds.link_content_genres (
    content_id INT REFERENCES dds.dim_content(content_id),
    genre_id INT REFERENCES dds.dim_genres(genre_id),
    PRIMARY KEY (content_id, genre_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_marketing_id ON dds.dim_marketing(campaign_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_devices_id ON dds.dim_devices(device_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_content_id ON dds.dim_content(content_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_users_id ON dds.dim_users(user_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_genres_name ON dds.dim_genres(genre_name);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_locations_unique ON dds.dim_locations(country, city);

CREATE INDEX IF NOT EXISTS idx_fact_sessions_user_id ON dds.fact_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_fact_events_session_id ON dds.fact_events(session_id);

CREATE INDEX IF NOT EXISTS idx_fact_sub_user_time ON dds.fact_subscription_changes(user_id, changed_at DESC);
