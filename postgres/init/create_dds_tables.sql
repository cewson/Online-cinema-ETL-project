CREATE SCHEMA IF NOT EXISTS dds;

-- 1. Справочники (Dimensions) - БЕЗ внешних ключей сначала
CREATE TABLE dds.dim_genres (
    genre_id SERIAL PRIMARY KEY,
    genre_name VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE dds.dim_devices (
    device_id VARCHAR(50) PRIMARY KEY,
    type VARCHAR(50),
    os VARCHAR(50),
    model VARCHAR(100)
);

CREATE TABLE dds.dim_locations (
    location_id SERIAL PRIMARY KEY,
    country VARCHAR(100),
    city VARCHAR(100),
    timezone VARCHAR(50),
    UNIQUE(country, city)
);

CREATE TABLE dds.dim_marketing (
    campaign_id VARCHAR(50) PRIMARY KEY,
    campaign_name VARCHAR(100),
    source VARCHAR(50)
);

CREATE TABLE dds.dim_content (
    content_id INT PRIMARY KEY,
    title VARCHAR(255),
    director VARCHAR(255),
    release_year INT
);

-- Теперь создаем таблицы, которые ссылаются на предыдущие
CREATE TABLE dds.dim_users (
    user_id UUID PRIMARY KEY,
    email VARCHAR(255),
    name VARCHAR(255),
    birth_date DATE,
    first_touch_campaign_id VARCHAR(50) REFERENCES dds.dim_marketing(campaign_id)
);

-- 2. Таблицы фактов и связей
CREATE TABLE dds.fact_sessions (
    session_id UUID PRIMARY KEY,
    user_id UUID REFERENCES dds.dim_users(user_id),
    device_id VARCHAR(50) REFERENCES dds.dim_devices(device_id),
    location_id INT REFERENCES dds.dim_locations(location_id),
    ip_address VARCHAR(45)
);

CREATE TABLE dds.fact_events (
    event_id UUID PRIMARY KEY,
    session_id UUID REFERENCES dds.fact_sessions(session_id),
    content_id INT REFERENCES dds.dim_content(content_id),
    event_type VARCHAR(20),
    event_date VARCHAR(20),
    event_details JSONB
);

CREATE TABLE dds.fact_subscription_changes (
    change_id SERIAL PRIMARY KEY,
    user_id UUID REFERENCES dds.dim_users(user_id),
    old_status VARCHAR(20),
    new_status VARCHAR(20),
    changed_at TIMESTAMP
);

-- 3. Связующие таблицы (Bridge Tables)
CREATE TABLE dds.link_content_genres (
    content_id INT REFERENCES dds.dim_content(content_id),
    genre_id INT REFERENCES dds.dim_genres(genre_id),
    PRIMARY KEY (content_id, genre_id)
);

-- Индексы для ускорения ON CONFLICT и поиска
-- 1. Индексы для обеспечения уникальности и быстрого поиска в справочниках
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_marketing_id ON dds.dim_marketing(campaign_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_devices_id ON dds.dim_devices(device_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_content_id ON dds.dim_content(content_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_users_id ON dds.dim_users(user_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_genres_name ON dds.dim_genres(genre_name);

-- Индекс для уникальности локаций
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_locations_unique ON dds.dim_locations(country, city);

-- 2. Индексы для фактовых таблиц (для ускорения JOIN и поиска)
CREATE INDEX IF NOT EXISTS idx_fact_sessions_user_id ON dds.fact_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_fact_events_session_id ON dds.fact_events(session_id);

-- 3. КРИТИЧЕСКИ ВАЖНЫЙ индекс для логики подписок
-- Он ускоряет поиск последнего статуса пользователя (ORDER BY changed_at DESC)
CREATE INDEX IF NOT EXISTS idx_fact_sub_user_time ON dds.fact_subscription_changes(user_id, changed_at DESC);

