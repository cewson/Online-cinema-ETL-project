-- =============================================================================
-- Инициализация витрин данных ClickHouse (схема dm)
--
-- Этот скрипт создаёт аналитические витрины:
-- 1. mart_events — детальный лог событий с атрибутами пользователя и контента.
-- 2. mart_sessions — агрегация событий по сессиям.
-- 3. mart_content_performance — метрики просмотров по фильмам.
-- 4. mart_subscription_changes — история смен статуса подписки.
--
-- Заполнение: DAG load_dds_to_clickhouse (полный пересчёт TRUNCATE + INSERT).
-- Визуализация: Metabase (metabase/queries/).
-- =============================================================================

CREATE DATABASE IF NOT EXISTS dm;

-- Детальный лог пользовательских действий
CREATE TABLE IF NOT EXISTS dm.mart_events (
    event_id UUID,
    session_id UUID,
    event_type String,
    event_date String,
    event_details String,
    user_id UUID,
    user_email String,
    user_name String,
    content_id Int32,
    content_title String,
    content_director String,
    release_year Int32,
    genres String,
    device_type String,
    device_os String,
    country String,
    city String,
    campaign_name String,
    marketing_source String
) ENGINE = MergeTree()
ORDER BY (event_date, event_id);

-- Агрегация действий внутри одной сессии
CREATE TABLE IF NOT EXISTS dm.mart_sessions (
    session_id UUID,
    user_id UUID,
    user_email String,
    user_name String,
    device_type String,
    device_os String,
    device_model String,
    country String,
    city String,
    ip_address String,
    events_count UInt32
) ENGINE = MergeTree()
ORDER BY (session_id);

-- Метрики эффективности контента
CREATE TABLE IF NOT EXISTS dm.mart_content_performance (
    content_id Int32,
    title String,
    director String,
    release_year Int32,
    genres String,
    event_count UInt32,
    unique_users UInt32,
    play_count UInt32
) ENGINE = MergeTree()
ORDER BY (content_id);

-- История изменений подписок
CREATE TABLE IF NOT EXISTS dm.mart_subscription_changes (
    change_id Int32,
    user_id UUID,
    user_email String,
    user_name String,
    old_status String,
    new_status String,
    changed_at DateTime
) ENGINE = MergeTree()
ORDER BY (changed_at, change_id);
