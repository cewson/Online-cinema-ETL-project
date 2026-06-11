import logging
import os
import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from clickhouse_driver import Client

from utils.datasets import DDS_UPDATED
from utils.tg_alert import alert_telegram

logger = logging.getLogger("airflow.task")

MART_QUERIES = {
    "mart_events": """
        SELECT
            fe.event_id,
            fe.session_id,
            COALESCE(fe.event_type, ''),
            fe.event_date,
            COALESCE(fe.event_details::text, ''),
            fs.user_id,
            COALESCE(du.email, ''),
            COALESCE(du.name, ''),
            COALESCE(dc.content_id, 0),
            COALESCE(dc.title, ''),
            COALESCE(dc.director, ''),
            COALESCE(dc.release_year, 0),
            COALESCE(gen.genres, ''),
            COALESCE(dd.type, ''),
            COALESCE(dd.os, ''),
            COALESCE(dl.country, ''),
            COALESCE(dl.city, ''),
            COALESCE(dm.campaign_name, ''),
            COALESCE(dm.source, '')
        FROM dds.fact_events fe
        JOIN dds.fact_sessions fs ON fe.session_id = fs.session_id
        JOIN dds.dim_users du ON fs.user_id = du.user_id
        LEFT JOIN dds.dim_content dc ON fe.content_id = dc.content_id
        LEFT JOIN dds.dim_devices dd ON fs.device_id = dd.device_id
        LEFT JOIN dds.dim_locations dl ON fs.location_id = dl.location_id
        LEFT JOIN dds.dim_marketing dm ON du.first_touch_campaign_id = dm.campaign_id
        LEFT JOIN (
            SELECT lcg.content_id,
                   string_agg(g.genre_name, ', ' ORDER BY g.genre_name) AS genres
            FROM dds.link_content_genres lcg
            JOIN dds.dim_genres g ON g.genre_id = lcg.genre_id
            GROUP BY lcg.content_id
        ) gen ON gen.content_id = fe.content_id
    """,
    "mart_sessions": """
        SELECT
            fs.session_id,
            fs.user_id,
            COALESCE(du.email, ''),
            COALESCE(du.name, ''),
            COALESCE(dd.type, ''),
            COALESCE(dd.os, ''),
            COALESCE(dd.model, ''),
            COALESCE(dl.country, ''),
            COALESCE(dl.city, ''),
            COALESCE(fs.ip_address, ''),
            COUNT(fe.event_id)::int
        FROM dds.fact_sessions fs
        JOIN dds.dim_users du ON fs.user_id = du.user_id
        LEFT JOIN dds.dim_devices dd ON fs.device_id = dd.device_id
        LEFT JOIN dds.dim_locations dl ON fs.location_id = dl.location_id
        LEFT JOIN dds.fact_events fe ON fe.session_id = fs.session_id
        GROUP BY
            fs.session_id, fs.user_id, du.email, du.name,
            dd.type, dd.os, dd.model, dl.country, dl.city, fs.ip_address
    """,
    "mart_content_performance": """
        SELECT
            dc.content_id,
            COALESCE(dc.title, ''),
            COALESCE(dc.director, ''),
            COALESCE(dc.release_year, 0),
            COALESCE(string_agg(DISTINCT g.genre_name, ', '), ''),
            COUNT(fe.event_id)::int,
            COUNT(DISTINCT fs.user_id)::int,
            COUNT(DISTINCT CASE WHEN fe.event_type = 'content_view_started' THEN fe.event_id END)::int
        FROM dds.dim_content dc
        LEFT JOIN dds.fact_events fe ON fe.content_id = dc.content_id
        LEFT JOIN dds.fact_sessions fs ON fe.session_id = fs.session_id
        LEFT JOIN dds.link_content_genres lcg ON lcg.content_id = dc.content_id
        LEFT JOIN dds.dim_genres g ON g.genre_id = lcg.genre_id
        GROUP BY dc.content_id, dc.title, dc.director, dc.release_year
    """,
    "mart_subscription_changes": """
        SELECT
            fsc.change_id,
            fsc.user_id,
            COALESCE(du.email, ''),
            COALESCE(du.name, ''),
            COALESCE(fsc.old_status, ''),
            COALESCE(fsc.new_status, ''),
            fsc.changed_at
        FROM dds.fact_subscription_changes fsc
        JOIN dds.dim_users du ON fsc.user_id = du.user_id
    """,
}


def get_clickhouse_client() -> Client:
    return Client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_PORT", "9000")),
        user=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse"),
        database=os.getenv("CLICKHOUSE_DB", "default"),
    )


def null_default(col_type):
    if col_type in (int,):
        return 0
    if col_type in (float, Decimal):
        return 0.0
    if col_type in (datetime, date):
        return datetime(1970, 1, 1)
    if col_type is uuid.UUID:
        return uuid.UUID(int=0)
    return ""


def sanitize_rows(rows):
    if not rows:
        return rows

    # 1. Сначала найдем типы для всех колонок, пропуская None
    num_cols = len(rows[0])
    col_types = [None] * num_cols
    
    for row in rows:
        for i in range(num_cols):
            if row[i] is not None and col_types[i] is None:
                col_types[i] = type(row[i])
    
    # Если для колонки тип так и не нашелся (все None), задаем дефолт
    for i in range(num_cols):
        if col_types[i] is None:
            col_types[i] = str  # Или другой тип по умолчанию

    # 2. Обработка значений
    sanitized = []
    for row in rows:
        new_row = []
        for i in range(num_cols):
            val = row[i]
            if val is None:
                new_row.append(null_default(col_types[i]))
            else:
                # ВАЖНО: убедимся, что datetime не содержит часового пояса, 
                # если ClickHouse настроен на локальное время, или приведем к нужному формату
                if isinstance(val, datetime):
                    # Если нужно убрать tzinfo (наивный datetime), используйте:
                    val = val.replace(tzinfo=None)
                new_row.append(val)
        sanitized.append(tuple(new_row))
    return sanitized


def reload_mart(table_name: str):
    pg_hook = PostgresHook(postgres_conn_id="warehouse_default")
    rows = sanitize_rows(pg_hook.get_records(MART_QUERIES[table_name]))
    client = get_clickhouse_client()
    client.execute(f"TRUNCATE TABLE dm.{table_name}")
    if rows:
        client.execute(f"INSERT INTO dm.{table_name} VALUES", rows)
    logger.info("Пересчитано dm.%s: %s rows", table_name, len(rows))


def rebuild_all_marts():
    try:
        for table_name in MART_QUERIES:
            reload_mart(table_name)
        alert_telegram(f"✓ load_dds_to_clickhouse: пересчитано {len(MART_QUERIES)} витрин", True)
    except Exception as exc:
        alert_telegram(f"✗ load_dds_to_clickhouse: {exc}")
        raise


default_args = {
    "owner": "vlada",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="load_dds_to_clickhouse",
    default_args=default_args,
    description="Пересчёт витрин ClickHouse из DDS PostgreSQL",
    schedule=[DDS_UPDATED],
    start_date=datetime(2026, 6, 8),
    catchup=False,
    tags=["dds", "clickhouse", "marts"],
) as dag:

    rebuild_marts = PythonOperator(
        task_id="rebuild_marts",
        python_callable=rebuild_all_marts,
    )
