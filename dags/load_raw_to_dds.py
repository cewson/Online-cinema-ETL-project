import logging

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

from utils.datasets import RAW_UPDATED, DDS_UPDATED
from utils.dds_loader import DdsLoader
from utils.tg_alert import alert_telegram

logger = logging.getLogger("airflow.task")


def transform_to_dds():
    pg_hook = PostgresHook(postgres_conn_id="warehouse_default")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    loader = DdsLoader(cursor)

    try:
        rows = loader.fetch_raw_events(limit=200)
        total = len(rows)
        processed_count = 0
        failed_count = 0

        for event_id, data in rows:
            if loader.process_event(event_id, data):
                processed_count += 1
            else:
                failed_count += 1

        conn.commit()

        if processed_count > 0 or failed_count > 0:
            alert_telegram(
                f"✓ load_raw_to_dds: обработано {processed_count}, "
                f"ошибок {failed_count} из {total}"
            )
        return processed_count

    except Exception as exc:
        conn.rollback()
        alert_telegram(f"✗ load_raw_to_dds: {exc}")
        raise

    finally:
        cursor.close()
        conn.close()


def dds_has_new_data(**context):
    processed_count = context["ti"].xcom_pull(task_ids="process_data")
    if processed_count and processed_count > 0:
        logger.info("DDS обновлён (%s событий), публикуем Dataset", processed_count)
        return True
    logger.info("DDS не изменился, пропускаем триггер витрин")
    return False


def mark_dds_updated():
    logger.info("Dataset DDS_UPDATED опубликован — запустится load_dds_to_clickhouse")


default_args = {
    "owner": "vlada",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="load_raw_to_dds",
    default_args=default_args,
    description="Загрузка данных из RAW в DDS PostgreSQL",
    schedule=[RAW_UPDATED],
    start_date=datetime(2026, 6, 8),
    catchup=False,
    tags=["dds", "raw", "postgresql"],
) as dag:

    task_process = PythonOperator(
        task_id="process_data",
        python_callable=transform_to_dds,
    )

    check_dds_updated = ShortCircuitOperator(
        task_id="check_dds_updated",
        python_callable=dds_has_new_data,
    )

    publish_dds_dataset = PythonOperator(
        task_id="publish_dds_dataset",
        python_callable=mark_dds_updated,
        outlets=[DDS_UPDATED],
    )

    task_process >> check_dds_updated >> publish_dds_dataset
