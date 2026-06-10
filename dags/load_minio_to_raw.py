import os
import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pydantic import ValidationError

from utils.datasets import RAW_UPDATED
from utils.validation.raw_models import RawEvent
from utils.tg_alert import alert_telegram

logger = logging.getLogger("airflow.task")

MINIO_BUCKET = os.getenv("MINIO_BUCKET", "raw-data")


def load_file_from_s3(s3: S3Hook, key: str) -> dict:
    file_obj = s3.get_key(key=key, bucket_name=MINIO_BUCKET)
    return json.loads(file_obj.get()["Body"].read().decode("utf-8"))


def insert_valid_event(cursor, event: RawEvent, raw_json: dict):
    cursor.execute(
        """
        INSERT INTO raw.events (event_id, event_date, data)
        VALUES (%s, %s, %s)
        """,
        (str(event.event_id), event.event_date, json.dumps(raw_json)),
    )


def insert_invalid_event(cursor, raw_json: dict, error: str):
    cursor.execute(
        """
        INSERT INTO raw.events_invalid (raw_data, error_message)
        VALUES (%s, %s)
        """,
        (json.dumps(raw_json), error),
    )


def extract_and_validate_task(ds: str, **_):
    prefix = f"{ds}/"
    s3 = S3Hook(aws_conn_id="minio_default")
    pg = PostgresHook(postgres_conn_id="warehouse_default")
    files = s3.list_keys(bucket_name=MINIO_BUCKET, prefix=prefix)

    if not files:
        logger.info("Нет файлов для обработки в %s", prefix)
        return 0

    stats = {"success": 0, "invalid": 0}
    files_to_delete = []

    connection = pg.get_conn()
    cursor = connection.cursor()

    try:
        logger.info("Обработка файлов из %s", prefix)

        for file_key in files:
            raw_json = load_file_from_s3(s3, file_key)

            try:
                event = RawEvent.model_validate(raw_json)
                insert_valid_event(cursor, event, raw_json)
                stats["success"] += 1

            except ValidationError as exc:
                logger.warning("Ошибка валидации %s: %s", file_key, exc)
                insert_invalid_event(cursor, raw_json, str(exc))
                stats["invalid"] += 1

            files_to_delete.append(file_key)

        connection.commit()

        for file_key in files_to_delete:
            s3.delete_objects(bucket=MINIO_BUCKET, keys=file_key)

    except Exception as exc:
        connection.rollback()
        logger.error("Критическая ошибка: %s", exc)
        alert_telegram(f"✗ load_minio_to_raw: {exc}")
        raise

    finally:
        cursor.close()
        connection.close()

    if stats["success"] > 0 or stats["invalid"] > 0:
        alert_telegram(
            f"✓ load_minio_to_raw: {stats['success']} успешно, "
            f"{stats['invalid']} с ошибкой валидации"
        )

    return stats["success"]


def raw_has_new_data(**context):
    loaded_count = context["ti"].xcom_pull(task_ids="process_minio_files")
    if loaded_count and loaded_count > 0:
        logger.info("RAW обновлён (%s событий), публикуем Dataset", loaded_count)
        return True
    
    alert_telegram(f"Новых данных в Minio не найдено")
    logger.info("RAW не изменился, пропускаем триггер DDS")
    return False


def mark_raw_updated():
    logger.info("Dataset RAW_UPDATED опубликован — запустится load_raw_to_dds")


default_args = {
    "owner": "vlada",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="load_minio_to_raw",
    default_args=default_args,
    description="Загрузка данных из MinIO в raw PostgreSQL",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2026, 6, 8),
    catchup=False,
    tags=["minio", "raw", "postgresql"],
) as dag:

    task_process = PythonOperator(
        task_id="process_minio_files",
        python_callable=extract_and_validate_task,
    )

    check_raw_updated = ShortCircuitOperator(
        task_id="check_raw_updated",
        python_callable=raw_has_new_data,
    )

    publish_raw_dataset = PythonOperator(
        task_id="publish_raw_dataset",
        python_callable=mark_raw_updated,
        outlets=[RAW_UPDATED],
    )

    task_process >> check_raw_updated >> publish_raw_dataset
