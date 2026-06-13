"""
DAG для загрузки данных из MinIO в PostgreSQL.

Этот процесс выполняет:
1. Поиск новых файлов в бакете MinIO.
2. Валидацию JSON-структур с использованием Pydantic-моделей.
3. Запись валидных событий в таблицу raw.events, а невалидных — в raw.events_invalid.
4. Удаление успешно обработанных или отклоненных файлов из MinIO.
5. Уведомление о результатах в Telegram.
"""

import os
import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pydantic import ValidationError

from utils.validation.raw_models import RawEvent
from utils.tg_alert import alert_telegram

logger = logging.getLogger(__name__)
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "raw-data")


def load_file_from_s3(s3: S3Hook, key: str) -> dict:
    """
    Скачивает и декодирует JSON-файл из S3/MinIO.

    :param s3: Инициализированный объект S3Hook.
    :param key: Путь к файлу в бакете.
    :return: Словарь с данными файла.
    """
    file_obj = s3.get_key(key=key, bucket_name=MINIO_BUCKET)
    return json.loads(file_obj.get()["Body"].read().decode("utf-8"))


def insert_valid_event(cursor, event: RawEvent, raw_json: dict) -> None:
    """
    Вставляет валидное событие в таблицу raw.events.

    :param cursor: Курсор базы данных Postgres.
    :param event: Валидированная Pydantic-модель.
    :param raw_json: Исходный JSON-словарь.
    """
    cursor.execute(
        """
        INSERT INTO raw.events (event_id, event_date, data)
        VALUES (%s, %s, %s)
        """,
        (str(event.event_id), event.event_date, json.dumps(raw_json)),
    )


def insert_invalid_event(cursor, raw_json: dict, error: str) -> None:
    """
    Вставляет данные с ошибкой валидации в таблицу raw.events_invalid.

    :param cursor: Курсор базы данных Postgres.
    :param raw_json: Исходный JSON-словарь.
    :param error: Текст ошибки валидации.
    """
    cursor.execute(
        """
        INSERT INTO raw.events_invalid (raw_data, error_message)
        VALUES (%s, %s)
        """,
        (json.dumps(raw_json), f"[RAW] {error}"),
    )


def extract_and_validate_task() -> int:
    """
    Основная задача DAG: читает файлы из MinIO и загружает их в PostgreSQL.

    :return: Количество успешно загруженных валидных событий.
    """
    s3 = S3Hook(aws_conn_id="minio_default")
    pg = PostgresHook(postgres_conn_id="warehouse_default")
    client = s3.get_conn()

    resp = client.list_objects_v2(Bucket=MINIO_BUCKET, Delimiter="/")
    prefixes = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]

    if not prefixes:
        logger.info("Нет файлов для обработки")
        return 0

    stats = {"success": 0, "invalid": 0}

    for prefix in prefixes:
        files = s3.list_keys(bucket_name=MINIO_BUCKET, prefix=prefix)
        if not files:
            continue

        for file_key in files:

            conn = pg.get_conn()
            cursor = conn.cursor()
            try:
                raw_json = load_file_from_s3(s3, file_key)
                
                try:
                    event = RawEvent.model_validate(raw_json)
                    insert_valid_event(cursor, event, raw_json)
                    stats["success"] += 1
                except ValidationError as exc:
                    insert_invalid_event(cursor, raw_json, str(exc))
                    stats["invalid"] += 1
                
                conn.commit()
                s3.delete_objects(bucket=MINIO_BUCKET, keys=file_key)
            except Exception as e:
                conn.rollback()
                logger.error(f"Файл {file_key} не обработан: {e}")
            finally:
                cursor.close()
                conn.close()

    if stats["success"] > 0 or stats["invalid"] > 0:
        alert_telegram(
            f"✓ load_minio_to_raw: {stats['success']} успешно, "
            f"{stats['invalid']} с ошибкой валидации", True
        )

    return stats["success"]


default_args = {
    "owner": "vlada",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="load_minio_to_raw",
    default_args=default_args,
    description="Загрузка данных из MinIO в raw PostgreSQL",
    schedule="*/1 * * * *",
    start_date=datetime(2026, 6, 8),
    catchup=False,
    tags=["minio", "raw", "postgresql"],
) as dag:

    task_process = PythonOperator(
        task_id="process_minio_files",
        python_callable=extract_and_validate_task,
    )
    
    task_process 
