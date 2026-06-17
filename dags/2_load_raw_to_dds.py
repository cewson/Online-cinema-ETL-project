"""
DAG для трансформации данных из слоя RAW в слой DDS.

Этот процесс выполняет:
1. Выбор необработанных событий из таблицы raw.events.
2. Бизнес-валидацию и трансформацию в DDS через DdsLoader + validate_for_dds (dds_models).
3. Запись измерений (dim_*) и фактов (fact_*) в схему dds PostgreSQL.
4. Отклонение DDS-невалидных событий в raw.events_invalid с меткой [DDS].
5. Публикацию Dataset DDS_UPDATED для запуска load_dds_to_clickhouse.
6. Уведомление о результатах в Telegram.
"""
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

from utils.datasets import DDS_UPDATED
from utils.dds_loader import DdsLoader
from utils.tg_alert import alert_telegram

logger = logging.getLogger(__name__)

def transform_to_dds() -> int:
    """
    Трансформирует сырые события в структурированные сущности DDS.

    :return: Количество успешно обработанных событий.
    :raises Exception: Если произошла ошибка БД или обработки.
    """
    pg_hook = PostgresHook(postgres_conn_id="warehouse_default")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    loader = DdsLoader(cursor)

    try:
        rows = loader.fetch_raw_events(limit=1000)
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
                f"ошибок валидации: {failed_count} из {total}", True
            )
        return processed_count

    except Exception as exc:
        conn.rollback()
        alert_telegram(f"✗ load_raw_to_dds: {exc}")
        raise

    finally:
        cursor.close()
        conn.close()


def dds_has_new_data(**context) -> bool:
    """
    Функция проверяет, были ли обновлены данные в DDS.

    :param context: Контекст Airflow (используется для получения XCom).
    :return: True, если данные были обновлены, иначе False.
    """
    processed_count = context["ti"].xcom_pull(task_ids="process_data")
    if processed_count and processed_count > 0:
        logger.info("DDS обновлён (%s событий), публикуется Dataset", processed_count)
        return True
    logger.info("DDS не изменился, пропускаем load_dds_to_clickhouse")
    return False


default_args = {
    "owner": "vlada",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="load_raw_to_dds",
    default_args=default_args,
    description="Загрузка данных из RAW в DDS PostgreSQL",
    schedule="*/1 * * * *",
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

    publish_dds_dataset = EmptyOperator(
        task_id="publish_dds_dataset",
        outlets=[DDS_UPDATED]
    )
    
    task_process >> check_dds_updated >> publish_dds_dataset 

