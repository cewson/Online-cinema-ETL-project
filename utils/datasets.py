"""
Airflow Dataset для оркестрации ETL-пайплайна.

Этот модуль определяет:
1. DDS_UPDATED — сигнал о том, что слой DDS в PostgreSQL обновлён.
2. Триггер для DAG load_dds_to_clickhouse (schedule=[DDS_UPDATED]).
"""

from airflow.datasets import Dataset

DDS_UPDATED = Dataset("warehouse://postgres/dds")
