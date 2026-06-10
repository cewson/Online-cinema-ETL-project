from airflow.datasets import Dataset

RAW_UPDATED = Dataset("warehouse://postgres/raw")
DDS_UPDATED = Dataset("warehouse://postgres/dds")
