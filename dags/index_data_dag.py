from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import datetime

from utils.index_utils import process_index_data

with DAG(
    dag_id="index_data_pipeline",
    schedule="45 12 * * Mon",
    start_date=datetime.datetime(2025, 4, 10),
    catchup=False,
    tags=["DataLake", "Index"],
) as dag:
    get_and_upload_task = PythonOperator(
        task_id="get_and_upload_index_data",
        python_callable=process_index_data,
        op_kwargs={
            "hdfs_conn_id": "webhdfs_default",
            "interval": "1m",
        },
    )

    msck_repair_task = SQLExecuteQueryOperator(
        task_id="msck_repair_index_data",
        sql="MSCK REPAIR TABLE raw_index_data",
        conn_id="hiveserver2_default",
    )

    get_and_upload_task >> msck_repair_task
