from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import datetime

from utils.stock_utils import process_stock_data

with DAG(
    dag_id="stock_data_pipeline",
    schedule="45 12 * * Mon",
    start_date=datetime.datetime(2025, 4, 10),
    catchup=False,
    tags=["DataLake", "Stock"],
) as dag:
    get_and_upload_task = PythonOperator(
        task_id="get_and_upload_stock_data",
        python_callable=process_stock_data,
        op_kwargs={
            "hdfs_conn_id": "webhdfs_default",
            "market": "prime",
            "interval": "1m",
        },
    )

    msck_repair_task = SQLExecuteQueryOperator(
        task_id="msck_repair_stock_data",
        sql="MSCK REPAIR TABLE raw_stock_data",
        conn_id="hiveserver2_default",
    )

    get_and_upload_task >> msck_repair_task
