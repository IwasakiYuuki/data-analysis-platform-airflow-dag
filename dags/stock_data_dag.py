from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime

from utils.stock_utils import process_stock_data

with DAG(
    dag_id="stock_data_pipeline",
    schedule="45 12 * * Mon",
    start_date=datetime.datetime(2025, 4, 10),
    catchup=False,
    tags=["stock_data"],
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
