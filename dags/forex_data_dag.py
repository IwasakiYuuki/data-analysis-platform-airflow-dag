from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime

from utils.forex_utils import process_forex_data

with DAG(
    dag_id="forex_data_pipeline",
    schedule="45 12 * * Mon",
    start_date=datetime.datetime(2025, 4, 10),
    catchup=False,
    tags=["forex_data"],
) as dag:
    get_and_upload_task = PythonOperator(
        task_id="get_and_upload_forex_data",
        python_callable=process_forex_data,
        op_kwargs={
            "hdfs_conn_id": "webhdfs_default",
            "interval": "1m",
        },
    )
