from __future__ import annotations

import pendulum
import yfinance as yf
import tempfile
import os

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook


def get_stock_data_and_upload_to_hdfs(ticker: str, hdfs_conn_id: str, hdfs_path: str):
    """
    Retrieves the previous day's 1-minute stock data for the specified ticker using yfinance,
    saves it as a CSV file locally, and then uploads it to HDFS.
    """
    today = pendulum.today(tz="Asia/Tokyo")
    today_str = today.to_date_string()
    yesterday = today.subtract(days=1)
    yesterday_str = yesterday.to_date_string()

    # Download the previous day's 1-minute data using yfinance
    data = yf.download(
        tickers=ticker,
        period="1d",  # Get 1 day's worth of data
        interval="1m",  # 1-minute interval
        start=yesterday_str,
        end=today_str,
    )

    if data is None or data.empty:
        print(f"No data found for {ticker} on {yesterday_str}.")
        return

    # Save the pandas DataFrame to a local temporary CSV file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=".csv") as tmp_file:
        data.to_csv(tmp_file, index=True)
        local_file_path = tmp_file.name
        print(f"Saved CSV data to temporary file '{local_file_path}'.")

    # Initialize the WebHDFSHook
    hook = WebHDFSHook(webhdfs_conn_id=hdfs_conn_id)

    try:
        # Upload the local file to HDFS
        hook.load_file(source=local_file_path, destination=hdfs_path, overwrite=True)
        print(f"Uploaded temporary file '{local_file_path}' to HDFS '{hdfs_path}'.")
    except Exception as e:
        print(f"Error occurred during upload to HDFS: {e}")
    finally:
        # Delete the local temporary file
        os.remove(local_file_path)
        print(f"Deleted temporary file '{local_file_path}'.")


with DAG(
    dag_id="get_stock_data_to_hdfs_with_load_file",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Tokyo"),
    schedule=None,
    catchup=False,
    tags=["yfinance", "hdfs", "load_file"],
) as dag:
    stock_ticker = "AAPL"  # Ticker symbol of the stock to retrieve
    hdfs_connection_id = "webhdfs_default"  # Airflow connection ID for webHDFS
    hdfs_file_path = "/tmp/stock_data_yesterday_load_file.csv"  # HDFS path to save the file

    get_stock_data_task = PythonOperator(
        task_id="get_stock_data_and_upload",
        python_callable=get_stock_data_and_upload_to_hdfs,
        op_kwargs={
            "ticker": stock_ticker,
            "hdfs_conn_id": hdfs_connection_id,
            "hdfs_path": hdfs_file_path,
        },
    )
