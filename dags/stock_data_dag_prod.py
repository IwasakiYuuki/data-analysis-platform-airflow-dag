from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import yfinance as yf
import time
import os
import tempfile
import concurrent.futures
import pandas as pd
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook

MARKET_COLUMN_NAMES = {
    "prime": "プライム（内国株式）",
    "standard": "スタンダード（内国株式）",
    "growth": "グロース（内国株式）",
    "eft": "ETF・ETN",
}
JPX_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"


def get_stock_list(market: str = "prime"):
    """
    JPXのウェブサイトから指定された市場の証券コードのリストを取得する。

    Args:
        market (str): 市場区分 (prime, standard, growth, eft)

    Returns:
        list: 証券コードのリスト
    """
    df_jpx = pd.read_excel(JPX_URL)
    stock_series = df_jpx["コード"][df_jpx["市場・商品区分"] == MARKET_COLUMN_NAMES[market]]
    stock_list = list(stock_series.astype(str) + ".T")
    return stock_list


def get_stock_data(ticker: str, start_date: datetime.date, end_date: datetime.date, interval: str = '1d'):
    """
    yfinanceを使って、指定期間・間隔の株価データを取得する。
    エラーハンドリングとリクエスト間隔を考慮。

    Args:
        ticker (str): 証券コード (例: '7203.T' (トヨタ自動車))
        start_date (datetime.date): 取得開始日
        end_date (datetime.date): 取得終了日
        interval (str): データの間隔 (例: '1d', '1h', '1m')。デフォルトは '1d'。

    Returns:
        pd.DataFrame: 株価データ。取得に失敗した場合はNoneを返す。
    """
    try:
        # 1分足データは期間制限があるため注意 (通常は直近7日間)
        data = yf.download(ticker, start=start_date, end=end_date, interval=interval)
        time.sleep(1)  # リクエスト間隔を1秒に設定 (調整可能)
        # データがない場合、空のDataFrameが返ることがある
        if not isinstance(data, pd.DataFrame) or data.empty:
            print(f"No data found for {ticker} between {start_date} and {end_date} with interval {interval}")
            return None
        return data
    except Exception as e:
        print(f"Error fetching data for {ticker}: {e}")
        return None


def get_stock_data_for_ticker(ticker: str):
    """
    指定された証券コードの先週1週間分の1分足株価データを取得する。

    Args:
        ticker (str): 証券コード

    Returns:
        pd.DataFrame: 1分足株価データ。取得できない場合はNone。
    """
    today = datetime.date.today()
    start_date = today - datetime.timedelta(days=today.weekday() + 7)
    end_date = today - datetime.timedelta(days=today.weekday() + 3)

    # 1分足データを取得
    return get_stock_data(ticker, start_date, end_date, interval='1m')


def fetch_stock_data(tickers: list, max_workers: int = 5) -> list:
    """
    与えられた証券コードのリストに対して、並列で株価データを取得する。

    Args:
        tickers (list): 証券コードのリスト
        max_workers (int): 並列処理の最大ワーカー数

    Returns:
        list: (ticker, data) のタプルのリスト。データ取得に失敗した場合は、tickerとNoneのタプルを返す。
    """
    stock_data_results = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_ticker = {executor.submit(get_stock_data_for_ticker, ticker): ticker for ticker in tickers}
        for future in concurrent.futures.as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                data = future.result()
                stock_data_results.append((ticker, data))
            except Exception as exc:
                print(f"{ticker} generated an exception: {exc}")
                stock_data_results.append((ticker, None))  # エラーが発生した場合、Noneをリストに追加
    return stock_data_results


def get_stock_data_from_list(max_workers: int = 5) -> dict:
    """
    証券コードのリストを取得し、株価データを取得する。

    Args:
        max_workers (int): 並列処理の最大ワーカー数

    Returns:
        dict: 証券コードをキー、株価データを値とする辞書
    """
    tickers = get_stock_list()  # 証券コードのリストを取得
    stock_data_results = fetch_stock_data(tickers, max_workers)
    stock_data = {ticker: data for ticker, data in stock_data_results if data is not None}  # Noneのデータを除外
    return stock_data


def write_stock_data_to_tmp_file(ticker: str, data: pd.DataFrame, tmp_file):
    """
    株価データを一時ファイルに書き込む。

    Args:
        hdfs_hook (WebHDFSHook): HDFS接続Hook
        ticker (str): 証券コード
        data (pd.DataFrame): 株価データ
        tmp_file: 一時ファイルオブジェクト
    """
    if data is None or data.empty:
        print(f"No data to write for {ticker}")
        return

    # 一時ファイルに書き込む
    try:
        data.to_csv(tmp_file, index=True, header=tmp_file.tell() == 0)
        print(f"Successfully wrote data for {ticker} to temporary file")
    except Exception as e:
        print(f"Error writing data for {ticker} to temporary file: {e}")


def process_stock_data(hdfs_conn_id: str, hdfs_path: str, market: str = "prime"):
    """
    株価データを取得し、HDFSに出力する。
    ファイル名を収集したデータの開始日と終了日、マーケットが分かるようにする。
    """
    today = datetime.date.today()
    start_date = today - datetime.timedelta(days=today.weekday() + 7)
    end_date = today - datetime.timedelta(days=today.weekday() + 3)
    
    stock_data = get_stock_data_from_list()
    
    hdfs_hook = WebHDFSHook(webhdfs_conn_id=hdfs_conn_id)

    # Create a temporary file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=".csv") as tmp_file:
        for ticker, data in stock_data.items():
            write_stock_data_to_tmp_file(ticker, data, tmp_file)
        
        tmp_file_path = tmp_file.name

        # Load the temporary file to HDFS
        hdfs_file_name = f"stock_data_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_{market}.csv"
        hdfs_file_path = f"{hdfs_path}/{hdfs_file_name}"
        hdfs_hook.load_file(
            source=tmp_file_path,
            destination=hdfs_file_path,
            overwrite=True
        )

        # Remove the temporary file
        os.remove(tmp_file_path)

        print(f"Successfully wrote all stock data to HDFS path: {hdfs_file_path}")


with DAG(
    dag_id="stock_data_pipeline_prod",
    schedule=None,
    start_date=datetime.datetime(2023, 1, 1),
    catchup=False,
    tags=["stock_data"],
) as dag:
    get_and_upload_task = PythonOperator(
        task_id="get_and_upload_stock_data",
        python_callable=process_stock_data,
        op_kwargs={
            "hdfs_conn_id": "webhdfs_default",
            "hdfs_path": "/tmp/stock_data",
            "market": "prime",
        },
    )
