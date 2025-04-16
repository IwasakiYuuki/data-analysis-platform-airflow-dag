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

INDEX_TICKERS = [
    "^GSPC",  # S&P500
    "^DJI",   # NYダウ
    "^IXIC", # NASDAQ
    "^NYA", # NYSE総合
    "^XAX", # AMEX総合
    "^BUK1000", # ブルームバーグ米国株式
    "^RUT", # ラッセル2000
    "^VIX", # VIX指数
    "^FTSE", # FTSE100
    "^GDAXI", # DAX
    "^FCHI", # CAC40
    "^STOXX50E", # EURO STOXX 50
    "^N100", # EURO STOXX 100
    "^BFX", # BEL 20
    "MOEX.ME", # MOEX
    "^HSI", # HSI
    "^STI", # STI
    "^AXJO", # ASX 200
    "^AORD", # S&P/ASX 200
    "^BSESN", # BSE SENSEX
    "^JKSE", # IDX Composite
    "^KLSE", # FTSE Bursa Malaysia KLCI
    "^NZ50", # S&P/NZX 50 Index
    "^KS11", # KOSPI Composite Index
    "^TWII", # TSEC Capitalization Weighted Stock Index
    "^GSPTSE", # TSEC Taiwan Weighted Index
    "^BVSP", # Bovespa Index
    "^MXX", # IPC Mexico
    "^IPSA", # IPC Mexico
    "^MERV", # MERVAL
    "^TA125.TA", # TA-125 Index
    "^CASE30", # EGX 30 Prise Return Index
    "^JNOU.JO", # Top 40 USD Net Total Return Index
    "DX-Y.NYB", # US Dollar Index
    "^125904-USD-STRD", # MSCI EUROPE
    "^XDB", # British Pound Currency Index
    "^XDE", # Euro Currency Index
    "000001.SS", # SSE Composite Index
    "^N225", # Nikkei 225
    "^XDN", # Japanese Yen Currency Index
    "^XDA", # Australian Dollar Currency Index
]


def get_index_data(ticker: str, start_date: datetime.date, end_date: datetime.date, interval: str = '1d'):
    """
    yfinanceを使って、指定期間・間隔の経済指標データを取得する。
    エラーハンドリングとリクエスト間隔を考慮。

    Args:
        ticker (str): 指標のティッカーシンボル (例: '^N225')
        start_date (datetime.date): 取得開始日
        end_date (datetime.date): 取得終了日
        interval (str): データの間隔 (例: '1d', '1h', '1m')。デフォルトは '1d'。

    Returns:
        pd.DataFrame: 指標データ。取得に失敗した場合はNoneを返す。
    """
    try:
        data = yf.download(ticker, start=start_date, end=end_date, interval=interval)
        time.sleep(1)  # リクエスト間隔を1秒に設定 (調整可能)
        # データがない場合、空のDataFrameが返ることがある
        if not isinstance(data, pd.DataFrame) or data.empty:
            print(f"No data found for {ticker} between {start_date} and {end_date} with interval {interval}")
            return None
        # マルチインデックスのカラムから 'Ticker' レベルを削除し、新しい 'Ticker' カラムを追加
        if isinstance(data.columns, pd.MultiIndex):
            data = data.droplevel(1, axis=1) # yfinanceのバージョンによってレベル名が異なる場合があるため、レベル番号で指定
        data["Ticker"] = ticker
        return data
    except Exception as e:
        print(f"Error fetching data for {ticker}: {e}")
        return None


def get_index_data_for_ticker(ticker: str):
    """
    指定された経済指標の過去1週間分のデータを取得する。

    Args:
        ticker (str): 指標のティッカーシンボル

    Returns:
        pd.DataFrame: 経済指標データ。取得できない場合はNone。
    """
    today = datetime.date.today()
    # 先週の月曜日から金曜日までのデータを取得
    start_date = today - datetime.timedelta(days=today.weekday() + 7)
    end_date = today - datetime.timedelta(days=today.weekday() + 2)

    # データを取得
    return get_index_data(ticker, start_date, end_date, interval='1m')


def fetch_index_data(tickers: list, max_workers: int = 5) -> list:
    """
    与えられた指標のリストに対して、並列でデータを取得する。

    Args:
        tickers (list): 指標のティッカーシンボルのリスト
        max_workers (int): 並列処理の最大ワーカー数

    Returns:
        list: (ticker, data) のタプルのリスト。データ取得に失敗した場合は、tickerとNoneのタプルを返す。
    """
    index_data_results = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_ticker = {executor.submit(get_index_data_for_ticker, ticker): ticker for ticker in tickers}
        for future in concurrent.futures.as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                data = future.result()
                index_data_results.append((ticker, data))
            except Exception as exc:
                print(f"{ticker} generated an exception: {exc}")
                index_data_results.append((ticker, None))  # エラーが発生した場合、Noneをリストに追加
    return index_data_results


def get_index_data_from_list(max_workers: int = 5) -> dict:
    """
    経済指標のリストを取得し、データを取得する。

    Args:
        max_workers (int): 並列処理の最大ワーカー数

    Returns:
        dict: 経済指標をキー、データを値とする辞書
    """
    tickers = INDEX_TICKERS
    index_data_results = fetch_index_data(tickers, max_workers)
    index_data = {ticker: data for ticker, data in index_data_results if data is not None}  # Noneのデータを除外
    return index_data


def write_index_data_to_tmp_file(ticker: str, data: pd.DataFrame, tmp_file):
    """
    指標データを一時ファイルに書き込む。

    Args:
        ticker (str): 指標のティッカーシンボル
        data (pd.DataFrame): 指標データ
        tmp_file: 一時ファイルオブジェクト
    """
    if data is None or data.empty:
        print(f"No data to write for {ticker}")
        return

    # 一時ファイルに書き込む (ヘッダーは最初の書き込み時のみ)
    try:
        data.to_csv(tmp_file, index=True, header=tmp_file.tell() == 0)
        print(f"Successfully wrote data for {ticker} to temporary file")
    except Exception as e:
        print(f"Error writing data for {ticker} to temporary file: {e}")


def process_index_data(hdfs_conn_id: str, hdfs_path: str):
    """
    経済指標データを取得し、HDFSに出力する。
    ファイル名を収集したデータの開始日と終了日、指標が分かるようにする。
    """
    today = datetime.date.today()
    start_date = today - datetime.timedelta(days=today.weekday() + 7)
    end_date = today - datetime.timedelta(days=today.weekday() + 3)

    index_data = get_index_data_from_list()

    if not index_data:
        print("No index data fetched. Exiting.")
        return

    hdfs_hook = WebHDFSHook(webhdfs_conn_id=hdfs_conn_id)

    # Create a temporary file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=".csv") as tmp_file:
        for ticker, data in index_data.items():
            write_index_data_to_tmp_file(ticker, data, tmp_file)

        tmp_file_path = tmp_file.name

    # Check if the temporary file was created and has content
    if os.path.exists(tmp_file_path) and os.path.getsize(tmp_file_path) > 0:
        # Load the temporary file to HDFS
        hdfs_file_name = f"index_data_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
        hdfs_file_path = f"{hdfs_path}/{hdfs_file_name}"
        try:
            hdfs_hook.load_file(
                source=tmp_file_path,
                destination=hdfs_file_path,
                overwrite=True
            )
            print(f"Successfully wrote all index data to HDFS path: {hdfs_file_path}")
        except Exception as e:
            print(f"Error uploading file to HDFS: {e}")
        finally:
            # Remove the temporary file
            os.remove(tmp_file_path)
            print(f"Removed temporary file: {tmp_file_path}")
    else:
        print("Temporary file is empty or does not exist. No upload to HDFS.")
        if os.path.exists(tmp_file_path):
            os.remove(tmp_file_path) # Remove empty file


with DAG(
    dag_id="index_data_pipeline_prod",
    schedule=None,
    start_date=datetime.datetime(2023, 1, 1),
    catchup=False,
    tags=["index_data", "prod"],
) as dag:
    get_and_upload_task = PythonOperator(
        task_id="get_and_upload_index_data",
        python_callable=process_index_data,
        op_kwargs={
            "hdfs_conn_id": "webhdfs_default",
            "hdfs_path": "/tmp/index_data", # HDFSのパスを適切に設定してください
        },
    )
