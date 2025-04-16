from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import yfinance as yf
import time
import pandas as pd

INDEX_TICKERS = [
    "^GSPC",  # S&P500
    "^DJI",   # NYダウ
    "^IXIC", # NASDAQ
    # "^NYA", # NYSE総合
    # "^XAX", # AMEX総合
    # "^BUK1000", # ブルームバーグ米国株式
    # "^RUT", # ラッセル2000
    # "^VIX", # VIX指数
    # "^FTSE", # FTSE100
    # "^GDAXI", # DAX
    # "^FCHI", # CAC40
    # "^STOXX50E", # EURO STOXX 50
    # "^N100", # EURO STOXX 100
    # "^BFX", # BEL 20
    # "MOEX.ME", # MOEX
    # "^HSI", # HSI
    # "^STI", # STI
    # "^AXJO", # ASX 200
    # "^AORD", # S&P/ASX 200
    # "^BSESN", # BSE SENSEX
    # "^JKSE", # IDX Composite
    # "^KLSE", # FTSE Bursa Malaysia KLCI
    # "^NZ50", # S&P/NZX 50 Index
    # "^KS11", # KOSPI Composite Index
    # "^TWII", # TSEC Capitalization Weighted Stock Index
    # "^GSPTSE", # TSEC Taiwan Weighted Index
    # "^BVSP", # Bovespa Index
    # "^MXX", # IPC Mexico
    # "^IPSA", # IPC Mexico
    # "^MERV", # MERVAL
    # "^TA125.TA", # TA-125 Index
    # "^CASE30", # EGX 30 Prise Return Index
    # "^JNOU.JO", # Top 40 USD Net Total Return Index
    # "DX-Y.NYB", # US Dollar Index
    # "^125904-USD-STRD", # MSCI EUROPE
    # "^XDB", # British Pound Currency Index
    # "^XDE", # Euro Currency Index
    # "000001.SS", # SSE Composite Index
    # "^N225", # Nikkei 225
    # "^XDN", # Japanese Yen Currency Index
    # "^XDA", # Australian Dollar Currency Index
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
    start_date = today - datetime.timedelta(days=today.weekday() + 7)
    end_date = today - datetime.timedelta(days=today.weekday() + 2)

    # データを取得
    return get_index_data(ticker, start_date, end_date, interval='1m')


def get_index_data_from_list() -> dict:
    """
    経済指標のリストを取得し、データを取得する。

    Returns:
        dict: 経済指標をキー、データを値とする辞書
    """
    index_data = {}
    for ticker in INDEX_TICKERS:
        data = get_index_data_for_ticker(ticker)
        if data is not None:
            index_data[ticker] = data
    return index_data


def process_index_data():
    """
    経済指標データを取得し、標準出力に出力する。
    """
    index_data = get_index_data_from_list()
    for ticker, data in index_data.items():
        print(f"Index: {ticker}")
        print(data.head(10))


with DAG(
    dag_id="index_data_pipeline_dev",
    schedule=None,
    start_date=datetime.datetime(2023, 1, 1),
    catchup=False,
    tags=["index_data"],
) as dag:
    get_and_upload_task = PythonOperator(
        task_id="get_and_upload_index_data",
        python_callable=process_index_data,
    )

