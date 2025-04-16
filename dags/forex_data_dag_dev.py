from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import yfinance as yf
import time
import concurrent.futures
import pandas as pd

# 通貨ペアのリスト（例）
FOREX_CURRENCY_PAIRS = [
    "EURUSD=X",
    "JPY=X",
    "GBPUSD=X",
    # "AUDUSD=X",  # 開発環境では3つの通貨ペアのみを使用
    # "NZDUSD=X",
    # "EURJPY=X",
    # "GBPJPY=X",
    # "EURGBP=X",
    # "EURCAD=X",
    # "EURSEK=X",
    # "EURCHF=X",
    # "EURHUF=X",
    # "CNY=X",
    # "HKD=X",
    # "SGD=X",
    # "INR=X",
    # "MXN=X",
    # "PHP=X",
    # "IDR=X",
    # "THB=X",
    # "MYR=X",
    # "ZAR=X",
    # "RUB=X",
]


def get_forex_data(currency_pair: str, start_date: datetime.date, end_date: datetime.date, interval: str = '1d'):
    """
    yfinanceを使って、指定期間・間隔の為替データを取得する。
    エラーハンドリングとリクエスト間隔を考慮。

    Args:
        currency_pair (str): 通貨ペア (例: 'EURUSD=X')
        start_date (datetime.date): 取得開始日
        end_date (datetime.date): 取得終了日
        interval (str): データの間隔 (例: '1d', '1h', '1m')。デフォルトは '1d'。

    Returns:
        pd.DataFrame: 為替データ。取得に失敗した場合はNoneを返す。
    """
    try:
        # 1分足データは期間制限があるため注意 (通常は直近7日間)
        data = yf.download(currency_pair, start=start_date, end=end_date, interval=interval)
        time.sleep(1)  # リクエスト間隔を1秒に設定 (調整可能)
        # データがない場合、空のDataFrameが返ることがある
        if not isinstance(data, pd.DataFrame) or data.empty:
            print(f"No data found for {currency_pair} between {start_date} and {end_date} with interval {interval}")
            return None
        data = data.droplevel("Ticker", axis=1)
        data["Ticker"] = currency_pair
        return data
    except Exception as e:
        print(f"Error fetching data for {currency_pair}: {e}")
        return None


def get_forex_data_for_pair(currency_pair: str):
    """
    指定された通貨ペアの先週1週間分の1分足為替データを取得する。

    Args:
        currency_pair (str): 通貨ペア

    Returns:
        pd.DataFrame: 1分足為替データ。取得できない場合はNone。
    """
    today = datetime.date.today()
    start_date = today - datetime.timedelta(days=today.weekday() + 7)
    end_date = today - datetime.timedelta(days=today.weekday() + 3)

    # 1分足データを取得
    return get_forex_data(currency_pair, start_date, end_date, interval='1m')


def fetch_forex_data(currency_pairs: list, max_workers: int = 5) -> list:
    """
    与えられた通貨ペアのリストに対して、並列で為替データを取得する。

    Args:
        currency_pairs (list): 通貨ペアのリスト
        max_workers (int): 並列処理の最大ワーカー数

    Returns:
        list: (currency_pair, data) のタプルのリスト。データ取得に失敗した場合は、currency_pairとNoneのタプルを返す。
    """
    forex_data_results = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_pair = {executor.submit(get_forex_data_for_pair, pair): pair for pair in currency_pairs}
        for future in concurrent.futures.as_completed(future_to_pair):
            pair = future_to_pair[future]
            try:
                data = future.result()
                forex_data_results.append((pair, data))
            except Exception as exc:
                print(f"{pair} generated an exception: {exc}")
                forex_data_results.append((pair, None))  # エラーが発生した場合、Noneをリストに追加
    return forex_data_results


def get_forex_data_from_list(max_workers: int = 5) -> dict:
    """
    通貨ペアのリストを取得し、為替データを取得する。

    Args:
        max_workers (int): 並列処理の最大ワーカー数

    Returns:
        dict: 通貨ペアをキー、為替データを値とする辞書
    """
    currency_pairs = FOREX_CURRENCY_PAIRS  # 通貨ペアのリストを取得
    forex_data_results = fetch_forex_data(currency_pairs, max_workers)
    forex_data = {pair: data for pair, data in forex_data_results if data is not None}  # Noneのデータを除外
    return forex_data


def process_forex_data():
    """
    為替データを取得し、標準出力に出力する。
    """
    forex_data = get_forex_data_from_list()
    for pair, data in forex_data.items():
        print(f"Currency Pair: {pair}")
        print(data.head(10))


with DAG(
    dag_id="forex_data_pipeline_dev",
    schedule=None,
    start_date=datetime.datetime(2023, 1, 1),
    catchup=False,
    tags=["forex_data"],
) as dag:
    get_and_upload_task = PythonOperator(
        task_id="get_and_upload_forex_data",
        python_callable=process_forex_data,
    )
