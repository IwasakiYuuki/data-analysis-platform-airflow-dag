"""
株価データ取得と処理に関するユーティリティ関数
"""
import pandas as pd
import datetime
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook

from utils.config import MARKET_COLUMN_NAMES, JPX_URL, HDFS_PATHS
from utils.data_utils import get_data_from_yfinance, get_previous_week_dates, write_to_hdfs

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
    指定された証券コードの株価データを取得する

    Args:
        ticker (str): 証券コード
        start_date (datetime.date): 取得開始日
        end_date (datetime.date): 取得終了日
        interval (str): データの間隔 (例: '1d', '1h', '1m')

    Returns:
        pd.DataFrame: 株価データ
    """
    data = get_data_from_yfinance(ticker, start_date, end_date, interval)
    if data is not None:
        data["Ticker"] = ticker  # ティッカーシンボル情報を追加
    return data

def get_stock_data_for_ticker(ticker: str, interval: str = '1m'):
    """
    指定された証券コードの先週1週間分のデータを取得する

    Args:
        ticker (str): 証券コード
        interval (str): データの間隔 (例: '1d', '1h', '1m')

    Returns:
        pd.DataFrame: 株価データ
    """
    start_date, end_date = get_previous_week_dates()
    return get_stock_data(ticker, start_date, end_date, interval=interval)

def fetch_stock_data(tickers: list, interval: str = '1m') -> list:
    """
    与えられた証券コードのリストに対して株価データを取得する

    Args:
        tickers (list): 証券コードのリスト
        interval (str): データの間隔

    Returns:
        list: (ticker, data) のタプルのリスト
    """
    stock_data_results = []
    for ticker in tickers:
        try:
            stock_data = get_stock_data_for_ticker(ticker, interval)
            stock_data_results.append((ticker, stock_data))
        except Exception as exc:
            print(f"{ticker} generated an exception: {exc}")
            stock_data_results.append((ticker, None))
    return stock_data_results

def get_stock_data_from_list(market: str = "prime", interval: str = '1m') -> dict:
    """
    指定された市場の証券コードリストを取得し、株価データを取得する

    Args:
        market (str): 市場区分 (prime, standard, growth, eft)
        interval (str): データの間隔

    Returns:
        dict: 証券コードをキー、株価データを値とする辞書
    """
    tickers = get_stock_list(market)
    stock_data_results = fetch_stock_data(tickers, interval)
    stock_data = {ticker: data for ticker, data in stock_data_results if data is not None}
    return stock_data

def process_stock_data(hdfs_conn_id: str, market: str = "prime", interval: str = '1m'):
    """
    株価データを取得し、HDFSに日ごとにパーティション化して出力する
    すべての銘柄のデータを1つのCSVファイルにまとめて保存する
    
    Args:
        hdfs_conn_id (str): HDFS接続ID
        market (str): 市場区分 (prime, standard, growth, eft)
        interval (str): データの間隔
    """
    # 株価データを取得
    stock_data = get_stock_data_from_list(market, interval)
    
    # HDFSに接続
    hdfs_hook = WebHDFSHook(webhdfs_conn_id=hdfs_conn_id)
    
    # 市場に対応するHDFSパスを取得
    base_hdfs_path = HDFS_PATHS["stock"].get(market, HDFS_PATHS["stock"]["prime"])
    hdfs_path = f"{base_hdfs_path}/{interval}"
    
    # すべての銘柄データを結合
    combined_data = pd.DataFrame()
    
    for ticker, data in stock_data.items():
        if data is None or data.empty:
            print(f"No data to process for {ticker}")
            continue
        
        # データをコピーして結合用のデータフレームに追加
        combined_data = pd.concat([combined_data, data])
    
    # 結合したデータがある場合、HDFSに書き込む
    if not combined_data.empty:
        write_to_hdfs(combined_data, hdfs_hook, hdfs_path)
    else:
        print(f"No stock data to write to HDFS for market: {market}")
