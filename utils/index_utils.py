"""
指数データ取得と処理に関するユーティリティ関数
"""
import pandas as pd
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook

from utils.config import INDEX_SYMBOLS, HDFS_PATHS
from utils.data_utils import get_data_from_yfinance, get_previous_week_dates, write_to_hdfs

def get_index_list():
    """
    取得対象の指数シンボルのリストを返す

    Returns:
        list: 指数シンボルのリスト
    """
    return INDEX_SYMBOLS

def get_index_data_for_symbol(symbol: str, interval: str = '1d'):
    """
    指定された指数の先週1週間分のデータを取得する

    Args:
        symbol (str): 指数シンボル (例: "^N225")
        interval (str): データの間隔 (例: '1d', '1h')

    Returns:
        pd.DataFrame: 指数データ
    """
    start_date, end_date = get_previous_week_dates()
    data = get_data_from_yfinance(symbol, start_date, end_date, interval)
    return data

def fetch_index_data(symbols: list = None, interval: str = '1d') -> dict:
    """
    指定された指数シンボルのリストに対してデータを取得する

    Args:
        symbols (list): 指数シンボルのリスト。Noneの場合はデフォルトリストを使用
        interval (str): データの間隔

    Returns:
        dict: 指数シンボルをキー、データを値とする辞書
    """
    if symbols is None:
        symbols = get_index_list()
    
    index_data = {}
    for symbol in symbols:
        try:
            data = get_index_data_for_symbol(symbol, interval)
            if data is not None and not data.empty:
                index_data[symbol] = data
            else:
                print(f"No data found for {symbol}")
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
    
    return index_data

def process_index_data(hdfs_conn_id: str, interval: str = '1d'):
    """
    指数データを取得し、HDFSに出力する
    
    Args:
        hdfs_conn_id (str): HDFS接続ID
        interval (str): データの間隔
    """
    # 指数データを取得
    index_data = fetch_index_data(interval=interval)
    
    # HDFSに接続
    hdfs_hook = WebHDFSHook(webhdfs_conn_id=hdfs_conn_id)
    
    # HDFSパスの設定
    base_hdfs_path = HDFS_PATHS["index"]
    hdfs_path = f"{base_hdfs_path}/{interval}"
    
    # 各指数のデータをHDFSに書き込む
    for symbol, data in index_data.items():
        if data is None or data.empty:
            print(f"No data to process for {symbol}")
            continue
        
        # 指数名に含まれる特殊文字を置換
        symbol_name = symbol.replace('^', '')
        symbol_path = f"{hdfs_path}/{symbol_name}"
        
        # データをHDFSに書き込む
        write_to_hdfs(data, hdfs_hook, symbol_path)