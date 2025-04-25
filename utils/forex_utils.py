"""
為替データ取得と処理に関するユーティリティ関数
"""
import pandas as pd
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook

from utils.config import FOREX_PAIRS, HDFS_PATHS
from utils.data_utils import get_data_from_yfinance, get_previous_week_dates, write_to_hdfs

def get_forex_list():
    """
    取得対象の為替ペアのリストを返す

    Returns:
        list: 為替ペアのリスト
    """
    return FOREX_PAIRS

def get_forex_data_for_pair(pair: str, interval: str = '1h'):
    """
    指定された為替ペアの先週1週間分のデータを取得する

    Args:
        pair (str): 為替ペア (例: "USDJPY=X")
        interval (str): データの間隔 (例: '1d', '1h', '5m')

    Returns:
        pd.DataFrame: 為替データ
    """
    start_date, end_date = get_previous_week_dates()
    data = get_data_from_yfinance(pair, start_date, end_date, interval)
    return data

def fetch_forex_data(pairs: list = None, interval: str = '1h') -> dict:
    """
    指定された為替ペアのリストに対してデータを取得する

    Args:
        pairs (list): 為替ペアのリスト。Noneの場合はデフォルトリストを使用
        interval (str): データの間隔

    Returns:
        dict: 為替ペアをキー、データを値とする辞書
    """
    if pairs is None:
        pairs = get_forex_list()
    
    forex_data = {}
    for pair in pairs:
        try:
            data = get_forex_data_for_pair(pair, interval)
            if data is not None and not data.empty:
                forex_data[pair] = data
            else:
                print(f"No data found for {pair}")
        except Exception as e:
            print(f"Error fetching data for {pair}: {e}")
    
    return forex_data

def process_forex_data(hdfs_conn_id: str, interval: str = '1h'):
    """
    為替データを取得し、HDFSに出力する
    すべての為替ペアのデータを1つのCSVファイルにまとめて保存する
    
    Args:
        hdfs_conn_id (str): HDFS接続ID
        interval (str): データの間隔
    """
    # 為替データを取得
    forex_data = fetch_forex_data(interval=interval)
    
    # HDFSに接続
    hdfs_hook = WebHDFSHook(webhdfs_conn_id=hdfs_conn_id)
    
    # HDFSパスの設定
    base_hdfs_path = HDFS_PATHS["forex"]
    hdfs_path = f"{base_hdfs_path}/{interval}"
    
    # すべての為替ペアのデータを結合
    combined_data = pd.DataFrame()
    
    for pair, data in forex_data.items():
        if data is None or data.empty:
            print(f"No data to process for {pair}")
            continue
            
        # データをコピーして結合用のデータフレームに追加
        combined_data = pd.concat([combined_data, data])
    
    # 結合したデータがある場合、HDFSに書き込む
    if not combined_data.empty:
        write_to_hdfs(combined_data, hdfs_hook, hdfs_path)
    else:
        print("No forex data to write to HDFS")
