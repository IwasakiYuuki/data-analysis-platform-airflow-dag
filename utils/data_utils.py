"""
データ取得と処理に関する共通のユーティリティ関数
"""
import datetime
import time
import os
import tempfile
import pandas as pd
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from utils.config import DEFAULT_REQUEST_DELAY

def get_data_from_yfinance(symbol: str, start_date: datetime.date, end_date: datetime.date, interval: str = '1d'):
    """
    yfinanceを使って、指定期間・間隔の金融データを取得する。
    エラーハンドリングとリクエスト間隔を考慮。

    Args:
        symbol (str): 銘柄コード (例: '7203.T', 'USDJPY=X', '^N225')
        start_date (datetime.date): 取得開始日
        end_date (datetime.date): 取得終了日
        interval (str): データの間隔 (例: '1d', '1h', '1m')。デフォルトは '1d'。

    Returns:
        pd.DataFrame: 金融データ。取得に失敗した場合はNoneを返す。
    """
    try:
        import yfinance as yf
        # APIリクエストとデータ取得
        data = yf.download(symbol, start=start_date, end=end_date, interval=interval)
        time.sleep(DEFAULT_REQUEST_DELAY)  # リクエスト間隔
        
        # データがない場合、空のDataFrameが返ることがある
        if not isinstance(data, pd.DataFrame) or data.empty:
            print(f"No data found for {symbol} between {start_date} and {end_date} with interval {interval}")
            return None
        
        # 複数銘柄の場合、列がマルチインデックスになっていることがある
        if isinstance(data.columns, pd.MultiIndex):
            data = data.droplevel("Ticker", axis=1)
        
        # シンボル情報をカラムに追加
        data["Symbol"] = symbol
        return data
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None

def get_previous_week_dates():
    """
    前週の開始日と終了日を計算する

    Returns:
        tuple: (start_date, end_date)
    """
    today = datetime.date.today()
    start_date = today - datetime.timedelta(days=today.weekday() + 7)
    end_date = today - datetime.timedelta(days=today.weekday() + 1)
    return start_date, end_date

def write_to_hdfs(data: pd.DataFrame, hdfs_hook: WebHDFSHook, hdfs_path: str, 
                  partition_cols: list = None, overwrite: bool = True):
    """
    データをHDFSに書き込む。日付でパーティション化する。
    すべての銘柄のデータを一つのファイルに統合する。

    Args:
        data (pd.DataFrame): 書き込むデータ
        hdfs_hook (WebHDFSHook): HDFS接続Hook
        hdfs_path (str): HDFSの基本パス
        partition_cols (list): パーティションに使用するカラム。デフォルトは日付
        overwrite (bool): 既存ファイルを上書きするかどうか
    """
    if data is None or data.empty:
        print("No data to write to HDFS")
        return
    
    # データに日付カラムがない場合は追加
    if 'Date' not in data.columns and data.index.name != 'Date':
        data = data.copy()  # 元のデータを変更しないようにコピー
        if isinstance(data.index, pd.DatetimeIndex):
            data['Date'] = data.index.date
        else:
            data['Date'] = datetime.date.today()
    
    # パーティション列が指定されていない場合はデフォルトで日付を使用
    if partition_cols is None:
        partition_cols = ['Date']
    
    # パーティションごとにデータを分割して書き込み
    if 'Date' in partition_cols:
        # 日付ごとにグループ化
        grouped_data = data.groupby('Date')
        for date, daily_data in grouped_data:
            # 基本パスから日付パーティションパスを構築
            base_dir = hdfs_path.split("/")[:-1]  # "/<SYMBOL>" 部分を削除
            base_path = "/".join(base_dir)
            
            # HDFSのパスを構築 - 銘柄フォルダを削除し、日付パーティションのみに
            hdfs_file_path = f"{base_path}/year={date.year}/month={date.month:02d}/day={date.day:02d}/data.csv"
            
            # 一時ファイルに書き込む
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=".csv") as tmp_file:
                daily_data.to_csv(tmp_file, index=True)
                tmp_file_path = tmp_file.name
            
            try:
                # ファイルが存在するかチェック
                try:
                    file_exists = hdfs_hook.check_for_file(hdfs_file_path)
                except:
                    file_exists = False
                
                if file_exists and overwrite:
                    # 既存ファイルを上書きする場合
                    hdfs_hook.load_file(
                        source=tmp_file_path,
                        destination=hdfs_file_path,
                        overwrite=True
                    )
                elif file_exists:
                    # 既存ファイルに追記する場合 (まず一時的にダウンロード)
                    local_tmp_existing = os.path.join(tempfile.gettempdir(), f"existing_data_{date}.csv")
                    hdfs_hook.download(hdfs_file_path, local_tmp_existing)
                    
                    # 既存データと新しいデータを結合
                    existing_data = pd.read_csv(local_tmp_existing)
                    new_data = pd.read_csv(tmp_file_path)
                    combined_data = pd.concat([existing_data, new_data])
                    
                    # 結合したデータを一時ファイルに書き込む
                    combined_tmp = os.path.join(tempfile.gettempdir(), f"combined_data_{date}.csv")
                    combined_data.to_csv(combined_tmp, index=False)
                    
                    # 結合したデータをHDFSに書き込む
                    hdfs_hook.load_file(
                        source=combined_tmp,
                        destination=hdfs_file_path,
                        overwrite=True
                    )
                    
                    # 一時ファイル削除
                    os.remove(local_tmp_existing)
                    os.remove(combined_tmp)
                else:
                    # 新規ファイル作成
                    hdfs_hook.load_file(
                        source=tmp_file_path,
                        destination=hdfs_file_path,
                        overwrite=False
                    )
                    
                print(f"Successfully wrote data to HDFS path: {hdfs_file_path}")
            except Exception as e:
                print(f"Error writing data to HDFS: {e}")
            finally:
                # 一時ファイルを削除
                os.remove(tmp_file_path)
    else:
        # 日付パーティションを使用しない場合
        # 一時ファイルに書き込む
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=".csv") as tmp_file:
            data.to_csv(tmp_file, index=True)
            tmp_file_path = tmp_file.name

        try:
            # 基本パスから銘柄部分を削除
            base_dir = hdfs_path.split("/")[:-1]
            base_path = "/".join(base_dir)
            
            # 現在の日付をパスに含める
            today = datetime.date.today()
            hdfs_file_path = f"{base_path}/year={today.year}/month={today.month:02d}/day={today.day:02d}/data.csv"
            
            # 同様にファイルの存在チェックと結合処理
            try:
                file_exists = hdfs_hook.check_for_file(hdfs_file_path)
            except:
                file_exists = False
                
            if file_exists and overwrite:
                hdfs_hook.load_file(
                    source=tmp_file_path,
                    destination=hdfs_file_path,
                    overwrite=True
                )
            elif file_exists:
                # 既存ファイルに追記
                local_tmp_existing = os.path.join(tempfile.gettempdir(), "existing_data.csv")
                hdfs_hook.download(hdfs_file_path, local_tmp_existing)
                
                existing_data = pd.read_csv(local_tmp_existing)
                new_data = pd.read_csv(tmp_file_path)
                combined_data = pd.concat([existing_data, new_data])
                
                combined_tmp = os.path.join(tempfile.gettempdir(), "combined_data.csv")
                combined_data.to_csv(combined_tmp, index=False)
                
                hdfs_hook.load_file(
                    source=combined_tmp,
                    destination=hdfs_file_path,
                    overwrite=True
                )
                
                os.remove(local_tmp_existing)
                os.remove(combined_tmp)
            else:
                hdfs_hook.load_file(
                    source=tmp_file_path,
                    destination=hdfs_file_path,
                    overwrite=False
                )
                
            print(f"Successfully wrote data to HDFS path: {hdfs_file_path}")
        except Exception as e:
            print(f"Error writing data to HDFS: {e}")
        finally:
            # 一時ファイルを削除
            os.remove(tmp_file_path)