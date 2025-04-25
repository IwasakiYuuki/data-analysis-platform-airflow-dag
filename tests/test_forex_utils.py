import unittest
from unittest import mock
import pandas as pd
import datetime
from utils.forex_utils import (
    get_forex_list,
    get_forex_data_for_pair,
    fetch_forex_data,
    process_forex_data
)
from utils.config import FOREX_PAIRS

class TestForexUtils(unittest.TestCase):
    
    def test_get_forex_list(self):
        """為替ペアのリスト取得関数のテスト"""
        result = get_forex_list()
        self.assertEqual(result, FOREX_PAIRS)
        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)
    
    @mock.patch('utils.forex_utils.get_data_from_yfinance')
    @mock.patch('utils.forex_utils.get_previous_week_dates')
    def test_get_forex_data_for_pair(self, mock_get_dates, mock_get_data):
        """特定の為替ペアのデータ取得関数のテスト"""
        # モックデータの準備
        mock_get_dates.return_value = (datetime.date(2025, 1, 6), datetime.date(2025, 1, 12))
        mock_df = pd.DataFrame({
            'Open': [110.5, 111.2],
            'High': [112.1, 112.8],
            'Low': [110.1, 111.0],
            'Close': [111.9, 112.5],
            'Volume': [0, 0]  # 為替データはボリュームが通常0
        }, index=pd.DatetimeIndex(['2025-01-06', '2025-01-07']))
        mock_get_data.return_value = mock_df
        
        # 関数実行
        result = get_forex_data_for_pair("USDJPY=X", "1h")
        
        # 結果確認
        # DataFrameを直接比較するのではなく、内容を確認
        self.assertIsInstance(result, pd.DataFrame)
        pd.testing.assert_frame_equal(result, mock_df)
        mock_get_dates.assert_called_once()
        mock_get_data.assert_called_once_with(
            "USDJPY=X", 
            datetime.date(2025, 1, 6), 
            datetime.date(2025, 1, 12), 
            "1h"
        )
    
    @mock.patch('utils.forex_utils.get_forex_data_for_pair')
    @mock.patch('utils.forex_utils.get_forex_list')
    def test_fetch_forex_data(self, mock_get_list, mock_get_data):
        """複数の為替ペアのデータを取得する関数のテスト"""
        # モックデータの準備
        mock_get_list.return_value = ["USDJPY=X", "EURUSD=X"]
        
        # 各ペアのデータを異なるものにする
        mock_df1 = pd.DataFrame({'Close': [110.5, 111.2]}, index=pd.DatetimeIndex(['2025-01-06', '2025-01-07']))
        mock_df1['Symbol'] = "USDJPY=X"
        
        mock_df2 = pd.DataFrame({'Close': [1.22, 1.23]}, index=pd.DatetimeIndex(['2025-01-06', '2025-01-07']))
        mock_df2['Symbol'] = "EURUSD=X"
        
        mock_get_data.side_effect = [mock_df1, mock_df2]
        
        # 関数実行
        result = fetch_forex_data(interval="1h")
        
        # 結果確認
        self.assertIsInstance(result, dict)
        self.assertEqual(len(result), 2)
        self.assertIn("USDJPY=X", result)
        self.assertIn("EURUSD=X", result)
        self.assertEqual(result["USDJPY=X"]["Symbol"].iloc[0], "USDJPY=X")
        mock_get_list.assert_called_once()
        self.assertEqual(mock_get_data.call_count, 2)
    
    @mock.patch('utils.forex_utils.fetch_forex_data')
    @mock.patch('utils.forex_utils.write_to_hdfs')
    @mock.patch('utils.forex_utils.WebHDFSHook')
    def test_process_forex_data(self, mock_hdfs_hook_class, mock_write_to_hdfs, mock_fetch_data):
        """為替データ処理の完全なフローのテスト"""
        # モックデータの準備
        mock_hdfs_hook = mock.Mock()
        mock_hdfs_hook_class.return_value = mock_hdfs_hook
        
        # 2つの為替ペアのデータを含む辞書を作成
        mock_df1 = pd.DataFrame({'Close': [110.5, 111.2]}, index=pd.DatetimeIndex(['2025-01-06', '2025-01-07']))
        mock_df2 = pd.DataFrame({'Close': [1.22, 1.23]}, index=pd.DatetimeIndex(['2025-01-06', '2025-01-07']))
        mock_fetch_data.return_value = {
            "USDJPY=X": mock_df1,
            "EURUSD=X": mock_df2,
        }
        
        # 関数実行
        process_forex_data("hdfs_conn_test", interval="1h")
        
        # 結果確認
        mock_hdfs_hook_class.assert_called_once_with(webhdfs_conn_id="hdfs_conn_test")
        mock_fetch_data.assert_called_once_with(interval="1h")
        # 2つの為替ペアのデータに対してwrite_to_hdfsが呼ばれることを確認
        self.assertEqual(mock_write_to_hdfs.call_count, 2)
        
    @mock.patch('utils.forex_utils.fetch_forex_data')
    @mock.patch('utils.forex_utils.write_to_hdfs')
    @mock.patch('utils.forex_utils.WebHDFSHook')
    def test_process_forex_data_empty(self, mock_hdfs_hook_class, mock_write_to_hdfs, mock_fetch_data):
        """空のデータセットがある場合の為替データ処理のテスト"""
        # モックデータの準備
        mock_hdfs_hook = mock.Mock()
        mock_hdfs_hook_class.return_value = mock_hdfs_hook
        
        # 1つの為替ペアにデータがあり、もう1つはデータがない場合
        mock_df = pd.DataFrame({'Close': [110.5, 111.2]}, index=pd.DatetimeIndex(['2025-01-06', '2025-01-07']))
        mock_fetch_data.return_value = {
            "USDJPY=X": mock_df,
            "EURUSD=X": None,  # データなし
        }
        
        # 関数実行
        process_forex_data("hdfs_conn_test")
        
        # 結果確認
        mock_hdfs_hook_class.assert_called_once_with(webhdfs_conn_id="hdfs_conn_test")
        mock_fetch_data.assert_called_once()
        # データがあるペアに対してのみwrite_to_hdfsが呼ばれることを確認
        mock_write_to_hdfs.assert_called_once()

if __name__ == '__main__':
    unittest.main()