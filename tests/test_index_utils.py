import unittest
from unittest import mock
import pandas as pd
import datetime
from utils.index_utils import (
    get_index_list,
    get_index_data_for_symbol,
    fetch_index_data,
    process_index_data
)
from utils.config import INDEX_SYMBOLS

class TestIndexUtils(unittest.TestCase):
    
    def test_get_index_list(self):
        """指数シンボルのリスト取得関数のテスト"""
        result = get_index_list()
        self.assertEqual(result, INDEX_SYMBOLS)
        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)
    
    @mock.patch('utils.index_utils.get_data_from_yfinance')
    @mock.patch('utils.index_utils.get_previous_week_dates')
    def test_get_index_data_for_symbol(self, mock_get_dates, mock_get_data):
        """特定の指数シンボルのデータ取得関数のテスト"""
        # モックデータの準備
        mock_get_dates.return_value = (datetime.date(2025, 1, 6), datetime.date(2025, 1, 12))
        mock_df = pd.DataFrame({
            'Open': [38000.5, 38100.2],
            'High': [38200.1, 38300.8],
            'Low': [37900.1, 38000.0],
            'Close': [38100.9, 38200.5],
            'Volume': [100000, 120000]
        }, index=pd.DatetimeIndex(['2025-01-06', '2025-01-07']))
        mock_get_data.return_value = mock_df
        
        # 関数実行
        result = get_index_data_for_symbol("^N225", "1d")
        
        # 結果確認 - DataFrameを直接比較するのではなく、内容を確認
        self.assertIsInstance(result, pd.DataFrame)
        pd.testing.assert_frame_equal(result, mock_df)
        mock_get_dates.assert_called_once()
        mock_get_data.assert_called_once_with(
            "^N225", 
            datetime.date(2025, 1, 6), 
            datetime.date(2025, 1, 12), 
            "1d"
        )
    
    @mock.patch('utils.index_utils.get_index_data_for_symbol')
    @mock.patch('utils.index_utils.get_index_list')
    def test_fetch_index_data(self, mock_get_list, mock_get_data):
        """複数の指数シンボルのデータを取得する関数のテスト"""
        # モックデータの準備
        mock_get_list.return_value = ["^N225", "^GSPC"]
        
        # 各指数のデータを異なるものにする
        mock_df1 = pd.DataFrame({'Close': [38000.5, 38100.2]}, index=pd.DatetimeIndex(['2025-01-06', '2025-01-07']))
        mock_df1['Symbol'] = "^N225"
        
        mock_df2 = pd.DataFrame({'Close': [4700.2, 4750.3]}, index=pd.DatetimeIndex(['2025-01-06', '2025-01-07']))
        mock_df2['Symbol'] = "^GSPC"
        
        mock_get_data.side_effect = [mock_df1, mock_df2]
        
        # 関数実行
        result = fetch_index_data(interval="1d")
        
        # 結果確認
        self.assertIsInstance(result, dict)
        self.assertEqual(len(result), 2)
        self.assertIn("^N225", result)
        self.assertIn("^GSPC", result)
        self.assertEqual(result["^N225"]["Symbol"].iloc[0], "^N225")
        mock_get_list.assert_called_once()
        self.assertEqual(mock_get_data.call_count, 2)
    
    @mock.patch('utils.index_utils.fetch_index_data')
    @mock.patch('utils.index_utils.write_to_hdfs')
    @mock.patch('utils.index_utils.WebHDFSHook')
    def test_process_index_data(self, mock_hdfs_hook_class, mock_write_to_hdfs, mock_fetch_data):
        """指数データ処理の完全なフローのテスト"""
        # モックデータの準備
        mock_hdfs_hook = mock.Mock()
        mock_hdfs_hook_class.return_value = mock_hdfs_hook
        
        # 2つの指数のデータを含む辞書を作成
        mock_df1 = pd.DataFrame({'Close': [38000.5, 38100.2]}, index=pd.DatetimeIndex(['2025-01-06', '2025-01-07']))
        mock_df2 = pd.DataFrame({'Close': [4700.2, 4750.3]}, index=pd.DatetimeIndex(['2025-01-06', '2025-01-07']))
        mock_fetch_data.return_value = {
            "^N225": mock_df1,
            "^GSPC": mock_df2,
        }
        
        # 関数実行
        process_index_data("hdfs_conn_test", interval="1d")
        
        # 結果確認
        mock_hdfs_hook_class.assert_called_once_with(webhdfs_conn_id="hdfs_conn_test")
        mock_fetch_data.assert_called_once_with(interval="1d")
        # 2つの指数のデータに対してwrite_to_hdfsが呼ばれることを確認
        self.assertEqual(mock_write_to_hdfs.call_count, 2)
        
    @mock.patch('utils.index_utils.fetch_index_data')
    @mock.patch('utils.index_utils.write_to_hdfs')
    @mock.patch('utils.index_utils.WebHDFSHook')
    def test_process_index_data_empty(self, mock_hdfs_hook_class, mock_write_to_hdfs, mock_fetch_data):
        """空のデータセットがある場合の指数データ処理のテスト"""
        # モックデータの準備
        mock_hdfs_hook = mock.Mock()
        mock_hdfs_hook_class.return_value = mock_hdfs_hook
        
        # 1つの指数にデータがあり、もう1つはデータがない場合
        mock_df = pd.DataFrame({'Close': [38000.5, 38100.2]}, index=pd.DatetimeIndex(['2025-01-06', '2025-01-07']))
        mock_fetch_data.return_value = {
            "^N225": mock_df,
            "^GSPC": None,  # データなし
        }
        
        # 関数実行
        process_index_data("hdfs_conn_test")
        
        # 結果確認
        mock_hdfs_hook_class.assert_called_once_with(webhdfs_conn_id="hdfs_conn_test")
        mock_fetch_data.assert_called_once_with(interval="1d")  # デフォルト値
        # データがある指数に対してのみwrite_to_hdfsが呼ばれることを確認
        mock_write_to_hdfs.assert_called_once()

    @mock.patch('utils.index_utils.fetch_index_data')
    @mock.patch('utils.index_utils.WebHDFSHook')
    def test_process_index_data_with_special_chars(self, mock_hdfs_hook_class, mock_fetch_data):
        """特殊文字を含むシンボル名のHDFSパス変換テスト"""
        # モックデータの準備
        mock_hdfs_hook = mock.Mock()
        mock_hdfs_hook_class.return_value = mock_hdfs_hook
        
        # ^記号を持つシンボルのデータを用意
        mock_df = pd.DataFrame({'Close': [38000.5]}, index=pd.DatetimeIndex(['2025-01-06']))
        mock_fetch_data.return_value = {
            "^N225": mock_df
        }
        
        # パッチを適用して、write_to_hdfsの呼び出しを監視
        with mock.patch('utils.index_utils.write_to_hdfs') as mock_write:
            process_index_data("hdfs_conn_test")
            
            # 呼び出し引数をキャプチャ
            args, kwargs = mock_write.call_args
            
            # ^記号が削除されたパスが使われていることを確認
            self.assertIn("/N225", args[2])
            self.assertNotIn("/^N225", args[2])

if __name__ == '__main__':
    unittest.main()