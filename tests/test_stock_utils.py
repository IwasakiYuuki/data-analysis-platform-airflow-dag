import unittest
from unittest import mock
import pandas as pd
import datetime
from utils.stock_utils import (
    get_stock_list,
    get_stock_data,
    get_stock_data_for_ticker,
    fetch_stock_data,
    get_stock_data_from_list,
    process_stock_data
)
from utils.config import MARKET_COLUMN_NAMES

class TestStockUtils(unittest.TestCase):
    
    @mock.patch('utils.stock_utils.pd.read_excel')
    def test_get_stock_list(self, mock_read_excel):
        """JPXウェブサイトから証券コードのリストを取得する関数のテスト"""
        # モックデータを準備
        mock_df = pd.DataFrame({
            'コード': [1301, 1302, 1303],
            '市場・商品区分': ['プライム（内国株式）', 'プライム（内国株式）', 'スタンダード（内国株式）']
        })
        mock_read_excel.return_value = mock_df
        
        # テスト実行
        result = get_stock_list('prime')
        
        # 結果確認
        self.assertEqual(result, ['1301.T', '1302.T'])
        mock_read_excel.assert_called_once()

    @mock.patch('utils.stock_utils.get_data_from_yfinance')
    def test_get_stock_data(self, mock_get_data):
        """特定の証券コードの株価データを取得する関数のテスト"""
        # モックデータの準備
        mock_df = pd.DataFrame({
            'Open': [2000.0, 2010.0],
            'High': [2030.0, 2050.0],
            'Low': [1990.0, 2000.0],
            'Close': [2020.0, 2040.0],
            'Volume': [100000, 120000]
        }, index=pd.DatetimeIndex(['2025-01-06', '2025-01-07']))
        mock_get_data.return_value = mock_df
        
        # テスト実行
        start_date = datetime.date(2025, 1, 6)
        end_date = datetime.date(2025, 1, 7)
        result = get_stock_data('7203.T', start_date, end_date)
        
        # 結果確認
        self.assertIsInstance(result, pd.DataFrame)
        self.assertIn('Ticker', result.columns)
        self.assertEqual(result['Ticker'].iloc[0], '7203.T')
        mock_get_data.assert_called_once()

    @mock.patch('utils.stock_utils.get_stock_data')
    @mock.patch('utils.stock_utils.get_previous_week_dates')
    def test_get_stock_data_for_ticker(self, mock_get_dates, mock_get_stock_data):
        """特定の証券コードの先週1週間分のデータを取得する関数のテスト"""
        # モックデータの準備
        mock_get_dates.return_value = (datetime.date(2025, 1, 6), datetime.date(2025, 1, 12))
        mock_df = pd.DataFrame({
            'Close': [2020.0, 2040.0],
            'Ticker': ['7203.T', '7203.T']
        })
        mock_get_stock_data.return_value = mock_df
        
        # テスト実行
        result = get_stock_data_for_ticker('7203.T', '1m')
        
        # 結果確認 - DataFrameを直接比較するのではなく内容を確認する
        self.assertIsInstance(result, pd.DataFrame)
        pd.testing.assert_frame_equal(result, mock_df)
        mock_get_dates.assert_called_once()
        mock_get_stock_data.assert_called_once_with(
            '7203.T',
            datetime.date(2025, 1, 6),
            datetime.date(2025, 1, 12),
            interval='1m'
        )

    @mock.patch('utils.stock_utils.get_stock_data_for_ticker')
    def test_fetch_stock_data(self, mock_get_data_for_ticker):
        """複数の証券コードのデータを取得する関数のテスト"""
        # モックデータの準備
        mock_df1 = pd.DataFrame({'Close': [2020.0]})
        mock_df2 = pd.DataFrame({'Close': [3500.0]})
        mock_get_data_for_ticker.side_effect = [mock_df1, mock_df2]
        
        # テスト実行
        result = fetch_stock_data(['7203.T', '9984.T'], '1m')
        
        # 結果確認
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0][0], '7203.T')
        self.assertEqual(result[1][0], '9984.T')
        pd.testing.assert_frame_equal(result[0][1], mock_df1)
        pd.testing.assert_frame_equal(result[1][1], mock_df2)
        self.assertEqual(mock_get_data_for_ticker.call_count, 2)

    @mock.patch('utils.stock_utils.get_stock_data_for_ticker')
    def test_fetch_stock_data_with_exception(self, mock_get_data_for_ticker):
        """データ取得時に例外が発生した場合のテスト"""
        # モックの設定
        mock_get_data_for_ticker.side_effect = [
            pd.DataFrame({'Close': [2020.0]}),
            Exception("API error")
        ]
        
        # テスト実行
        result = fetch_stock_data(['7203.T', '9984.T'], '1m')
        
        # 結果確認
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0][1], pd.DataFrame)
        self.assertIsNone(result[1][1])

    @mock.patch('utils.stock_utils.fetch_stock_data')
    @mock.patch('utils.stock_utils.get_stock_list')
    def test_get_stock_data_from_list(self, mock_get_list, mock_fetch_data):
        """指定された市場の全銘柄データを取得する関数のテスト"""
        # モックデータの準備
        mock_get_list.return_value = ['7203.T', '9984.T']
        mock_df1 = pd.DataFrame({'Close': [2020.0]})
        mock_df2 = pd.DataFrame({'Close': [3500.0]})
        mock_fetch_data.return_value = [('7203.T', mock_df1), ('9984.T', mock_df2)]
        
        # テスト実行
        result = get_stock_data_from_list('prime', '1m')
        
        # 結果確認
        self.assertIsInstance(result, dict)
        self.assertEqual(len(result), 2)
        self.assertIn('7203.T', result)
        self.assertIn('9984.T', result)
        mock_get_list.assert_called_once_with('prime')
        mock_fetch_data.assert_called_once_with(['7203.T', '9984.T'], '1m')

    @mock.patch('utils.stock_utils.get_stock_data_from_list')
    @mock.patch('utils.stock_utils.write_to_hdfs')
    @mock.patch('utils.stock_utils.WebHDFSHook')
    def test_process_stock_data(self, mock_hdfs_hook_class, mock_write_to_hdfs, mock_get_data):
        """株価データ処理の完全なフローのテスト"""
        # モックデータの準備
        mock_hdfs_hook = mock.Mock()
        mock_hdfs_hook_class.return_value = mock_hdfs_hook
        
        mock_df1 = pd.DataFrame({'Close': [2020.0]})
        mock_df2 = pd.DataFrame({'Close': [3500.0]})
        mock_get_data.return_value = {
            '7203.T': mock_df1,
            '9984.T': mock_df2
        }
        
        # テスト実行
        process_stock_data('hdfs_conn_test', 'prime', '1m')
        
        # 結果確認
        mock_hdfs_hook_class.assert_called_once_with(webhdfs_conn_id='hdfs_conn_test')
        mock_get_data.assert_called_once_with('prime', '1m')
        self.assertEqual(mock_write_to_hdfs.call_count, 2)  # 2銘柄分のデータ書き込み
        
    @mock.patch('utils.stock_utils.get_stock_data_from_list')
    @mock.patch('utils.stock_utils.write_to_hdfs')
    @mock.patch('utils.stock_utils.WebHDFSHook')
    def test_process_stock_data_empty(self, mock_hdfs_hook_class, mock_write_to_hdfs, mock_get_data):
        """一部のデータが空の場合の株価データ処理のテスト"""
        # モックデータの準備
        mock_hdfs_hook = mock.Mock()
        mock_hdfs_hook_class.return_value = mock_hdfs_hook
        
        mock_df = pd.DataFrame({'Close': [2020.0]})
        mock_get_data.return_value = {
            '7203.T': mock_df,
            '9984.T': None  # データなし
        }
        
        # テスト実行
        process_stock_data('hdfs_conn_test')
        
        # 結果確認
        mock_get_data.assert_called_once()
        # データがある銘柄に対してのみwrite_to_hdfsが呼ばれることを確認
        self.assertEqual(mock_write_to_hdfs.call_count, 1)

if __name__ == '__main__':
    unittest.main()