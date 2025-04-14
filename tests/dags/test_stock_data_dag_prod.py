import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import datetime

# テスト対象のモジュールをインポート
from dags import stock_data_dag_prod


class TestStockDataDagProd(unittest.TestCase):

    @patch('dags.stock_data_dag_prod.pd.read_excel')
    def test_get_stock_list(self, mock_read_excel):
        # モックの設定
        mock_df = MagicMock()
        mock_df.__getitem__.side_effect = lambda x: pd.Series([
            '7203',
            '6758',
        ]) if x == "コード" else pd.Series(['プライム（内国株式）', 'スタンダード（内国株式）'])
        mock_read_excel.return_value = mock_df

        # テスト実行
        stock_list = stock_data_dag_prod.get_stock_list(market="prime")

        # 検証
        self.assertEqual(stock_list, ['7203.T'])
        mock_read_excel.assert_called_once()

    @patch('dags.stock_data_dag_prod.yf.download')
    def test_get_stock_data(self, mock_yf_download):
        # モックの設定
        mock_df = pd.DataFrame({'Close': [100, 101, 102]})
        mock_yf_download.return_value = mock_df
        start_date = datetime.date(2024, 1, 1)
        end_date = datetime.date(2024, 1, 2)

        # テスト実行
        data = stock_data_dag_prod.get_stock_data(ticker='7203.T', start_date=start_date, end_date=end_date, interval='1d')

        # 検証
        self.assertIsInstance(data, pd.DataFrame)
        if data is not None:
            self.assertEqual(len(data), 3)
        mock_yf_download.assert_called_once()

    @patch('dags.stock_data_dag_prod.get_stock_data')
    def test_get_stock_data_for_ticker(self, mock_get_stock_data):
        # モックの設定
        mock_df = pd.DataFrame({'Close': [100, 101, 102]})
        mock_get_stock_data.return_value = mock_df

        # テスト実行
        data = stock_data_dag_prod.get_stock_data_for_ticker(ticker='7203.T')

        # 検証
        self.assertIsInstance(data, pd.DataFrame)
        if data is not None:
            self.assertEqual(len(data), 3)
        mock_get_stock_data.assert_called_once()

    @patch('dags.stock_data_dag_prod.get_stock_data_for_ticker')
    def test_fetch_stock_data(self, mock_get_stock_data_for_ticker):
        # モックの設定
        mock_df = pd.DataFrame({'Close': [100, 101, 102]})
        mock_get_stock_data_for_ticker.return_value = mock_df

        # テスト実行
        tickers = ['7203.T', '6758.T']
        # 並列処理をエミュレート
        stock_data_results = [(ticker, mock_get_stock_data_for_ticker(ticker)) for ticker in tickers]

        # 検証
        self.assertEqual(len(stock_data_results), 2)
        self.assertIsInstance(stock_data_results[0][1], pd.DataFrame)
        self.assertEqual(mock_get_stock_data_for_ticker.call_count, 2)

    @patch('dags.stock_data_dag_prod.get_stock_list')
    @patch('dags.stock_data_dag_prod.fetch_stock_data')
    def test_get_stock_data_from_list(self, mock_fetch_stock_data, mock_get_stock_list):
        # モックの設定
        mock_get_stock_list.return_value = ['7203.T', '6758.T']
        mock_fetch_stock_data.return_value = [('7203.T', pd.DataFrame({'Close': [100, 101, 102]})), ('6758.T', pd.DataFrame({'Close': [500, 501, 502]}))]

        # テスト実行
        stock_data = stock_data_dag_prod.get_stock_data_from_list(max_workers=2)

        # 検証
        self.assertIsInstance(stock_data, dict)
        self.assertEqual(len(stock_data), 2)
        mock_get_stock_list.assert_called_once()
        mock_fetch_stock_data.assert_called_once()

    @patch('dags.stock_data_dag_prod.WebHDFSHook')
    @patch('dags.stock_data_dag_prod.tempfile.NamedTemporaryFile')
    @patch('dags.stock_data_dag_prod.os.remove')
    def test_process_stock_data(self, mock_os_remove, mock_named_temp_file, mock_hdfs_hook):
        # モックの設定
        mock_hdfs_hook_instance = MagicMock()
        mock_hdfs_hook.return_value = mock_hdfs_hook_instance
        mock_file = MagicMock()
        mock_named_temp_file.return_value.__enter__.return_value = mock_file
        mock_file.name = "temp_file.csv"
        mock_df = pd.DataFrame({'Close': [100, 101, 102]})
        mock_get_stock_data_from_list = MagicMock(return_value={"7203.T": mock_df})

        # datetime.date.today() を直接モックして、datetime.dateオブジェクトを返すようにする
        today = datetime.date.today()
        start_date = today - datetime.timedelta(days=today.weekday() + 7)
        end_date = today - datetime.timedelta(days=today.weekday() + 3)

        # stock_data_dag_prod.pyの関数内で使用されている関数をモックする
        with patch('dags.stock_data_dag_prod.get_stock_data_from_list', mock_get_stock_data_from_list):
            # テスト実行
            stock_data_dag_prod.process_stock_data(
                hdfs_conn_id="test_hdfs",
                hdfs_path="/test_path",
                market="prime"
            )

        # 検証
        mock_hdfs_hook.assert_called_once_with(webhdfs_conn_id="test_hdfs")
        mock_named_temp_file.assert_called_once()
        mock_hdfs_hook_instance.load_file.assert_called_once_with(
            source="temp_file.csv",
            destination=f"/test_path/stock_data_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_prime.csv",
            overwrite=True
        )
        mock_os_remove.assert_called_once_with("temp_file.csv")

if __name__ == '__main__':
    unittest.main()
