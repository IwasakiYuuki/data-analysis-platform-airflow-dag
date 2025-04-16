import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import datetime

# テスト対象のモジュールをインポート
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import datetime

# テスト対象のモジュールをインポート
from dags import forex_data_dag_prod


class TestForexDataDagProd(unittest.TestCase):
    @patch('dags.forex_data_dag_prod.yf.download')
    def test_get_forex_data(self, mock_yf_download):
        mock_df = pd.DataFrame({('Close', 'EURUSD=X'): [100, 101, 102]})
        mock_df.columns.names = ["", "Ticker"]
        mock_yf_download.return_value = mock_df
        start_date = datetime.date(2024, 1, 1)
        end_date = datetime.date(2024, 1, 2)
        data = forex_data_dag_prod.get_forex_data(currency_pair='EURUSD=X', start_date=start_date, end_date=end_date, interval='1d')
        self.assertIsInstance(data, pd.DataFrame)
        if data is not None:
            self.assertEqual(len(data), 3)
        mock_yf_download.assert_called_once()

    @patch('dags.forex_data_dag_prod.get_forex_data')
    def test_get_forex_data_for_pair(self, mock_get_forex_data):
        mock_df = pd.DataFrame({'Close': [100, 101, 102]})
        mock_get_forex_data.return_value = mock_df
        data = forex_data_dag_prod.get_forex_data_for_pair(currency_pair='EURUSD=X')
        self.assertIsInstance(data, pd.DataFrame)
        if data is not None:
            self.assertEqual(len(data), 3)
        mock_get_forex_data.assert_called_once()

    @patch('dags.forex_data_dag_prod.get_forex_data_for_pair')
    def test_fetch_forex_data(self, mock_get_forex_data_for_pair):
        mock_df = pd.DataFrame({'Close': [100, 101, 102]})
        mock_get_forex_data_for_pair.return_value = mock_df
        currency_pairs = ['EURUSD=X', 'GBPUSD=X']
        forex_data_results = [(pair, mock_get_forex_data_for_pair(pair)) for pair in currency_pairs]
        self.assertEqual(len(forex_data_results), 2)
        self.assertIsInstance(forex_data_results[0][1], pd.DataFrame)
        self.assertEqual(mock_get_forex_data_for_pair.call_count, 2)

    @patch('dags.forex_data_dag_prod.fetch_forex_data')
    def test_get_forex_data_from_list(self, mock_fetch_forex_data):
        mock_fetch_forex_data.return_value = [('EURUSD=X', pd.DataFrame({'Close': [100, 101, 102]})), ('GBPUSD=X', pd.DataFrame({'Close': [500, 501, 502]}))]
        forex_data = forex_data_dag_prod.get_forex_data_from_list(max_workers=2)
        self.assertIsInstance(forex_data, dict)
        self.assertEqual(len(forex_data), 2)
        mock_fetch_forex_data.assert_called_once()

    @patch('dags.forex_data_dag_prod.WebHDFSHook')
    @patch('dags.forex_data_dag_prod.tempfile.NamedTemporaryFile')
    @patch('dags.forex_data_dag_prod.os.remove')
    def test_process_forex_data(self, mock_os_remove, mock_named_temp_file, mock_hdfs_hook):
        mock_hdfs_hook_instance = MagicMock()
        mock_hdfs_hook.return_value = mock_hdfs_hook_instance
        mock_file = MagicMock()
        mock_named_temp_file.return_value.__enter__.return_value = mock_file
        mock_file.name = "temp_file.csv"
        mock_df = pd.DataFrame({'Close': [100, 101, 102]})
        mock_get_forex_data_from_list = MagicMock(return_value={"EURUSD=X": mock_df})
        today = datetime.date.today()
        start_date = today - datetime.timedelta(days=today.weekday() + 7)
        end_date = today - datetime.timedelta(days=today.weekday() + 3)

        with patch('dags.forex_data_dag_prod.get_forex_data_from_list', mock_get_forex_data_from_list):
            forex_data_dag_prod.process_forex_data(
                hdfs_conn_id="test_hdfs",
                hdfs_path="/test_path",
            )

        mock_hdfs_hook.assert_called_once_with(webhdfs_conn_id="test_hdfs")
        mock_named_temp_file.assert_called_once()
        mock_hdfs_hook_instance.load_file.assert_called_once_with(
            source="temp_file.csv",
            destination=f"/test_path/forex_data_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv",
            overwrite=True
        )
        mock_os_remove.assert_called_once_with("temp_file.csv")

if __name__ == '__main__':
    unittest.main()
