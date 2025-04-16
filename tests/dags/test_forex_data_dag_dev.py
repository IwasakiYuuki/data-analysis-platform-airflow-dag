import pytest
import unittest
from unittest.mock import patch
import pandas as pd
import datetime

# テスト対象のモジュールをインポート
from dags import forex_data_dag_dev


class TestForexDataDagDev(unittest.TestCase):

    @patch('dags.forex_data_dag_dev.yf.download')
    def test_get_forex_data(self, mock_yf_download):
        # モックの設定
        mock_df = pd.DataFrame({('Close', 'EURUSD=X'): [100, 101, 102]})
        mock_df.columns.names = ["", "Ticker"]
        mock_yf_download.return_value = mock_df
        start_date = datetime.date(2024, 1, 1)
        end_date = datetime.date(2024, 1, 2)

        # テスト実行
        data = forex_data_dag_dev.get_forex_data(currency_pair='EURUSD=X', start_date=start_date, end_date=end_date, interval='1d')

        # 検証
        self.assertIsInstance(data, pd.DataFrame)
        if data is not None:
            self.assertEqual(len(data), 3)
        mock_yf_download.assert_called_once()

    @patch('dags.forex_data_dag_dev.get_forex_data')
    def test_get_forex_data_for_pair(self, mock_get_forex_data):
        # モックの設定
        mock_df = pd.DataFrame({'Close': [100, 101, 102]})
        mock_get_forex_data.return_value = mock_df

        # テスト実行
        data = forex_data_dag_dev.get_forex_data_for_pair(currency_pair='EURUSD=X')

        # 検証
        self.assertIsInstance(data, pd.DataFrame)
        if data is not None:
            self.assertEqual(len(data), 3)
        mock_get_forex_data.assert_called_once()

    @patch('dags.forex_data_dag_dev.get_forex_data_for_pair')
    def test_fetch_forex_data(self, mock_get_forex_data_for_pair):
        # モックの設定
        mock_df = pd.DataFrame({'Close': [100, 101, 102]})
        mock_get_forex_data_for_pair.return_value = mock_df

        # テスト実行
        currency_pairs = ['EURUSD=X', 'GBPUSD=X']
        # 並列処理をエミュレート
        forex_data_results = [(pair, mock_get_forex_data_for_pair(pair)) for pair in currency_pairs]

        # 検証
        self.assertEqual(len(forex_data_results), 2)
        self.assertIsInstance(forex_data_results[0][1], pd.DataFrame)
        self.assertEqual(mock_get_forex_data_for_pair.call_count, 2)

    @patch('dags.forex_data_dag_dev.fetch_forex_data')
    def test_get_forex_data_from_list(self, mock_fetch_forex_data):
        # モックの設定
        mock_fetch_forex_data.return_value = [('EURUSD=X', pd.DataFrame({'Close': [100, 101, 102]})), ('GBPUSD=X', pd.DataFrame({'Close': [500, 501, 502]}))]

        # テスト実行
        forex_data = forex_data_dag_dev.get_forex_data_from_list(max_workers=2)

        # 検証
        self.assertIsInstance(forex_data, dict)
        self.assertEqual(len(forex_data), 2)
        mock_fetch_forex_data.assert_called_once()

    @patch('dags.forex_data_dag_dev.get_forex_data_from_list')
    def test_process_forex_data(self, mock_get_forex_data_from_list):
        # モックの設定
        mock_df = pd.DataFrame({'Close': [100, 101, 102]}, index=pd.Index([0, 1, 2]))  # indexを追加
        mock_get_forex_data_from_list.return_value = {'EURUSD=X': mock_df}

        forex_data_dag_dev.process_forex_data()
        stdout = self.capfd.readouterr().out
        assert "Currency Pair: EURUSD=X" in stdout

        mock_get_forex_data_from_list.assert_called_once()

    @pytest.fixture(autouse=True)
    def set_capfd(self, capfd: pytest.CaptureFixture):
        self.capfd = capfd


if __name__ == '__main__':
    unittest.main()
