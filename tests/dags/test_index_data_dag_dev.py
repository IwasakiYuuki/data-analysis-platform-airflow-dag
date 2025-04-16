import pytest
import unittest
from unittest.mock import patch
import pandas as pd
import datetime

# テスト対象のモジュールをインポート
from dags import index_data_dag_dev


class TestIndexDataDagDev(unittest.TestCase):

    @patch('dags.index_data_dag_dev.yf.download')
    def test_get_index_data(self, mock_yf_download):
        mock_df = pd.DataFrame({('Close', '^N225'): [200, 201, 202]})
        mock_df.columns.names = ["", "Ticker"]
        mock_yf_download.return_value = mock_df
        start_date = datetime.date(2024, 1, 1)
        end_date = datetime.date(2024, 1, 2)

        data = index_data_dag_dev.get_index_data(ticker='^N225', start_date=start_date, end_date=end_date, interval='1d')

        self.assertIsInstance(data, pd.DataFrame)
        if data is not None:
            self.assertEqual(len(data), 3)
        mock_yf_download.assert_called_once()

    @patch('dags.index_data_dag_dev.get_index_data')
    def test_get_index_data_for_ticker(self, mock_get_index_data):
        mock_df = pd.DataFrame({'Close': [200, 201, 202]})
        mock_get_index_data.return_value = mock_df

        data = index_data_dag_dev.get_index_data_for_ticker(ticker='^N225')

        self.assertIsInstance(data, pd.DataFrame)
        if data is not None:
            self.assertEqual(len(data), 3)
        mock_get_index_data.assert_called_once()

    @patch('dags.index_data_dag_dev.get_index_data_for_ticker')
    def test_get_index_data_from_list(self, mock_get_index_data_for_ticker):
        mock_get_index_data_for_ticker.return_value = pd.DataFrame({'Close': [200, 201, 202]})

        index_data = index_data_dag_dev.get_index_data_from_list()

        self.assertIsInstance(index_data, dict)
        self.assertEqual(
            mock_get_index_data_for_ticker.call_count,
            len(index_data_dag_dev.INDEX_TICKERS)
        )

    @patch('dags.index_data_dag_dev.get_index_data_from_list')
    def test_process_index_data(self, mock_get_index_data_from_list):
        mock_df = pd.DataFrame({'Close': [200, 201, 202]}, index=pd.Index([0, 1, 2]))
        mock_get_index_data_from_list.return_value = {'^N225': mock_df}

        index_data_dag_dev.process_index_data()
        stdout = self.capfd.readouterr().out
        self.assertIn("Index: ^N225", stdout)

        mock_get_index_data_from_list.assert_called_once()

    @pytest.fixture(autouse=True)
    def set_capfd(self, capfd: pytest.CaptureFixture):
        self.capfd = capfd


if __name__ == '__main__':
    unittest.main()
