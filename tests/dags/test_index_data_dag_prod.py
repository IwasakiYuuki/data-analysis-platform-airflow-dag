import unittest
from unittest.mock import patch, MagicMock, ANY
import pandas as pd
import datetime
import tempfile

# テスト対象のモジュールをインポート
from dags import index_data_dag_prod


class TestIndexDataDagProd(unittest.TestCase):

    @patch('dags.index_data_dag_prod.yf.download')
    def test_get_index_data(self, mock_yf_download):
        # モックの設定 (MultiIndex カラムを模倣)
        mock_df = pd.DataFrame({('Close', '^GSPC'): [100, 101, 102]})
        mock_df.columns.names = ["", "Ticker"] # yfinanceが返す可能性のあるカラム構造
        mock_yf_download.return_value = mock_df

        start_date = datetime.date(2024, 1, 1)
        end_date = datetime.date(2024, 1, 3)
        ticker = '^GSPC'

        # テスト実行
        data = index_data_dag_prod.get_index_data(ticker=ticker, start_date=start_date, end_date=end_date, interval='1d')

        # アサーション
        mock_yf_download.assert_called_once_with(ticker, start=start_date, end=end_date, interval='1d')
        self.assertIsInstance(data, pd.DataFrame)
        if isinstance(data, pd.DataFrame):
            self.assertIn('Close', data.columns) # droplevelが機能したか
            self.assertIn('Ticker', data.columns) # Tickerカラムが追加されたか
        if data is not None:
            self.assertEqual(len(data), 3)
            self.assertEqual(data['Ticker'].iloc[0], ticker)

    @patch('dags.index_data_dag_prod.yf.download')
    def test_get_index_data_no_data(self, mock_yf_download):
        # データがない場合 (空のDataFrame)
        mock_yf_download.return_value = pd.DataFrame()
        start_date = datetime.date(2024, 1, 1)
        end_date = datetime.date(2024, 1, 3)
        ticker = '^DJI'

        data = index_data_dag_prod.get_index_data(ticker=ticker, start_date=start_date, end_date=end_date)
        self.assertIsNone(data)

    @patch('dags.index_data_dag_prod.get_index_data')
    def test_get_index_data_for_ticker(self, mock_get_index_data):
        # モックの設定
        mock_df = pd.DataFrame({'Close': [100, 101, 102], 'Ticker': ['^IXIC']*3})
        mock_get_index_data.return_value = mock_df
        ticker = '^IXIC'

        # テスト実行
        data = index_data_dag_prod.get_index_data_for_ticker(ticker=ticker)

        # アサーション
        self.assertIsInstance(data, pd.DataFrame)
        if data is not None:
            self.assertEqual(len(data), 3)
        # 呼び出し引数を検証 (日付は動的に計算されるためANYを使用)
        mock_get_index_data.assert_called_once_with(ticker, ANY, ANY, interval='1m')
        # 日付の妥当性を確認 (呼び出し引数から取得)
        args, kwargs = mock_get_index_data.call_args
        call_start_date = args[1]
        call_end_date = args[2]
        today = datetime.date.today()
        expected_start_date = today - datetime.timedelta(days=today.weekday() + 7)
        expected_end_date = today - datetime.timedelta(days=today.weekday() + 2)
        self.assertEqual(call_start_date, expected_start_date)
        self.assertEqual(call_end_date, expected_end_date)


    @patch('dags.index_data_dag_prod.get_index_data_for_ticker')
    @patch('concurrent.futures.ProcessPoolExecutor')
    def test_fetch_index_data(self, mock_executor, mock_get_index_data_for_ticker):
        # モックの設定
        mock_df_gspc = pd.DataFrame({'Close': [100], 'Ticker': ['^GSPC']})
        mock_df_dji = pd.DataFrame({'Close': [200], 'Ticker': ['^DJI']})
        # tickerに応じて異なるDataFrameを返すように設定
        mock_get_index_data_for_ticker.side_effect = lambda t: mock_df_gspc if t == '^GSPC' else mock_df_dji if t == '^DJI' else None

        # ProcessPoolExecutorのモック設定
        mock_future_gspc = MagicMock()
        mock_future_gspc.result.return_value = mock_df_gspc
        mock_future_dji = MagicMock()
        mock_future_dji.result.return_value = mock_df_dji

        # ProcessPoolExecutorのモック設定
        mock_executor_instance: MagicMock = mock_executor.return_value.__enter__.return_value

        # submit のモック設定: tickerに応じて対応するfutureを返す
        def submit_side_effect(_, ticker):
            if ticker == '^GSPC':
                return mock_future_gspc
            elif ticker == '^DJI':
                return mock_future_dji
            else:
                m = MagicMock()
                m.result.side_effect = ValueError(f"Unexpected ticker for submit mock: {ticker}")
                return m
        mock_executor_instance.submit.side_effect = submit_side_effect

        # concurrent.futures.as_completed のモック設定
        # submitが返すであろうfutureのリストを返すように設定
        with patch('concurrent.futures.as_completed', return_value=[mock_future_gspc, mock_future_dji]) as mock_as_completed:

            tickers = ['^GSPC', '^DJI']
            max_workers = 2

            # テスト実行
            results = index_data_dag_prod.fetch_index_data(tickers, max_workers)

            # as_completed に渡された引数 (futureのイテラブル) を検証
            futures_passed_to_as_completed = set(mock_as_completed.call_args[0][0])
            self.assertEqual(futures_passed_to_as_completed, {mock_future_gspc, mock_future_dji})


        # アサーション
        self.assertEqual(len(results), 2)
        # 結果の順序は保証されないため、内容を確認
        self.assertTrue(any(r == ("^GSPC", mock_df_gspc) for r in results))
        self.assertTrue(any(r == ("^DJI", mock_df_dji) for r in results))

        # ProcessPoolExecutorが適切なmax_workersで呼ばれたか
        mock_executor.assert_called_once_with(max_workers=max_workers)

        # submitが各tickerで呼ばれたか
        self.assertEqual(mock_executor_instance.submit.call_count, len(tickers))
        # 呼び出し時の関数とtickerを検証
        mock_executor_instance.submit.assert_any_call(index_data_dag_prod.get_index_data_for_ticker, '^GSPC')
        mock_executor_instance.submit.assert_any_call(index_data_dag_prod.get_index_data_for_ticker, '^DJI')


    @patch('dags.index_data_dag_prod.fetch_index_data')
    def test_get_index_data_from_list(self, mock_fetch_index_data):
        # モックの設定
        mock_df_gspc = pd.DataFrame({'Close': [100], 'Ticker': ['^GSPC']})
        mock_df_dji = pd.DataFrame({'Close': [200], 'Ticker': ['^DJI']})
        # fetch_index_dataが返す値を設定 (Noneを含むケースも考慮)
        mock_fetch_index_data.return_value = [
            ('^GSPC', mock_df_gspc),
            ('^DJI', mock_df_dji),
            ('^IXIC', None) # データ取得失敗ケース
        ]
        max_workers = 3

        # テスト実行
        index_data = index_data_dag_prod.get_index_data_from_list(max_workers=max_workers)

        # アサーション
        mock_fetch_index_data.assert_called_once_with(index_data_dag_prod.INDEX_TICKERS, max_workers)
        self.assertIsInstance(index_data, dict)
        self.assertEqual(len(index_data), 2) # Noneが除外されているか
        self.assertIn('^GSPC', index_data)
        self.assertIn('^DJI', index_data)
        self.assertNotIn('^IXIC', index_data)
        pd.testing.assert_frame_equal(index_data['^GSPC'], mock_df_gspc)
        pd.testing.assert_frame_equal(index_data['^DJI'], mock_df_dji)


    def test_write_index_data_to_tmp_file(self):
        # モックファイルオブジェクトの作成し、必要なメソッドを追加
        mock_file = MagicMock(spec=tempfile.NamedTemporaryFile)
        mock_file.tell = MagicMock(return_value=0) # tellメソッドをモックに追加
        mock_file.write = MagicMock() # writeメソッドをモックに追加

        ticker = '^GSPC'
        data = pd.DataFrame({'Close': [100, 101], 'Ticker': [ticker]*2}, index=pd.to_datetime(['2024-01-01', '2024-01-02']))

        # テスト実行
        index_data_dag_prod.write_index_data_to_tmp_file(ticker, data, mock_file)

        # アサーション
        # to_csvが正しい引数で呼ばれたか (ヘッダーあり)
        mock_file.write.assert_called() # to_csvは内部でwriteを呼ぶ
        # to_csvの呼び出しを直接モックするのは難しいので、writeが呼ばれたかで代替
        # data.to_csv(mock_file, index=True, header=True) # 本来はこの呼び出しを確認したい

        # tellが0以外の場合 (ヘッダーなし)
        mock_file.reset_mock()
        mock_file.tell.return_value = 100
        index_data_dag_prod.write_index_data_to_tmp_file(ticker, data, mock_file)
        # data.to_csv(mock_file, index=True, header=False) # 本来はこの呼び出しを確認したい
        mock_file.write.assert_called()

    def test_write_index_data_to_tmp_file_no_data(self):
        # モックファイルオブジェクトの作成し、必要なメソッドを追加
        mock_file = MagicMock(spec=tempfile.NamedTemporaryFile)
        mock_file.write = MagicMock() # writeメソッドをモックに追加
        ticker = '^DJI'
        data = None # データがない場合

        index_data_dag_prod.write_index_data_to_tmp_file(ticker, data, mock_file)  # type: ignore
        mock_file.write.assert_not_called() # 何も書き込まれないはず

        data = pd.DataFrame() # 空のDataFrameの場合
        index_data_dag_prod.write_index_data_to_tmp_file(ticker, data, mock_file)
        mock_file.write.assert_not_called()


    @patch('dags.index_data_dag_prod.get_index_data_from_list')
    @patch('dags.index_data_dag_prod.WebHDFSHook')
    @patch('dags.index_data_dag_prod.tempfile.NamedTemporaryFile')
    @patch('dags.index_data_dag_prod.write_index_data_to_tmp_file')
    @patch('dags.index_data_dag_prod.os.path.exists')
    @patch('dags.index_data_dag_prod.os.path.getsize')
    @patch('dags.index_data_dag_prod.os.remove')
    def test_process_index_data(self, mock_os_remove, mock_os_getsize, mock_os_exists, mock_write_tmp, mock_named_temp_file, mock_hdfs_hook, mock_get_list):
        # モックの設定
        hdfs_conn_id = "test_hdfs_conn"
        hdfs_path = "/test/hdfs/path"
        mock_df_gspc = pd.DataFrame({'Close': [100], 'Ticker': ['^GSPC']})
        mock_df_dji = pd.DataFrame({'Close': [200], 'Ticker': ['^DJI']})
        mock_get_list.return_value = {
            '^GSPC': mock_df_gspc,
            '^DJI': mock_df_dji
        }

        # HDFS Hookのモック
        mock_hdfs_hook_instance = MagicMock()
        mock_hdfs_hook.return_value = mock_hdfs_hook_instance

        # tempfileのモック
        mock_file = MagicMock()
        mock_file.name = "fake_temp_file.csv"
        mock_named_temp_file.return_value.__enter__.return_value = mock_file

        # os.path.exists と os.path.getsize のモック (ファイルが存在し、中身がある場合)
        mock_os_exists.return_value = True
        mock_os_getsize.return_value = 100 # 0より大きい値

        # テスト実行
        index_data_dag_prod.process_index_data(hdfs_conn_id=hdfs_conn_id, hdfs_path=hdfs_path)

        # アサーション
        # 1. データ取得関数が呼ばれたか
        mock_get_list.assert_called_once_with()

        # 2. HDFS Hookが初期化されたか
        mock_hdfs_hook.assert_called_once_with(webhdfs_conn_id=hdfs_conn_id)

        # 3. 一時ファイルが作成されたか
        mock_named_temp_file.assert_called_once_with(mode='w', delete=False, suffix=".csv")

        # 4. 各データが一時ファイルに書き込まれたか
        self.assertEqual(mock_write_tmp.call_count, 2)
        mock_write_tmp.assert_any_call('^GSPC', mock_df_gspc, mock_file)
        mock_write_tmp.assert_any_call('^DJI', mock_df_dji, mock_file)

        # 5. ファイルの存在とサイズがチェックされたか
        mock_os_exists.assert_called_once_with("fake_temp_file.csv")
        mock_os_getsize.assert_called_once_with("fake_temp_file.csv")

        # 6. HDFSにファイルがアップロードされたか
        today = datetime.date.today()
        start_date = today - datetime.timedelta(days=today.weekday() + 7)
        end_date = today - datetime.timedelta(days=today.weekday() + 3)
        expected_hdfs_filename = f"index_data_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
        expected_hdfs_filepath = f"{hdfs_path}/{expected_hdfs_filename}"
        mock_hdfs_hook_instance.load_file.assert_called_once_with(
            source="fake_temp_file.csv",
            destination=expected_hdfs_filepath,
            overwrite=True
        )

        # 7. 一時ファイルが削除されたか
        mock_os_remove.assert_called_once_with("fake_temp_file.csv")


    @patch('dags.index_data_dag_prod.get_index_data_from_list')
    @patch('dags.index_data_dag_prod.WebHDFSHook')
    @patch('dags.index_data_dag_prod.tempfile.NamedTemporaryFile')
    @patch('dags.index_data_dag_prod.write_index_data_to_tmp_file')
    @patch('dags.index_data_dag_prod.os.path.exists')
    @patch('dags.index_data_dag_prod.os.path.getsize')
    @patch('dags.index_data_dag_prod.os.remove')
    def test_process_index_data_no_data_fetched(self, mock_os_remove, mock_os_getsize, mock_os_exists, mock_write_tmp, mock_named_temp_file, mock_hdfs_hook, mock_get_list):
        # データが取得できなかった場合
        mock_get_list.return_value = {} # 空の辞書
        hdfs_conn_id = "test_hdfs_conn"
        hdfs_path = "/test/hdfs/path"

        index_data_dag_prod.process_index_data(hdfs_conn_id=hdfs_conn_id, hdfs_path=hdfs_path)

        mock_get_list.assert_called_once_with()
        mock_hdfs_hook.assert_not_called() # HDFSフックは呼ばれない
        mock_named_temp_file.assert_not_called() # 一時ファイルも作成されない
        mock_write_tmp.assert_not_called()
        mock_os_exists.assert_not_called()
        mock_os_getsize.assert_not_called()
        mock_os_remove.assert_not_called()


    @patch('dags.index_data_dag_prod.get_index_data_from_list')
    @patch('dags.index_data_dag_prod.WebHDFSHook')
    @patch('dags.index_data_dag_prod.tempfile.NamedTemporaryFile')
    @patch('dags.index_data_dag_prod.write_index_data_to_tmp_file')
    @patch('dags.index_data_dag_prod.os.path.exists')
    @patch('dags.index_data_dag_prod.os.path.getsize')
    @patch('dags.index_data_dag_prod.os.remove')
    def test_process_index_data_empty_temp_file(self, mock_os_remove, mock_os_getsize, mock_os_exists, mock_write_tmp, mock_named_temp_file, mock_hdfs_hook, mock_get_list):
        # 一時ファイルが空だった場合 (write_index_data_to_tmp_fileが何も書き込まなかった場合など)
        mock_get_list.return_value = {'^GSPC': pd.DataFrame()} # データはあるが空
        hdfs_conn_id = "test_hdfs_conn"
        hdfs_path = "/test/hdfs/path"
        mock_hdfs_hook_instance = MagicMock()
        mock_hdfs_hook.return_value = mock_hdfs_hook_instance
        mock_file = MagicMock()
        mock_file.name = "fake_empty_temp_file.csv"
        mock_named_temp_file.return_value.__enter__.return_value = mock_file
        mock_os_exists.return_value = True
        mock_os_getsize.return_value = 0 # ファイルサイズが0

        index_data_dag_prod.process_index_data(hdfs_conn_id=hdfs_conn_id, hdfs_path=hdfs_path)

        mock_get_list.assert_called_once_with()
        mock_hdfs_hook.assert_called_once_with(webhdfs_conn_id=hdfs_conn_id)
        mock_named_temp_file.assert_called_once()
        mock_write_tmp.assert_called_once() # 書き込みは試みられる (空データで)
        # os.path.exists は if 条件と else 内の削除前チェックで2回呼ばれる
        self.assertEqual(mock_os_exists.call_count, 2)
        mock_os_exists.assert_any_call("fake_empty_temp_file.csv")
        mock_os_getsize.assert_called_once_with("fake_empty_temp_file.csv")
        mock_hdfs_hook_instance.load_file.assert_not_called() # HDFSアップロードはされない
        mock_os_remove.assert_called_once_with("fake_empty_temp_file.csv") # 空でも削除はされる


if __name__ == '__main__':
    unittest.main()
