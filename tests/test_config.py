import unittest
from utils.config import (
    MARKET_COLUMN_NAMES, JPX_URL, 
    FOREX_PAIRS, INDEX_SYMBOLS, 
    HDFS_PATHS, DEFAULT_REQUEST_DELAY
)

class TestConfig(unittest.TestCase):
    
    def test_market_column_names(self):
        """市場カラム名の設定が正しいことを確認"""
        self.assertIsInstance(MARKET_COLUMN_NAMES, dict)
        self.assertIn("prime", MARKET_COLUMN_NAMES)
        self.assertIn("standard", MARKET_COLUMN_NAMES)
        self.assertIn("growth", MARKET_COLUMN_NAMES)
        self.assertIn("eft", MARKET_COLUMN_NAMES)
        self.assertEqual(MARKET_COLUMN_NAMES["prime"], "プライム（内国株式）")
        
    def test_jpx_url(self):
        """JPX URLが正しい形式であることを確認"""
        self.assertIsInstance(JPX_URL, str)
        self.assertTrue(JPX_URL.startswith("https://"))
        self.assertTrue(JPX_URL.endswith(".xls"))
        
    def test_forex_pairs(self):
        """為替ペアのリストが正しく設定されていることを確認"""
        self.assertIsInstance(FOREX_PAIRS, list)
        self.assertGreater(len(FOREX_PAIRS), 0)
        # 代表的な通貨ペアがリストに含まれているか検証
        self.assertIn("JPY=X", FOREX_PAIRS)
        self.assertIn("EURUSD=X", FOREX_PAIRS)
        
    def test_index_symbols(self):
        """指数シンボルのリストが正しく設定されていることを確認"""
        self.assertIsInstance(INDEX_SYMBOLS, list)
        self.assertGreater(len(INDEX_SYMBOLS), 0)
        # 代表的な指数がリストに含まれているか検証
        self.assertIn("^N225", INDEX_SYMBOLS)  # 日経225
        self.assertIn("^GSPC", INDEX_SYMBOLS)  # S&P500
        
    def test_hdfs_paths(self):
        """HDFSパス設定が正しい形式であることを確認"""
        self.assertIsInstance(HDFS_PATHS, dict)
        # 株式データのパス設定
        self.assertIn("stock", HDFS_PATHS)
        self.assertIsInstance(HDFS_PATHS["stock"], dict)
        self.assertIn("prime", HDFS_PATHS["stock"])
        # 為替データのパス設定
        self.assertIn("forex", HDFS_PATHS)
        self.assertIsInstance(HDFS_PATHS["forex"], str)
        # 指数データのパス設定
        self.assertIn("index", HDFS_PATHS)
        self.assertIsInstance(HDFS_PATHS["index"], str)
        
    def test_default_request_delay(self):
        """デフォルトのリクエスト間隔が正しく設定されていることを確認"""
        self.assertIsInstance(DEFAULT_REQUEST_DELAY, int)
        self.assertGreaterEqual(DEFAULT_REQUEST_DELAY, 0)
        
if __name__ == '__main__':
    unittest.main()