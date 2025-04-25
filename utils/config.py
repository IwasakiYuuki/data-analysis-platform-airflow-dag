"""
データ処理に関する共通の設定や定数を定義するモジュール
"""

# JPX市場のカラム名定義
MARKET_COLUMN_NAMES = {
    "prime": "プライム（内国株式）",
    "standard": "スタンダード（内国株式）",
    "growth": "グロース（内国株式）",
    "eft": "ETF・ETN",
}

# データソースのURL
JPX_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"

# 為替データの設定
FOREX_PAIRS = [
    "EURUSD=X",
    "JPY=X",
    "GBPUSD=X",
    "AUDUSD=X",
    "NZDUSD=X",
    "EURJPY=X",
    "GBPJPY=X",
    "EURGBP=X",
    "EURCAD=X",
    "EURSEK=X",
    "EURCHF=X",
    "EURHUF=X",
    "CNY=X",
    "HKD=X",
    "SGD=X",
    "INR=X",
    "MXN=X",
    "PHP=X",
    "IDR=X",
    "THB=X",
    "MYR=X",
    "ZAR=X",
    "RUB=X",
]

# 指数データの設定
INDEX_SYMBOLS = [
    "^GSPC",  # S&P500
    "^DJI",   # NYダウ
    "^IXIC", # NASDAQ
    "^NYA", # NYSE総合
    "^XAX", # AMEX総合
    "^BUK1000", # ブルームバーグ米国株式
    "^RUT", # ラッセル2000
    "^VIX", # VIX指数
    "^FTSE", # FTSE100
    "^GDAXI", # DAX
    "^FCHI", # CAC40
    "^STOXX50E", # EURO STOXX 50
    "^N100", # EURO STOXX 100
    "^BFX", # BEL 20
    "MOEX.ME", # MOEX
    "^HSI", # HSI
    "^STI", # STI
    "^AXJO", # ASX 200
    "^AORD", # S&P/ASX 200
    "^BSESN", # BSE SENSEX
    "^JKSE", # IDX Composite
    "^KLSE", # FTSE Bursa Malaysia KLCI
    "^NZ50", # S&P/NZX 50 Index
    "^KS11", # KOSPI Composite Index
    "^TWII", # TSEC Capitalization Weighted Stock Index
    "^GSPTSE", # TSEC Taiwan Weighted Index
    "^BVSP", # Bovespa Index
    "^MXX", # IPC Mexico
    "^IPSA", # IPC Mexico
    "^MERV", # MERVAL
    "^TA125.TA", # TA-125 Index
    "^CASE30", # EGX 30 Prise Return Index
    "^JNOU.JO", # Top 40 USD Net Total Return Index
    "DX-Y.NYB", # US Dollar Index
    "^125904-USD-STRD", # MSCI EUROPE
    "^XDB", # British Pound Currency Index
    "^XDE", # Euro Currency Index
    "000001.SS", # SSE Composite Index
    "^N225", # Nikkei 225
    "^XDN", # Japanese Yen Currency Index
    "^XDA", # Australian Dollar Currency Index
]

# HDFSのパス設定
HDFS_PATHS = {
    "stock": {
        "prime": "/data/lake/yfinance/stock/price/jp_prime",
        "standard": "/data/lake/yfinance/stock/price/jp_standard",
        "growth": "/data/lake/yfinance/stock/price/jp_growth",
        "etf": "/data/lake/yfinance/stock/price/jp_etf",
    },
    "forex": "/data/lake/yfinance/forex/price/main",
    "index": "/data/lake/yfinance/index/value/main",
}

# データ取得間隔の設定
DEFAULT_REQUEST_DELAY = 1  # 秒