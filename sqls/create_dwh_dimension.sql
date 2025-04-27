-- 日付ディメンションテーブル
CREATE EXTERNAL TABLE IF NOT EXISTS default.dim_date (
  date_key INT,          -- 代理キー (例: YYYYMMDD 形式)
  full_date DATE,        -- 日付 (YYYY-MM-DD 形式)
  year INT,
  month INT,
  day INT,
  day_of_week INT,       -- 曜日 (例: 1=月曜日, 7=日曜日)
  day_name STRING,       -- 曜日名 (例: 'Monday')
  week_of_year INT,
  quarter INT,
  is_weekend BOOLEAN,
  is_holiday BOOLEAN     -- 祝日フラグ (必要に応じて後で更新)
)
STORED AS PARQUET        -- パフォーマンス向上のため Parquet 形式を推奨
LOCATION '/data/warehouse/dim_date/';

-- 銘柄ディメンションテーブル
CREATE EXTERNAL TABLE IF NOT EXISTS default.dim_instrument (
  instrument_key BIGINT, -- 代理キー (シーケンスやハッシュ関数で生成)
  symbol STRING,         -- 銘柄シンボル (例: '1301.T', 'EURUSD=X', '^N225')
  name STRING,           -- 銘柄名 (例: '極洋', 'EUR/USD', 'Nikkei 225') - 必要に応じて追加
  instrument_type STRING,-- 銘柄種別 ('stock', 'forex', 'index')
  market STRING,         -- 市場区分 (例: 'prime', 'standard', 'growth', 'etf', 'forex', 'index')
  currency STRING,       -- 通貨 (例: 'JPY', 'USD') - 必要に応じて追加
  `exchange` STRING,       -- 取引所 (例: 'TSE', 'FX', 'OSA') - 必要に応じて追加
  effective_start_date DATE, -- 有効開始日 (SCD Type 2 を実装する場合)
  effective_end_date DATE,   -- 有効終了日 (SCD Type 2 を実装する場合)
  is_current BOOLEAN     -- 現在有効なレコードか (SCD Type 2 を実装する場合)
)
STORED AS PARQUET        -- パフォーマンス向上のため Parquet 形式を推奨
LOCATION '/data/warehouse/dim_instrument/';
