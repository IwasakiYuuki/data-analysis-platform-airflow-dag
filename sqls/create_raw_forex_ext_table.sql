CREATE EXTERNAL TABLE IF NOT EXISTS default.raw_forex_data (
  -- CSVファイルに含まれるカラム定義
  datetime_str STRING, -- Datetime カラムを文字列として読み込む
  close DOUBLE,
  high DOUBLE,
  low DOUBLE,
  open DOUBLE,
  volume BIGINT,
  symbol STRING,
  date_str STRING
)
PARTITIONED BY (
  year STRING,
  month STRING,
  day STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' -- CSV用のSerDeを使用
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar'     = '"',
  'escapeChar'    = '\\',
  'skip.header.line.count'='1' -- ヘッダー行をスキップする設定
)
STORED AS TEXTFILE
LOCATION '/data/lake/yfinance/forex/price/main/';
