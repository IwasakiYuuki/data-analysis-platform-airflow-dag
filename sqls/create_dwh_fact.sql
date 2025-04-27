-- 市場データファクトテーブル
CREATE EXTERNAL TABLE IF NOT EXISTS default.fact_market_data (
  date_key INT,          -- dim_date への外部キー
  instrument_key BIGINT, -- dim_instrument への外部キー
  datetime TIMESTAMP,    -- 正確な日時 (時間足データなどの場合)
  open DOUBLE,
  high DOUBLE,
  low DOUBLE,
  close DOUBLE,
  volume BIGINT,
  -- データソースやロード日時などの監査情報を追加することも可能
  load_timestamp TIMESTAMP
)
PARTITIONED BY (          -- 日付によるパーティショニングでクエリ性能向上
  instrument_type STRING,
  year INT,
  month INT,
  day INT
)
STORED AS PARQUET        -- パフォーマンス向上のため Parquet 形式を推奨
LOCATION '/data/warehouse/fact_market_data/';
