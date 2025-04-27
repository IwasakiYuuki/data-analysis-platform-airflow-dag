-- 前週月曜〜金曜の株式データを fact_market_data に挿入（パーティション上書き）
INSERT OVERWRITE TABLE default.fact_market_data
PARTITION (instrument_type, year, month, day) -- 動的パーティションカラムを指定
SELECT
    -- fact_market_data のカラム
    dd.date_key,
    di.instrument_key,
    -- rawテーブルの datetime_str は様々な形式がありうるため、適切な形式に変換する
    -- 例: 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DDTHH:MM:SS...'
    -- ここでは to_timestamp が解釈できる標準的な形式を想定
    -- 必要であれば regexp_replace などで前処理する
    CASE
        WHEN raw.datetime_str LIKE '% %'
        THEN FROM_UNIXTIME(UNIX_TIMESTAMP(raw.datetime_str, 'yyyy-MM-dd HH:mm:ss'))
        ELSE NULL
    END AS datetime,
    raw.open,
    raw.high,
    raw.low,
    raw.close,
    raw.volume,
    current_timestamp() AS load_timestamp,

    -- 動的パーティション用のカラム (SELECTリストの最後に配置)
    di.instrument_type,
    CAST(raw.year AS INT) AS year,
    CAST(raw.month AS INT) AS month,
    CAST(raw.day AS INT) AS day
FROM
    default.raw_stock_data raw
JOIN
    default.dim_instrument di ON raw.symbol = di.symbol AND di.instrument_type = 'stock'
JOIN
    default.dim_date dd ON to_date(raw.date_str) = dd.full_date
WHERE
    -- 前週の月曜日から金曜日の範囲で raw テーブルのデータをフィルタリング
    to_date(raw.date_str) >= date '{{ params.prev_week_monday }}'
    AND to_date(raw.date_str) <= date '{{ params.prev_week_friday }}'
