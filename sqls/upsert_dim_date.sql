-- 実行週の前週月曜から金曜までの日付を dim_date に追加する
-- 既に存在する日付は無視する (冪等性)
INSERT INTO default.dim_date
SELECT
    CAST(date_format(d.date_col, 'yyyyMMdd') AS INT) AS date_key,
    d.date_col AS full_date,
    year(d.date_col) AS year,
    month(d.date_col) AS month,
    day(d.date_col) AS day,
    CASE date_format(d.date_col, 'E') -- Hive 1.2+
        WHEN 'Mon' THEN 1 WHEN 'Tue' THEN 2 WHEN 'Wed' THEN 3
        WHEN 'Thu' THEN 4 WHEN 'Fri' THEN 5 WHEN 'Sat' THEN 6
        WHEN 'Sun' THEN 7 ELSE NULL
    END AS day_of_week,
    date_format(d.date_col, 'EEEE') AS day_name, -- 例: Monday
    weekofyear(d.date_col) AS week_of_year,
    quarter(d.date_col) AS quarter,
    CASE WHEN date_format(d.date_col, 'E') IN ('Sat', 'Sun') THEN true ELSE false END AS is_weekend,
    false AS is_holiday -- 祝日判定は別途実装が必要
FROM (
    -- 前週の月曜から金曜までの日付を生成
    -- sequence 関数は Spark SQL で利用可能。Hive の場合は別の方法が必要になる場合がある
    -- ここではテンプレートで日付範囲を直接指定する
    SELECT DISTINCT to_date(date_str) AS date_col
    FROM default.raw_stock_data -- いずれかのRAWテーブルから日付を取得 (stockを代表として使用)
    WHERE to_date(date_str) >= date '{{ params.prev_week_monday }}'
      AND to_date(date_str) <= date '{{ params.prev_week_friday }}'
    UNION
    SELECT DISTINCT to_date(date_str) AS date_col
    FROM default.raw_forex_data
    WHERE to_date(date_str) >= date '{{ params.prev_week_monday }}'
      AND to_date(date_str) <= date '{{ params.prev_week_friday }}'
    UNION
    SELECT DISTINCT to_date(date_str) AS date_col
    FROM default.raw_index_data
    WHERE to_date(date_str) >= date '{{ params.prev_week_monday }}'
      AND to_date(date_str) <= date '{{ params.prev_week_friday }}'
) d
LEFT JOIN default.dim_date existing ON CAST(date_format(d.date_col, 'yyyyMMdd') AS INT) = existing.date_key
WHERE existing.date_key IS NULL -- 存在しない日付のみINSERT
