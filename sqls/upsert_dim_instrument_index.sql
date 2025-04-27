-- raw_index_data から新しい銘柄を dim_instrument に追加する
INSERT INTO default.dim_instrument
SELECT
    hash(i.symbol) AS instrument_key,
    i.symbol,
    NULL AS name,
    'index' AS instrument_type,
    'index' AS market,
    -- 指数によって通貨を決定 (例: ^N225 -> JPY, ^GSPC -> USD)
    CASE
        WHEN i.symbol IN ('^N225', '^TPX') THEN 'JPY' -- 仮
        WHEN i.symbol IN ('^GSPC', '^DJI', '^IXIC') THEN 'USD' -- 仮
        ELSE NULL
    END AS currency,
    -- 指数によって取引所を決定
    CASE
        WHEN i.symbol IN ('^N225', '^TPX') THEN 'TSE' -- 仮
        WHEN i.symbol IN ('^GSPC', '^DJI', '^IXIC') THEN 'NYSE/NASDAQ' -- 仮
        ELSE NULL
    END AS `exchange`,
    NULL AS effective_start_date,
    NULL AS effective_end_date,
    NULL AS is_current
FROM (
    SELECT DISTINCT symbol
    FROM default.raw_index_data
    WHERE to_date(date_str) >= date '{{ params.prev_week_monday }}'
      AND to_date(date_str) <= date '{{ params.prev_week_friday }}'
) i
LEFT JOIN default.dim_instrument existing ON i.symbol = existing.symbol
WHERE existing.symbol IS NULL
