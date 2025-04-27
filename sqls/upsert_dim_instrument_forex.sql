-- raw_forex_data から新しい銘柄を dim_instrument に追加する
INSERT INTO default.dim_instrument
SELECT
    hash(f.symbol) AS instrument_key,
    f.symbol,
    NULL AS name,
    'forex' AS instrument_type,
    'forex' AS market,
    -- 通貨ペアから通貨を推測 (例: EURUSD=X -> USD)
    CASE
        WHEN f.symbol LIKE '%USD=X' THEN 'USD'
        WHEN f.symbol LIKE '%JPY=X' THEN 'JPY'
        WHEN f.symbol LIKE '%EUR=X' THEN 'EUR'
        WHEN f.symbol LIKE '%GBP=X' THEN 'GBP'
        ELSE NULL
    END AS currency,
    'FX' AS `exchange`,
    NULL AS effective_start_date,
    NULL AS effective_end_date,
    NULL AS is_current
FROM (
    SELECT DISTINCT symbol
    FROM default.raw_forex_data
    WHERE to_date(date_str) >= date '{{ params.prev_week_monday }}'
      AND to_date(date_str) <= date '{{ params.prev_week_friday }}'
) f
LEFT JOIN default.dim_instrument existing ON f.symbol = existing.symbol
WHERE existing.symbol IS NULL
