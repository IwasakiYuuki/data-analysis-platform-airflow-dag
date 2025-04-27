-- raw_stock_data から新しい銘柄を dim_instrument に追加する
-- 既に存在する銘柄は無視する (冪等性)
-- instrument_key は symbol のハッシュ値を使用 (衝突の可能性に注意)
INSERT INTO default.dim_instrument
SELECT
    hash(s.symbol) AS instrument_key, -- hash関数はHive/Sparkで利用可能
    s.symbol,
    NULL AS name, -- 名称は別途取得・更新が必要
    'stock' AS instrument_type,
    CASE -- 市場区分を判定 (rawテーブルに市場情報がないためsymbolから推測、または固定値)
        WHEN s.symbol LIKE '%.T' THEN 'prime' -- 仮: .Tならプライム (要見直し)
        ELSE 'unknown' -- 必要に応じて修正
    END AS market,
    'JPY' AS currency, -- 仮: 日本株はJPY (要見直し)
    'TSE' AS `exchange`, -- 仮: .Tなら東証 (要見直し)
    NULL AS effective_start_date,
    NULL AS effective_end_date,
    NULL AS is_current
FROM (
    -- 期間内のユニークなシンボルを取得
    SELECT DISTINCT symbol
    FROM default.raw_stock_data
    WHERE to_date(date_str) >= date '{{ params.prev_week_monday }}'
      AND to_date(date_str) <= date '{{ params.prev_week_friday }}'
) s
LEFT JOIN default.dim_instrument existing ON s.symbol = existing.symbol
WHERE existing.symbol IS NULL -- 存在しないシンボルのみINSERT
