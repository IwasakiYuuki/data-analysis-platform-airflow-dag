INSERT OVERWRITE TABLE default.fact_market_data
PARTITION (instrument_type, year, month, day)
SELECT
    dd.date_key,
    di.instrument_key,
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
    di.instrument_type,
    CAST(raw.year AS INT) AS year,
    CAST(raw.month AS INT) AS month,
    CAST(raw.day AS INT) AS day
FROM
    default.raw_forex_data raw
JOIN
    default.dim_instrument di ON raw.symbol = di.symbol AND di.instrument_type = 'forex'
JOIN
    default.dim_date dd ON to_date(raw.date_str) = dd.full_date
WHERE
    to_date(raw.date_str) >= date '{{ params.prev_week_monday }}'
    AND to_date(raw.date_str) <= date '{{ params.prev_week_friday }}'
