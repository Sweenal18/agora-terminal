

SELECT
    symbol,
    date::DATE           AS trade_date,
    open::DOUBLE         AS open,
    high::DOUBLE         AS high,
    low::DOUBLE          AS low,
    close::DOUBLE        AS close,
    volume::BIGINT       AS volume,
    vwap::DOUBLE         AS vwap,
    trade_count::INTEGER AS trade_count,
    source::VARCHAR      AS source,
    adjusted::BOOLEAN    AS adjusted,
    CURRENT_TIMESTAMP    AS processed_at
FROM read_json_auto('/app/polygon_bronze.jsonl')
WHERE close > 0
  AND volume > 0
  AND open > 0
  AND high >= low
  AND symbol IS NOT NULL