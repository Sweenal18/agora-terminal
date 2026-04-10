import duckdb
con = duckdb.connect(r'C:\Projects\agora-terminal\agora-terminal\transform\dbt\agora.duckdb')
print("Before:", con.execute("SELECT COUNT(1) FROM agora.main.silver_equity_ohlcv_daily").fetchone())
con.execute("""
CREATE OR REPLACE TABLE agora.main.silver_equity_ohlcv_daily AS
SELECT DISTINCT ON (symbol, trade_date)
    symbol, trade_date, open, high, low, close, volume,
    vwap, trade_count, source, adjusted, processed_at
FROM agora.main.silver_equity_ohlcv_daily
ORDER BY symbol, trade_date, processed_at DESC
""")
print("After:", con.execute("SELECT COUNT(1) FROM agora.main.silver_equity_ohlcv_daily").fetchone())
con.close()
print("Done")