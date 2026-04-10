import duckdb
import sys

db_path = r'C:\Projects\agora-terminal\agora-terminal\transform\dbt\agora.duckdb'
con = duckdb.connect(db_path)

before = con.execute("SELECT COUNT(1) FROM agora.main.silver_equity_ohlcv_daily").fetchone()[0]
print(f"Before: {before} rows")

con.execute("""
CREATE OR REPLACE TABLE agora.main.silver_equity_ohlcv_daily AS
SELECT DISTINCT ON (symbol, trade_date)
    symbol, trade_date, open, high, low, close, volume,
    vwap, trade_count, source, adjusted, processed_at
FROM agora.main.silver_equity_ohlcv_daily
ORDER BY symbol, trade_date, processed_at DESC
""")

after = con.execute("SELECT COUNT(1) FROM agora.main.silver_equity_ohlcv_daily").fetchone()[0]
print(f"After: {after} rows (removed {before - after} duplicates)")
con.close()
sys.exit(0)