import duckdb
con = duckdb.connect('transform/dbt/agora.duckdb')
count = con.execute("SELECT COUNT(DISTINCT symbol) FROM agora.main.silver_equity_ohlcv_daily").fetchone()[0]
print(f"Distinct symbols in silver_equity_ohlcv_daily: {count}")
rows = con.execute("SELECT COUNT(*) FROM agora.main.silver_equity_ohlcv_daily").fetchone()[0]
print(f"Total rows: {rows}")
print("\nDate range:")
dates = con.execute("SELECT MIN(trade_date), MAX(trade_date) FROM agora.main.silver_equity_ohlcv_daily").fetchone()
print(f"  {dates[0]} to {dates[1]}")