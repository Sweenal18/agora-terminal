import duckdb
con = duckdb.connect('transform/dbt/agora.duckdb')
count = con.execute("SELECT COUNT(DISTINCT symbol) FROM agora.main.silver_equity_ohlcv_daily").fetchone()[0]
rows = con.execute("SELECT COUNT(*) FROM agora.main.silver_equity_ohlcv_daily").fetchone()[0]
print(f"Distinct symbols: {count}")
print(f"Total rows: {rows}")
print("\nMost recently added symbols:")
recent = con.execute("SELECT DISTINCT symbol, MAX(processed_at) as last_added FROM agora.main.silver_equity_ohlcv_daily GROUP BY symbol ORDER BY last_added DESC LIMIT 10").fetchall()
for r in recent:
    print(f"  {r[0]} | {r[1]}")