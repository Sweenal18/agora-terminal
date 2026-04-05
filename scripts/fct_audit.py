import duckdb
conn = duckdb.connect('transform/dbt/agora.duckdb')

print("=== SYMBOLS IN silver_equity_ohlcv_daily ===")
print("Count:", conn.execute("SELECT COUNT(DISTINCT symbol) FROM main.silver_equity_ohlcv_daily").fetchone()[0])

print("\n=== SYMBOLS IN fct_prices NOT IN silver_equity_ohlcv_daily ===")
rows = conn.execute("""
    SELECT DISTINCT p.symbol
    FROM main_gold.fct_prices p
    LEFT JOIN main.silver_equity_ohlcv_daily s ON p.symbol = s.symbol
    WHERE s.symbol IS NULL
    ORDER BY p.symbol
    LIMIT 10
""").fetchall()
for r in rows:
    print(f'  {r[0]}')

print("\n=== fct_prices instrument_key sample for orphan ===")
rows = conn.execute("""
    SELECT symbol, instrument_key, source
    FROM main_gold.fct_prices
    WHERE symbol = 'TSLA'
    LIMIT 3
""").fetchall()
for r in rows:
    print(f'  symbol={r[0]} key={r[1]} source={r[2]}')
conn.close()
