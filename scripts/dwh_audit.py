import duckdb
conn = duckdb.connect('transform/dbt/agora.duckdb')

print("=== ROW COUNTS ===")
tables = [
    ('main', 'silver_equity_ohlcv_daily'),
    ('main', 'silver_equity_fundamentals'),
    ('main', 'silver_instruments'),
    ('main', 'silver_macro_indicators'),
    ('main', 'silver_macro_pulse'),
    ('main_gold', 'dim_instruments'),
    ('main_gold', 'dim_time'),
    ('main_gold', 'fct_prices'),
    ('main_gold', 'fct_fundamentals'),
    ('main_gold', 'fct_macro'),
]
for schema, table in tables:
    count = conn.execute(f'SELECT COUNT(*) FROM {schema}.{table}').fetchone()[0]
    print(f'  {schema}.{table:35s} {count:>8,} rows')

print("\n=== DATA FRESHNESS ===")
print("fct_prices last date:      ", conn.execute("SELECT MAX(trade_date) FROM main_gold.fct_prices").fetchone()[0])
print("fct_fundamentals last date:", conn.execute("SELECT MAX(snapshot_date) FROM main_gold.fct_fundamentals").fetchone()[0])
print("fct_macro last date:       ", conn.execute("SELECT MAX(observation_date) FROM main_gold.fct_macro").fetchone()[0])
print("silver_ohlcv last date:    ", conn.execute("SELECT MAX(trade_date) FROM main.silver_equity_ohlcv_daily").fetchone()[0])

print("\n=== SYMBOL COVERAGE ===")
print("Symbols in silver_ohlcv:   ", conn.execute("SELECT COUNT(DISTINCT symbol) FROM main.silver_equity_ohlcv_daily").fetchone()[0])
print("Symbols in dim_instruments:", conn.execute("SELECT COUNT(DISTINCT symbol) FROM main_gold.dim_instruments").fetchone()[0])
print("Symbols in fct_prices:     ", conn.execute("SELECT COUNT(DISTINCT symbol) FROM main_gold.fct_prices").fetchone()[0])
print("Symbols in fct_fundamentals:", conn.execute("SELECT COUNT(DISTINCT symbol) FROM main_gold.fct_fundamentals").fetchone()[0])

print("\n=== MACRO SERIES ===")
rows = conn.execute("SELECT series_id, series_name, COUNT(*) as obs, MAX(observation_date) as latest FROM main_gold.fct_macro GROUP BY series_id, series_name ORDER BY series_id").fetchall()
for r in rows:
    print(f'  {r[0]:12s} {r[1]:40s} {r[2]:>5} obs  latest: {r[3]}')

conn.close()
