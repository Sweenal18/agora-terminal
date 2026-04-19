import duckdb
conn = duckdb.connect(r'transform\dbt\agora.duckdb')

print('=== SILVER LAYER ===')
for t in conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name").fetchall():
    count = conn.execute(f'SELECT COUNT(*) FROM main.{t[0]}').fetchone()[0]
    print(f'  {t[0]}: {count:,} rows')

print()
print('=== GOLD LAYER ===')
for t in conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main_gold' ORDER BY table_name").fetchall():
    count = conn.execute(f'SELECT COUNT(*) FROM main_gold.{t[0]}').fetchone()[0]
    print(f'  {t[0]}: {count:,} rows')

print()
print('=== DATA FRESHNESS ===')
print('  fct_prices latest:    ', conn.execute('SELECT MAX(trade_date) FROM main_gold.fct_prices').fetchone()[0])
print('  silver_ohlcv latest:  ', conn.execute('SELECT MAX(trade_date) FROM main.silver_equity_ohlcv_daily').fetchone()[0])
print('  fct_macro latest:     ', conn.execute('SELECT MAX(observation_date) FROM main_gold.fct_macro').fetchone()[0])
print('  fundamentals updated: ', conn.execute('SELECT MAX(updated_at) FROM main.silver_equity_fundamentals').fetchone()[0])
print('  cik_mapping count:    ', conn.execute('SELECT COUNT(*) FROM main.silver_cik_mapping').fetchone()[0])

print()
print('=== SYMBOL COVERAGE ===')
print('  dim_instruments (current):', conn.execute("SELECT COUNT(*) FROM main_gold.dim_instruments WHERE is_current = TRUE").fetchone()[0])
print('  silver_ohlcv symbols:     ', conn.execute('SELECT COUNT(DISTINCT symbol) FROM main.silver_equity_ohlcv_daily').fetchone()[0])
print('  fct_prices symbols:       ', conn.execute('SELECT COUNT(DISTINCT symbol) FROM main_gold.fct_prices').fetchone()[0])
print('  fundamentals symbols:     ', conn.execute('SELECT COUNT(DISTINCT symbol) FROM main.silver_equity_fundamentals').fetchone()[0])

print()
print('=== SILVER INSTRUMENTS (CDC) ===')
print('  silver_instruments rows:  ', conn.execute('SELECT COUNT(*) FROM main.silver_instruments').fetchone()[0])
print('  dim_instruments SCD2 total:', conn.execute('SELECT COUNT(*) FROM main_gold.dim_instruments').fetchone()[0])

conn.close()
