import duckdb
conn = duckdb.connect('transform/dbt/agora.duckdb')

print("=== ALL ORPHAN SYMBOLS (not in dim_instruments) ===")
rows = conn.execute("""
    SELECT DISTINCT p.symbol
    FROM main_gold.fct_prices p
    LEFT JOIN main_gold.dim_instruments d ON p.symbol = d.symbol
    WHERE d.symbol IS NULL
    ORDER BY p.symbol
""").fetchall()
for r in rows:
    print(f'  {r[0]}')
print(f'\nTotal: {len(rows)}')

print("\n=== SYMBOLS IN dim_instruments ===")
rows2 = conn.execute("SELECT symbol FROM main_gold.dim_instruments ORDER BY symbol").fetchall()
for r in rows2:
    print(f'  {r[0]}')
print(f'\nTotal: {len(rows2)}')
conn.close()
