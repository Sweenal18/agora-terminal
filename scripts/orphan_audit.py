import duckdb
conn = duckdb.connect('transform/dbt/agora.duckdb')

print("=== SYMBOLS IN fct_prices BUT NOT IN dim_instruments ===")
rows = conn.execute("""
    SELECT DISTINCT p.symbol, COUNT(*) as price_rows
    FROM main_gold.fct_prices p
    LEFT JOIN main_gold.dim_instruments d ON p.symbol = d.symbol
    WHERE d.symbol IS NULL
    GROUP BY p.symbol
    ORDER BY price_rows DESC
    LIMIT 20
""").fetchall()
for r in rows:
    print(f'  {r[0]:15s} {r[1]:>6} rows')

print(f'\nTotal orphan symbols: {len(rows)}')
conn.close()
