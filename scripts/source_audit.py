import duckdb
conn = duckdb.connect('transform/dbt/agora.duckdb')

print("=== DISTINCT SOURCES IN fct_prices ===")
rows = conn.execute("SELECT DISTINCT source, COUNT(*) as rows FROM main_gold.fct_prices GROUP BY source ORDER BY rows DESC").fetchall()
for r in rows:
    print(f'  {r[0]:20s} {r[1]:>8,} rows')

print("\n=== SAMPLE ORPHAN SYMBOLS - WHAT SOURCE ===")
rows = conn.execute("""
    SELECT p.symbol, p.source, COUNT(*) as rows
    FROM main_gold.fct_prices p
    LEFT JOIN main_gold.dim_instruments d ON p.symbol = d.symbol
    WHERE d.symbol IS NULL
    GROUP BY p.symbol, p.source
    ORDER BY rows DESC
    LIMIT 10
""").fetchall()
for r in rows:
    print(f'  {r[0]:15s} source={r[1]:15s} {r[2]:>6} rows')
conn.close()
