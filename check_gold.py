import duckdb
con = duckdb.connect('transform/dbt/agora.duckdb')
tables = ['fct_prices', 'fct_fundamentals', 'fct_macro', 'dim_instruments', 'dim_time']
for t in tables:
    count = con.execute(f"SELECT COUNT(*) FROM agora.main_gold.{t}").fetchone()[0]
    syms = ''
    if t in ['fct_prices', 'fct_fundamentals']:
        syms = con.execute(f"SELECT COUNT(DISTINCT symbol) FROM agora.main_gold.{t}").fetchone()[0]
        syms = f' | {syms} symbols'
    print(f"{t}: {count:,} rows{syms}")