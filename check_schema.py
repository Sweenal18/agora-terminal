import duckdb
con = duckdb.connect('transform/dbt/agora.duckdb')
cols = con.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'main' AND table_name = 'silver_equity_ohlcv_daily'").fetchall()
for c in cols:
    print(c[0], '|', c[1])