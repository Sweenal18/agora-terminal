import duckdb
con = duckdb.connect('transform/dbt/agora.duckdb')
silver = con.execute("SELECT COUNT(DISTINCT symbol) FROM agora.main.silver_equity_ohlcv_daily").fetchone()[0]
gold = con.execute("SELECT COUNT(DISTINCT symbol) FROM agora.main_gold.fct_prices WHERE asset_class = 'equity'").fetchone()[0]
print(f"Silver symbols: {silver}")
print(f"Gold fct_prices equity symbols: {gold}")
print("Gap:", silver - gold, "symbols not yet in Gold")