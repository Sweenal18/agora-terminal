import duckdb
con = duckdb.connect('/app/transform/dbt/agora.duckdb')

print("=== silver_macro_indicators schema ===")
cols = con.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'main' AND table_name = 'silver_macro_indicators'").fetchall()
for c in cols:
    print(c[0], '|', c[1])

print("\n=== silver_macro_pulse sample ===")
rows = con.execute("SELECT * FROM agora.main.silver_macro_pulse LIMIT 3").fetchall()
for r in rows:
    print(r)