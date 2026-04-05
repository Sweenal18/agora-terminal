import duckdb
conn = duckdb.connect('transform/dbt/agora.duckdb')
tables = conn.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema IN ('main', 'main_gold') ORDER BY table_schema, table_name").fetchall()
for schema, table in tables:
    print(f'\n=== {schema}.{table} ===')
    cols = conn.execute(f'DESCRIBE {schema}.{table}').fetchall()
    for col in cols:
        print(f'  {col[0]:30s} {col[1]}')
conn.close()
