path = r'C:\Projects\agora-terminal\agora-terminal\orchestration\dagster\assets\equity.py'
with open(path, 'r', encoding='utf-8') as f:
    c = f.read()

old = '        count = conn.execute("SELECT COUNT(*) FROM agora.main.silver_equity_ohlcv_daily").fetchone()[0]'
new = '''        # Deduplicate: keep latest processed_at per (symbol, trade_date)
        conn.execute("""
            CREATE OR REPLACE TABLE agora.main.silver_equity_ohlcv_daily AS
            SELECT DISTINCT ON (symbol, trade_date)
                symbol, trade_date, open, high, low, close, volume,
                vwap, trade_count, source, adjusted, processed_at
            FROM agora.main.silver_equity_ohlcv_daily
            ORDER BY symbol, trade_date, processed_at DESC
        """)
        count = conn.execute("SELECT COUNT(*) FROM agora.main.silver_equity_ohlcv_daily").fetchone()[0]'''

if old in c:
    c = c.replace(old, new)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(c)
    print('SUCCESS')
else:
    print('NOT FOUND')