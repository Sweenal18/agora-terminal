path = r'C:\Projects\agora-terminal\agora-terminal\orchestration\dagster\assets\equity.py'
with open(path, 'r', encoding='utf-8') as f:
    c = f.read()

old_try = '    conn = duckdb.connect(DUCKDB_PATH, read_only=True)\n    try:\n        symbols = [\n            r[0] for r in conn.execute("""\n                SELECT DISTINCT symbol\n                FROM agora.main_gold.dim_instruments\n                WHERE is_current = TRUE\n                  AND asset_class = \'equity\'\n                ORDER BY symbol\n            """).fetchall()\n        ]\n        latest_dates = {\n            r[0]: r[1].strftime("%Y-%m-%d")\n            for r in conn.execute("""\n                SELECT symbol, MAX(trade_date)::DATE as latest\n                FROM agora.main.silver_equity_ohlcv_daily\n                GROUP BY symbol\n            """).fetchall()\n        }\n    finally:\n        conn.close()\n    return symbols, latest_dates'

new_try = '    conn = duckdb.connect(DUCKDB_PATH, read_only=True)\n    try:\n        try:\n            symbols = [r[0] for r in conn.execute("""SELECT DISTINCT symbol FROM agora.main_gold.dim_instruments WHERE is_current = TRUE AND asset_class = \'equity\' ORDER BY symbol""").fetchall()]\n            if not symbols:\n                log.warning("dim_instruments empty, using fallback")\n                symbols = ["AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA","JPM","V","XOM","UNH","JNJ","PG","MA","AVGO","HD","MRK","COST","CVX","ABBV","WMT","BAC","CSCO","CRM","MCD","PEP","TMO","ACN","LIN","AMD","INTC","INTU","IBM","DHR","GOOG","ABT","ADBE","GE","HON","NOW","DE","CAT","TXN","AMAT","NFLX"]\n        except Exception as e:\n            log.warning(f"dim_instruments not available, using fallback: {e}")\n            symbols = ["AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA","JPM","V","XOM","UNH","JNJ","PG","MA","AVGO","HD","MRK","COST","CVX","ABBV","WMT","BAC","CSCO","CRM","MCD","PEP","TMO","ACN","LIN","AMD","INTC","INTU","IBM","DHR","GOOG","ABT","ADBE","GE","HON","NOW","DE","CAT","TXN","AMAT","NFLX"]\n        try:\n            latest_dates = {r[0]: r[1].strftime("%Y-%m-%d") for r in conn.execute("""SELECT symbol, MAX(trade_date)::DATE as latest FROM agora.main.silver_equity_ohlcv_daily GROUP BY symbol""").fetchall()}\n        except Exception:\n            latest_dates = {}\n    finally:\n        conn.close()\n    return symbols, latest_dates'

if old_try in c:
    c = c.replace(old_try, new_try)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(c)
    print('SUCCESS')
else:
    print('NOT FOUND')
    idx = c.find('conn = duckdb.connect(DUCKDB_PATH, read_only=True)')
    print(repr(c[idx:idx+200]))