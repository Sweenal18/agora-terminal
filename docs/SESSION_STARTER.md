# AGORA TERMINAL - Session Starter

*Paste this at the start of every new Claude chat to restore full context*

## What This Is

Open source Bloomberg Terminal alternative. Free forever. All data sources are free.

- GitHub: github.com/Sweenal18/agora-terminal | License: Apache 2.0
- Dashboard (local): http://localhost:8080/market_overview/index.html
- Dashboard (public): https://agora-terminal.com
- API (public): https://api.agora-terminal.com
- API (local): http://localhost:8000

---

## Current State

- **Phase 1 COMPLETE:** Binance WebSocket -> Kafka -> Bronze Iceberg (MinIO) + Bytewax OHLCV (QuestDB) -> dbt Silver (DuckDB) -> FastAPI -> Live Dashboard
- **Phase 2 COMPLETE:** Forex [OK] Commodities [OK] FRED live data [OK] CI green [OK]
- **Phase 3 COMPLETE:** Sprint 1 Dagster [OK] Sprint 2 dbt Gold (61 tests) [OK] Sprint 3 Great Expectations (23 checks) [OK] Sprint 4 CDC/Debezium [OK] Sprint 5 Cloud migration [HOLD]
- **Phase 4 IN PROGRESS:**
  - AI Query Engine [OK] (Groq llama-3.1-8b-instant, POST /api/ai/query)
  - fct_macro populated [OK] (9097 rows, 9 FRED series)
  - Chart Terminal [MERGED into Research Terminal]
  - Asset Screener [OK] (Gold layer, 16 columns, filters)
  - Research Terminal [OK] (3 tabs: Chart/Financials/AI Research, default AAPL, timeframe buttons 1W/1M/3M/6M/1Y/MAX)
  - Nav consolidated [OK] (4 items: Overview/Research/Screener/Portfolio)
  - Smart search in Overview [OK] (ticker -> Research chart, question -> AI tab)
  - Repo public [OK] (github.com/Sweenal18/agora-terminal)
  - GitHub Pages [OK] (agora-terminal.com, deployed via Actions)
  - Cloudflare Tunnel [OK] (permanent, runs as Windows service, api.agora-terminal.com)
  - Equity data automated [OK] (Dagster equity_daily fetches Yahoo Finance incrementally, appends to Bronze)
  - Dagster gRPC [OK] (dagster-user-code service, all 6 assets green, 5 schedules running)
  - **Remaining:** Peer comparison strip, Portfolio nav (coming soon), dbt Gold automation, cloud migration

---

## Gold Layer Status

| Table | Rows | Notes |
|---|---|---|
| fct_prices | 27,216 | Equities OHLCV, updated to 2026-04-02 |
| fct_macro | 9,097 | 9 FRED series 2015-present |
| fct_fundamentals | 100 | P/B, ROE, beta etc. |
| dim_instruments | 50 | S&P 500 subset (GOOG sector fixed) |
| dim_time | 5,844 | Date dimension |

---

## Dashboard Modules

| Module | Status | Notes |
|---|---|---|
| market_overview | Live | Smart search, heatmap working, real data |
| research | Live | Default AAPL, 3 tabs, timeframe buttons, equities/indices/crypto/forex |
| screener | Live | Gold layer, 16 cols, symbol click -> Research |
| chart | Redirect | Redirects to research/index.html |
| ai_query | Redirect | Redirects to research/index.html |
| auth | Live | JWT login, bcrypt passwords |

---

## Next Steps (in order)

1. Add peer comparison strip to Research Terminal
2. Replace Portfolio nav with Watchlist or Coming Soon
3. Automate dbt Gold (currently manual after equity fetch)
4. Sprint 5 cloud migration (Oracle ARM A1 when available)
5. Make Cloudflare tunnel survive PC sleep/hibernate (currently survives reboot via service)

---

## Infrastructure

- Local dev: cd C:\Projects\agora-terminal\agora-terminal
- Start stack: docker compose -f infra/docker/docker-compose.yml up -d
- Start producer: python -m ingestion.producers.binance.producer
- Local dashboard: Start-Job -ScriptBlock { Set-Location "C:\Projects\agora-terminal\agora-terminal\dashboard\src\modules"; python -m http.server 8080 }
- Oracle server SSH: $keyPath = 'C:\Users\Sweetan Bandodkar\Downloads\ssh-key-2026-03-22.key'
- SSH command: ssh -i $keyPath ubuntu@140.245.250.232 (slow, 1GB AMD micro, API only)
- Cloudflare tunnel: Runs as Windows service (cloudflared), starts on boot automatically
- dbt Gold (manual): dbt run --select tag:gold --profiles-dir transform\dbt --project-dir transform\dbt\agora

---

## Dagster Schedules (all enabled)

| Schedule | Cron | What it does |
|---|---|---|
| equity_daily_schedule | 0 0 * * * | Fetch Yahoo Finance OHLCV, append Bronze, rebuild Silver |
| macro_daily_schedule | 0 1 * * * | Fetch FRED macro indicators |
| fundamentals_daily_schedule | 0 2 * * * | Fetch FMP fundamentals |
| data_quality_daily_schedule | 0 3 * * * | Run Great Expectations checks |
| cdc_instruments_schedule | */15 * * * * | Consume Debezium CDC events |

After equity_daily runs, manually run dbt Gold:
`dbt run --select tag:gold --profiles-dir transform\dbt --project-dir transform\dbt\agora`

---

## CRITICAL Rules -- Must Know

- PowerShell file writes: ALWAYS use [System.IO.File]::WriteAllText with (New-Object System.Text.UTF8Encoding $false) -- never Set-Content or Add-Content for Python files
- API restart: NEVER use `docker restart agora-api` -- it loses env vars from .env. ALWAYS use: `docker compose -f infra/docker/docker-compose.yml up -d --no-deps --force-recreate api`
- API code changes: Need REBUILD not just restart -- `docker compose -f infra/docker/docker-compose.yml build api` then force-recreate. Hot-copy alone does NOT work (FastAPI caches modules at startup)
- Dagster code changes: Need REBUILD of dagster-user-code -- hot-copy does NOT work (gRPC server caches modules at startup). Always: build + force-recreate dagster-user-code
- Yahoo Finance: Direct HTTP only, NEVER yfinance download(). Use browser User-Agent + 0.5s sleep + 3 retries
- QuestDB SQL: Use LATEST ON timestamp PARTITION BY symbol -- NOT MAX subquery
- DuckDB table full path: agora.main.silver_equity_ohlcv_daily (Silver), agora.main_gold.* (Gold)
- FRED key: Goes in infra/docker/.env not project root .env -- needs full stop/start not restart
- Gold tables: agora.main_gold schema. dim_instruments has no is_current column. fct_prices uses instrument_key/date_key/close
- CDC: Debezium connector password is change_me_in_production (from infra/docker/.env). Schema Registry must be started separately. Kafka Connect on port 8083.
- AI Query Engine: Uses Groq llama-3.1-8b-instant (NOT Ollama). GROQ_API_KEY in infra/docker/.env, wired into docker-compose.yml under api service
- Groq network: requests library works from container (urllib gets 403 from Groq)
- dbt: dbt-core 1.11.7 on Windows via Python 3.12 venv at C:\dbt-env. Activate: C:\dbt-env\Scripts\Activate.ps1
- Research Terminal: Supports equities (DuckDB Gold), indices (^GSPC, ^IXIC via Yahoo), crypto (BTC/ETH), forex (EURUSD), commodities (GOLD)
- Nav: 4 items only -- Overview / Research / Screener / Portfolio
- API URL: Auto-detects localhost vs production. localhost:8000 locally, api.agora-terminal.com in production
- DuckDB is mounted via volume into API container -- changes to agora.duckdb on disk are immediately visible to API
- Equity data: polygon_bronze.jsonl is NOT in git (gitignored). 54 symbols, data from 2024-03-15. Dagster appends new dates daily.

---

## API Endpoints

| Endpoint | Source | Status |
|---|---|---|
| GET /health | Internal | Live |
| GET /api/crypto/prices | QuestDB | Live (change% shows 0 -- needs fix) |
| GET /api/crypto/ohlcv/{symbol} | QuestDB | Live |
| GET /api/equity/symbols | DuckDB Silver | Live |
| GET /api/equity/ohlcv/{symbol} | DuckDB Silver | Live |
| GET /api/equity/indices | Yahoo Finance | Live |
| GET /api/macro/pulse | FRED API | Live |
| GET /api/macro/forex | Yahoo Finance | Live |
| GET /api/macro/commodities | Yahoo Finance | Live |
| POST /api/ai/query | Groq + DuckDB Gold | Live (NL to SQL, ~1.5s) |
| GET /api/chart/ohlcv/{symbol} | DuckDB Silver / Yahoo | Live (timeframe: 1W/1M/3M/6M/1Y/MAX) |
| GET /api/chart/info/{symbol} | DuckDB Gold | Live |
| GET /api/chart/symbols | DuckDB Silver | Live (57 symbols) |
| GET /api/screener/screen | DuckDB Gold | Live (16 metrics, filters) |
| GET /api/screener/sectors | DuckDB Gold | Live |
| GET /api/screener/search | DuckDB Gold | Live (fuzzy search) |

---

## Key Services (Local)

| Service | Port | Notes |
|---|---|---|
| FastAPI | 8000 | Main API (also via api.agora-terminal.com) |
| Dagster | 3000 | Orchestration UI |
| MinIO | 9001 | Object storage UI |
| QuestDB | 9002 | Time-series DB UI |
| Kafka Connect | 8083 | CDC connector REST |
| Schema Registry | 8081 | Start separately |
| Dashboard | 8080 | python -m http.server from modules/ |
| Cloudflare Tunnel | - | Windows service, auto-starts on boot |

---

## Key Technical Decisions (Don't Re-litigate)

- Yahoo Finance direct HTTP over yfinance library -- yfinance gets rate limited in Docker
- Cloudflare Tunnel for public API -- no exposed home IP, free, permanent, DDoS protected
- agora-terminal.com served via GitHub Pages (dashboard) + Cloudflare Tunnel (API)
- Dagster uses dagster-user-code gRPC server pattern (3 containers: webserver, daemon, user-code)
- dbt Gold cannot run inside dagster-user-code (protobuf conflict with dagster 1.7.7 + dbt-core 1.11.7) -- run manually
- Oracle free tier kept as backup server -- API only, not primary
- Dagster 1.7.7 with software-defined assets, daily schedules, GE standalone (no dagster-ge)
- DuckDB for Silver -- in-process, reads Parquet, perfect for FastAPI container
- QuestDB for OHLCV -- time-series optimized, ILP write, PostgreSQL wire for reads
- Groq for AI Query Engine -- much faster than Ollama CPU, follows schema instructions reliably
- Research Terminal is the unified module -- Chart Terminal and AI Query merged in
- TradingView Lightweight Charts v4 (pinned, MIT) for charting
- Non-equity symbols (indices, crypto, forex) use Yahoo Finance fallback in chart API
