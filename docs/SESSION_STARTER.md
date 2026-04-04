# AGORA TERMINAL - Session Starter

*Paste this at the start of every new Claude chat to restore full context*

## What This Is

Open source Bloomberg Terminal alternative. Free forever. All data sources are free.

- GitHub: github.com/Sweenal18/agora-terminal | License: Apache 2.0
- Dashboard: http://localhost:8080/research/index.html (local) | GitHub Pages: broken (repo private)
- Production API: http://140.245.250.232:8000

---

## Current State

- **Phase 1 COMPLETE:** Binance WebSocket -> Kafka -> Bronze Iceberg (MinIO) + Bytewax OHLCV (QuestDB) -> dbt Silver (DuckDB) -> FastAPI -> Live Dashboard
- **Phase 2 COMPLETE:** Forex [OK] Commodities [OK] FRED live data [OK] CI green [OK] Oracle server deployed [OK]
- **Phase 3 COMPLETE:** Sprint 1 Dagster [OK] Sprint 2 dbt Gold (61 tests) [OK] Sprint 3 Great Expectations (23 checks) [OK] Sprint 4 CDC/Debezium [OK] Sprint 5 Cloud migration [HOLD] (Oracle ARM capacity unavailable)
- **Phase 4 IN PROGRESS:**
  - AI Query Engine [OK] (Groq llama-3.1-8b-instant, POST /api/ai/query, commit ca8cf01)
  - fct_macro populated [OK] (9097 rows, 9 FRED series)
  - Chart Terminal [MERGED into Research Terminal]
  - Asset Screener [OK] (Gold layer, 16 columns, filters, commit 9ee67ed)
  - Research Terminal [OK] (3 tabs: Chart/Financials/AI Research, default AAPL, commit latest)
  - Nav consolidated [OK] (4 items: Overview/Research/Screener/Portfolio)
  - Smart search in Overview [OK] (ticker -> Research chart, question -> AI tab)
  - Remaining: Font sizes, heatmap 0% bug, peer comparison strip, make repo public, cloud migration

---

## Gold Layer Status

| Table | Rows | Notes |
|---|---|---|
| fct_prices | 25,000 | Equities + crypto OHLCV |
| fct_macro | 9,097 | 9 FRED series 2015-present |
| fct_fundamentals | 100 | P/B, ROE, beta etc. |
| dim_instruments | 50 | S&P 500 subset |
| dim_time | 5,844 | Date dimension |

---

## Dashboard Modules

| Module | Status | Notes |
|---|---|---|
| market_overview | Live | Smart search: ticker->Research, question->AI tab |
| research | Live | Default AAPL, 3 tabs, supports equities/indices/crypto/forex |
| screener | Live | Gold layer, 16 cols, symbol click -> Research |
| chart | Redirect | Redirects to research/index.html |
| ai_query | Redirect | Redirects to research/index.html |
| auth | Live | JWT login, bcrypt passwords |

---

## Next Steps (in order)

1. Fix font sizes (too small across all modules)
2. Fix heatmap showing 0% for all stocks in Overview
3. Add peer comparison strip to Research Terminal
4. Make repo public + fix GitHub Pages
5. Sprint 5 cloud migration (Oracle ARM A1 when available, Hetzner CAX21 fallback)

---

## Infrastructure

- Local dev: cd C:\Projects\agora-terminal\agora-terminal
- Start stack: docker compose -f infra/docker/docker-compose.yml up -d
- Start producer: python -m ingestion.producers.binance.producer
- Local dashboard: Start-Job -ScriptBlock { Set-Location "C:\Projects\agora-terminal\agora-terminal\dashboard\src\modules"; python -m http.server 8080 }
- Oracle server SSH: $keyPath = 'C:\Users\Sweetan Bandodkar\Downloads\ssh-key-2026-03-22.key'
- SSH command: ssh -i $keyPath ubuntu@140.245.250.232
- Server runs: agora-api container only (pipeline too heavy for 1GB RAM)

---

## CRITICAL Rules -- Must Know

- PowerShell file writes: ALWAYS use [System.IO.File]::WriteAllText with (New-Object System.Text.UTF8Encoding $false) -- never Set-Content or Add-Content for Python files
- API restart: NEVER use `docker restart agora-api` -- it loses env vars from .env. ALWAYS use: `docker compose -f infra/docker/docker-compose.yml up -d --no-deps --force-recreate api`
- API code changes: Need REBUILD not just restart -- `docker compose -f infra/docker/docker-compose.yml build api` then force-recreate. Hot-copy alone does NOT work (FastAPI caches modules at startup)
- Yahoo Finance: Direct HTTP only, NEVER yfinance download(). Use browser User-Agent + 0.5s sleep + 3 retries
- QuestDB SQL: Use LATEST ON timestamp PARTITION BY symbol -- NOT MAX subquery
- Server: Use docker-compose (hyphenated v1) not docker compose (v2 not installed on Oracle server)
- DuckDB table full path: agora.main.silver_equity_ohlcv_daily (Silver), agora.main_gold.* (Gold)
- FRED key: Goes in infra/docker/.env not project root .env -- needs full stop/start not restart
- Dagster path quirk: PYTHONPATH=/app + symlink definitions.py into dagster_home required. Hot-copy assets via docker cp to /app/assets/ then restart (never copy to dagster_home directly)
- Gold tables: agora.main_gold schema. dim_instruments has no is_current column. fct_prices uses instrument_key/date_key/close
- CDC: Debezium connector password is change_me_in_production (from infra/docker/.env). Schema Registry must be started separately. Kafka Connect on port 8083.
- AI Query Engine: Uses Groq llama-3.1-8b-instant (NOT Ollama). GROQ_API_KEY must be in infra/docker/.env and is wired into docker-compose.yml under api service environment. engine.py is baked into Docker image -- changes require rebuild
- Groq network: requests library works from container (urllib gets 403 from Groq). engine.py uses requests
- dbt: dbt-core 1.11.7 on Windows via Python 3.12 venv at C:\dbt-env. Activate: C:\dbt-env\Scripts\Activate.ps1
- Research Terminal: Supports equities (DuckDB Gold), indices (^GSPC, ^IXIC via Yahoo), crypto (BTC/ETH), forex (EURUSD), commodities (GOLD). Non-equity symbols show clean left panel without stale fundamentals
- Nav: 4 items only -- Overview / Research / Screener / Portfolio. Chart and AI Query are redirects to Research

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
| GET /api/ai/health | Ollama host | Live (note: health checks Ollama but engine uses Groq) |
| GET /api/chart/ohlcv/{symbol} | DuckDB Silver / Yahoo | Live (timeframe: 1W-MAX) |
| GET /api/chart/info/{symbol} | DuckDB Gold | Live |
| GET /api/chart/symbols | DuckDB Silver | Live (57 symbols) |
| GET /api/screener/screen | DuckDB Gold | Live (16 metrics, filters) |
| GET /api/screener/sectors | DuckDB Gold | Live |
| GET /api/screener/search | DuckDB Gold | Live (fuzzy search) |

---

## Key Services (Local)

| Service | Port | Notes |
|---|---|---|
| FastAPI | 8000 | Main API |
| Dagster | 3000 | Orchestration UI |
| MinIO | 9001 | Object storage UI |
| QuestDB | 9002 | Time-series DB UI |
| Kafka Connect | 8083 | CDC connector REST |
| Schema Registry | 8081 | Start separately |
| Dashboard | 8080 | python -m http.server from modules/ |

---

## Key Technical Decisions (Don't Re-litigate)

- Yahoo Finance direct HTTP over yfinance library -- yfinance gets rate limited in Docker
- Oracle free tier for server -- API only, pipeline stays local until ARM tier obtained (Hetzner CAX21 at 6.49/mo is backup)
- Dagster 1.7.7 with software-defined assets, daily schedules, GE standalone (no dagster-ge)
- DuckDB for Silver -- in-process, reads Parquet, perfect for FastAPI container
- QuestDB for OHLCV -- time-series optimized, ILP write, PostgreSQL wire for reads
- Groq for AI Query Engine -- much faster than Ollama CPU, follows schema instructions reliably. Ollama still installed on Windows host for future use
- Research Terminal is the unified module -- Chart Terminal and AI Query merged in. Nav stays at 4 items
- TradingView Lightweight Charts (free, MIT) for charting
- Non-equity symbols (indices, crypto, forex) use Yahoo Finance fallback in chart API
