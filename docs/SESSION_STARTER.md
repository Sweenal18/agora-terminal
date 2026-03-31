# AGORA TERMINAL — Session Starter

*For use when starting a new Claude chat outside this Project to restore full context*

## What This Is

Open source Bloomberg Terminal alternative. Free forever. All data sources are free.

- GitHub: github.com/Sweenal18/agora-terminal | License: Apache 2.0
- Dashboard: https://sweenal18.github.io/agora-terminal/dashboard/src/modules/market_overview/
- Production API: http://140.245.250.232:8000

---

## Current State

- **Phase 1 COMPLETE:** Binance WebSocket → Kafka → Bronze Iceberg (MinIO) + Bytewax OHLCV (QuestDB) → dbt Silver (DuckDB) → FastAPI → Live Dashboard
- **Phase 2 COMPLETE:** Forex ✅ Commodities ✅ FRED live data ✅ CI green ✅ Oracle server deployed ✅ GitHub Pages ✅ (now private)
- **Phase 3 COMPLETE:** Sprint 1 Dagster ✅ Sprint 2 dbt Gold (61 tests) ✅ Sprint 3 Great Expectations (23 checks) ✅ Sprint 4 CDC/Debezium ✅ Sprint 5 Cloud migration ⏸ (on hold — Oracle ARM capacity unavailable)
- **Phase 4 IN PROGRESS:**
  - Sprint 1 AI Query Engine ✅ (Ollama + qwen2.5-coder:3b, POST /api/ai/query, commit c4caa30)
  - fct_macro populated ✅ (9097 rows across 9 FRED series, commit 2aa5591)
  - Chart Terminal backend ✅ (info/ohlcv/symbols endpoints, timeframe filtering, commit 7370d40)
  - Remaining: Chart Terminal frontend, Asset Screener, Research Terminal, AI Query Engine UI

---

## Gold Layer Status

| Table | Rows | Notes |
|---|---|---|
| fct_prices | 25,000 | Equities + crypto OHLCV |
| fct_macro | 9,097 | 9 FRED series 2015–present |
| fct_fundamentals | 100 | P/B, ROE, beta etc. |
| dim_instruments | 50 | S&P 500 subset |
| dim_time | 5,844 | Date dimension |

Missing FRED series (returned 0 obs, backlog): FEDFUNDS, DFF, T5Y5E, CPILFESL, UNRATE, ICSA

---

## Next Steps (in order)

- Phase 4 Sprint 3 — Chart Terminal frontend (TradingView Lightweight Charts UI)
- Phase 4 Sprint 4 — AI Query Engine UI (chat interface in dashboard)
- Make repo public + fix dashboard (GitHub Pages needs public repo)
- Sprint 5 cloud migration — Oracle ARM A1 (4 OCPU/24GB) when capacity available; Hetzner CAX21 at €6.49/mo as fallback

---

## Infrastructure

- Local dev: `cd C:\Projects\agora-terminal\agora-terminal`
- Start stack: `docker compose -f infra/docker/docker-compose.yml up -d`
- Start producer: `python -m ingestion.producers.binance.producer`
- Oracle server SSH:
  ```
  $keyPath = 'C:\Users\Sweetan Bandodkar\Downloads\ssh-key-2026-03-22.key'
  ssh -i $keyPath ubuntu@140.245.250.232
  ```
- Server runs: agora-api container only (pipeline too heavy for 1GB RAM)
- Oracle ARM A1 (4 OCPU/24GB) requested — capacity unavailable, retrying

---

## CRITICAL Rules — Must Know

- **PowerShell file writes:** ALWAYS use `[System.IO.File]::WriteAllText` with `(New-Object System.Text.UTF8Encoding $false)` — never `Set-Content` or `Add-Content` for Python files
- **Docker code changes:** Need REBUILD not just restart — `docker compose build api && docker compose up -d api`
- **Yahoo Finance:** Direct HTTP only, NEVER yfinance download(). Use browser User-Agent + 0.5s sleep + 3 retries
- **QuestDB SQL:** Use `LATEST ON timestamp PARTITION BY symbol` — NOT MAX subquery
- **Server:** Use `docker-compose` (hyphenated v1) not `docker compose` (v2 not installed)
- **DuckDB table full path:** `agora.main.silver_equity_ohlcv_daily`
- **FRED key:** Goes in `infra/docker/.env` not project root `.env` — needs full stop/start not restart
- **Dagster path quirk:** `PYTHONPATH=/app` + symlink `definitions.py` into `dagster_home` required. Hot-copy assets via `docker cp` to `/app/assets/` then restart (never copy to `dagster_home` directly)
- **Gold tables:** in `agora.main_gold` schema (NOT `agora.main`). Silver tables in `agora.main`. `dim_instruments` has no `is_current` column. `fct_prices` uses `instrument_key/date_key/close`
- **CDC:** Debezium connector password is `change_me_in_production` (from `infra/docker/.env`). Schema Registry must be started separately. Kafka Connect on port 8083.
- **Ollama:** Runs on Windows host (NOT in Docker). Installed at `C:\Users\Sweetan Bandodkar\AppData\Local\Programs\Ollama\`. Reachable from containers via `host.docker.internal:11434`. Model: `qwen2.5-coder:3b`
- **AI Query Engine:** `api/ai_query/engine.py` — `DUCKDB_PATH=/app/transform/dbt/agora.duckdb`. fct_macro series_ids: T10Y2Y, T10Y3M, CPIAUCSL, PCEPI, PAYEMS, GDP, GDPC1, INDPRO, VIXCLS
- **dbt:** dbt-fusion 2.0 on host is INCOMPATIBLE with project syntax. dbt-core runs only inside Dagster ephemeral containers. For one-off Gold table population use Python direct write scripts in `ingestion/fetchers/`
- **Chart Terminal:** `api/routes/chart.py` — timeframes: 1W, 1M, 3M, 6M, 1Y, 2Y, 5Y, MAX. Equity data from DuckDB Silver, Yahoo Finance fallback for forex/commodities/unknown symbols. Info endpoint pulls from dim_instruments + fct_fundamentals + fct_prices.

---

## API Endpoints

| Endpoint | Source | Status |
|---|---|---|
| GET /health | Internal | Live |
| GET /api/crypto/prices | QuestDB | Live (change% shows 0 — needs fix) |
| GET /api/crypto/ohlcv/{symbol} | QuestDB | Live |
| GET /api/equity/symbols | DuckDB Silver | Live |
| GET /api/equity/ohlcv/{symbol} | DuckDB Silver | Live |
| GET /api/equity/indices | Yahoo Finance | Live |
| GET /api/macro/pulse | FRED API | Live |
| GET /api/macro/forex | Yahoo Finance | Live |
| GET /api/macro/commodities | Yahoo Finance | Live |
| POST /api/ai/query | Ollama + DuckDB Gold | Live (NL to SQL) |
| GET /api/ai/health | Ollama host | Live |
| GET /api/chart/ohlcv/{symbol} | DuckDB Silver / Yahoo | Live (timeframe: 1W-MAX) |
| GET /api/chart/info/{symbol} | DuckDB Gold | Live |
| GET /api/chart/symbols | DuckDB Silver | Live (57 symbols) |

---

## Key Technical Decisions (Don't Re-litigate)

- Yahoo Finance direct HTTP over yfinance library — yfinance gets rate limited in Docker
- Oracle free tier for server — API only, pipeline stays local until ARM tier obtained
- Dagster 1.7.7 with software-defined assets, daily schedules, GE standalone (no dagster-ge)
- DuckDB for Silver — in-process, reads Parquet, perfect for FastAPI container
- QuestDB for OHLCV — time-series optimized, ILP write, PostgreSQL wire for reads
- Ollama on Windows host, not Docker — AMD GPU has no ROCm support on Windows, CPU inference ~5-40s per query
- AI Query Engine uses qwen2.5-coder:3b (SQL-optimized, 3B fits in RAM)
- dbt Gold population via Python scripts when dbt-fusion incompatibility blocks normal dbt run
- TradingView Lightweight Charts (free, MIT) for Chart Terminal frontend

---

*For full technical details see: Agora Terminal Master Technical Reference v2.0*
