# AGORA TERMINAL - Session Starter

*Paste this at the start of every new Claude chat to restore full context*

## What This Is

Financial intelligence platform. Bloomberg charges $24,000/year. We think that's wrong.

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
  - fct_macro populated [OK] (9 FRED series, schema fixed to use silver_macro_pulse columns: indicator/value/date)
  - Chart Terminal [MERGED into Research Terminal]
  - Asset Screener [OK] (Gold layer, filters, debt_to_equity + free_cash_flow_yield + avg_volume added)
  - Research Terminal [OK] (4 tabs: Chart/Financials/AI Research/Filings, default AAPL, timeframe buttons 1W/1M/3M/6M/1Y/MAX)
  - SEC EDGAR Document Intelligence [OK] (silver_cik_mapping 10,426 companies, /api/filings/{symbol}, Groq AI summaries, Filings tab)
  - Nav [OK] (4 items: Overview/Research/Screener/Portfolio)
  - Smart search in Overview [OK] (ticker -> Research chart, question -> AI tab)
  - Repo public [OK] (github.com/Sweenal18/agora-terminal)
  - GitHub Pages [OK] (agora-terminal.com, deployed via Actions)
  - Cloudflare Tunnel [OK] (permanent, runs as Windows service, api.agora-terminal.com)
  - Equity data automated [OK] (Dagster equity_daily fetches Yahoo Finance for all 502 symbols, deduplicates Silver after each run)
  - Dagster gRPC [OK] (dagster-user-code service, all 7 assets green, 6 schedules running)
  - Auth [OK] (JWT login, bcrypt, Google OAuth, GitHub OAuth, soft gate)
  - Soft gate [OK] (3 free actions, reappears after 2 more on dismiss, gate.js in shared/)
  - Peer comparison strip [OK] (/api/screener/peers/{symbol}, sector peers from fct_prices + dim_instruments)
  - Search UX [OK] (unified dropdown: price + change + pin star, 480px width, z-index fixed, overflow fixed)
  - Pinned chip bar [OK] (localStorage, draggable to reorder, pin/unpin from search dropdown)
  - dbt Gold automated [OK] (Windows Task Scheduler 1:00 PM IST, run_dbt_gold.ps1: backup→cleanup→dedup→snapshot→Gold→alert)
  - Portfolio page [OK] (coming soon placeholder at dashboard/src/modules/portfolio/)
  - About page [OK] (dashboard/src/modules/about/)
  - User menu [OK] (avatar initials dropdown: Account, About Agora, Sign Out -- all 3 nav files)
  - Account page [OK] (dashboard/src/modules/account/)
  - DWH audit [OK] (dim_instruments now covers all 502 symbols, 0 orphans)
  - Fundamentals fetcher [OK] (data-driven, reads symbols from silver_equity_ohlcv_daily)
  - Screener API [OK] (symbol filter, debt_to_equity, free_cash_flow_yield, avg_volume, /api/screener/peers/{symbol})
  - Screener UI [OK] (Max Debt/Equity + Min FCF Yield filters added)
  - dim_instruments SCD Type 2 [OK] (is_current, valid_to, exchange columns, 552 total rows, 50 historical)
  - fct_fundamentals [OK] (debt_to_equity, free_cash_flow_yield, avg_volume, country added)
  - Gold schema tests [OK] (60 pass, 1 warn on exchange null for historical SCD2 rows)
  - DuckDB concurrency [OK] (dagster.yaml mounted into daemon + webserver via volume)
  - API Dockerfile [OK] (no longer copies DuckDB -- uses volume mount only, avoids lock conflicts)
  - Silver dedup [OK] (dedup_silver.py runs daily, DISTINCT ON symbol/trade_date in silver_equity_ohlcv_daily)
  - Heatmap [OK] (dynamic top 55 by market cap from /api/screener/screen, not hardcoded SP500_TICKERS)
  - Bronze backup [OK] (backup_bronze.py uploads polygon_bronze.jsonl to MinIO agora-bronze/equity/ daily, 7-day retention)
  - Pipeline alerting [OK] (alerts@agora-terminal.com via Resend on failure + success, scripts/alert.py)
  - chart.py dedup [OK] (GROUP BY trade_date eliminates duplicate OHLCV rows, column order: r[0]=date, r[1-5]=OHLCV)
  - equity.py bootstrap [OK] (falls back to hardcoded 50 symbols if dim_instruments missing on cold start)
  - - TTL cache [OK] (all API routes: chart 15min/5min, screener 5min/60min, macro 10min/2min, equity 60min/5min)
  - FRED expanded [OK] (9 indicators: fed_rate, treasury_10y, cpi, unemployment, gdp, vix, gdp_growth, retail_sales, housing)
  - FMP rotation [OK] (80 stalest symbols/day) -- being replaced by Finnhub
  - Finnhub switch [IN PROGRESS] (key in .env, docker-compose patched, fundamentals.py rewritten -- needs dagster-user-code rebuild)
  - startup.ps1 [OK] (scripts/startup.ps1 -- one command after sleep/reboot)
  - README rewrite [OK] (architecture-first, encoding fixed, pushed to GitHub)
  - CI green [OK] (ruff check passing)
  - /api/health/data [OK] (data freshness endpoint)
  - **Remaining:** Portfolio module, Finnhub rebuild, cloud migration, Show HN (needs HN karma)
  
---

## DWH Status (April 16 2026)

| Table | Rows | Notes |
|---|---|---|
| fct_prices | 283,785 | 502 symbols, deduped, full-refresh April 10, updated to 2026-04-09 |
| fct_macro | ~9,000 | 9 FRED series, schema fixed (silver_macro_pulse: indicator/value/date) |
| fct_fundamentals | ~500 | Full FMP run completed April 10 |
| dim_instruments | 552 | Full SCD2 -- 502 current, 50 historical |
| dim_time | 5,844 | Date dimension |
| silver_equity_ohlcv_daily | 283,785 | Deduped. Fetcher reads 502 symbols from dim_instruments |
| silver_equity_fundamentals | ~500 | Full run completed April 10 |
| silver_macro_pulse | 145+ | 9 FRED series with history, upsert on (indicator, date) |

### DWH Notes
- silver_equity_ohlcv_daily deduped daily (DISTINCT ON symbol, trade_date, keep latest processed_at)
- Equity fetcher reads symbols from dim_instruments (502) with fallback to 50 hardcoded on cold start
- fct_prices rebuilt with --full-refresh April 10 to fix stale 0% daily returns from old Polygon bulk load
- fct_macro schema: silver_macro_pulse columns are (indicator, value, date, fetched_at) -- NOT series_id/observation_date
- Streaming pipeline (Binance, CDC, bronze-writer) still local -- crypto % change shows 0 until cloud migration

---

## Dashboard Modules

| Module | Status | Notes |
|---|---|---|
| market_overview | Live | Heatmap dynamic top 55 by market cap, real % changes |
| research | Live | 4 tabs, peer strip, pinned chip bar, user menu |
| screener | Live | Gold layer, debt_to_equity + FCF yield filters added |
| portfolio | Live | Coming soon placeholder |
| about | Live | Product page, no price commitment |
| auth | Live | JWT + Google OAuth + GitHub OAuth |
| auth/callback | Live | GitHub OAuth callback handler |
| account | Live | Profile, plan, usage placeholders |
| chart | Redirect | Redirects to research/index.html |
| ai_query | Redirect | Redirects to research/index.html |

---

## Next Steps (in order)

1. Complete Finnhub switch -- rebuild dagster-user-code, materialize silver_equity_fundamentals
2. Verify all 502 symbols covered after first full Finnhub run
3. Portfolio module -- build actual tracker
4. HN karma building -- comment on threads to unlock Show HN
5. Reddit posts -- r/dataengineering, r/algotrading
6. Cloud migration -- Hetzner CAX21

---

## Infrastructure

- Local dev: cd C:\Projects\agora-terminal\agora-terminal
- Start stack: docker compose -f infra/docker/docker-compose.yml up -d
- Start producer: python -m ingestion.producers.binance.producer
- Local dashboard: Start-Job -ScriptBlock { Set-Location "C:\Projects\agora-terminal\agora-terminal\dashboard\src\modules"; python -m http.server 8080 }
- Oracle server SSH: $keyPath = 'C:\Users\Sweetan Bandodkar\Downloads\ssh-key-2026-03-22.key'
- SSH command: ssh -i $keyPath ubuntu@140.245.250.232 (slow, 1GB AMD micro, API only)
- Cloudflare tunnel: Runs as Windows service (cloudflared), starts on boot automatically
- dbt Gold: run_dbt_gold.ps1 via Task Scheduler at 1:00 PM IST. Steps: Bronze backup → orphan cleanup → Silver dedup → dbt snapshot → dbt Gold → email alert. Log: scripts/dbt_gold.log.
- dbt venv: C:\dbt-env. Activate: C:\dbt-env\Scripts\Activate.ps1
- dbt run: dbt run --profiles-dir transform\dbt --project-dir transform\dbt\agora --target dev --select tag:gold
- dbt snapshot: dbt snapshot --profiles-dir transform\dbt --project-dir transform\dbt\agora --target dev
- MinIO credentials: user=agora, pass=change_me_in_production (in infra/docker/.env)
- Resend API key: re_d5YKQTBr_MauCmbp7csTU4QEzJL48dUc8 (alerts@agora-terminal.com sender, Tokyo region)

---

## Auth & OAuth

- JWT secret: JWT_SECRET_KEY in infra/docker/.env
- Google OAuth Client ID: 300776466538-8f4p3u97g82al2kn39d5vd77fpihqi65.apps.googleusercontent.com
- Google authorized origins: https://agora-terminal.com, http://localhost:8080
- GitHub OAuth App: Client ID Ov23lia7gxDNXwoMfQyf, callback: https://api.agora-terminal.com/auth/github/callback
- GITHUB_CLIENT_ID + GITHUB_CLIENT_SECRET in infra/docker/.env, wired into docker-compose.yml under api service
- GitHub callback flow: /auth/github -> GitHub -> /auth/github/callback -> auth/callback.html -> localStorage -> market_overview

---

## Dagster Schedules (all enabled)

| Schedule | Cron | What it does |
|---|---|---|
| equity_daily_schedule | 0 0 * * * | Fetch Yahoo Finance OHLCV for all 502 symbols, deduplicate Silver |
| macro_daily_schedule | 0 1 * * * | Fetch FRED macro indicators |
| fundamentals_daily_schedule | 0 2 * * * | Fetch Finnhub fundamentals (all 502 symbols, ~17 min) |
| data_quality_daily_schedule | 0 3 * * * | Run Great Expectations checks |
| cdc_instruments_schedule | */15 * * * * | Consume Debezium CDC events |

dbt Gold runs automatically via Task Scheduler at 1:00 PM IST (after all Dagster assets complete).
DuckDB concurrency: dagster.yaml mounted via volume into both dagster-daemon and dagster-webserver containers.

---

## CRITICAL Rules -- Must Know

- PowerShell file writes: ALWAYS use [System.IO.File]::WriteAllText with (New-Object System.Text.UTF8Encoding $false) -- never Set-Content or Add-Content for Python files
- Python edits with JS/HTML content: ALWAYS use Python scripts, never PowerShell string replacement
- API restart: NEVER use `docker restart agora-api` -- it loses env vars from .env. ALWAYS use: `docker compose -f infra/docker/docker-compose.yml up -d --no-deps --force-recreate api`
- API code changes: Need REBUILD not just restart -- `docker compose -f infra/docker/docker-compose.yml build api` then force-recreate. Hot-copy alone does NOT work (FastAPI caches modules at startup)
- API Dockerfile: does NOT copy DuckDB -- volume mount only. No rebuild needed after dbt Gold runs.
- Dagster code changes: Need REBUILD of dagster-user-code -- hot-copy does NOT work (gRPC server caches modules at startup). Always: build + force-recreate dagster-user-code
- Yahoo Finance: Direct HTTP only, NEVER yfinance download(). Use browser User-Agent + 0.5s sleep + 3 retries
- QuestDB SQL: Use LATEST ON timestamp PARTITION BY symbol -- NOT MAX subquery
- DuckDB table full path: agora.main.silver_equity_ohlcv_daily (Silver), agora.main_gold.* (Gold)
- DuckDB lock: If "Permission denied" on agora.duckdb, check for docker-dbt-run-* orphan containers and kill them with: docker ps -a --filter "name=docker-dbt-run" --format "{{.Names}}" | ForEach-Object { docker rm -f $_ }
- FRED key: Goes in infra/docker/.env not project root .env -- needs full stop/start not restart
- Gold tables: agora.main_gold schema. dim_instruments has is_current column (SCD2). Filter WHERE is_current = TRUE for current records only.
- CDC: Debezium connector password is change_me_in_production (from infra/docker/.env). Schema Registry must be started separately. Kafka Connect on port 8083.
- AI Query Engine: Uses Groq llama-3.1-8b-instant (NOT Ollama). GROQ_API_KEY in infra/docker/.env, wired into docker-compose.yml under api service
- Groq network: requests library works from container (urllib gets 403 from Groq)
- dbt: dbt-core 1.11.7 on Windows via Python 3.12 venv at C:\dbt-env. Activate: C:\dbt-env\Scripts\Activate.ps1
- fct_macro schema: reads silver_macro_pulse NOT silver_macro_indicators. Columns: indicator, value, date, fetched_at
- fct_prices stale returns: run `dbt run --select fct_prices --full-refresh` if change_1d_pct shows 0 for symbols with data
- chart.py column order: after GROUP BY dedup, SELECT is (trade_date, open, high, low, close, volume) so r[0]=date, r[1]=open, r[2]=high, r[3]=low, r[4]=close, r[5]=volume
- Research Terminal: Supports equities (DuckDB Gold), indices (^GSPC, ^IXIC via Yahoo), crypto (BTC/ETH), forex (EURUSD), commodities (GOLD)
- Nav: 4 items only -- Overview / Research / Screener / Portfolio
- API URL: Auto-detects localhost vs production. localhost:8000 locally, api.agora-terminal.com in production
- DuckDB is mounted via volume into API container -- changes to agora.duckdb on disk are immediately visible to API
- Equity data: polygon_bronze.jsonl is NOT in git (gitignored). 502 symbols active. Backed up daily to MinIO agora-bronze/equity/.
- Silver dedup: dedup_silver.py runs on Windows host (not container). Uses Windows DuckDB path. Run: python scripts/dedup_silver.py
- Bronze backup: backup_bronze.py uses boto3 S3 API to MinIO on localhost:9000. Run: python scripts/backup_bronze.py
- Resend alerting: uses requests library (urllib gets 403 from Cloudflare). alert.py usage: python scripts/alert.py "subject" "body"
- Heatmap: SP500_TICKERS is now `let` (not const), dynamically populated from screener API top 55 by market cap
- Soft gate: gate.js in dashboard/src/modules/shared/. Tracks actions in localStorage key agora_action_count. Limit=3, resets to 1 on dismiss.
- User menu: toggleUserMenu() function in all 3 nav files. Avatar shows email initials. Menu has Account/About/Sign Out.
- httpx==0.27.0 added to api/Dockerfile for GitHub OAuth backend calls.
- dim_instruments: SCD Type 2 -- use WHERE is_current = TRUE for current records. 552 total rows (502 current, 50 historical).
- Screener: filters dim_instruments WHERE is_current IS TRUE to avoid duplicate rows from SCD2 history.
- Fundamentals symbol list: data-driven from silver_equity_ohlcv_daily, not hardcoded
- Screener API: has symbol filter parameter (GET /api/screener/screen?symbol=TSLA&limit=1)
- Research Terminal: screener call is now symbol-specific (not limit=200 bulk fetch)
- DWH audit scripts: scripts/schema_audit.py, scripts/dwh_audit.py, scripts/orphan_audit.py
- Finnhub API key: FINNHUB_API_KEY=d7hl6npr01qhiu0c1g40d7hl6npr01qhiu0c1g4g in infra/docker/.env, wired into docker-compose.yml under all 3 dagster services
- Finnhub base URL: https://finnhub.io/api/v1, uses X-Finnhub-Token header
- Finnhub endpoints: /stock/profile2 (company info) + /stock/metric?metric=all (all ratios)
- Market cap from Finnhub is in millions -- multiply by 1,000,000 before storing
- startup.ps1: cd C:\Projects\agora-terminal\agora-terminal && .\scripts\startup.ps1
- dbt Gold startup: startup.ps1 does fct_prices --full-refresh automatically
- DuckDB lock on dbt run: stop API first with `docker stop agora-api`, run dbt, then force-recreate api
- Fundamentals now uses Finnhub (not FMP) -- no daily cap, 60 calls/min, covers all 502 symbols in one run

---

## API Endpoints

| Endpoint | Source | Status |
|---|---|---|
| GET /health | Internal | Live |
| GET /api/crypto/prices | QuestDB | Live (change% shows 0 -- streaming pipeline down locally) |
| GET /api/crypto/ohlcv/{symbol} | QuestDB | Live |
| GET /api/equity/symbols | DuckDB Silver | Live |
| GET /api/equity/ohlcv/{symbol} | DuckDB Silver | Live |
| GET /api/equity/heatmap | DuckDB Silver | Live (daily % change for all 502 symbols) |
| GET /api/equity/indices | Yahoo Finance | Live |
| GET /api/macro/pulse | FRED API | Live |
| GET /api/macro/forex | Yahoo Finance | Live |
| GET /api/macro/commodities | Yahoo Finance | Live |
| POST /api/ai/query | Groq + DuckDB Gold | Live (NL to SQL, ~1.5s) |
| GET /api/chart/ohlcv/{symbol} | DuckDB Silver / Yahoo | Live (GROUP BY trade_date deduped, timeframe: 1W/1M/3M/6M/1Y/MAX) |
| GET /api/chart/info/{symbol} | DuckDB Gold | Live |
| GET /api/chart/symbols | DuckDB Silver | Live (502 symbols from dim_instruments) |
| GET /api/screener/screen | DuckDB Gold | Live (limit, sort_by, sort_dir, symbol filter, top 55 used by heatmap) |
| GET /api/screener/sectors | DuckDB Gold | Live |
| GET /api/screener/search | DuckDB Gold | Live (fuzzy search) |
| GET /api/screener/peers/{symbol} | DuckDB Gold | Live (sector peers from fct_prices + dim_instruments) |
| GET /api/filings/{symbol} | EDGAR + Groq | Live (10-K/10-Q/8-K with AI summaries) |
| POST /auth/register | PostgreSQL | Live |
| POST /auth/login | PostgreSQL | Live |
| POST /auth/google | Google OAuth | Live |
| GET /auth/github | GitHub OAuth | Live (redirects to GitHub) |
| GET /auth/github/callback | GitHub OAuth | Live (exchanges code, issues JWT) |

---

## Key Services (Local)

| Service | Port | Notes |
|---|---|---|
| FastAPI | 8000 | Main API (also via api.agora-terminal.com) |
| Dagster | 3000 | Orchestration UI |
| MinIO | 9001 | Object storage UI (Bronze backup at agora-bronze/equity/) |
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
- dbt Gold cannot run inside dagster-user-code (protobuf conflict with dagster 1.7.7 + dbt-core 1.11.7) -- automated via Windows Task Scheduler instead
- API Dockerfile does NOT copy DuckDB -- volume mount only. Avoids lock conflicts and stale data after dbt runs.
- DuckDB single-writer limitation: only one read-write connection at a time. docker-dbt-run-* orphans accumulate and cause lock -- cleaned up daily by run_dbt_gold.ps1
- Oracle free tier kept as backup server -- API only, not primary
- Dagster 1.7.7 with software-defined assets, daily schedules, GE standalone (no dagster-ge)
- DuckDB for Silver -- in-process, reads Parquet, perfect for FastAPI container
- QuestDB for OHLCV -- time-series optimized, ILP write, PostgreSQL wire for reads
- Groq for AI Query Engine -- much faster than Ollama CPU, follows schema instructions reliably
- Research Terminal is the unified module -- Chart Terminal and AI Query merged in
- TradingView Lightweight Charts v4 (pinned, MIT) for charting
- Non-equity symbols (indices, crypto, forex) use Yahoo Finance fallback in chart API
- GitHub OAuth uses httpx for backend token exchange (requests also works, urllib gets 403)
- Peer comparison strip uses /api/screener/peers/{symbol}, returns sector peers from fct_prices + dim_instruments
- Pinned chips stored in localStorage key agora_pinned_chips, defaults defined in DEFAULT_PINS array
- About page copy: no price commitment -- "at a fraction of the cost" positioning
- dim_instruments SCD Type 2: full history preserved, filter WHERE is_current = TRUE for current records
- Screener deduplication: WHERE is_current IS TRUE prevents duplicate rows from SCD2 history
- Heatmap uses screener API (not /api/equity/heatmap) for top 55 -- screener has market_cap sorting and change_1d_pct
- Fundamentals symbol list: data-driven from silver_equity_ohlcv_daily, not hardcoded
- Screener API: has symbol filter parameter (GET /api/screener/screen?symbol=TSLA&limit=1)
- Research Terminal: screener call is now symbol-specific (not limit=200 bulk fetch)
- DWH audit scripts: scripts/schema_audit.py, scripts/dwh_audit.py, scripts/orphan_audit.py
- Resend for email alerting -- self-hosted email rejected by Gmail/Outlook. Resend free tier (3K/month) sufficient.
- Bronze JSONL backed up to MinIO daily -- restore from agora-bronze/equity/polygon_bronze_YYYY-MM-DD.jsonl if deleted
- fct_macro schema mismatch fixed: silver_macro_pulse has (indicator, value, date) not (series_id, observation_date)
- Silver dedup runs on Windows host not in container -- needs write access to DuckDB file directly
- Social Sentiment Engine: planned (Reddit PRAW + VADER + QuestDB), Reddit API policy flow blocked, retry later
- Cloud migration: Hetzner CAX21 target, on hold until Sweenal says go