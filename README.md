# Agora Terminal

> Bloomberg charges $24,000/year. We think that is wrong.

**[Live Demo](https://agora-terminal.com)** | [API Docs](https://api.agora-terminal.com/docs) | Apache 2.0

---

## Architecture

A production-grade financial data lakehouse built on the modern open source stack. Every number on the dashboard has full lineage from raw source to final pixel.

Binance WebSocket -> Kafka -> Bronze Iceberg (MinIO) -> Bytewax -> QuestDB (streaming)
Yahoo Finance / FRED / SEC EDGAR -> Dagster -> dbt Silver -> dbt Gold -> FastAPI -> Dashboard

**Data engineering patterns:**
- Medallion lakehouse (Bronze / Silver / Gold)
- CDC with Debezium + SCD Type 2 on dim_instruments
- Software-defined assets with Dagster (6 assets, 5 schedules)
- dbt dimensional models: fct_prices, fct_macro, fct_fundamentals, dim_instruments, dim_time
- Great Expectations data quality (23 checks across Silver and Gold)
- Natural language to SQL via Groq LLM

---

## Stack -- 100% Open Source

| Layer | Tool | Purpose |
|---|---|---|
| Ingestion | Apache Kafka | Message bus for all data streams |
| Ingestion | Debezium 2.6 | CDC for reference data changes |
| Storage | Apache Iceberg | Open table format (Bronze layer) |
| Storage | MinIO | S3-compatible local object storage |
| Storage | QuestDB | Time-series DB for streaming OHLCV |
| Processing | Bytewax | Python stream processing (1m candles) |
| Query | DuckDB | In-process OLAP -- Silver and Gold layers |
| Orchestration | Dagster 1.7.7 | Software-defined assets, lineage, scheduling |
| Transform | dbt Core | SQL transformations with 61 schema tests |
| Quality | Great Expectations | 23 data quality checks |
| AI | Groq (llama-3.1-8b) | NL to SQL inference |
| Serving | FastAPI | REST API |
| Frontend | Vanilla JS | TradingView Lightweight Charts v4 |
| Auth | Google + GitHub OAuth | JWT authentication |
| Infrastructure | Cloudflare Tunnel | Zero-config public API tunnel |

---

## Data Warehouse

| Table | Rows | Description |
|---|---|---|
| fct_prices | 283,000+ | S&P 500 daily OHLCV, 502 symbols |
| fct_macro | 9,000+ | 9 FRED series from 2015 |
| fct_fundamentals | 500+ | P/E, ROE, beta, EV/EBITDA, debt/equity |
| dim_instruments | 552 | Full SCD Type 2 -- 502 current, 50 historical |
| dim_time | 5,844 | Date dimension |

---

## Live Modules

| Module | Description | Status |
|---|---|---|
| Market Overview | Global indices, S&P 500 sector heatmap, macro pulse, real-time crypto ticker | Live |
| Research Terminal | Candlestick charts, fundamentals, SEC filings with AI summaries, peer comparison | Live |
| Asset Screener | Filter 502 symbols by P/B, ROE, EV/EBITDA, beta, debt/equity, FCF yield | Live |
| AI Query Engine | Natural language to SQL -- auditable answers with full SQL transparency | Live |
| Portfolio | Coming soon | Planned |

---

## API

GET  /api/chart/ohlcv/{symbol}     # OHLCV for any equity, index, crypto, forex
GET  /api/chart/info/{symbol}      # Fundamentals + price summary
GET  /api/screener/screen          # Filter assets by 16 metrics
GET  /api/screener/peers/{symbol}  # Sector peer comparison
GET  /api/macro/pulse              # Live FRED macro indicators (9 series)
GET  /api/filings/{symbol}         # SEC 10-K/10-Q/8-K with AI summaries
POST /api/ai/query                 # Natural language -> SQL -> results
GET  /api/health/data              # Data freshness per source

---

## Running Locally

```bash
git clone https://github.com/Sweenal18/agora-terminal.git
cd agora-terminal
docker compose -f infra/docker/docker-compose.yml up -d
cd dashboard/src/modules && python -m http.server 8080
```

Open http://localhost:8080/market_overview/index.html

Requirements: Docker Desktop, Python 3.11+

---

## Project Status

| Phase | Status |
|---|---|
| Phase 1: Kafka -> Bronze -> Silver -> Dashboard | Complete |
| Phase 2: Forex, commodities, FRED macro | Complete |
| Phase 3: Medallion lakehouse, Dagster, dbt Gold, GE, CDC | Complete |
| Phase 4: Research Terminal, Screener, AI Query Engine, SEC EDGAR | Active |
| Phase 5: Cloud migration, public API | Planned |

---

## Why This Exists

Bloomberg said it was very hard to build. He charged $24,000 a year and called it necessary. This project exists because one data engineer decided to find out how hard it really is.

Not to clone Bloomberg. To make Bloomberg irrelevant for the half of the world it never cared about. To give a researcher in Nairobi, a quant in Jakarta, a student in Sao Paulo -- the same quality of financial intelligence that today only exists inside the world's largest banks.

Linux did not beat Windows by being a better Windows. It made the question irrelevant by powering the entire internet underneath it. That is the model. That is the ambition.

---

## License

Apache 2.0 -- free to use, modify, and distribute.