# Agora Terminal

> Institutional-grade financial intelligence. Open source. Free forever.

Bloomberg Terminal charges $24,000/year. Agora Terminal is free.

![Stack](https://img.shields.io/badge/stack-Kafka%20%7C%20Iceberg%20%7C%20DuckDB%20%7C%20dbt%20%7C%20Dagster-00D4AA?style=flat-square)
![License](https://img.shields.io/badge/license-Apache%202.0-blue?style=flat-square)
![Status](https://img.shields.io/badge/status-Phase%204%20Active-green?style=flat-square)

---

## What It Is

Agora Terminal is a hybrid financial intelligence terminal — Bloomberg's analytical power, modern design language, open source architecture, and AI-native interface.

- **Infrastructure** — a production-grade financial data pipeline built on the modern open source data stack
- **Platform** — a composable API layer exposing clean, normalized financial metrics
- **Application** — a browser-based terminal that finance professionals open every morning

---

## Live Modules

| Module | Description | Status |
|--------|-------------|--------|
| **Market Overview** | Live global indices, S&P 500 sector heatmap, macro pulse (Fed rate, CPI, VIX), real-time crypto ticker, forex, commodities | ✅ Live |
| **Research Terminal** | Candlestick charts for any equity/index/crypto/forex, full fundamentals panel, AI research co-pilot with natural language queries | ✅ Live |
| **Asset Screener** | Filter the S&P 500 by 16 fundamental and technical metrics — P/B, ROE, EV/EBITDA, beta, MA position, 52W range | ✅ Live |
| **AI Query Engine** | Natural language to SQL against the Gold layer — auditable answers with full SQL transparency | ✅ Live |
| **Portfolio** | Coming soon | 🔜 Planned |

---

## Architecture

The system is built as a full medallion lakehouse — every data point carries lineage from raw source to final number.

```
Binance WebSocket ──► Kafka ──► Bronze Iceberg (MinIO)
                                      │
                               Bytewax OHLCV ──► QuestDB (tick data)
                                      │
                              dbt Silver (DuckDB)
                                      │
                               dbt Gold (DuckDB)
                                      │
                    ┌─────────────────┼─────────────────┐
                    │                 │                 │
               FastAPI            Dagster          Great Expectations
                    │
              Vanilla JS Dashboard
```

### Stack — 100% Open Source, $0

| Layer | Tool | Purpose |
|-------|------|---------|
| Ingestion | Apache Kafka | Message bus for all data streams |
| Ingestion | Debezium | CDC for reference data changes |
| Storage | Apache Iceberg | Open table format (Bronze layer) |
| Storage | MinIO | S3-compatible local object storage |
| Storage | QuestDB | Time-series database for OHLCV tick data |
| Processing | Bytewax | Python-native stream processing (1m candles) |
| Query | DuckDB | In-process OLAP — Silver and Gold layers |
| Orchestration | Dagster | Software-defined assets, lineage, scheduling |
| Transform | dbt Core | SQL transformations with 61 tests |
| Quality | Great Expectations | 23 data quality checks across all layers |
| AI | Groq API | LLM inference for natural language to SQL |
| Serving | FastAPI | REST API for dashboard and external consumers |
| Frontend | Vanilla JS | Zero-framework dashboard (TradingView charts) |

---

## Data Sources — All Free

| Source | Data | Cost |
|--------|------|------|
| Binance WebSocket | Real-time crypto tick data | Free forever |
| Yahoo Finance (HTTP) | Equities, indices, forex, commodities OHLCV | Free |
| FRED API | US macro data (Fed rate, CPI, GDP, unemployment, VIX) | Free forever |
| SEC EDGAR | US public filings | Free forever |
| FMP API | Fundamentals (P/B, ROE, beta, EV/EBITDA) | Free tier |

---

## API

The FastAPI backend exposes clean endpoints for all data layers:

```bash
GET  /api/chart/ohlcv/{symbol}     # OHLCV for any equity, index, crypto, forex
GET  /api/chart/info/{symbol}      # Fundamentals + price summary
GET  /api/screener/screen          # Filter assets by 16 metrics
GET  /api/screener/search          # Fuzzy search across symbols and companies
GET  /api/macro/pulse              # Live FRED macro indicators
GET  /api/crypto/prices            # Real-time crypto prices from QuestDB
POST /api/ai/query                 # Natural language → SQL → results
```

Every AI answer returns the SQL it ran, the data source queried, and the timestamp of the underlying data. No black boxes.

---

## Getting Started

```bash
# Clone the repo
git clone https://github.com/Sweenal18/agora-terminal.git
cd agora-terminal

# Start the full local stack
docker compose -f infra/docker/docker-compose.yml up -d

# Start the dashboard
cd dashboard/src/modules
python -m http.server 8080
# Open http://localhost:8080/research/index.html
```

**Requirements:** Docker Desktop, Python 3.11+

---

## Project Status

| Phase | Description | Status |
|-------|-------------|--------|
| Phase 1 | End-to-end prototype: Kafka → Bronze → Silver → Dashboard | ✅ Complete |
| Phase 2 | Forex, commodities, FRED macro, Oracle server deployment | ✅ Complete |
| Phase 3 | Full medallion lakehouse, Dagster orchestration, dbt Gold, Great Expectations, CDC | ✅ Complete |
| Phase 4 | Research Terminal, Asset Screener, AI Query Engine | 🔄 Active |
| Phase 5 | Cloud migration, public API, open source release | 🔜 Planned |

---

## Why This Exists

One person. One room. One mission.

Bloomberg said it was very hard to build. He charged $24,000 a year and called it necessary. This project exists because one data engineer — who learned everything from scratch — decided to find out how hard it really is.

Not to clone Bloomberg. To make Bloomberg irrelevant for the half of the world it never cared about. To give a researcher in Nairobi, a quant in Jakarta, a student in São Paulo — the same quality of financial intelligence that today only exists inside the world's largest banks.

**Linux did not beat Windows by being a better Windows. It made the question irrelevant by powering the entire internet underneath it. That is the model. That is the ambition. One commit at a time.**

---

## License

Apache 2.0 — free to use, modify, and distribute.
