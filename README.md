# Agora Terminal

> Institutional-grade financial intelligence. Open source. Free forever.

Bloomberg Terminal charges $24,000/year. Agora Terminal is free.

## What It Is

Agora Terminal is a hybrid financial intelligence terminal — Bloomberg's analytical power, modern design language, open source architecture, and AI-native interface.

- **Infrastructure** — a production-grade financial data pipeline any developer can deploy
- **Platform** — a composable API layer exposing clean, normalized financial metrics
- **Application** — a browser-based terminal that finance professionals open every morning

## The Five Modules

| Module | Description | Status |
|--------|-------------|--------|
| Market Overview | Live indices, sector heatmap, macro pulse, crypto, forex, AI signals | UI Prototype Done |
| Chart Terminal | Candlestick charts, 50+ indicators, multi-timeframe, AI analysis | Not Started |
| Asset Screener | Filter by fundamentals, technicals, macro — natural language queries | Not Started |
| Research Terminal | SEC filings, earnings, insider data, AI research co-pilot | Not Started |
| AI Query Engine | Natural language to SQL — auditable, data-backed answers | Not Started |

## Stack — 100% Open Source, $0

| Tool | Purpose |
|------|---------|
| Apache Kafka | Message bus for all data streams |
| Apache Iceberg | Open table format (lakehouse) |
| MinIO | S3-compatible object storage |
| QuestDB | Time-series database for tick data |
| Bytewax | Python-native stream processing |
| DuckDB | In-process OLAP query engine |
| Dagster | Pipeline orchestration |
| dbt Core | SQL transformations |
| Great Expectations | Data quality |
| Cube.dev | Metrics semantic layer |
| Ollama + LangChain | Local AI inference |
| React + Vite | Frontend |

## Project Phases

| Phase | Timeline | Cost | Status |
|-------|----------|------|--------|
| Phase 0 — Planning | Weeks 1-2 | $0/mo | In Progress |
| Phase 1 — Prototype | Months 1-2 | $0/mo | Not Started |
| Phase 2 — First Users | Months 3-5 | $46/mo | Not Started |
| Phase 3 — Rebuild Right | Months 6-10 | $564/mo | Not Started |
| Phase 4 — Platform | Months 11-18 | $564+/mo | Not Started |

## Getting Started
```bash
# Start the full local stack
docker compose -f infra/docker/docker-compose.yml up -d

# Verify all services are healthy
python scripts/verify_stack.py
```

See [docs/runbooks/local-setup.md](docs/runbooks/local-setup.md) for the full guide.

## Why This Exists

One person. One room. One mission.

Bloomberg said it was very hard to build. He charged $24,000 a year and called it necessary. This project exists because one data engineer decided to find out how hard it really is.

Not to clone Bloomberg. To make Bloomberg irrelevant for the half of the world it never cared about.

**Linux did not beat Windows by being a better Windows. It made the question irrelevant by powering the entire internet underneath it. That is the model. That is the ambition. One commit at a time.**

## License

Apache 2.0
