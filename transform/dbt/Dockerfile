FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir --timeout=300 dbt-duckdb==1.9.1

COPY transform/dbt/ ./transform/dbt/
COPY polygon_bronze.jsonl ./polygon_bronze.jsonl

CMD ["dbt", "run", "--project-dir", "transform/dbt/agora", "--profiles-dir", "transform/dbt"]
