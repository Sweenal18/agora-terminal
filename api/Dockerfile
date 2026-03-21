FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir --timeout=300 \
    fastapi==0.110.0 \
    uvicorn[standard]==0.27.0 \
    psycopg2-binary==2.9.9 \
    duckdb==1.1.0 \
    requests==2.31.0 \
    yfinance==0.2.54

COPY api/ ./api/
COPY transform/dbt/agora.duckdb ./transform/dbt/agora.duckdb

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]