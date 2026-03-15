FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir --timeout=300 \
    bytewax==0.19.0 \
    confluent-kafka==2.3.0

COPY processing/ ./processing/

CMD ["python", "-m", "processing.bytewax.jobs.ohlcv_processor"]