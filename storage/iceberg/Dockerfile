FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir --timeout=300 \
    "numpy<2" \
    confluent-kafka==2.3.0 \
    "pyiceberg[s3filesystem,sql-sqlite]==0.6.1" \
    pyarrow==14.0.2 \
    boto3==1.34.0

COPY ingestion/ ./ingestion/
COPY storage/ ./storage/

CMD ["python", "-m", "storage.iceberg.bronze_writer"]
