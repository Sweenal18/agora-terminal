FROM python:3.11-slim

WORKDIR /app

COPY orchestration/dagster/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY orchestration/dagster/ .

ENV DAGSTER_HOME=/app/dagster_home
RUN mkdir -p /app/dagster_home
RUN cp /app/dagster.yaml /app/dagster_home/dagster.yaml

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-f", "definitions.py"]