FROM python:3.11-slim
WORKDIR /app
COPY orchestration/dagster/requirements.txt .
RUN pip install --no-cache-dir --timeout=600 --retries=5 -r requirements.txt
COPY orchestration/dagster/ .
COPY great_expectations/ ./great_expectations/
ENV DAGSTER_HOME=/app/dagster_home
ENV GE_ROOT=/app/great_expectations
ENV PYTHONPATH=/app
RUN mkdir -p /app/dagster_home
RUN cp /app/dagster.yaml /app/dagster_home/dagster.yaml
RUN cp /app/workspace.yaml /app/dagster_home/workspace.yaml
RUN ln -sf /app/definitions.py /app/dagster_home/definitions.py
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "/app/dagster_home/workspace.yaml"]