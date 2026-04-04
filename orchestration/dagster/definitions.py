"""
Agora Terminal -- Dagster Definitions
All software-defined assets for the financial data pipeline
"""
from dagster import (
    Definitions,
    load_assets_from_modules,
    define_asset_job,
    ScheduleDefinition,
    RetryPolicy,
    Backoff,
)
from assets import fundamentals, equity, macro, data_quality, cdc

duckdb_retry = RetryPolicy(max_retries=3, delay=30, backoff=Backoff.EXPONENTIAL)

all_assets = load_assets_from_modules(
    [fundamentals, equity, macro, data_quality, cdc],
)

fundamentals_job = define_asset_job(name="fundamentals_daily", selection=["silver_equity_fundamentals"])
macro_job = define_asset_job(name="macro_daily", selection=["silver_macro_pulse"])
equity_job = define_asset_job(name="equity_daily", selection=["silver_equity_ohlcv_daily"])
data_quality_job = define_asset_job(name="data_quality_daily", selection=["silver_data_quality", "gold_data_quality"])
cdc_job = define_asset_job(name="cdc_instruments", selection=["silver_instruments_cdc"])

fundamentals_schedule = ScheduleDefinition(job=fundamentals_job, cron_schedule="0 2 * * *", name="fundamentals_daily_schedule")
macro_schedule = ScheduleDefinition(job=macro_job, cron_schedule="0 1 * * *", name="macro_daily_schedule")
equity_schedule = ScheduleDefinition(job=equity_job, cron_schedule="0 0 * * *", name="equity_daily_schedule")
data_quality_schedule = ScheduleDefinition(job=data_quality_job, cron_schedule="0 3 * * *", name="data_quality_daily_schedule")
cdc_schedule = ScheduleDefinition(job=cdc_job, cron_schedule="*/15 * * * *", name="cdc_instruments_schedule")

defs = Definitions(
    assets=all_assets,
    jobs=[fundamentals_job, macro_job, equity_job, data_quality_job, cdc_job],
    schedules=[fundamentals_schedule, macro_schedule, equity_schedule, data_quality_schedule, cdc_schedule],
)