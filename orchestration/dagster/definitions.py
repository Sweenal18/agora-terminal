"""
Agora Terminal — Dagster Definitions
All software-defined assets for the financial data pipeline
"""
from dagster import (
    Definitions,
    load_assets_from_modules,
    define_asset_job,
    ScheduleDefinition,
)
from assets import fundamentals, equity, macro

all_assets = load_assets_from_modules([fundamentals, equity, macro])

fundamentals_job = define_asset_job(
    name="fundamentals_daily",
    selection=["silver_equity_fundamentals"],
)

macro_job = define_asset_job(
    name="macro_daily",
    selection=["silver_macro_pulse"],
)

equity_job = define_asset_job(
    name="equity_daily",
    selection=["silver_equity_ohlcv_daily"],
)

fundamentals_schedule = ScheduleDefinition(
    job=fundamentals_job,
    cron_schedule="0 2 * * *",  # 2am UTC daily
    name="fundamentals_daily_schedule",
)

macro_schedule = ScheduleDefinition(
    job=macro_job,
    cron_schedule="0 1 * * *",  # 1am UTC daily
    name="macro_daily_schedule",
)

defs = Definitions(
    assets=all_assets,
    jobs=[fundamentals_job, macro_job, equity_job],
    schedules=[fundamentals_schedule, macro_schedule],
)