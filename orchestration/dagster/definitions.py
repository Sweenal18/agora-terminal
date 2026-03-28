"""
Agora Terminal — Dagster Definitions
All software-defined assets for the financial data pipeline
"""
from dagster import Definitions, load_assets_from_modules
from assets import fundamentals, equity, macro

all_assets = load_assets_from_modules([fundamentals, equity, macro])

defs = Definitions(assets=all_assets)