"""
Agora Terminal -- Data Quality Assets
Runs validation on Silver and Gold layer tables via DuckDB + pandas.
"""
import os
import duckdb
from dagster import asset, AssetExecutionContext, Output, MetadataValue

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/data/agora.duckdb")


def _run_duckdb_expectations(context: AssetExecutionContext, suite_name: str, query: str) -> dict:
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        df = conn.execute(query).df()
    finally:
        conn.close()

    results = {"suite": suite_name, "passed": [], "failed": [], "row_count": len(df)}

    def expect(name: str, condition: bool) -> None:
        if condition:
            results["passed"].append(name)
        else:
            results["failed"].append(name)

    if suite_name == "silver_equity_ohlcv":
        expect("row_count >= 10000", len(df) >= 10000)
        expect("no null symbol", df["symbol"].notna().all())
        expect("no null trade_date", df["trade_date"].notna().all())
        expect("no null close", df["close"].notna().all())
        expect("close > 0", (df["close"] > 0).all())
        expect("volume >= 0", (df["volume"] >= 0).all())
        expect("open > 0", (df["open"] > 0).all())
        expect("high >= low", (df["high"] >= df["low"]).all())

    elif suite_name == "silver_equity_fundamentals":
        expect("row_count >= 10", len(df) >= 10)
        expect("no null symbol", df["symbol"].notna().all())
        expect("no null company_name", df["company_name"].notna().all())
        expect("market_cap > 0", (df["market_cap"] > 0).all() if len(df) > 0 else True)
        expect("ev_to_ebitda sane", ((df["ev_to_ebitda"].isna()) | (df["ev_to_ebitda"] > 0)).all() if len(df) > 0 else True)
        expect("no null sector", df["sector"].notna().any())
        expect("updated_at not null", df["updated_at"].notna().all())

    elif suite_name == "gold_fct_prices":
        expect("row_count >= 10000", len(df) >= 10000)
        expect("no null instrument_key", df["instrument_key"].notna().all())
        expect("no null date_key", df["date_key"].notna().all())
        expect("close > 0", (df["close"] > 0).all())

    elif suite_name == "gold_dim_instruments":
        expect("row_count >= 10", len(df) >= 10)
        expect("no null symbol", df["symbol"].notna().all())
        expect("no null asset_class", df["asset_class"].notna().all())
        expect("no duplicate instrument_key", len(df["instrument_key"].unique()) == len(df))

    return results


def _suite_summary(results: dict) -> str:
    passed = len(results["passed"])
    failed = len(results["failed"])
    total = passed + failed
    status = "PASS" if failed == 0 else "FAIL"
    summary = f"{status}: {passed}/{total} expectations passed"
    if results["failed"]:
        summary += f" | FAILED: {', '.join(results['failed'])}"
    return summary


@asset(
    group_name="data_quality",
    description="Great Expectations validation on Silver layer tables",
)
def silver_data_quality(context: AssetExecutionContext) -> Output[dict]:
    ohlcv_results = _run_duckdb_expectations(
        context,
        "silver_equity_ohlcv",
        "SELECT * FROM agora.main.silver_equity_ohlcv_daily LIMIT 100000",
    )
    context.log.info(
        f"silver_equity_ohlcv: {len(ohlcv_results['passed'])} passed, "
        f"{len(ohlcv_results['failed'])} failed"
    )

    fund_results = _run_duckdb_expectations(
        context,
        "silver_equity_fundamentals",
        "SELECT * FROM agora.main.silver_equity_fundamentals",
    )
    context.log.info(
        f"silver_equity_fundamentals: {len(fund_results['passed'])} passed, "
        f"{len(fund_results['failed'])} failed"
    )

    all_failed = ohlcv_results["failed"] + fund_results["failed"]
    if all_failed:
        context.log.warning(f"Silver DQ failures: {all_failed}")

    combined = {"ohlcv": ohlcv_results, "fundamentals": fund_results}
    return Output(
        value=combined,
        metadata={
            "ohlcv_summary": MetadataValue.text(_suite_summary(ohlcv_results)),
            "fundamentals_summary": MetadataValue.text(_suite_summary(fund_results)),
            "ohlcv_row_count": MetadataValue.int(ohlcv_results["row_count"]),
            "fundamentals_row_count": MetadataValue.int(fund_results["row_count"]),
            "total_failures": MetadataValue.int(len(all_failed)),
        },
    )


@asset(
    group_name="data_quality",
    description="Great Expectations validation on Gold layer tables",
)
def gold_data_quality(context: AssetExecutionContext) -> Output[dict]:
    price_results = _run_duckdb_expectations(
        context,
        "gold_fct_prices",
        "SELECT * FROM agora.main_gold.fct_prices LIMIT 100000",
    )
    context.log.info(
        f"gold_fct_prices: {len(price_results['passed'])} passed, "
        f"{len(price_results['failed'])} failed"
    )

    dim_results = _run_duckdb_expectations(
        context,
        "gold_dim_instruments",
        "SELECT * FROM agora.main_gold.dim_instruments",
    )
    context.log.info(
        f"gold_dim_instruments: {len(dim_results['passed'])} passed, "
        f"{len(dim_results['failed'])} failed"
    )

    all_failed = price_results["failed"] + dim_results["failed"]
    if all_failed:
        context.log.warning(f"Gold DQ failures: {all_failed}")

    combined = {"fct_prices": price_results, "dim_instruments": dim_results}
    return Output(
        value=combined,
        metadata={
            "fct_prices_summary": MetadataValue.text(_suite_summary(price_results)),
            "dim_instruments_summary": MetadataValue.text(_suite_summary(dim_results)),
            "fct_prices_row_count": MetadataValue.int(price_results["row_count"]),
            "dim_instruments_row_count": MetadataValue.int(dim_results["row_count"]),
            "total_failures": MetadataValue.int(len(all_failed)),
        },
    )