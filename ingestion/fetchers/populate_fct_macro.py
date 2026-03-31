"""
Agora Terminal - Direct fct_macro populator
Replicates the dbt fct_macro model logic in Python.
Run locally then docker cp DuckDB into container.
"""

import duckdb

DUCKDB_PATH = "transform/dbt/agora.duckdb"

SQL_DELETE = "DELETE FROM agora.main_gold.fct_macro"

SQL_INSERT = """
INSERT INTO agora.main_gold.fct_macro
SELECT
    CAST(STRFTIME(CAST(observation_date AS DATE), '%Y%m%d') AS INTEGER) AS date_key,
    series_id,
    CAST(observation_date AS DATE)                                       AS observation_date,
    series_name,
    unit,
    value                                                                AS indicator_value,
    CASE
        WHEN series_id IN ('DFF', 'FEDFUNDS')                          THEN 'interest_rate'
        WHEN series_id IN ('T10Y2Y', 'T10Y3M', 'T5Y5E')               THEN 'yield_curve'
        WHEN series_id IN ('T10YIE', 'CPIAUCSL', 'CPILFESL', 'PCEPI') THEN 'inflation'
        WHEN series_id IN ('UNRATE', 'ICSA', 'PAYEMS')                 THEN 'employment'
        WHEN series_id IN ('GDP', 'GDPC1', 'INDPRO')                   THEN 'growth'
        WHEN series_id IN ('VIXCLS', 'BAMLH0A0HYM2')                  THEN 'risk_sentiment'
        WHEN series_id IN ('M2SL', 'M1SL')                             THEN 'money_supply'
        ELSE 'other'
    END::VARCHAR                                                         AS series_category,
    CASE
        WHEN series_id IN ('DFF','FEDFUNDS','T10Y2Y','T10Y3M','T10YIE','VIXCLS','BAMLH0A0HYM2') THEN 'daily'
        WHEN series_id IN ('CPIAUCSL','CPILFESL','UNRATE','ICSA','PAYEMS','M2SL','M1SL')         THEN 'monthly'
        WHEN series_id IN ('GDP','GDPC1','INDPRO')                                               THEN 'quarterly'
        ELSE 'unknown'
    END::VARCHAR                                                         AS reporting_frequency,
    processed_at                                                         AS source_processed_at,
    CURRENT_TIMESTAMP::TIMESTAMPTZ                                       AS dbt_loaded_at
FROM agora.main.silver_macro_indicators
WHERE observation_date IS NOT NULL
  AND value IS NOT NULL
"""

def main():
    print("=== Populating fct_macro from silver_macro_indicators ===")
    con = duckdb.connect(DUCKDB_PATH)
    try:
        silver_count = con.execute("SELECT COUNT(*) FROM agora.main.silver_macro_indicators").fetchone()[0]
        print(f"silver_macro_indicators: {silver_count} rows")

        print("Clearing fct_macro...")
        con.execute(SQL_DELETE)

        print("Running transformation...")
        con.execute(SQL_INSERT)

        gold_count = con.execute("SELECT COUNT(*) FROM agora.main_gold.fct_macro").fetchone()[0]
        print(f"fct_macro: {gold_count} rows")

        print("\nRows by series_id:")
        rows = con.execute("""
            SELECT series_id, series_category, reporting_frequency,
                   COUNT(*) as cnt,
                   MIN(observation_date) as earliest,
                   MAX(observation_date) as latest
            FROM agora.main_gold.fct_macro
            GROUP BY series_id, series_category, reporting_frequency
            ORDER BY series_id
        """).fetchall()
        for r in rows:
            print(f"  {r[0]:12} | {r[1]:15} | {r[2]:10} | {r[3]:5} rows | {r[4]} to {r[5]}")

    finally:
        con.close()
    print("\nDone. Next: docker cp transform/dbt/agora.duckdb agora-api:/app/transform/dbt/agora.duckdb")

if __name__ == "__main__":
    main()