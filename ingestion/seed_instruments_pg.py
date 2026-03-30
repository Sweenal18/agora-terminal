"""
Agora Terminal -- Seed PostgreSQL instruments table from DuckDB silver layer.
Reads silver_equity_fundamentals and upserts into PostgreSQL public.instruments.
Debezium will then capture these as CDC events.
"""
import os
import duckdb
import psycopg2
from datetime import datetime, timezone

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "transform/dbt/agora.duckdb")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "agora")
PG_USER = os.getenv("PG_USER", "agora")
PG_PASSWORD = os.getenv("PG_PASSWORD", "change_me_in_production")


def main() -> None:
    print("Connecting to DuckDB...")
    duck = duckdb.connect(DUCKDB_PATH, read_only=True)
    df = duck.execute("""
        SELECT
            symbol,
            company_name,
            sector,
            industry,
            exchange,
            country,
            market_cap,
            updated_at
        FROM agora.main.silver_equity_fundamentals
        WHERE symbol IS NOT NULL
        ORDER BY symbol
    """).df()
    duck.close()
    print(f"Loaded {len(df)} instruments from DuckDB")

    print("Connecting to PostgreSQL...")
    pg = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )
    cur = pg.cursor()

    upserted = 0
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO public.instruments
                (symbol, company_name, sector, industry, exchange, country,
                 currency, asset_class, market_cap, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol) DO UPDATE SET
                company_name = EXCLUDED.company_name,
                sector = EXCLUDED.sector,
                industry = EXCLUDED.industry,
                exchange = EXCLUDED.exchange,
                country = EXCLUDED.country,
                currency = EXCLUDED.currency,
                asset_class = EXCLUDED.asset_class,
                market_cap = EXCLUDED.market_cap,
                updated_at = EXCLUDED.updated_at
        """, (
            row["symbol"],
            row["company_name"],
            row["sector"],
            row["industry"],
            row["exchange"],
            row["country"],
            "USD",
            "equity",
            float(row["market_cap"]) if row["market_cap"] else None,
            row["updated_at"].to_pydatetime() if row["updated_at"] is not None else datetime.now(timezone.utc),
        ))
        upserted += 1

    pg.commit()
    cur.close()
    pg.close()
    print(f"Done. {upserted} instruments upserted into PostgreSQL.")


if __name__ == "__main__":
    main()