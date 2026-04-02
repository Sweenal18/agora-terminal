"""
Fundamentals fetcher - fetches company data from FMP and stores in DuckDB
Run this script daily to refresh fundamentals for all tracked symbols
"""
import os
import time
import logging
import requests
import duckdb
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("fundamentals")

FMP_API_KEY = os.getenv("FMP_API_KEY", "VnzpZ9HOSVb2qYeOLuSXqgqID58jFHsX")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "transform/dbt/agora.duckdb")
FMP_BASE    = "https://financialmodelingprep.com/stable"

SYMBOLS = ['MMM', 'AOS', 'ABT', 'ABBV', 'ACN', 'ADBE', 'AMD', 'AES', 'AFL', 'A', 'APD', 'ABNB', 'AKAM', 'ALB', 'ARE', 'ALGN', 'ALLE', 'LNT', 'ALL', 'GOOGL', 'GOOG', 'MO', 'AMZN', 'AMCR', 'AEE', 'AEP', 'AXP', 'AIG', 'AMT', 'AWK', 'AMP', 'AME', 'AMGN', 'APH', 'ADI', 'AON', 'APA', 'APO', 'AAPL', 'AMAT', 'APP', 'APTV', 'ACGL', 'ADM', 'ARES', 'ANET', 'AJG', 'AIZ', 'T', 'ATO', 'ADSK', 'ADP', 'AZO', 'AVB', 'AVY', 'AXON', 'BKR', 'BALL', 'BAC', 'BAX', 'BDX', 'BRK-B', 'BBY', 'TECH', 'BIIB', 'BLK', 'BX', 'XYZ', 'BK', 'BA', 'BKNG', 'BSX', 'BMY', 'AVGO', 'BR', 'BRO', 'BF-B', 'BLDR', 'BG', 'BXP', 'CHRW', 'CDNS', 'CPT', 'CPB', 'COF', 'CAH', 'CCL', 'CARR', 'CVNA', 'CAT', 'CBOE', 'CBRE', 'CDW', 'COR', 'CNC', 'CNP', 'CF', 'CRL', 'SCHW', 'CHTR', 'CVX', 'CMG', 'CB', 'CHD', 'CIEN', 'CI', 'CINF', 'CTAS', 'CSCO', 'C', 'CFG', 'CLX', 'CME', 'CMS', 'KO', 'CTSH', 'COHR', 'COIN', 'CL', 'CMCSA', 'FIX', 'CAG', 'COP', 'ED', 'STZ', 'CEG', 'COO', 'CPRT', 'GLW', 'CPAY', 'CTVA', 'CSGP', 'COST', 'CTRA', 'CRH', 'CRWD', 'CCI', 'CSX', 'CMI', 'CVS', 'DHR', 'DRI', 'DDOG', 'DVA', 'DECK', 'DE', 'DELL', 'DAL', 'DVN', 'DXCM', 'FANG', 'DLR', 'DG', 'DLTR', 'D', 'DPZ', 'DASH', 'DOV', 'DOW', 'DHI', 'DTE', 'DUK', 'DD', 'ETN', 'EBAY', 'SATS', 'ECL', 'EIX', 'EW', 'EA', 'ELV', 'EME', 'EMR', 'ETR', 'EOG', 'EPAM', 'EQT', 'EFX', 'EQIX', 'EQR', 'ERIE', 'ESS', 'EL', 'EG', 'EVRG', 'ES', 'EXC', 'EXE', 'EXPE', 'EXPD', 'EXR', 'XOM', 'FFIV', 'FDS', 'FICO', 'FAST', 'FRT', 'FDX', 'FIS', 'FITB', 'FSLR', 'FE', 'FISV', 'F', 'FTNT', 'FTV', 'FOXA', 'FOX', 'BEN', 'FCX', 'GRMN', 'IT', 'GE', 'GEHC', 'GEV', 'GEN', 'GNRC', 'GD', 'GIS', 'GM', 'GPC', 'GILD', 'GPN', 'GL', 'GDDY', 'GS', 'HAL', 'HIG', 'HAS', 'HCA', 'DOC', 'HSIC', 'HSY', 'HPE', 'HLT', 'HOLX', 'HD', 'HON', 'HRL', 'HST', 'HWM', 'HPQ', 'HUBB', 'HUM', 'HBAN', 'HII', 'IBM', 'IEX', 'IDXX', 'ITW', 'INCY', 'IR', 'PODD', 'INTC', 'IBKR', 'ICE', 'IFF', 'IP', 'INTU', 'ISRG', 'IVZ', 'INVH', 'IQV', 'IRM', 'JBHT', 'JBL', 'JKHY', 'J', 'JNJ', 'JCI', 'JPM', 'KVUE', 'KDP', 'KEY', 'KEYS', 'KMB', 'KIM', 'KMI', 'KKR', 'KLAC', 'KHC', 'KR', 'LHX', 'LH', 'LRCX', 'LVS', 'LDOS', 'LEN', 'LII', 'LLY', 'LIN', 'LYV', 'LMT', 'L', 'LOW', 'LULU', 'LITE', 'LYB', 'MTB', 'MPC', 'MAR', 'MRSH', 'MLM', 'MAS', 'MA', 'MKC', 'MCD', 'MCK', 'MDT', 'MRK', 'META', 'MET', 'MTD', 'MGM', 'MCHP', 'MU', 'MSFT', 'MAA', 'MRNA', 'TAP', 'MDLZ', 'MPWR', 'MNST', 'MCO', 'MS', 'MOS', 'MSI', 'MSCI', 'NDAQ', 'NTAP', 'NFLX', 'NEM', 'NWSA', 'NWS', 'NEE', 'NKE', 'NI', 'NDSN', 'NSC', 'NTRS', 'NOC', 'NCLH', 'NRG', 'NUE', 'NVDA', 'NVR', 'NXPI', 'ORLY', 'OXY', 'ODFL', 'OMC', 'ON', 'OKE', 'ORCL', 'OTIS', 'PCAR', 'PKG', 'PLTR', 'PANW', 'PSKY', 'PH', 'PAYX', 'PYPL', 'PNR', 'PEP', 'PFE', 'PCG', 'PM', 'PSX', 'PNW', 'PNC', 'POOL', 'PPG', 'PPL', 'PFG', 'PG', 'PGR', 'PLD', 'PRU', 'PEG', 'PTC', 'PSA', 'PHM', 'PWR', 'QCOM', 'DGX', 'Q', 'RL', 'RJF', 'RTX', 'O', 'REG', 'REGN', 'RF', 'RSG', 'RMD', 'RVTY', 'HOOD', 'ROK', 'ROL', 'ROP', 'ROST', 'RCL', 'SPGI', 'CRM', 'SNDK', 'SBAC', 'SLB', 'STX', 'SRE', 'NOW', 'SHW', 'SPG', 'SWKS', 'SJM', 'SW', 'SNA', 'SOLV', 'SO', 'LUV', 'SWK', 'SBUX', 'STT', 'STLD', 'STE', 'SYK', 'SMCI', 'SYF', 'SNPS', 'SYY', 'TMUS', 'TROW', 'TTWO', 'TPR', 'TRGP', 'TGT', 'TEL', 'TDY', 'TER', 'TSLA', 'TXN', 'TPL', 'TXT', 'TMO', 'TJX', 'TKO', 'TTD', 'TSCO', 'TT', 'TDG', 'TRV', 'TRMB', 'TFC', 'TYL', 'TSN', 'USB', 'UBER', 'UDR', 'ULTA', 'UNP', 'UAL', 'UPS', 'URI', 'UNH', 'UHS', 'VLO', 'VTR', 'VLTO', 'VRSN', 'VRSK', 'VZ', 'VRTX', 'VRT', 'VTRS', 'VICI', 'V', 'VST', 'VMC', 'WRB', 'GWW', 'WAB', 'WMT', 'DIS', 'WBD', 'WM', 'WAT', 'WEC', 'WFC', 'WELL', 'WST', 'WDC', 'WY', 'WSM', 'WMB', 'WTW', 'WDAY', 'WYNN', 'XEL', 'XYL', 'YUM', 'ZBRA', 'ZBH', 'ZTS']

def fetch_profile(symbol: str) -> dict:
    """Fetch company profile from FMP."""
    try:
        url = f"{FMP_BASE}/profile?symbol={symbol}&apikey={FMP_API_KEY}"
        resp = requests.get(url, timeout=10)
        data = resp.json()
        if data and len(data) > 0:
            return data[0]
        return {}
    except Exception as e:
        log.error(f"Profile fetch failed for {symbol}: {e}")
        return {}

def fetch_ratios(symbol: str) -> dict:
    """Fetch financial ratios from FMP."""
    try:
        url = f"{FMP_BASE}/ratios?symbol={symbol}&apikey={FMP_API_KEY}"
        resp = requests.get(url, timeout=10)
        data = resp.json()
        if data and len(data) > 0:
            return data[0]
        return {}
    except Exception as e:
        log.error(f"Ratios fetch failed for {symbol}: {e}")
        return {}

def fetch_key_metrics(symbol: str) -> dict:
    """Fetch key metrics from FMP."""
    try:
        url = f"{FMP_BASE}/key-metrics?symbol={symbol}&apikey={FMP_API_KEY}"
        resp = requests.get(url, timeout=10)
        data = resp.json()
        if data and len(data) > 0:
            return data[0]
        return {}
    except Exception as e:
        log.error(f"Key metrics fetch failed for {symbol}: {e}")
        return {}

def safe_float(val, default=None):
    try:
        if val is None or val == "":
            return default
        return float(val)
    except Exception:
        return default

def create_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS silver_equity_fundamentals (
            symbol VARCHAR,
            company_name VARCHAR,
            sector VARCHAR,
            industry VARCHAR,
            exchange VARCHAR,
            country VARCHAR,
            market_cap DOUBLE,
            beta DOUBLE,
            avg_volume BIGINT,
            week_52_high DOUBLE,
            week_52_low DOUBLE,
            price_to_sales DOUBLE,
            price_to_book DOUBLE,
            dividend_yield DOUBLE,
            roe DOUBLE,
            roic DOUBLE,
            ev_to_ebitda DOUBLE,
            current_ratio DOUBLE,
            debt_to_equity DOUBLE,
            free_cash_flow_yield DOUBLE,
            description VARCHAR,
            logo_url VARCHAR,
            ceo VARCHAR,
            employees INTEGER,
            website VARCHAR,
            updated_at TIMESTAMP,
            PRIMARY KEY (symbol)
        )
    """)

def upsert_fundamentals(conn, data: dict):
    conn.execute("""
        INSERT OR REPLACE INTO silver_equity_fundamentals VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """, [
        data.get("symbol"),
        data.get("company_name"),
        data.get("sector"),
        data.get("industry"),
        data.get("exchange"),
        data.get("country"),
        safe_float(data.get("market_cap")),
        safe_float(data.get("beta")),
        data.get("avg_volume"),
        safe_float(data.get("week_52_high")),
        safe_float(data.get("week_52_low")),
        safe_float(data.get("price_to_sales")),
        safe_float(data.get("price_to_book")),
        safe_float(data.get("dividend_yield")),
        safe_float(data.get("roe")),
        safe_float(data.get("roic")),
        safe_float(data.get("ev_to_ebitda")),
        safe_float(data.get("current_ratio")),
        safe_float(data.get("debt_to_equity")),
        safe_float(data.get("free_cash_flow_yield")),
        data.get("description"),
        data.get("logo_url"),
        data.get("ceo"),
        data.get("employees"),
        data.get("website"),
        datetime.now(timezone.utc),
    ])

def main():
    log.info(f"Starting fundamentals fetch for {len(SYMBOLS)} symbols")
    conn = duckdb.connect(DUCKDB_PATH)
    create_table(conn)
    success = 0
    for i, symbol in enumerate(SYMBOLS):
        log.info(f"[{i+1}/{len(SYMBOLS)}] Fetching {symbol}...")
        profile     = fetch_profile(symbol)
        ratios      = fetch_ratios(symbol)
        metrics     = fetch_key_metrics(symbol)
        if not profile:
            log.warning(f"No profile data for {symbol}, skipping")
            time.sleep(0.5)
            continue
        range_str = profile.get("range", "-")
        week_52_low, week_52_high = None, None
        if "-" in str(range_str):
            parts = str(range_str).split("-")
            if len(parts) == 2:
                week_52_low  = safe_float(parts[0].strip())
                week_52_high = safe_float(parts[1].strip())
        data = {
            "symbol":             symbol,
            "company_name":       profile.get("companyName"),
            "sector":             profile.get("sector"),
            "industry":           profile.get("industry"),
            "exchange":           profile.get("exchange"),
            "country":            profile.get("country"),
            "market_cap":         profile.get("marketCap"),
            "beta":               profile.get("beta"),
            "avg_volume":         profile.get("averageVolume"),
            "week_52_high":       week_52_high,
            "week_52_low":        week_52_low,
            "price_to_sales":     ratios.get("priceToSalesRatio"),
            "price_to_book":      ratios.get("priceToBookRatio"),
            "dividend_yield":     ratios.get("dividendYield"),
            "roe":                metrics.get("returnOnEquity"),
            "roic":               metrics.get("returnOnInvestedCapital"),
            "ev_to_ebitda":       metrics.get("evToEBITDA"),
            "current_ratio":      metrics.get("currentRatio"),
            "debt_to_equity":     ratios.get("debtToEquity"),
            "free_cash_flow_yield": metrics.get("freeCashFlowYield"),
            "description":        profile.get("description"),
            "logo_url":           profile.get("image"),
            "ceo":                profile.get("ceo"),
            "employees":          profile.get("fullTimeEmployees"),
            "website":            profile.get("website"),
        }
        upsert_fundamentals(conn, data)
        success += 1
        log.info(f"  Saved {symbol} — {data.get('company_name')} ({data.get('sector')})")
        time.sleep(0.3)
    conn.close()
    log.info(f"Done. {success}/{len(SYMBOLS)} symbols saved successfully.")

if __name__ == "__main__":
    main()