import requests
import duckdb
import time
import re
import logging
import os
from fastapi import APIRouter, HTTPException
from groq import Groq

router = APIRouter()
logger = logging.getLogger(__name__)

DUCKDB_PATH = "/app/transform/dbt/agora.duckdb"
EDGAR_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
EDGAR_FILING_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession_plain}/{filename}"
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")

HEADERS = {
    "User-Agent": "Agora Terminal contact@agora-terminal.com",
    "Accept-Encoding": "gzip, deflate"
}

SUPPORTED_FORMS = {"10-K", "10-Q", "8-K"}


def get_cik(symbol: str) -> str:
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    row = con.execute(
        "SELECT cik FROM main.silver_cik_mapping WHERE symbol = ?",
        [symbol.upper()]
    ).fetchone()
    con.close()
    if not row:
        raise HTTPException(status_code=404, detail=f"No CIK found for symbol {symbol}")
    return row[0]


def fetch_recent_filings(cik: str, form_types: list, limit: int = 5) -> list:
    url = EDGAR_SUBMISSIONS_URL.format(cik=cik)
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    filings = data.get("filings", {}).get("recent", {})
    forms = filings.get("form", [])
    dates = filings.get("filingDate", [])
    accessions = filings.get("accessionNumber", [])
    docs = filings.get("primaryDocument", [])
    descriptions = filings.get("primaryDocDescription", [])

    results = []
    for i, form in enumerate(forms):
        if form in form_types and len(results) < limit:
            results.append({
                "form": form,
                "date": dates[i],
                "accession": accessions[i],
                "primary_doc": docs[i],
                "description": descriptions[i] if i < len(descriptions) else "",
            })
    return results


def fetch_filing_text(cik: str, accession: str, filename: str) -> str:
    accession_plain = accession.replace("-", "")
    url = EDGAR_FILING_URL.format(cik=int(cik), accession_plain=accession_plain, filename=filename)
    resp = requests.get(url, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    text = re.sub(r"<[^>]+>", " ", resp.text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:12000]


def summarize_with_groq(text: str, form_type: str, company: str) -> str:
    if not GROQ_API_KEY:
        return "AI summary unavailable — GROQ_API_KEY not set."
    client = Groq(api_key=GROQ_API_KEY)
    prompt = f"""You are a financial analyst. Summarize this {form_type} SEC filing for {company} in 4-5 sentences.
Focus on: key financial results, major risks, forward guidance, and anything unusual.
Be specific with numbers where present. Do not use filler phrases.

Filing text:
{text[:8000]}"""
    response = client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=400,
        temperature=0.3,
    )
    return response.choices[0].message.content.strip()


@router.get("/filings/{symbol}")
def get_filings(symbol: str, forms: str = "10-K,10-Q,8-K", limit: int = 5):
    """Get recent SEC filings with AI summaries for a symbol."""
    form_types = [f.strip() for f in forms.split(",") if f.strip() in SUPPORTED_FORMS]
    if not form_types:
        form_types = list(SUPPORTED_FORMS)

    cik = get_cik(symbol)
    time.sleep(0.1)
    filings = fetch_recent_filings(cik, form_types, limit)

    if not filings:
        return {"symbol": symbol, "cik": cik, "filings": []}

    results = []
    for filing in filings:
        summary = None
        try:
            time.sleep(0.15)
            text = fetch_filing_text(cik, filing["accession"], filing["primary_doc"])
            summary = summarize_with_groq(text, filing["form"], symbol)
        except Exception as e:
            logger.warning(f"Could not fetch/summarize {filing['accession']}: {e}")
            summary = "Summary unavailable for this filing."

        results.append({
            "form": filing["form"],
            "date": filing["date"],
            "accession": filing["accession"],
            "description": filing["description"],
            "summary": summary,
            "edgar_url": f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{filing['accession'].replace('-','')}/{filing['primary_doc']}",
        })

    return {"symbol": symbol.upper(), "cik": cik, "filings": results}