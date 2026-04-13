"""
utils.py — Shared Utilities for Institutional Intelligence Platform
============================================================
Consolidated from schema_map.py and sec_url_utils.py.
Provides financial column mapping, fuzzy matching, and SEC EDGAR URL resolution.
"""

import difflib
import time
import logging
import re
import requests
from typing import Optional, List, Dict, Any
from platform_config import TARGET_COMPANIES

log = logging.getLogger("utils")

# ── 1. Financial Schema Mapping ───────────────────────────────────────────────

FINANCIAL_COLUMNS = {
    "ticker", "company_name", "fiscal_year", "fiscal_period", "end_date",
    "revenue", "net_income", "total_assets", "total_liabilities",
    "total_equity", "eps_diluted", "operating_income", "cash_on_hand",
    "operating_expense", "gross_profit", "ebitda", "free_cash_flow"
}

ENHANCED_METRIC_MAP = {
    "sales": "revenue", "net sales": "revenue", "total revenue": "revenue",
    "revenue from operations": "revenue", "revenue": "revenue",
    "net profit": "net_income", "profit after tax": "net_income", "pat": "net_income",
    "net income": "net_income", "net loss": "net_income",
    "operating profit": "operating_income", "operating income": "operating_income", "ebit": "operating_income",
    "total assets": "total_assets", "assets": "total_assets",
    "total liabilities": "total_liabilities", "liabilities": "total_liabilities",
    "total equity": "total_equity", "shareholders equity": "total_equity", "equity": "total_equity",
    "eps": "eps_diluted", "earnings per share": "eps_diluted", "dividend": "dividend_yield",
    "cash & equivalents": "cash_on_hand", "cash equivalents": "cash_on_hand", "cash": "cash_on_hand",
    "operating expense": "operating_expense", "total expenses": "operating_expense",
    "gross profit": "gross_profit", "ebitda": "ebitda", "free cash flow": "free_cash_flow", "fcf": "free_cash_flow"
}

def fuzzy_match(label: str, choices: list[str], threshold: float = 0.85) -> str | None:
    label = label.lower().strip()
    matches = difflib.get_close_matches(label, choices, n=1, cutoff=threshold)
    return matches[0] if matches else None


# ── 2. SEC URL Utilities ──────────────────────────────────────────────────────

SEC_HEADERS = {
    "User-Agent": "Doc-extract-and-report samaygangwal21@gmail.com",
    "Accept": "application/json",
}

SUBMISSIONS_BASE = "https://data.sec.gov/submissions"
ARCHIVES_BASE    = "https://www.sec.gov/Archives/edgar/data"
IX_BASE          = "https://www.sec.gov/ix?doc=/Archives/edgar/data"
_SEC_DELAY       = 0.11

def normalize_accession(accession_number: str) -> str:
    return accession_number.replace("-", "")

def _fetch_submissions(cik_padded: str) -> Optional[dict]:
    url = f"{SUBMISSIONS_BASE}/CIK{cik_padded}.json"
    try:
        time.sleep(_SEC_DELAY)
        resp = requests.get(url, headers=SEC_HEADERS, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.warning(f"SEC submissions fetch failed for CIK {cik_padded}: {e}")
    return None

def _find_in_recent(filings_block: dict, accn_nodash: str) -> Optional[str]:
    recent = filings_block.get("recent", {})
    accns  = recent.get("accessionNumber", [])
    docs   = recent.get("primaryDocument", [])
    for i, raw_accn in enumerate(accns):
        if normalize_accession(raw_accn) == accn_nodash:
            return docs[i] if i < len(docs) else None
    return None

def _scrape_filing_index(cik_int: int, accn_nodash: str) -> Optional[str]:
    url = f"{ARCHIVES_BASE}/{cik_int}/{accn_nodash}/{accn_nodash}-index.json"
    try:
        time.sleep(_SEC_DELAY)
        resp = requests.get(url, headers=SEC_HEADERS, timeout=30)
        resp.raise_for_status()
        items = resp.json().get("directory", {}).get("item", [])
        for item in items:
            name = item.get("name", "")
            if name.lower().endswith((".htm", ".html")) and "-index" not in name.lower():
                return name
    except Exception: pass
    return None

def get_primary_document(cik: str, accession_number: str) -> Optional[str]:
    cik_padded = str(int(str(cik).lstrip("0") or "0")).zfill(10)
    cik_int    = int(cik_padded)
    accn_nodash = normalize_accession(accession_number)
    data = _fetch_submissions(cik_padded)
    if data:
        doc = _find_in_recent(data.get("filings", {}), accn_nodash)
        if doc: return doc
    return _scrape_filing_index(cik_int, accn_nodash)

def build_raw_url(cik: str, accession_number: str) -> Optional[str]:
    if not accession_number: return None
    cik_int = int(str(cik).lstrip("0") or "0")
    return f"{ARCHIVES_BASE}/{cik_int}/{normalize_accession(accession_number)}/"

def build_sec_ix_url(cik: str, accession_number: str) -> Optional[str]:
    if not accession_number: return None
    primary_doc = get_primary_document(cik, accession_number)
    if not primary_doc: return None
    cik_int = int(str(cik).lstrip("0") or "0")
    return f"{IX_BASE}/{cik_int}/{normalize_accession(accession_number)}/{primary_doc}"

def extract_accn_from_url(url: str) -> Optional[str]:
    if not url: return None
    match = re.search(r"/data/\d+/([0-9\-]+)(?:/|$)", url)
    return match.group(1).rstrip("/") if match else None

def backfill_sec_urls(supabase: Any, table_name: str):
    res = supabase.table(table_name).select("*").execute()
    for rec in (res.data or []):
        if rec.get("sec_ix_url"): continue
        ticker = rec.get("ticker")
        cik = rec.get("sec_cik") or (TARGET_COMPANIES.get(ticker, {}).get("cik") if ticker else None)
        accn = extract_accn_from_url(rec.get("sec_filing_url"))
        if not accn or not cik: continue
        ix_url = build_sec_ix_url(cik, accn)
        if ix_url:
            supabase.table(table_name).update({"sec_ix_url": ix_url, "sec_filing_url": ix_url}).eq("id", rec["id"]).execute()
