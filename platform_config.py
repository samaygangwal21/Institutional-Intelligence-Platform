"""
Central Configuration for Company Insights (Flairminds)
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(override=True)

# ── Supabase Config ──────────────────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://diuksxvmsmpvuaseszmb.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")   # Set via env var

from supabase import create_client
def get_supabase():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# ── API Keys & Endpoints ─────────────────────────────────────────────────────
GEMINI_API_KEY  = os.getenv("GEMINI_API_KEY", "")
GEMINI_ENDPOINT = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"

FINNHUB_KEY = os.getenv("FINNHUB_KEY", "")

# ── Azure Blob Storage Config ────────────────────────────────────────────────────────
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
AZURE_STORAGE_CONTAINER_NAME = os.getenv("AZURE_STORAGE_CONTAINER_NAME", "financial-uploads")

# ── SEC EDGAR Config ─────────────────────────────────────────────────────────
SEC_HEADERS = {
    "User-Agent": "Doc-extract-and-report samaygangwal21@gmail.com",
    "Accept": "application/json"
}

def load_target_companies():
    """Dynamically loads target companies from Supabase target_companies table."""
    base = {
        "AAPL":  {"name": "Apple Inc.",            "cik": "0000320193", "sector": "Technology", "fiscal_year_end_month": 9},
        "AMZN":  {"name": "Amazon.com Inc.",       "cik": "0001018724", "sector": "Consumer Discretionary", "fiscal_year_end_month": 12},
        "MSFT":  {"name": "Microsoft Corporation", "cik": "0000789019", "sector": "Technology", "fiscal_year_end_month": 6},
        "GOOGL": {"name": "Alphabet Inc.",         "cik": "0001652044", "sector": "Communication Services", "fiscal_year_end_month": 12},
        "TSLA":  {"name": "Tesla Inc.",            "cik": "0001318605", "sector": "Consumer Discretionary", "fiscal_year_end_month": 12},
    }
    try:
        sb = get_supabase()
        res = sb.table("target_companies").select("*").execute()
        if res.data:
            merged = base.copy()
            for row in res.data:
                ticker = row["ticker"]
                merged[ticker] = {
                    "name": row["company_name"],
                    "cik": row["sec_cik"],
                    "sector": row.get("sector", "Other"),
                    "fiscal_year_end_month": 12, # Default
                }
            return merged
    except Exception:
        pass
    return base

TARGET_COMPANIES = load_target_companies()
TARGET_TICKERS = list(TARGET_COMPANIES.keys())

# ── Sector Grouping ─────────────────────────────────────────────────────────
SECTOR_ICONS = {
    "Technology":               "💻",
    "Consumer Discretionary":   "🛒",
    "Communication Services":   "📡",
    "Financials":               "🏦",
    "Healthcare":               "🏥",
    "Energy":                   "⚡",
    "Industrials":              "🏭",
    "Materials":                "⛏️",
    "Real Estate":              "🏠",
    "Utilities":                "💡",
    "Consumer Staples":         "🛒",
}

COMPANY_ICONS = {
    "AAPL":  "🍎",
    "AMZN":  "📦",
    "MSFT":  "🪟",
    "GOOGL": "🔍",
    "TSLA":  "⚡",
}
