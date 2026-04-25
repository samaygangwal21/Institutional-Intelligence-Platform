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
GEMINI_KEYS = [
    os.getenv(f"GEMINI_API_KEY_{i}") 
    for i in range(1, 6) 
    if os.getenv(f"GEMINI_API_KEY_{i}")
]
# Fallback to single key if no numbered keys found
if not GEMINI_KEYS:
    GEMINI_KEYS = [os.getenv("GEMINI_API_KEY", "")]

# Used for round-robin rotation
_key_counter = 0

def get_gemini_key():
    global _key_counter
    if not GEMINI_KEYS: return ""
    key = GEMINI_KEYS[_key_counter % len(GEMINI_KEYS)]
    _key_counter += 1
    return key

GEMINI_API_KEY = GEMINI_KEYS[0] if GEMINI_KEYS else ""
GEMINI_ENDPOINT = "https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-latest:generateContent"

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
    """
    Dynamically loads target companies from Supabase.
    In a scale-ready design, we avoid loading the full table (8,000+).
    This now returns a limited set or empty dict by default.
    """
    try:
        sb = get_supabase()
        # Scale-ready search index: Fetch up to 10,000 companies for autocomplete
        res = sb.table("target_companies").select("*").limit(10000).execute()
        if res.data:
            companies = {}
            for row in res.data:
                ticker = row["ticker"]
                companies[ticker] = {
                    "name": row["company_name"],
                    "cik": row["sec_cik"],
                    "sector": row.get("sector", "Other"),
                    "fiscal_year_end_month": 12, # Default
                }
            return companies
    except Exception:
        pass
    return {}

# ── Metadata Service ─────────────────────────────────────────────────────────

import streamlit as st # type: ignore

@st.cache_data(ttl=3600)
def get_company_meta(ticker: str) -> dict:
    """
    Scale-ready metadata fetcher. Fetches only required fields for a single ticker.
    Used to replace global TARGET_COMPANIES lookups.
    """
    if not ticker:
        return {}
    
    try:
        sb = get_supabase()
        res = sb.table("target_companies").select("ticker, company_name, sec_cik, sector").eq("ticker", ticker).execute()
        if res.data:
            row = res.data[0]
            return {
                "name": row["company_name"],
                "cik": row["sec_cik"],
                "sector": row.get("sector", "Other"),
                "ticker": row["ticker"]
            }
    except Exception:
        pass
    return {"name": ticker, "ticker": ticker, "cik": None, "sector": "N/A"}

# Initial load (limited to top 50 for local context/search)
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

# ── UI/Icons (Kept for current dataset visuals) ──────────────────────────────
