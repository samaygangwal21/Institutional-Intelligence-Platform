"""
ingest.py — Central Data Inflow Hub for Company Insights (Flairminds)
========================================================================
Consolidated from extractor.py and financial_ingestor.py.
Handles both SEC EDGAR REST API ingestion and unstructured Document/URL extraction.
"""

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime, date, timedelta, timezone
from io import BytesIO
from typing import Optional, Any, Dict, List, Tuple, cast
from urllib.parse import urlparse

import pdfplumber
import requests
from bs4 import BeautifulSoup
try:
    from crawl4ai import AsyncWebCrawler
    HAS_CRAWL4AI = True
except ImportError:
    HAS_CRAWL4AI = False
from docx import Document
from supabase import create_client, Client
try:
    from youtube_transcript_api import YouTubeTranscriptApi
    HAS_YOUTUBE = True
except ImportError:
    HAS_YOUTUBE = False

from platform_config import (
    SUPABASE_URL, SUPABASE_KEY, GEMINI_API_KEY, GEMINI_ENDPOINT,
    FINNHUB_KEY, TARGET_COMPANIES, SEC_HEADERS as HEADERS
)
from utils import (
    FINANCIAL_COLUMNS, ENHANCED_METRIC_MAP, fuzzy_match,
    build_raw_url, build_sec_ix_url, upload_to_azure_blob,
    fetch_page_content
)

log = logging.getLogger("Ingest")

# ── CONFIG & CONSTANTS ────────────────────────────────────────────────────────

LIVE_COLUMNS = FINANCIAL_COLUMNS | {
    "flex_metrics", "sec_filing_url", "sec_ix_url", "sec_raw_url", "filing_type", "created_at"
}

NUMERIC_FINANCIAL_COLS = {
    "revenue", "net_income", "operating_income", "total_assets",
    "total_liabilities", "total_equity", "cash_on_hand",
    "eps_diluted", "gross_profit", "ebitda", "free_cash_flow"
}

# ── 1. SEC API INGESTOR (Legacy FinancialIngestor) ───────────────────────────

HARD_COLUMN_TAGS = {
    "revenue": ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"],
    "net_income": ["NetIncomeLoss", "ProfitLoss", "NetIncomeLossAttributableToParent"],
    "total_assets": ["Assets"],
    "total_liabilities": ["Liabilities"],
    "total_equity": ["StockholdersEquity"],
    "eps_diluted": ["EarningsPerShareDiluted"],
    "operating_income": ["OperatingIncomeLoss", "IncomeLossFromContinuingOperationsBeforeIncomeTaxes"],
    "cash_on_hand": ["CashAndCashEquivalentsAtCarryingValue", "CashCashEquivalentsAndShortTermInvestments"],
    "gross_profit": ["GrossProfit"],
    "ebitda": ["EarningsBeforeInterestTaxesDepreciationAndAmortization"],
    "free_cash_flow": ["PaymentsToAcquirePropertyPlantAndEquipment"],
}

class SECDataMatcher:
    def __init__(self, cutoff_years: int = 3):
        self.cutoff_date = date(date.today().year - cutoff_years, 1, 1)

    def extract_points(self, tag_data: dict) -> dict:
        result = {}
        units = tag_data.get("units", {})
        for u_key in ["USD", "pure", "shares"]:
            if u_key in units:
                for entry in units[u_key]:
                    form = entry.get("form", "")
                    end = entry.get("end", "")
                    if form not in ["10-K", "10-Q", "10-K/A", "10-Q/A"]: continue
                    try: 
                        if date.fromisoformat(end) < self.cutoff_date: continue
                    except: continue
                    
                    start = entry.get("start")
                    duration = (date.fromisoformat(end) - date.fromisoformat(start)).days if start else 0
                    target = 365 if form.startswith("10-K") else 90
                    
                    if end not in result or abs(duration - target) < abs(result[end]["duration"] - target):
                        result[end] = {"val": entry["val"], "accn": entry["accn"], "form": form, "duration": duration}
                break
        return result

def ingest_sec_ticker(ticker: str, meta: dict, sb: Client, matcher: SECDataMatcher):
    # ... (keeping existing financial logic) ...
    cik = meta["cik"].zfill(10)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        
        # Archive raw company facts JSON to Azure
        upload_to_azure_blob(resp.text, f"sec_filings/{ticker}_companyfacts.json")
        
        data = resp.json().get("facts", {}).get("us-gaap", {})
        
        all_dates = {}
        for col, tags in HARD_COLUMN_TAGS.items():
            for tag in tags:
                if tag in data:
                    pts = matcher.extract_points(data[tag])
                    for d, info in pts.items():
                        if d not in all_dates or info["form"].startswith("10-K"):
                            all_dates[d] = info
                            
        rows = []
        for d, info in all_dates.items():
            dt = date.fromisoformat(d)
            f_year = dt.year
            f_period = "FY" if info["form"].startswith("10-K") else f"Q{(dt.month-1)//3 + 1}"
            
            row = {"ticker": ticker, "company_name": meta["name"], "fiscal_year": f_year, "fiscal_period": f_period, "end_date": d, "filing_type": "10-K" if "10-K" in info["form"] else "10-Q"}
            for col, tags in HARD_COLUMN_TAGS.items():
                for tag in tags:
                    if tag in data:
                        p = matcher.extract_points(data[tag])
                        if d in p: row[col] = p[d]["val"]; break
            
            row["sec_raw_url"] = build_raw_url(cik, info["accn"])
            row["sec_ix_url"] = build_sec_ix_url(cik, info["accn"])
            row["sec_filing_url"] = row["sec_ix_url"] or row["sec_raw_url"]
            
            # Archive individual raw SEC filing to Azure
            if row.get("sec_ix_url"):
                try:
                    # extract true raw path from the ix viewer link
                    raw_path = row["sec_ix_url"].split("?doc=")[-1]
                    raw_doc_url = "https://www.sec.gov" + raw_path
                    time.sleep(0.15) # respect rate limit
                    doc_resp = requests.get(raw_doc_url, headers=HEADERS, timeout=30)
                    if doc_resp.ok:
                        azure_url = upload_to_azure_blob(doc_resp.content, f"sec_filings/{ticker}_{info['form']}_{d}.htm")
                        row["archived_url"] = azure_url
                except Exception as e:
                    log.warning(f"Could not archive raw SEC filing for {ticker} {d}: {e}")
            
            rows.append({k:v for k,v in row.items() if k in LIVE_COLUMNS})
            
        if rows:
            sb.table("financials").upsert(rows, on_conflict="ticker,fiscal_year,fiscal_period").execute()
            log.info(f"[{ticker}] SEC Ingested {len(rows)} rows.")
    except Exception as e: log.error(f"[{ticker}] SEC Failed: {e}")

def ingest_news(ticker: str, sb: Client, days: int = 30):
    """Fetches latest news from Finnhub and stores in market_intelligence."""
    if not FINNHUB_KEY:
        log.warning("FINNHUB_KEY not set. Skipping news.")
        return
    
    end = date.today().isoformat()
    start = (date.today() - timedelta(days=days)).isoformat()
    url = f"https://finnhub.io/api/v1/company-news?symbol={ticker}&from={start}&to={end}&token={FINNHUB_KEY}"
    
    try:
        resp = requests.get(url, timeout=30)
        articles = resp.json()
        if not isinstance(articles, list):
            log.error(f"[{ticker}] Unexpected news response: {articles}")
            return
            
        rows = []
        for a in articles:
            rows.append({
                "ticker": ticker,
                "headline": a.get("headline"),
                "summary": a.get("summary"),
                "source": a.get("source"),
                "url": a.get("url"),
                "published_at": datetime.fromtimestamp(a.get("datetime"), tz=timezone.utc).isoformat(),
                "category": a.get("category"),
                "data_source": "FINNHUB"
            })
        
        if rows:
            sb.table("market_intelligence").upsert(rows, on_conflict="ticker,headline,published_at").execute()
            
            # Archive raw Finnhub News Payload to Azure
            upload_to_azure_blob(json.dumps(articles), f"news/payloads/{ticker}_news_{date.today().isoformat()}.json")
            
            # Archive the ENTIRE content for each individual article
            for row in rows:
                content = fetch_page_content(row["url"])
                if content:
                    # Sanitize headline for filename
                    safe_headline = re.sub(r'[^\w\s-]', '', row["headline"]).strip().replace(' ', '_')[:60]
                    file_path = f"news/full_articles/{ticker}/{date.today().isoformat()}_{safe_headline}.html"
                    azure_url = upload_to_azure_blob(content, file_path)
                    
                    # Update Supabase with the archived URL (Using URL for matching to avoid encoding issues)
                    if azure_url:
                        sb.table("market_intelligence").update({"archived_url": azure_url}).eq("ticker", ticker).eq("url", row["url"]).execute()
            
            # Maintain 50-article limit in Azure vault
            from utils import prune_azure_news_blobs
            prune_azure_news_blobs(ticker, max_count=50)
            
            log.info(f"[{ticker}] Ingested {len(rows)} news articles + archived full content.")
    except Exception as e:
        log.error(f"[{ticker}] News Failed: {e}")

# ── 2. DOCUMENT & URL EXTRACTOR (Legacy ExtractorEngine) ─────────────────────

def normalize_value(val: Any, currency: str = "USD") -> Optional[float]:
    if val is None or val == "": return None
    if isinstance(val, (int, float)): 
        num_val = float(val)
    else:
        s = str(val).lower().replace(",", "").replace("$", "").replace("₹", "").strip()
        mult = 1.0
        # Standard Multipliers
        if "billion" in s or " b" in s: mult = 1e9
        elif "million" in s or " m" in s: mult = 1e6
        elif "trillion" in s or " t" in s: mult = 1e12
        # Indian Multipliers
        elif "crore" in s or " cr" in s: mult = 1e7
        elif "lakh" in s or " l" in s: mult = 1e5

        num_match = re.search(r"[-+]?\d*\.\d+|\d+", s)
        if not num_match: return None
        num_val = float(num_match.group()) * mult

    # Currency Conversion (User requested Rate: 90)
    if currency.upper() in ["INR", "RUPEES", "RS"]:
        num_val = num_val / 90.0
        
    return num_val

def analyze_with_gemini(text: str) -> Dict[str, Any]:
    prompt = f"""Extract financial metrics from this text. Respond ONLY with JSON.
    Fields: company_name, ticker, period, end_date, currency, revenue, net_income, operating_income, total_assets, total_liabilities, total_equity, cash_on_hand, eps_diluted, insights, sector, sec_cik.
    Note: 
    - end_date should be in YYYY-MM-DD format (if only year is known, use YYYY-12-31).
    - currency: detect the original currency (e.g., USD, INR, EUR).
    - numerical fields: return the raw string from text (e.g., "229,171 Cr" or "$1.2B").
    - sec_cik: 10-digit SEC Key if US-listed, else null.
    TEXT: {text[:100000]}"""
    try:
        resp = requests.post(f"{GEMINI_ENDPOINT}?key={GEMINI_API_KEY}", json={"contents": [{"parts": [{"text": prompt}]}]}, timeout=60)
        return json.loads(re.sub(r"```(?:json)?", "", resp.json()["candidates"][0]["content"]["parts"][0]["text"]).strip("` \n"))
    except Exception as e: return {"error": str(e)}

class ExtractorEngine:
    def __init__(self):
        self.db = create_client(SUPABASE_URL, SUPABASE_KEY)
    def process(self, input_source, filename=None, ticker_override=None, company_override=None, push_to_supabase=True):
        source_label = input_source if isinstance(input_source, str) else (filename or "uploaded_file")
        source_type = "url" if isinstance(input_source, str) else "file"
        raw_text = ""
        
        if isinstance(input_source, str):
            # ── URL extraction ──────────────────────────────────────
            url = input_source.strip()
            
            # YouTube: extract transcript
            yt_match = re.search(r"(?:v=|youtu\.be/)([\w-]{11})", url)
            if yt_match and HAS_YOUTUBE:
                source_type = "youtube"
                video_id = yt_match.group(1)
                try:
                    transcript = YouTubeTranscriptApi.get_transcript(video_id)
                    raw_text = " ".join([t["text"] for t in transcript])
                except Exception as e:
                    raw_text = f"YouTube transcript unavailable: {e}"
            elif yt_match:
                raw_text = f"YouTube URL detected but youtube_transcript_api not installed."
            else:
                raw_text = None
                if HAS_CRAWL4AI:
                    import asyncio
                    try:
                        async def _fetch_c4ai(u):
                            async with AsyncWebCrawler() as crawler:
                                res = await crawler.arun(url=u)
                                return getattr(res, "markdown", getattr(res, "html", ""))
                        raw_text = asyncio.run(_fetch_c4ai(url))
                    except Exception as e:
                        print(f"crawl4ai fetch failed, falling back to bs4: {e}")
                
                if not raw_text:
                    # Web page: fallback to requests + BeautifulSoup
                    try:
                        resp = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
                        raw_text = BeautifulSoup(resp.text, "html.parser").get_text(separator=" ", strip=True)
                    except Exception as e:
                        raise RuntimeError(f"URL fetch failed: {e}")
        else:
            # ── File extraction ─────────────────────────────────────
            file_bytes = input_source
            fname = (filename or "").lower()
            
            if fname.endswith(".pdf"):
                try:
                    with pdfplumber.open(BytesIO(file_bytes)) as pdf:
                        raw_text = "\n".join(p.extract_text() or "" for p in pdf.pages)
                except Exception as e:
                    raise RuntimeError(f"PDF parsing failed: {e}")
            
            elif fname.endswith(".docx"):
                try:
                    doc = Document(BytesIO(file_bytes))
                    raw_text = "\n".join([para.text for para in doc.paragraphs if para.text.strip()])
                except Exception as e:
                    raise RuntimeError(f"DOCX parsing failed: {e}")
            
            elif fname.endswith((".txt", ".md")):
                try:
                    raw_text = file_bytes.decode("utf-8", errors="ignore")
                except Exception as e:
                    raise RuntimeError(f"Text file parsing failed: {e}")
            
            else:
                # Try PDF as fallback
                try:
                    with pdfplumber.open(BytesIO(file_bytes)) as pdf:
                        raw_text = "\n".join(p.extract_text() or "" for p in pdf.pages)
                except Exception:
                    raise RuntimeError(f"Unsupported file format: {fname}")
        
        if not raw_text.strip():
            raise RuntimeError("No text could be extracted from the source.")
        
        log.info(f"Extracted {len(raw_text)} chars from {source_label}")
        analysis = analyze_with_gemini(raw_text)
        ticker = (ticker_override or analysis.get("ticker", "TBD")).upper()
        company = company_override or analysis.get("company_name", "Unnamed Company")
        
        year = int(re.search(r"(\d{4})", analysis.get("period", "")).group(1)) if re.search(r"(\d{4})", analysis.get("period", "")) else datetime.now().year
        end_date = analysis.get("end_date") or f"{year}-12-31"
        currency = analysis.get("currency", "USD")

        # Build Row with Normalization
        row = {
            "ticker": ticker, 
            "company_name": company, 
            "fiscal_year": year,
            "end_date": end_date,
            "fiscal_period": "FY", 
            "revenue":          normalize_value(analysis.get("revenue"), currency),
            "net_income":       normalize_value(analysis.get("net_income"), currency),
            "operating_income": normalize_value(analysis.get("operating_income"), currency),
            "total_assets":      normalize_value(analysis.get("total_assets"), currency),
            "total_liabilities": normalize_value(analysis.get("total_liabilities"), currency),
            "total_equity":      normalize_value(analysis.get("total_equity"), currency),
            "cash_on_hand":      normalize_value(analysis.get("cash_on_hand"), currency),
            "eps_diluted":       normalize_value(analysis.get("eps_diluted"), currency),
            "data_source": "DOCUMENT_EXTRACTOR", 
            "filing_type": "EXTRACTED", 
            "sec_filing_url": source_label if isinstance(input_source, str) else None,
            "created_at": datetime.now(timezone.utc).isoformat()
        }

        # Vault to Azure
        from utils import upload_to_azure_blob
        vault_name = f"manual_uploads/{ticker}/{datetime.now().strftime('%Y%m%d_%H%M%S')}_{source_label}"
        archived_url = upload_to_azure_blob(raw_text, vault_name)
        if archived_url:
            row["archived_url"] = archived_url
        
        if push_to_supabase:
            # Register company
            co_data = {
                "ticker": ticker, 
                "company_name": company, 
                "sector": analysis.get("sector", "Other")
            }
            if analysis.get("sec_cik"):
                co_data["sec_cik"] = analysis["sec_cik"]
            
            self.db.table("target_companies").upsert(co_data, on_conflict="ticker").execute()
            # Upsert financials
            self.db.table("financials").upsert([row], on_conflict="ticker,fiscal_year,fiscal_period").execute()
            # Log extraction
            self.db.table("extracted_documents").insert({
                "ticker": ticker, 
                "company_name": company, 
                "source_url": source_label, 
                "source_type": source_type,
                "extraction_status": "SUCCESS",
                "archived_url": archived_url,
                "raw_text": raw_text # Store text for Report Builder context
            }).execute()

        
        return {"status": "SUCCESS", "row": row}

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="IIP Ingestion Hub")
    parser.add_argument("--ticker", type=str, help="Specific ticker to refresh")
    parser.add_argument("--news-only", action="store_true", help="Only refresh news")
    parser.add_argument("--sec-only", action="store_true", help="Only refresh SEC financials")
    parser.add_argument("--skip-financials", action="store_true", help="Alias for news-only")
    args = parser.parse_args()

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    matcher = SECDataMatcher()
    
    # Reload target companies to ensure TATASTEEL etc. are in the list
    from platform_config import load_target_companies
    CURRENT_COMPANIES = load_target_companies()
    
    tickers_to_run = [args.ticker] if args.ticker else CURRENT_COMPANIES.keys()
    
    only_news = args.news_only or args.skip_financials
    only_sec = args.sec_only

    for t in tickers_to_run:
        meta = CURRENT_COMPANIES.get(t)
        if not meta:
            log.error(f"Ticker {t} not found in registry.")
            continue
            
        if not only_news and meta.get("cik"):
            ingest_sec_ticker(t, meta, sb, matcher)
        
        if not only_sec:
            ingest_news(t, sb)

