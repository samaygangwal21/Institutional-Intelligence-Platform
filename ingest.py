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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log = logging.getLogger("Ingest")

# ── CONFIG & CONSTANTS ────────────────────────────────────────────────────────

LIVE_COLUMNS = FINANCIAL_COLUMNS | {
    "flex_metrics", "sec_filing_url", "filing_type", "created_at",
    "sec_cik", "data_source", "archived_url"
}
NUMERIC_FINANCIAL_COLS = {
    "revenue", "net_income", "operating_income", "total_assets",
    "total_liabilities", "total_equity", "cash_on_hand",
    "eps_diluted", "gross_profit", "ebitda", "free_cash_flow"
}

# ── 1. SEC API INGESTOR (Legacy FinancialIngestor) ───────────────────────────

HARD_COLUMN_TAGS = {
    "revenue": [
        "Revenues", 
        "RevenueFromContractWithCustomerExcludingAssessedTax", 
        "SalesRevenueNet",
        "SalesRevenueGoodsNet",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "TotalRevenues"
    ],
    "net_income": [
        "NetIncomeLoss", 
        "ProfitLoss", 
        "NetIncomeLossAttributableToParent",
        "NetIncomeLossAvailableToCommonStockholdersBasic"
    ],
    "total_assets": ["Assets", "TotalAssets"],
    "total_liabilities": ["Liabilities", "TotalLiabilities"],
    "total_equity": ["StockholdersEquity", "TotalEquity"],
    "eps_diluted": ["EarningsPerShareDiluted", "EarningsPerShareBasicAndDiluted", "NetIncomeLossPerShareDiluted"],
    "operating_income": ["OperatingIncomeLoss", "IncomeLossFromContinuingOperationsBeforeIncomeTaxes", "OperatingProfitLoss"],
    "cash_on_hand": ["CashAndCashEquivalentsAtCarryingValue", "CashAndCashEquivalents", "CashCashEquivalentsAndShortTermInvestments"],
    "gross_profit": ["GrossProfit"],
    "cost_of_revenue": ["CostOfRevenue", "CostOfGoodsAndServicesSold", "CostOfGoodsSold"],
    "ebitda": ["EarningsBeforeInterestTaxesDepreciationAndAmortization"],
    "free_cash_flow": ["PaymentsToAcquirePropertyPlantAndEquipment"],
}

def get_sector_from_sic(sic_code):
    """
    Categorizes SEC SIC codes into high-level human-readable sectors.
    Source: https://www.sec.gov/corpfin/division-of-corporation-finance-standard-industrial-classification-sic-code-list
    """
    if not sic_code: return "Uncategorized"
    try:
        sic = int(sic_code)
    except: return "Uncategorized"

    if 100 <= sic <= 999:    return "Resources"
    if 1000 <= sic <= 1499:  return "Resources"
    if 1500 <= sic <= 1799:  return "Industrials"
    if 2000 <= sic <= 2799:  return "Consumer Staples"
    if 2800 <= sic <= 2829:  return "Industrials" # Chemicals
    if 2830 <= sic <= 2836:  return "Healthcare"   # Pharma/Biotech
    if 2840 <= sic <= 3499:  return "Industrials"
    if 3500 <= sic <= 3569:  return "Industrials"
    if 3570 <= sic <= 3579:  return "Technology"    # Computer Hardware
    if 3580 <= sic <= 3669:  return "Industrials"
    if 3670 <= sic <= 3679:  return "Technology"    # Semiconductors/Electronic
    if 3700 <= sic <= 3799:  return "Industrials"   # Transportation Equip
    if 3800 <= sic <= 3845:  return "Healthcare"    # Medical Instruments
    if 3846 <= sic <= 3999:  return "Industrials"
    if 4000 <= sic <= 4799:  return "Industrials"   # Transport
    if 4800 <= sic <= 4899:  return "Communication Services"
    if 4900 <= sic <= 4999:  return "Utilities"
    if 5000 <= sic <= 5999:  return "Consumer Discretionary" # Retail
    if 6000 <= sic <= 6799:  return "Financials"
    if 7000 <= sic <= 7299:  return "Consumer Services"
    if 7370 <= sic <= 7379:  return "Technology"    # Software/Data
    if 7380 <= sic <= 7999:  return "Industrials"
    if 8000 <= sic <= 8099:  return "Healthcare"
    if 8700 <= sic <= 8748:  return "Industrials"
    
    return "Industrials" # Default fallback for miscellaneous manufacturing/services

class SECDataMatcher:
    def __init__(self, cutoff_years: int = 3):
        self.cutoff_date = date(date.today().year - cutoff_years, 1, 1)

    def extract_points(self, tag_data: dict) -> dict:
        result = {}
        units = tag_data.get("units", {})
        # Support various SEC units including per-share metrics
        for u_key in ["USD", "pure", "shares", "USD/shares"]:
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
                    
                    # Store SEC's official fiscal identifiers (fy, fp) if present
                    fy = entry.get("fy")
                    fp = entry.get("fp")
                    
                    if end not in result or abs(duration - target) < abs(result[end]["duration"] - target):
                        result[end] = {
                            "val": entry["val"], 
                            "accn": entry["accn"], 
                            "form": form, 
                            "duration": duration,
                            "fy": fy,
                            "fp": fp
                        }
                break
        return result

def ingest_sec_ticker(ticker: str, meta: dict, sb: Client, matcher: SECDataMatcher):
    """
    Fetches and parses financial facts from SEC EDGAR. Supports us-gaap and ifrs-full.
    """
    cik = meta["cik"].zfill(10)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            log.error(f"[{ticker}] SEC Error {resp.status_code}")
            return
            
        facts_json = resp.json().get("facts", {})
        data_gaap = facts_json.get("us-gaap", {})
        data_ifrs = facts_json.get("ifrs-full", {})
        
        # Archive raw company facts JSON to Azure
        upload_to_azure_blob(resp.text, f"sec_filings/{ticker}_companyfacts.json")
        
        all_dates = {}
        # 1. Map all available filing dates for core tags
        for col, tags in HARD_COLUMN_TAGS.items():
            for tag in tags:
                tag_data = data_gaap.get(tag) or data_ifrs.get(tag)
                if tag_data:
                    pts = matcher.extract_points(tag_data)
                    for d, info in pts.items():
                        # Priority for 10-K to ensure FY coverage
                        if d not in all_dates or info["form"].startswith("10-K"):
                            all_dates[d] = info
        
        if not all_dates:
            log.warning(f"[{ticker}] No usable financial data points found in SEC registries.")
            return

        rows = []
        # 2. Build canonical financial rows
        for d, info in all_dates.items():
            dt = date.fromisoformat(d)
            # Use SEC-provided fy/fp if they exist, otherwise try calendar heuristic
            f_year = info.get("fy") or dt.year
            f_period = info.get("fp") or ("FY" if info["form"].startswith("10-K") else f"Q{(dt.month-1)//3 + 1}")
            
            row = {
                "ticker": ticker, 
                "company_name": meta["name"], 
                "fiscal_year": int(f_year), 
                "fiscal_period": f_period, 
                "end_date": d, 
                "filing_type": "10-K" if "10-K" in info["form"] else "10-Q",
                "data_source": "SEC_EDGAR",
                "flex_metrics": {}
            }
            
            # Fill metrics from prioritized tags
            hard_tags_accounted = set()
            for col, tags in HARD_COLUMN_TAGS.items():
                for tag in tags:
                    hard_tags_accounted.add(tag)
                    tag_data = data_gaap.get(tag) or data_ifrs.get(tag)
                    if tag_data:
                        p = matcher.extract_points(tag_data)
                        if d in p: 
                            row[col] = p[d]["val"]
                            break
            
            # --- 2.5 Flex Metrics Sweep (All other numeric facts) ---
            # We look at ALL tags that aren't already captured or boring meta-data
            for raw_facts in [data_gaap, data_ifrs]:
                for tag, tag_data in raw_facts.items():
                    if tag in hard_tags_accounted: continue
                    if any(skip in tag for skip in ["Entity", "Document", "Amendment", "AmendmentFlag", "City", "State", "Address"]): continue
                    
                    p = matcher.extract_points(tag_data)
                    if d in p:
                        # Only add if numeric (and not a date)
                        val = p[d]["val"]
                        if isinstance(val, (int, float)) and not isinstance(val, bool):
                            row["flex_metrics"][tag] = val

            # --- Synthetic Calculations (Fill Nulls) ---
            if row.get("revenue") and row.get("cost_of_revenue") and not row.get("gross_profit"):
                row["gross_profit"] = row["revenue"] - row["cost_of_revenue"]
            
            row["sec_cik"] = cik
            row["sec_filing_url"] = build_sec_ix_url(cik, info["accn"]) or build_raw_url(cik, info["accn"])
            
            # 3. Archive individual raw filings to Storage Vault
            if row.get("sec_filing_url"):
                try:
                    # if it's an IX viewer link, extract true raw path
                    if "/ix?doc=" in row["sec_filing_url"]:
                        raw_path = row["sec_filing_url"].split("?doc=")[-1]
                        raw_doc_url = "https://www.sec.gov" + raw_path
                    else:
                        raw_doc_url = row["sec_filing_url"]
                    
                    time.sleep(0.12) # Respect SEC rate limits
                    doc_resp = requests.get(raw_doc_url, headers=HEADERS, timeout=30)
                    if doc_resp.ok:
                        azure_url = upload_to_azure_blob(doc_resp.content, f"sec_filings/{ticker}_{info['form']}_{d}.htm")
                        row["archived_url"] = azure_url
                except Exception as e:
                    log.debug(f"[{ticker}] Vault archive skip for {d}: {e}")
            
            rows.append({k:v for k,v in row.items() if k in LIVE_COLUMNS})
            
        if rows:
            # --- 4. Final Deduplication (Ticker + Period) ---
            # SEC sometimes has multiple entries for the same period (e.g. amendments).
            # We pick the row with the most recent end_date for each period.
            deduped = {}
            for r in rows:
                key = (r["ticker"], r["fiscal_year"], r["fiscal_period"])
                if key not in deduped or r["end_date"] > deduped[key]["end_date"]:
                    deduped[key] = r
            
            final_rows = list(deduped.values())
            print(f"DEBUG: Attempting to upsert {len(final_rows)} deduped rows for {ticker}...")
            sb.table("financials").upsert(final_rows, on_conflict="ticker,fiscal_year,fiscal_period").execute()
            log.info(f"[{ticker}] SEC Analytics Engine: Ingested {len(final_rows)} data points.")
            print(f"SUCCESS: [{ticker}] Ingested {len(final_rows)} data points.")
        else:
            log.warning(f"[{ticker}] Financial synthesis resulted in zero rows.")
            print(f"WARNING: [{ticker}] Zero rows synthesized.")

    except Exception as e:
        log.error(f"[{ticker}] Critical Ingestion Engine Error: {e}")
        print(f"CRITICAL ERROR: [{ticker}] {e}")
        raise e

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

        
# ── 4. TICKER DISCOVERY & ENROLLMENT (Scale-Ready) ───────────────────────────

SEC_TICKER_MAPPING_CACHE = {}

def get_sec_ticker_mapping():
    """
    Fetches and caches the SEC ticker -> CIK mapping from official sources.
    Source: https://www.sec.gov/files/company_tickers.json
    """
    global SEC_TICKER_MAPPING_CACHE
    if SEC_TICKER_MAPPING_CACHE:
        return SEC_TICKER_MAPPING_CACHE
    
    url = "https://www.sec.gov/files/company_tickers.json"
    try:
        # User-Agent and Timeout are required or it may hang/403
        response = requests.get(url, headers=HEADERS, timeout=12)
        if response.status_code == 200:
            raw_data = response.json()
            # Convert to {TICKER: {cik, title}} for O(1) lookup
            mapping = {}
            for key in raw_data:
                item = raw_data[key]
                t = str(item["ticker"]).upper()
                mapping[t] = {
                    "cik": str(item["cik_str"]).zfill(10),
                    "title": item["title"]
                }
            SEC_TICKER_MAPPING_CACHE = mapping
            log.info(f"Loaded {len(mapping)} tickers into SEC mapping cache.")
            return mapping
        else:
            log.error(f"SEC Mapping Fetch Failed: Status {response.status_code}")
    except Exception as e:
        log.error(f"Exception during SEC mapping fetch: {e}")
    return {}

def discover_and_enroll_ticker(ticker: str, sb: Client):
    """
    Discovery layer: Normalizes ticker, checks for duplicates, and enrolls 
    new companies from SEC registry if found.
    """
    ticker = ticker.upper().strip()
    if not ticker: return {"status": "INVALID"}
    
    # 1. Check if already in DB
    try:
        res = sb.table("target_companies").select("*").eq("ticker", ticker).execute()
        if res.data:
            row = res.data[0]
            log.info(f"[{ticker}] Already enrolled: {row['company_name']}")
            return {"status": "EXISTS", "ticker": ticker, "name": row["company_name"], "cik": row["sec_cik"]}
    except Exception: pass

    # 2. Discovery via SEC
    mapping = get_sec_ticker_mapping()
    if ticker not in mapping:
        log.warning(f"[{ticker}] Ticker not found in SEC registry.")
        return {"status": "NOT_FOUND", "ticker": ticker}
    
    match = mapping[ticker]
    name = match["title"]
    cik = match["cik"]

    # 3. Enhanced Discovery: Fetch Sector from SEC Submissions API
    sector = "Uncategorized"
    try:
        sub_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        resp = requests.get(sub_url, headers=HEADERS, timeout=8)
        if resp.status_code == 200:
            sub_data = resp.json()
            sic = sub_data.get("sic")
            sector = get_sector_from_sic(sic)
            log.info(f"[{ticker}] Discovered SIC {sic} -> Sector: {sector}")
    except Exception as e:
        log.warning(f"[{ticker}] Sector discovery failed: {e}")

    # 4. Enroll in DB
    try:
        sb.table("target_companies").upsert({
            "ticker": ticker,
            "company_name": name,
            "sec_cik": cik,
            "sector": sector,
            "added_at": datetime.now().isoformat()
        }).execute()
        log.info(f"[{ticker}] Enrolled in registry: {name} (Sector: {sector}, CIK: {cik})")
    except Exception as e:
        log.error(f"[{ticker}] Enrollment failed: {e}")
        return {"status": "ERROR", "message": str(e)}

    return {"status": "SUCCESS", "ticker": ticker, "name": name, "cik": cik}

if __name__ == "__main__":
    import argparse
    from platform_config import get_company_meta, get_supabase
    
    parser = argparse.ArgumentParser(description="IIP Ingestion Hub")
    parser.add_argument("--ticker", type=str, help="Specific ticker to refresh")
    parser.add_argument("--news-only", action="store_true", help="Only refresh news")
    parser.add_argument("--sec-only", action="store_true", help="Only refresh SEC financials")
    parser.add_argument("--skip-financials", action="store_true", help="Alias for news-only")
    args = parser.parse_args()

    sb = get_supabase()
    matcher = SECDataMatcher()
    
    only_news = args.news_only or args.skip_financials
    only_sec = args.sec_only

    if args.ticker:
        # Scale-ready Single Ticker Ingestion
        t = args.ticker.upper().strip()
        
        # 1. Discovery/Enrollment Step
        discovery = discover_and_enroll_ticker(t, sb)
        
        if discovery["status"] == "NOT_FOUND":
            print(f"DISCOVERY_ERROR: Ticker {t} not found in SEC database.")
            exit(0)
            
        meta = {
            "name": discovery.get("name"),
            "cik": discovery.get("cik"),
            "sector": discovery.get("sector", "Other")
        }
        
        if not meta or not meta.get("cik"):
            log.warning(f"[{t}] Metadata missing or no CIK found. SEC ingestion may fail.")
            # We skip SEC but still allow news if ticker exists
            if not meta: meta = {"name": t, "cik": None}

        if not only_news and meta.get("cik"):
            ingest_sec_ticker(t, meta, sb, matcher)
        if not only_sec:
            ingest_news(t, sb)
    else:
        # Bulk run (Restricted in scale-ready design)
        log.warning("Bulk ingestion triggered. Loading default registry set...")
        from platform_config import load_target_companies
        CURRENT_COMPANIES = load_target_companies()
        
        for t, meta in CURRENT_COMPANIES.items():
            if not only_news and meta.get("cik"):
                ingest_sec_ticker(t, meta, sb, matcher)
            if not only_sec:
                ingest_news(t, sb)

