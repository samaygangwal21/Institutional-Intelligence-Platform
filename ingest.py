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

import tempfile
import pymupdf4llm
import requests
from bs4 import BeautifulSoup
try:
    from crawl4ai import AsyncWebCrawler
    HAS_CRAWL4AI = True
except ImportError:
    HAS_CRAWL4AI = False
from supabase import create_client, Client
from platform_config import (
    SUPABASE_URL, SUPABASE_KEY, GEMINI_API_KEY, GEMINI_ENDPOINT,
    FINNHUB_KEY, TARGET_COMPANIES, SEC_HEADERS, GEMINI_KEYS
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
        resp = requests.get(url, headers=SEC_HEADERS, timeout=30)
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
                    doc_resp = requests.get(raw_doc_url, headers=SEC_HEADERS, timeout=30)
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

def clean_financials(data: Dict[str, Any]) -> Dict[str, Any]:
    """Post-processing validator to ensure numerical sanity and correct margins."""
    def safe(val):
        if val is None or str(val).lower() == "nan": return None
        try: return float(val)
        except: return None

    # Handle single row or list of rows
    is_list = isinstance(data, list)
    rows = data if is_list else [data]
    cleaned = []

    for row in rows:
        r = {k: safe(v) for k, v in row.items()}
        rev = r.get("revenue") or r.get("total_revenue")
        ni = r.get("net_income")
        oi = r.get("operating_income")

        # Recompute / Validate Margins
        if rev and ni and rev != 0:
            margin = (ni / rev) * 100
            r["net_margin"] = margin if 0 <= margin <= 100 else None
        
        if rev and oi and rev != 0:
            margin = (oi / rev) * 100
            r["operating_margin"] = margin if 0 <= margin <= 100 else None
            
        cleaned.append(r)

    return cleaned if is_list else cleaned[0]

def normalize_value(val: Any, currency: str = "USD") -> Optional[float]:
    if val is None or val == "": return None
    if isinstance(val, (int, float)): 
        num_val = float(val)
    else:
        s = str(val).lower().replace(",", "").replace("$", "").replace("₹", "").strip()
        mult = 1.0
        # Strict Multiplier Detection (using word boundaries or regex)
        if re.search(r"\b(billion|b)\b", s): mult = 1e9
        elif re.search(r"\b(million|m)\b", s): mult = 1e6
        elif re.search(r"\b(trillion|t)\b", s): mult = 1e12
        elif re.search(r"\b(crore|cr)\b", s): mult = 1e7
        elif re.search(r"\b(lakh|l)\b", s): mult = 1e5

        num_match = re.search(r"[-+]?\d*\.\d+|\d+", s)
        if not num_match: return None
        num_val = float(num_match.group()) * mult

    return num_val

def analyze_with_gemini(text: str, file_bytes: bytes = None, mime_type: str = "application/pdf") -> Dict[str, Any]:
    instruction = f"""You are a financial data extraction and normalization engine.
Your task is to extract, clean, normalize, validate, and return structured financial metrics from unstructured input (PDF, Transcript, or URL).

## STEP 1: VALUE EXTRACTION
Extract values for: Total Revenue, Net Income (PAT preferred), Operating Income, Cash & Equivalents, EPS (Diluted), Net Margin, Operating Margin, Debt/Equity, Total Assets, Total Equity, Total Liabilities.
If missing or "–", return null. NEVER return "nan".

## STEP 2: SPOKEN NUMBER PARSING
Convert phrases like: "32 like 19964" -> 3219964 | "31 like 11 thousand 653" -> 3111653. Treat "like" as separator.

## STEP 3: UNIT NORMALIZATION
Convert all values into absolute numbers:
1 Lakh (L) -> 100,000 | 1 Crore (Cr) -> 10,000,000 | 1 Million -> 1,000,000 | 1 Billion -> 1,000,000,000 | 1 Trillion (T) -> 1,000,000,000,000.
Remove symbols like ₹, $, commas, and text before conversion.

## STEP 4: CURRENCY HANDLING
DO NOT convert currencies. Detect: ₹ -> "INR", $ -> "USD". Else -> null.

## STEP 5: DATA VALIDATION
1. If margin > 100% -> INVALID -> null.
2. RECOMPUTE MARGINS if revenue and income exist:
   net_margin = (net_income / total_revenue) * 100
   operating_margin = (operating_income / total_revenue) * 100
   Keep only if <= 100%.
3. Consistency: total_assets ≈ total_equity + total_liabilities. Do not fabricate.

## STEP 6: MANDATORY MULTI-YEAR
If the document covers multiple years (e.g. 2021-2025), you MUST return an array of objects in the 'financials' field, one for each year.

## OUTPUT FORMAT (STRICT JSON ONLY)
{{
  "metadata": {{
    "company_name": "String", "ticker": "String", "sector": "String",
    "period": "e.g. FY 2025", "end_date": "YYYY-MM-DD", "sec_cik": "String or null"
  }},
  "financials": [
    {{
      "year": 2025,
      "revenue": number or null,
      "net_income": number or null,
      "operating_income": number or null,
      "cash_and_equivalents": number or null,
      "eps_diluted": number or null,
      "total_assets": number or null,
      "total_equity": number or null,
      "total_liabilities": number or null,
      "currency": "INR" or "USD"
    }}
  ],
  "insights": "Executive summary of business performance",
  "sections": {{ "Risk Factors": "...", "Business Outlook": "..." }},
  "tables": ["Array of reconstructed markdown financial tables"]
}}
"""

    parts = [{"text": instruction}]
    if file_bytes:
        # Cap at 4MB for inline transmission to avoid 413 errors
        if len(file_bytes) > 4 * 1024 * 1024:
            log.warning("File is larger than 4MB, sending only raw text fallback.")
        else:
            import base64
            parts.append({
                "inline_data": {
                    "mime_type": mime_type,
                    "data": base64.b64encode(file_bytes).decode("utf-8")
                }
            })
    if text:
        parts.append({"text": f"DOCUMENT TEXT CONTENT:\n{text[:100000]}"})

    from platform_config import get_gemini_key
    try:
        for attempt in range(5):
            current_key = get_gemini_key()
            try:
                payload = {
                    "contents": [{"parts": parts}],
                    "generationConfig": {
                        "maxOutputTokens": 8192,
                        "temperature": 0.1
                    }
                }
                resp = requests.post(f"{GEMINI_ENDPOINT}?key={current_key}", json=payload, timeout=120)
                data = resp.json()
                
                if data and "error" in data:
                    err_msg = data["error"].get("message", "Unknown Gemini Error")
                    log.warning(f"Key {attempt+1} failed with error: {err_msg}. Rotating...")
                    
                    if attempt == 4: return {"error": f"Gemini high demand (All keys exhausted): {err_msg}"}
                    
                    # Quota reset logic: If we hit a rate limit, wait longer
                    if "quota" in err_msg.lower():
                        log.info("Rate limit hit. Waiting 60s for quota to reset...")
                        time.sleep(60)
                    else:
                        time.sleep(2)
                    continue
                
                break
            except Exception as e:
                if attempt == 4:
                    log.error(f"Gemini analysis failed after 5 attempts: {e}")
                    return {"error": f"Gemini connection failed: {e}"}
                time.sleep(3 * (attempt + 1))  # 3, 6, 9, 12s backoff
                continue
        
        if not data or not isinstance(data, dict):
            return {"error": "Invalid API response from Gemini"}
            
        if "error" in data:
            log.error(f"Gemini API Error: {data['error'].get('message', 'Unknown Error')}")
            return {"error": data["error"].get("message")}
        
        # Guard against safety filters (empty candidates)
        if not data.get("candidates") or not data["candidates"][0].get("content"):
            return {"error": "AI response was blocked by safety filters or returned empty candidates."}
            
        raw_output = data["candidates"][0]["content"]["parts"][0]["text"]
        json_match = re.search(r"(\{.*\})", raw_output, re.DOTALL)
        if json_match:
            return json.loads(json_match.group(1))
        
        return json.loads(re.sub(r"```(?:json)?", "", raw_output).strip("` \n"))
    except Exception as e:
        log.error(f"Final analysis orchestrator failure: {e}")
        return {"error": f"Internal Analysis Error: {e}"}


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
                    # Initial URL Fetch with Retry
                    for attempt in range(3):
                        try:
                            resp = requests.get(url, headers=SEC_HEADERS, timeout=30)
                            resp.raise_for_status()
                            soup = BeautifulSoup(resp.text, 'html.parser')
                            break
                        except Exception as e:
                            if attempt == 2: raise RuntimeError(f"URL fetch failed after 3 attempts: {e}")
                            time.sleep(2 ** attempt)
                    
                    # Remove noisy elements
                    for s in soup(["script", "style", "nav", "footer", "header"]):
                        s.decompose()
                    
                    # Convert tables into pseudo-markdown before extracting text
                    for table in soup.find_all("table"):
                        rows = []
                        for tr in table.find_all("tr"):
                            cells = [td.get_text(separator=" ", strip=True) for td in tr.find_all(['th', 'td'])]
                            rows.append(" | ".join(cells))
                        markdown_table = "\n\n[TABLE_START]\n" + "\n".join(rows) + "\n[TABLE_END]\n\n"
                        table.replace_with(markdown_table)
                        
                    raw_text = soup.get_text(separator="\n", strip=True)
                except Exception as e:
                    raise RuntimeError(f"URL fetch failed: {e}")
        else:
            # ── File extraction ─────────────────────────────────────
            file_bytes = input_source
            fname = (filename or "").lower()
            
            if fname.endswith((".txt", ".md")):
                try:
                    raw_text = file_bytes.decode("utf-8", errors="ignore")
                except Exception as e:
                    raise RuntimeError(f"Text file parsing failed: {e}")
            else:
                # Treat as PDF and parse using pymupdf4llm
                try:
                    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                        tmp.write(file_bytes)
                        tmp_path = tmp.name
                    raw_text = pymupdf4llm.to_markdown(tmp_path)
                    os.unlink(tmp_path)
                except Exception as e:
                    raise RuntimeError(f"PDF parsing failed: {e}")
        
        if not raw_text.strip() and source_type != "file":
            raise RuntimeError("No text could be extracted from the source. (Non-file source)")
        elif not raw_text.strip():
            log.info("Local text extraction yielded NO text. Proceeding to Multimodal/Vision OCR.")
            raw_text = "[IMAGE_SCANNED_PDF_NO_TEXT_LAYER]"
        
        log.info(f"Extracted {len(raw_text)} chars from {source_label}")
        
        # Multimodal OCR Backup: If it's a file, send the bytes to Gemini for vision-based OCR
        file_send = None
        mtype = "application/pdf"
        if source_type == "file":
            file_send = input_source
            if fname.endswith(".docx"): mtype = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            elif fname.endswith(".txt"): mtype = "text/plain"
            elif fname.endswith(".md"): mtype = "text/markdown"
            
        analysis = analyze_with_gemini(raw_text, file_bytes=file_send, mime_type=mtype)
        
        if not analysis or not isinstance(analysis, dict):
            return {"status": "FAILED", "error": "AI analysis returned empty or invalid data."}
            
        if "error" in analysis:
            return {
                "status": "FAILED",
                "error": analysis["error"],
                "raw_text": raw_text
            }
            
        meta_data = analysis.get("metadata", {})
        financials_input = analysis.get("financials", [])
        if isinstance(financials_input, dict): financials_input = [financials_input]
        
        ticker = str(ticker_override or meta_data.get("ticker") or "TBD").upper()
        company = str(company_override or meta_data.get("company_name") or "Unnamed Company")
        
        # Vault to Azure logic
        from utils import upload_to_azure_blob
        safe_source = re.sub(r"[^a-zA-Z0-9\._\-]", "_", source_label)
        vault_name = f"manual_uploads/{ticker}/{datetime.now().strftime('%Y%m%d_%H%M%S')}_{safe_source}"
        upload_content = input_source if source_type == "file" else raw_text
        archived_url = upload_to_azure_blob(upload_content, vault_name)

        rows = []
        for fin in financials_input:
            f_year = fin.get("year") or int(re.search(r"(\d{4})", meta_data.get("period", "")).group(1)) if re.search(r"(\d{4})", meta_data.get("period", "")) else datetime.now().year
            f_currency = fin.get("currency") or "INR"
            
            raw_row = {
                "ticker": ticker, 
                "company_name": company, 
                "fiscal_year": f_year,
                "end_date": fin.get("end_date") or f"{f_year}-12-31",
                "fiscal_period": "FY", 
                "revenue":          normalize_value(fin.get("revenue") or fin.get("total_revenue"), f_currency),
                "net_income":       normalize_value(fin.get("net_income"), f_currency),
                "operating_income": normalize_value(fin.get("operating_income"), f_currency),
                "total_assets":      normalize_value(fin.get("total_assets"), f_currency),
                "total_liabilities": normalize_value(fin.get("total_liabilities"), f_currency),
                "total_equity":      normalize_value(fin.get("total_equity"), f_currency),
                "cash_on_hand":      normalize_value(fin.get("cash_on_hand") or fin.get("cash_and_equivalents"), f_currency),
                "eps_diluted":       normalize_value(fin.get("eps_diluted"), f_currency),
                "data_source": "DOCUMENT_EXTRACTOR", 
                "filing_type": "EXTRACTED", 
                "sec_filing_url": source_label if isinstance(input_source, str) else None,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "archived_url": archived_url
            }
            # Apply Post-Processing Validator
            final_row = clean_financials(raw_row)
            rows.append(final_row)

        if push_to_supabase and rows:
            # Register company
            co_data = {"ticker": ticker, "company_name": company, "sector": meta_data.get("sector", "Other")}
            if meta_data.get("sec_cik"): co_data["sec_cik"] = meta_data["sec_cik"]
            self.db.table("target_companies").upsert(co_data, on_conflict="ticker").execute()
            
            # Upsert financials
            self.db.table("financials").upsert(rows, on_conflict="ticker,fiscal_year,fiscal_period").execute()
            
            # Log extraction
            self.db.table("extracted_documents").insert({
                "ticker": ticker, 
                "company_name": company, 
                "source_url": source_label, 
                "source_type": source_type,
                "extraction_status": "SUCCESS",
                "archived_url": archived_url,
                "raw_text": raw_text,
            }).execute()

        return {
            "status": "SUCCESS",
            "row": rows[0] if rows else {},
            "raw_text": raw_text,
            "analysis": analysis
        }

        
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
        response = requests.get(url, headers=SEC_HEADERS, timeout=12)
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
        resp = requests.get(sub_url, headers=SEC_HEADERS, timeout=8)
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

