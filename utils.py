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


# ── 3. Azure Blob Storage Utilities ───────────────────────────────────────────



def generate_signed_blob_url(blob_name: str, expiry_months: int = 3) -> Optional[str]:
    """
    Generates a Shared Access Signature (SAS) URL for a private blob.
    Useful when public access is disabled on the storage account.
    """
    from platform_config import AZURE_STORAGE_CONNECTION_STRING, AZURE_STORAGE_CONTAINER_NAME
    if not AZURE_STORAGE_CONNECTION_STRING:
        return None

    try:
        from azure.storage.blob import (
            BlobServiceClient, generate_blob_sas, BlobSasPermissions
        )
        from datetime import datetime, timedelta, timezone

        # Parse connection string for account name and key
        conn_dict = {kv.split('=', 1)[0]: kv.split('=', 1)[1] for kv in AZURE_STORAGE_CONNECTION_STRING.split(';')}
        account_name = conn_dict.get('AccountName')
        account_key = conn_dict.get('AccountKey')

        if not account_name or not account_key:
            log.error("Could not parse AccountName/Key from connection string.")
            return None

        # Calculate expiry
        expiry_time = datetime.now(timezone.utc) + timedelta(days=30 * expiry_months)
        
        sas_token = generate_blob_sas(
            account_name=account_name,
            container_name=AZURE_STORAGE_CONTAINER_NAME,
            blob_name=blob_name,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=expiry_time
        )

        sas_url = f"https://{account_name}.blob.core.windows.net/{AZURE_STORAGE_CONTAINER_NAME}/{blob_name}?{sas_token}"
        return sas_url
    except Exception as e:
        log.error(f"Failed to generate SAS URL for {blob_name}: {e}")
    return None


def upload_to_azure_blob(file_bytes_or_str: Any, filename_path: str) -> Optional[str]:
    """
    Uploads file content to Azure Blob Storage using the configured connection string.
    Returns the SECURE (SAS-signed) public URL of the uploaded blob.
    """
    from platform_config import AZURE_STORAGE_CONNECTION_STRING, AZURE_STORAGE_CONTAINER_NAME
    
    if not AZURE_STORAGE_CONNECTION_STRING or not AZURE_STORAGE_CONTAINER_NAME:
        log.warning("Azure Storage credentials not configured. Skipping upload for: " + filename_path)
        return None
        
    try:
        from azure.storage.blob import BlobServiceClient, ContentSettings
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(AZURE_STORAGE_CONTAINER_NAME)
        
        # Ensure container exists
        if not container_client.exists():
            container_client.create_container()
            
        # Clean filename path
        safe_filename = filename_path.replace("\\", "/")
        
        blob_client = container_client.get_blob_client(safe_filename)
        
        # Determine Content-Type
        content_type = "application/octet-stream"
        if safe_filename.lower().endswith((".html", ".htm")):
            content_type = "text/html"
        elif safe_filename.lower().endswith(".json"):
            content_type = "application/json"
        elif safe_filename.lower().endswith(".pdf"):
            content_type = "application/pdf"
            
        content_settings = ContentSettings(content_type=content_type)
        
        # Encode string to bytes if needed
        upload_data = file_bytes_or_str.encode('utf-8') if isinstance(file_bytes_or_str, str) else file_bytes_or_str
            
        blob_client.upload_blob(upload_data, overwrite=True, content_settings=content_settings)
        log.info(f"Successfully uploaded to Azure Blob ({content_type}): {safe_filename}")
        
        # Return the signed URL instead of the raw one
        return generate_signed_blob_url(safe_filename)
    except Exception as e:
        log.error(f"Failed to upload to Azure Blob Storage ({filename_path}): {e}")
        return None


def delete_from_azure_blob(blob_path: str) -> bool:
    """
    Deletes a blob from Azure Storage. Used for pruning stale data.
    """
    from platform_config import AZURE_STORAGE_CONNECTION_STRING, AZURE_STORAGE_CONTAINER_NAME
    if not AZURE_STORAGE_CONNECTION_STRING or not AZURE_STORAGE_CONTAINER_NAME:
        return False
    try:
        from azure.storage.blob import BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=AZURE_STORAGE_CONTAINER_NAME, blob=blob_path)
        if blob_client.exists():
            blob_client.delete_blob()
            log.info(f"Deleted stale blob from Azure: {blob_path}")
            return True
    except Exception as e:
        log.error(f"Error deleting blob {blob_path}: {e}")
    return False



def fetch_page_content(url: str) -> Optional[bytes]:
    """
    Fetches the raw content of a page, following redirects.
    Useful for getting 'entire' news articles from various sources.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    }
    try:
        # Finnhub links redirect to news sources. We follow them.
        resp = requests.get(url, headers=headers, timeout=20, allow_redirects=True)
        if resp.ok:
            return resp.content
        log.warning(f"Failed to fetch content from {url}: {resp.status_code}")
    except Exception as e:
        log.error(f"Error fetching page content from {url}: {e}")
    return None

