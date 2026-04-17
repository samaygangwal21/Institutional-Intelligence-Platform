import os
import json
import time
import requests
import logging
import re
from datetime import datetime, date, timedelta, timezone
from platform_config import get_supabase
from utils import upload_to_azure_blob, SEC_HEADERS, fetch_page_content
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("backfill")

def run_backfill():
    log.info("Starting historical Azure backfill...")
    sb = get_supabase()
    
    # 1. Backfill Past Uploads (extracted_documents)
    log.info("Backfilling past uploaded documents...")
    try:
        docs = sb.table("extracted_documents").select("file_name,raw_text,source_type").eq("source_type", "file").execute().data
        for doc in (docs or []):
            if doc.get("raw_text") and doc.get("file_name"):
                log.info(f"Archiving unstructured upload: {doc['file_name']}")
                upload_to_azure_blob(doc["raw_text"], f"uploads/{doc['file_name']}")
    except Exception as e:
        log.error(f"Error backfilling uploads: {e}")

    # 2. Backfill 10-K / 10-Q (Financials)
    log.info("Backfilling historical SEC Filings (10-K/10-Q)...")
    try:
        fin = sb.table("financials").select("ticker,fiscal_year,fiscal_period,filing_type,sec_filing_url").execute().data
        for row in (fin or []):
            url = row.get("sec_filing_url")
            if url:
                try:
                    # If it's an SEC viewer link, resolve raw path
                    if "ix?doc=" in url:
                        raw_path = url.split("?doc=")[-1]
                        raw_doc_url = "https://www.sec.gov" + raw_path
                    else:
                        raw_doc_url = url
                        
                    log.info(f"Downloading historical 10-K/Q: {raw_doc_url}")
                    
                    time.sleep(0.15)
                    doc_resp = requests.get(raw_doc_url, headers=SEC_HEADERS, timeout=30)
                    if doc_resp.ok:
                        fname = f"sec_filings/{row['ticker']}_{row['filing_type']}_{row['fiscal_year']}_{row['fiscal_period']}.htm"
                        upload_to_azure_blob(doc_resp.content, fname)
                except Exception as e:
                    log.error(f"Error archiving {row['ticker']} 10-K/Q: {e}")
    except Exception as e:
        log.error(f"Error backfilling 10-K/Q: {e}")

    # 3. Backfill 8-K (Corporate Connections)
    log.info("Backfilling historical SEC Filings (8-K)...")
    try:
        conns = sb.table("corporate_connections").select("source_ticker,sec_filing_url").neq("sec_filing_url", None).execute().data
        for conn in (conns or []):
            url = conn.get("sec_filing_url")
            ticker = conn.get("source_ticker")
            if url and "8-K" in url:
                try:
                    log.info(f"Downloading historical 8-K: {url}")
                    time.sleep(0.15)
                    doc_resp = requests.get(url, headers=SEC_HEADERS, timeout=30)
                    if doc_resp.ok:
                        accn = url.split("/")[-2]
                        # Organized path
                        fname = f"sec_filings/8-K/{ticker}/{accn}.htm"
                        upload_to_azure_blob(doc_resp.content, fname)
                except Exception as e:
                    log.error(f"Error archiving {ticker} 8-K: {e}")
    except Exception as e:
        log.error(f"Error backfilling 8-Ks: {e}")

    # 4. Backfill News (market_intelligence)
    log.info("Backfilling full content for news from the last 30 days...")
    try:
        thirty_days_ago = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        news = sb.table("market_intelligence").select("*").gt("published_at", thirty_days_ago).execute().data
        
        if news:
            log.info(f"Found {len(news)} news items from last 30 days for full-content scraping.")
            for item in (news or []):
                ticker = item.get("ticker")
                url = item.get("url")
                headline = item.get("headline", "article")
                pub_date = item.get("published_at", "")[:10]
                
                if url:
                    content = fetch_page_content(url)
                    if content:
                        # Sanitize headline
                        safe_headline = re.sub(r'[^\w\s-]', '', headline).strip().replace(' ', '_')[:60]
                        file_path = f"news/full_articles/{ticker}/{pub_date}_{safe_headline}.html"
                        log.info(f"Archiving full article: {file_path}")
                        upload_to_azure_blob(content, file_path)
                    
                    # Also archive the JSON snapshot for this specific article if not already done
                    article_json = json.dumps(item)
                    upload_to_azure_blob(article_json, f"news/payloads/{ticker}/{pub_date}_{safe_headline}.json")
                        
    except Exception as e:
        log.error(f"Error backfilling news full-content: {e}")
    
    log.info("✅ Backfill Complete! All historical databases entries backed up to Azure.")


if __name__ == "__main__":
    run_backfill()
