import os
import logging
import time
from datetime import datetime, date, timedelta, timezone
from platform_config import get_supabase, TARGET_COMPANIES
from ingest import ingest_sec_ticker, ingest_news, SECDataMatcher
from utils import delete_from_azure_blob

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("scheduler")

def run_automated_maintenance():
    """
    1. Prunes news older than 90 days (DB + Azure Blobs)
    2. Runs ingestion for any company overdue for a quarterly update
    """
    sb = get_supabase()
    
    # --- 1. DATA PRUNING (90-Day Retention) ---
    log.info("Starting 90-day data pruning...")
    ninety_days_ago = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
    
    try:
        # Get news to delete
        stale_news = sb.table("market_intelligence").select("id, archived_url").lt("published_at", ninety_days_ago).execute().data
        if stale_news:
            log.info(f"Found {len(stale_news)} stale news items to prune.")
            for item in stale_news:
                # Delete from Azure if archived_url exists
                if item.get("archived_url"):
                    # Extract blob path from SAS URL (rudimentary)
                    # Example: https://.../container/news/full_articles/TSLA/...html?sas...
                    try:
                        blob_path = item["archived_url"].split(".net/")[1].split("?")[0]
                        # Remove the container name if it's there
                        from platform_config import AZURE_STORAGE_CONTAINER_NAME
                        blob_path = blob_path.replace(f"{AZURE_STORAGE_CONTAINER_NAME}/", "")
                        delete_from_azure_blob(blob_path)
                    except: pass
                
                # Delete from Supabase
                sb.table("market_intelligence").delete().eq("id", item["id"]).execute()
            log.info("Pruning complete.")
        else:
            log.info("No stale news found.")
    except Exception as e:
        log.error(f"Error during pruning: {e}")

    # --- 1b. VAULT CONSOLIDATION (50-Article limit per Ticker) ---
    log.info("Starting vault consolidation (Max 50 articles per company)...")
    from utils import prune_azure_news_blobs
    for ticker in TARGET_COMPANIES:
        try:
            prune_azure_news_blobs(ticker, max_count=50)
        except Exception as e:
            log.error(f"[{ticker}] Failed to prune vault: {e}")
    log.info("Vault consolidation complete.")

    # --- 2. QUARTERLY INGESTION CHECK ---
    log.info("Checking for overdue quarterly ingestion...")
    matcher = SECDataMatcher(cutoff_years=1)
    
    for ticker in TARGET_COMPANIES:
        try:
            # Check ingestion_metadata
            meta = sb.table("ingestion_metadata").select("*").eq("ticker", ticker).execute().data
            should_run = False
            last_run = None
            
            if not meta:
                log.info(f"[{ticker}] No metadata found. Scheduling initial ingest.")
                should_run = True
            else:
                last_run_str = meta[0].get("last_financial_ingest")
                if last_run_str:
                    last_run = datetime.fromisoformat(last_run_str)
                    # If last run was more than 90 days ago, it's definitely time
                    if (datetime.now(timezone.utc) - last_run).days >= 90:
                        should_run = True
                else:
                    should_run = True

            if should_run:
                log.info(f"[{ticker}] Running scheduled ingestion...")
                
                # Fetch CIK and Name from target_companies
                company_data = sb.table("target_companies").select("company_name, sec_cik").eq("ticker", ticker).execute().data
                if company_data:
                    c_name = company_data[0].get("company_name", ticker)
                    c_cik = company_data[0].get("sec_cik", "").zfill(10)
                    
                    # Run SEC ingestion
                    ingest_sec_ticker(ticker, {"name": c_name, "cik": c_cik}, sb, matcher)
                    # Run News ingestion (last 30 days)
                    ingest_news(ticker, sb, days=30)
                    
                    # Update metadata
                    sb.table("ingestion_metadata").upsert({
                        "ticker": ticker,
                        "last_news_ingest": datetime.now(timezone.utc).isoformat(),
                        "last_financial_ingest": datetime.now(timezone.utc).isoformat()
                    }).execute()
                else:
                    log.warning(f"[{ticker}] Not found in target_companies table. Skipping scheduled ingest.")

                
        except Exception as e:
            log.error(f"[{ticker}] Scheduler error: {e}")

if __name__ == "__main__":
    run_automated_maintenance()
