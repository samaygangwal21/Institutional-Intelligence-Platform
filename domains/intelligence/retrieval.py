from typing import List, Dict, Any, Optional
from supabase import Client
from infrastructure.base_infra import cache, resilience, obs
from infrastructure.config import get_supabase, SEC_HEADERS, get_company_meta, get_gemini_key, GEMINI_ENDPOINT
from infrastructure.storage.semantic_index import semantic_index
from domains.reports.vault import vault_service
import requests
import json
import re
from loguru import logger

class RetrievalService:
    """
    Service Layer for External Intelligence Retrieval.
    Handles News, SEC connections, and other market intelligence.
    """
    
    def __init__(self, supabase: Optional[Client] = None):
        self.sb = supabase or get_supabase()

    @cache.cached(namespace="retrieval", ttl=300)
    def get_news(self, ticker: str, limit: int = 60) -> List[Dict[str, Any]]:
        with obs.track_latency("retrieval.get_news"):
            try:
                res = self.sb.table("market_intelligence").select("headline, published_at, url, sentiment_score").eq("ticker", ticker).order("published_at", desc=True).limit(limit).execute()
                return res.data or []
            except Exception as e:
                obs.track_error("retrieval_service.news", e)
                return []

    @cache.cached(namespace="retrieval", ttl=600)
    def get_connections(self, ticker: str) -> List[Dict[str, Any]]:
        with obs.track_latency("retrieval.get_connections"):
            try:
                res = self.sb.table("corporate_connections").select("*").eq("source_ticker", ticker).execute()
                return res.data or []
            except Exception as e:
                obs.track_error("retrieval_service.connections", e)
                return []

    async def hybrid_search(self, query: str, ticker: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Institutional Hybrid Search: Combines Vector Similarity with Keyword filtering.
        Searches across news and connections.
        """
        with obs.track_latency("retrieval.hybrid_search"):
            return await semantic_index.hybrid_search(query, "market_intelligence", ticker=ticker)

    class SEC8KFetcher:
        FILING_BASE = "https://www.sec.gov/Archives/edgar/data"
        def __init__(self, cik: str, ticker: str):
            self.cik = cik.lstrip("0").zfill(10)
            self.ticker = ticker
        
        @resilience.with_retry(retries=3, backoff=2.0)
        def get_8k_filings(self) -> List[Dict]:
            url = f"https://data.sec.gov/submissions/CIK{self.cik}.json"
            resp = requests.get(url, headers=SEC_HEADERS, timeout=20)
            resp.raise_for_status()
            recent = resp.json().get("filings", {}).get("recent", {})
            return [{"accn": accn, "doc": doc, "url": f"{self.FILING_BASE}/{int(self.cik)}/{accn.replace('-','')}/{doc}"} 
                    for i, (form, accn, doc) in enumerate(zip(recent.get("form", []), recent.get("accessionNumber", []), recent.get("primaryDocument", [])))
                    if form == "8-K"][:5]

    def discover_connections(self, ticker: str):
        """
        Deep Network Discovery: Parses SEC 8-K filings for corporate relationships.
        This is designed to run in a background worker thread.
        """
        meta = get_company_meta(ticker)
        if not meta or not meta.get("cik"): return
        source_company = meta.get("name", ticker)
        fetcher = self.SEC8KFetcher(meta["cik"], ticker)
        
        try:
            filings = fetcher.get_8k_filings()
        except Exception as e:
            logger.warning(f"Could not fetch 8-K index for {ticker}: {e}")
            return

        for f in filings:
            try:
                resp = requests.get(f["url"], headers=SEC_HEADERS, timeout=15)
                resp.raise_for_status()
                text = resp.text[:5000]
                
                # Archive the raw 8-K filing to Azure via VaultService
                vault_service.upload_to_azure(resp.content, f"sec_filings/8-K/{ticker}/{f['accn']}.htm")
                
            except Exception as e:
                logger.warning(f"Could not fetch or archive 8-K {f['accn']}: {e}")
                continue
                
            prompt = f"Extract corporate relationships (Acquisitions, Partnerships, etc) for {ticker} from: {text}. Respond ONLY with a JSON array of objects with keys: target_company (string), relationship_type (one of: ACQUISITION, INVESTMENT, PARTNERSHIP, SUPPLIER, CUSTOMER, SUBSIDIARY, JOINT_VENTURE, LICENSING, COMPETITOR, STRATEGIC_ALLIANCE), relationship_detail (string)."
            
            try:
                # Direct Gemini Call with retries handled manually or by resilience
                @resilience.with_retry(retries=2)
                def _call_gemini():
                    key = get_gemini_key()
                    payload = {"contents": [{"parts": [{"text": prompt}]}]}
                    r = requests.post(f"{GEMINI_ENDPOINT}?key={key}", json=payload, timeout=60)
                    r.raise_for_status()
                    return r.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
                
                raw = re.sub(r"```(?:json)?", "", _call_gemini()).strip("` \n")
                if not raw.startswith("["):
                    continue
                rels = json.loads(raw)
                
                for rel in rels:
                    if not rel.get("target_company"): continue
                    rel_type = rel.get("relationship_type", "PARTNERSHIP")
                    valid_types = {"ACQUISITION","INVESTMENT","PARTNERSHIP","SUPPLIER","CUSTOMER","SUBSIDIARY","JOINT_VENTURE","LICENSING","COMPETITOR","STRATEGIC_ALLIANCE"}
                    if rel_type not in valid_types:
                        rel_type = "PARTNERSHIP"
                    
                    self.sb.table("corporate_connections").upsert({
                        "source_ticker": ticker,
                        "source_company": source_company,
                        "target_company": rel.get("target_company"),
                        "relationship_type": rel_type,
                        "relationship_detail": rel.get("relationship_detail", ""),
                    }).execute()
            except Exception as e:
                logger.warning(f"[{ticker}] Connection parse error: {e}")
                continue


# Global singleton
retrieval_service = RetrievalService()
