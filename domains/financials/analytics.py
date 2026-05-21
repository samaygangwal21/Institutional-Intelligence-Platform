import pandas as pd
from typing import List, Dict, Any, Optional
from supabase import Client
from infrastructure.base_infra import cache, resilience, obs
from infrastructure.config import get_supabase

class AnalyticsService:
    """
    Service Layer for Institutional Analytics.
    Wraps database calls with caching, metrics, and failure resilience.
    """
    
    def __init__(self, supabase: Optional[Client] = None):
        self.sb = supabase or get_supabase()

    @cache.cached(namespace="analytics", ttl=300)
    def get_financials(self, ticker: str) -> List[Dict[str, Any]]:
        """
        Fetches structured financials for a ticker.
        """
        with obs.track_latency("analytics.get_financials"):
            try:
                @resilience.with_retry(retries=3)
                def _fetch():
                    res = self.sb.table("financials").select("*").eq("ticker", ticker).order("end_date").execute()
                    return res.data or []
                
                return _fetch()
            except Exception as e:
                obs.track_error("analytics_service", e)
                return []

    @cache.cached(namespace="analytics", ttl=600)
    def get_sector_snapshot(self, sector: str) -> pd.DataFrame:
        """
        Loads latest financial snapshot for companies in a specific sector.
        Institutional Postgres path (Direct Supabase Retrieval).
        """
        if not sector or sector == "-- Select Sector --":
            return pd.DataFrame()

        with obs.track_latency("analytics.get_sector_snapshot"):
            try:
                # ── Tier 1: Supabase Primary Path ───────────────────────────
                # 1. Fetch companies in sector
                query = self.sb.table("target_companies").select("ticker, company_name, sector")
                if sector != "All":
                    query = query.eq("sector", sector)
                else:
                    query = query.limit(100)
                
                co_res = query.execute()
                if not co_res.data:
                    return pd.DataFrame()
                
                # 2. Batch fetch financials for all tickers in one go
                tickers = [co["ticker"] for co in co_res.data]
                fin_res = self.sb.table("financials").select("*").in_("ticker", tickers).order("end_date", desc=True).execute()
                
                # Group by ticker to get the latest
                latest_fins = {}
                for f in (fin_res.data or []):
                    t = f["ticker"]
                    if t not in latest_fins:
                        latest_fins[t] = f
                
                rows = []
                for co in co_res.data:
                    t = co["ticker"]
                    r = latest_fins.get(t)
                    if r:
                        rows.append({
                            "ticker":     t,
                            "company":    co["company_name"],
                            "sector":     co.get("sector", "Uncategorized"),
                            "revenue":    r.get("revenue"),
                            "net_income": r.get("net_income"),
                            "op_income":  r.get("operating_income"),
                            "cash":       r.get("cash_on_hand"),
                            "equity":     r.get("total_equity"),
                            "assets":     r.get("total_assets"),
                            "eps":        r.get("eps_diluted"),
                        })
                return pd.DataFrame(rows)
            except Exception as e:
                obs.track_error("analytics_service.snapshot", e)
                return pd.DataFrame()

    @cache.cached(namespace="analytics", ttl=3600)
    def get_all_sectors(self) -> List[str]:
        with obs.track_latency("analytics.get_all_sectors"):
            try:
                res = self.sb.table("target_companies").select("sector").execute()
                return sorted(list(set(r["sector"] for r in (res.data or []) if r.get("sector"))))
            except Exception as e:
                obs.track_error("analytics_service.sectors", e)
                return ["Technology", "Financials", "Healthcare", "Energy"]

# Global singleton
analytics_service = AnalyticsService()
