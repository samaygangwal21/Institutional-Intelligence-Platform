from datetime import datetime
from typing import List, Dict, Any, Optional, Union
from supabase import Client
from infrastructure.base_infra import cache, resilience, obs
from infrastructure.config import get_supabase, get_company_meta, AZURE_STORAGE_CONNECTION_STRING, AZURE_STORAGE_CONTAINER_NAME
from infrastructure.storage.semantic_index import semantic_index
import logging as log

# ── Azure Blob Storage Setup ──────────────────────────────────────────────────
try:
    from azure.storage.blob import BlobServiceClient # type: ignore
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING) if AZURE_STORAGE_CONNECTION_STRING else None
except ImportError:
    blob_service_client = None

class VaultService:
    """
    Service Layer for Institutional Research Vault.
    Handles storage and retrieval of reports and documents.
    """
    
    def __init__(self, supabase: Optional[Client] = None):
        self.sb = supabase or get_supabase()

    @cache.cached(namespace="vault", ttl=300)
    def get_all_reports(self, ticker: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Loads historical reports from the vault.
        """
        with obs.track_latency("vault.get_all_reports"):
            try:
                query = self.sb.table("reports").select("*").not_.is_("report_markdown", "null").neq("report_markdown", "").order("created_at", desc=True)
                if ticker:
                    query = query.eq("ticker", ticker)
                
                res = query.execute()
                return res.data or []
            except Exception as e:
                obs.track_error("vault_service", e)
                return []

    def get_latest_report(self, ticker: str) -> Optional[Dict[str, Any]]:
        reports = self.get_all_reports(ticker)
        return reports[0] if reports else None

    @cache.cached(namespace="vault", ttl=300)
    def get_extracted_docs(self, ticker: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Loads vaulted extraction documents.
        """
        with obs.track_latency("vault.get_extracted_docs"):
            try:
                query = self.sb.table("extracted_documents").select("*").order("created_at", desc=True)
                if ticker:
                    query = query.eq("ticker", ticker)
                
                res = query.execute()
                return res.data or []
            except Exception as e:
                obs.track_error("vault_service.docs", e)
                return []

    async def search_reports(self, query: str) -> List[Dict[str, Any]]:
        """
        Uses semantic search to find relevant institutional reports in the vault.
        """
        with obs.track_latency("vault.search_reports"):
            try:
                # Perform hybrid search on the 'reports' table
                # We prioritize the search over 'report_markdown' or 'report_title'
                return await semantic_index.hybrid_search(query, "reports")
            except Exception as e:
                obs.track_error("vault_service.search", e)
                return []

    def upload_to_azure(self, content: Union[str, bytes], filename: str) -> Optional[str]:
        """Uploads content to Azure Blob Storage and returns the URL."""
        if not blob_service_client or not AZURE_STORAGE_CONTAINER_NAME:
            return None
        with obs.track_latency("vault.azure_upload"):
            try:
                blob_client = blob_service_client.get_blob_client(container=AZURE_STORAGE_CONTAINER_NAME, blob=filename)
                blob_client.upload_blob(content, overwrite=True)
                return blob_client.url
            except Exception as e:
                obs.track_error("vault_service.azure", e)
                log.error(f"Azure Upload Failed: {e}")
                return None

    def save_report_to_vault(self, ticker: str, report_md: str, query: str, manifest: List[Dict[str, Any]] = None, mode: str = "orchestrated") -> Optional[Dict[str, Any]]:
        """
        Saves a generated report to Supabase and archives a copy in Azure Blob Storage.
        """
        with obs.track_latency("vault.save_report"):
            timestamp = datetime.now().isoformat()
            ticker_meta = get_company_meta(ticker)
            company_name = ticker_meta.get("name", ticker) if ticker_meta else ticker

            report_data = {
                "ticker": ticker,
                "company_name": company_name,
                "report_markdown": report_md,
                "report_title": report_md.split('\n')[0].replace('# ', '').strip() if report_md.startswith('# ') else f"Institutional Research: {ticker}",
                "fiscal_year": datetime.now().year,
                "fiscal_period": "ORCHESTRATED",
                "verification_status": "VERIFIED",
                "compliance_score": 100,
                "data_snapshot": {"query": query, "mode": mode, "manifest": manifest or []},
                "created_at": timestamp
            }
            
            # Azure Archiving
            azure_filename = f"reports/{ticker}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
            azure_url = self.upload_to_azure(report_md, azure_filename)
            if azure_url:
                report_data["data_snapshot"]["azure_url"] = azure_url

            try:
                @resilience.with_retry(retries=3)
                def _insert():
                    return self.sb.table("reports").insert(report_data).execute()
                res = _insert()
                return res.data[0] if res.data else None
            except Exception as e:
                obs.track_error("vault_service.save_db", e)
                log.error(f"Failed to save report to vault DB: {e}")
                return None

# Global singleton
vault_service = VaultService()
