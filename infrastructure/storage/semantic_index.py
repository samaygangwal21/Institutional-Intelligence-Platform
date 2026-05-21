import logging
from typing import List, Dict, Any, Optional
from infrastructure.config import get_supabase
from infrastructure.llm.router import get_embedding # Need to make sure this exists

log = logging.getLogger("SemanticIndex")

class SemanticIndex:
    """
    Institutional Semantic Indexing & Hybrid Search Engine.
    Handles vector-based retrieval and keyword-based filtering.
    """
    
    def __init__(self):
        self.sb = get_supabase()

    async def hybrid_search(self, query: str, table: str, ticker: Optional[str] = None, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Executes a hybrid search (Vector + Keyword) on the specified table.
        """
        log.info(f"🔍 Hybrid search in {table} for: {query} (Ticker: {ticker})")
        
        try:
            # 1. Generate embedding
            embedding = await get_embedding(query)
            
            # 2. Execute Supabase RPC for vector match
            # The RPC names follow a convention: match_{table}
            rpc_name = f"match_{table}"
            
            params = {
                "query_embedding": embedding,
                "match_threshold": 0.5,
                "match_count": limit
            }
            
            if ticker:
                params["p_ticker"] = ticker
            
            res = self.sb.rpc(rpc_name, params).execute()
            return res.data or []
            
        except Exception as e:
            log.warning(f"Vector search failed for {table}, falling back to keyword: {e}")
            
            # 3. Fallback: Keyword search
            try:
                query_builder = self.sb.table(table).select("*")
                if ticker:
                    query_builder = query_builder.eq("ticker", ticker)
                
                # Simple keyword match on headline or summary/text
                search_col = "headline" if table == "market_intelligence" else "extracted_text"
                if table == "reports": search_col = "report_markdown"
                
                res = query_builder.ilike(search_col, f"%{query}%").limit(limit).execute()
                return res.data or []
            except Exception as e2:
                log.error(f"Fallback keyword search failed: {e2}")
                return []

semantic_index = SemanticIndex()
