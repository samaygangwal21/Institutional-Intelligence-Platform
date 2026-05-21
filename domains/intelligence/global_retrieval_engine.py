import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

log = logging.getLogger("GlobalRetrieval")

class GlobalRetrievalEngine:
    """
    Master Orchestrator for Real-Time External Intelligence.
    Executes concurrent retrieval from live web sources.
    """
    
    def __init__(self, agents: Dict[str, Any]):
        self.agents = agents

    async def execute_live_research(self, query: str, tickers: List[str]) -> Dict[str, Any]:
        """
        Runs parallel retrieval tasks for news, SEC, and global web context.
        """
        log.info(f"🚀 Executing global live research for {tickers}...")
        
        tasks = []
        
        # 1. Ticker-specific live research
        for ticker in tickers:
            if ticker == "GLOBAL": continue
            
            # Live News (Finnhub)
            if "live_news" in self.agents:
                tasks.append(self.agents["live_news"].fetch(ticker, None))
            
            # Live SEC (EDGAR)
            if "live_sec" in self.agents:
                tasks.append(self.agents["live_sec"].fetch(ticker, None))

        # 2. Global Deep Web Search (Unrestricted)
        if "web_search" in self.agents:
            tasks.append(self.agents["web_search"].fetch(query, None))

        if not tasks:
            return {"live_intel": [], "status": "no_tasks"}

        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_packets = []
        for res in results:
            if isinstance(res, list):
                all_packets.extend(res)
            elif isinstance(res, Exception):
                log.error(f"Live research task failed: {res}")

        log.info(f"✅ Live research complete. Gathered {len(all_packets)} intelligence packets.")
        return {
            "live_intel": all_packets,
            "status": "success",
            "timestamp": datetime.now().isoformat()
        }
