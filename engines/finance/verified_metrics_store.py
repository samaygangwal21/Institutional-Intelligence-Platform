from typing import Dict, Any, Optional
import threading

class VerifiedMetricsStore:
    """
    Persistence layer for verified numerical data.
    Ensures the LLM consumes the SAME verified metrics across all report sections.
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(VerifiedMetricsStore, cls).__new__(cls)
                cls._instance.store = {}
            return cls._instance

    def save_metrics(self, ticker: str, data: Dict[str, Any]):
        """Saves verified metrics for a specific ticker/company."""
        self.store[ticker.upper()] = {
            "timestamp": "now", # Placeholder for real timestamp
            "data": data
        }

    def get_metrics(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Retrieves verified metrics."""
        entry = self.store.get(ticker.upper())
        return entry["data"] if entry else None

    def clear(self):
        self.store = {}

metrics_store = VerifiedMetricsStore()
