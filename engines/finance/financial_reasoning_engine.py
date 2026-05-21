from typing import List, Dict, Any, Optional
from .deterministic_finance_engine import finance_engine
from .verified_metrics_store import metrics_store
from .precision_control_engine import precision_engine

class FinancialReasoningEngine:
    """
    Deterministic Financial Analysis Engine.
    Now delegates to specialized computation engines to ensure institutional accuracy.
    """
    
    def calculate_metrics(self, financials: List[Dict[str, Any]], ticker: Optional[str] = None) -> Dict[str, Any]:
        """
        Processes raw financial rows and returns verified deterministic metrics.
        """
        if not financials:
            return {"status": "no_data", "metrics": {}}

        # 1. Compute deterministic metrics
        report = finance_engine.compute_all_metrics(financials)
        
        # 2. Apply Institutional Precision Control
        for period in report.get("periods", []):
            for m_key, m_val in period["metrics"].items():
                period["metrics"][m_key] = precision_engine.apply_realism(m_val)
        
        # 3. Save to Verified Metrics Store for cross-section consistency
        if ticker:
            metrics_store.save_metrics(ticker, report)
            
        return report

financial_engine = FinancialReasoningEngine()
