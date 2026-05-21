import logging
from typing import List, Dict, Any, Optional
from .metric_registry import metric_registry

log = logging.getLogger("DeterministicFinance")

class DeterministicFinanceEngine:
    """
    Central authority for all financial calculations.
    Ensures that ALL quantitative metrics are computed deterministically
    rather than estimated by an LLM.
    """
    
    def __init__(self):
        self.registry = metric_registry

    def compute_all_metrics(self, raw_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Takes raw financial data (e.g. from Supabase) and computes a full suite
        of institutional metrics.
        """
        if not raw_data:
            return {"status": "no_data", "metrics": []}

        # Ensure data is sorted by date DESC
        sorted_data = sorted(raw_data, key=lambda x: x.get('end_date', ''), reverse=True)
        
        computed_periods = []
        for i, row in enumerate(sorted_data):
            period = {
                "label": f"{row.get('fiscal_period')} {row.get('fiscal_year')}",
                "date": row.get('end_date'),
                "metrics": self._calculate_period_metrics(row)
            }
            
            # Growth calculations
            if i + 1 < len(sorted_data):
                prev_row = sorted_data[i+1]
                period["growth"] = self._calculate_growth_metrics(row, prev_row)
                
            computed_periods.append(period)

        return {
            "status": "success",
            "periods": computed_periods,
            "summary": self._generate_summary_trends(computed_periods)
        }

    def _calculate_period_metrics(self, row: Dict[str, Any]) -> Dict[str, float]:
        """Calculates point-in-time ratios."""
        metrics = {}
        data_map = {
            "revenue": self._to_float(row.get("revenue")),
            "gross_profit": self._to_float(row.get("gross_profit")),
            "operating_income": self._to_float(row.get("operating_income")),
            "net_income": self._to_float(row.get("net_income")),
            "total_debt": self._to_float(row.get("long_term_debt")),
            "total_equity": self._to_float(row.get("total_equity")),
            "ebitda": self._to_float(row.get("ebitda")) # if available
        }
        
        # Calculate standard ratios from registry
        for metric in ["gross_margin", "operating_margin", "net_margin", "debt_to_equity"]:
            try:
                metrics[metric] = self.registry.calculate(metric, **data_map)
            except Exception as e:
                log.warning(f"Failed to calculate {metric}: {e}")
        
        return metrics

    def _calculate_growth_metrics(self, curr: Dict[str, Any], prev: Dict[str, Any]) -> Dict[str, float]:
        """Calculates YoY growth metrics."""
        growth = {}
        keys = ["revenue", "net_income", "operating_income"]
        
        for key in keys:
            c_val = self._to_float(curr.get(key))
            p_val = self._to_float(prev.get(key))
            if c_val is not None and p_val:
                growth[f"{key}_growth"] = self.registry.calculate("yoy_growth", curr=c_val, prev=p_val)
                
        return growth

    def _generate_summary_trends(self, periods: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Detects institutional trends like margin compression or expansion."""
        if len(periods) < 2:
            return {}
            
        latest = periods[0]
        prev = periods[1]
        
        analysis = {}
        
        # Margin Check
        l_nm = latest["metrics"].get("net_margin", 0)
        p_nm = prev["metrics"].get("net_margin", 0)
        
        if l_nm < p_nm - 0.01:
            analysis["margin_trend"] = "COMPRESSION"
        elif l_nm > p_nm + 0.01:
            analysis["margin_trend"] = "EXPANSION"
        else:
            analysis["margin_trend"] = "STABLE"
            
        return analysis

    def _to_float(self, val: Any) -> Optional[float]:
        try:
            return float(val) if val is not None else None
        except (ValueError, TypeError):
            return None

finance_engine = DeterministicFinanceEngine()
