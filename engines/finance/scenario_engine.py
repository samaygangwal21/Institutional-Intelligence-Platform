from typing import Dict, Any, List
from .assumption_engine import assumption_engine

class ScenarioEngine:
    """
    Generates deterministic sensitivity ranges and downside/upside cases.
    """
    
    def generate_scenarios(self, base_metrics: Dict[str, Any], sector: str = "generic") -> Dict[str, Any]:
        """
        Creates Bull, Base, and Bear cases based on deterministic sensitivity analysis.
        """
        scenarios = {
            "base": base_metrics.copy(),
            "bull": self._shift_metrics(base_metrics, 1.1, 0.9), # 10% better
            "bear": self._shift_metrics(base_metrics, 0.9, 1.2)  # 10% worse / 20% higher costs
        }
        
        return scenarios

    def _shift_metrics(self, metrics: Dict[str, Any], multiplier: float, cost_multiplier: float) -> Dict[str, Any]:
        shifted = {}
        for k, v in metrics.items():
            if v is None:
                shifted[k] = None
                continue
            
            if any(x in k for x in ["revenue", "margin", "profit", "fcf"]):
                shifted[k] = v * multiplier
            elif any(x in k for x in ["cost", "debt", "expense"]):
                shifted[k] = v * cost_multiplier
            else:
                shifted[k] = v
        return shifted

    def perform_sensitivity_analysis(self, target_metric: str, input_var: str, range_pct: float = 0.2) -> List[Dict[str, float]]:
        """
        Computes how target_metric changes as input_var varies.
        Deterministic - no LLM involved.
        """
        # Logic to be implemented with specific formulas
        return []

scenario_engine = ScenarioEngine()
