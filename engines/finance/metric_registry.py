from typing import Dict, Callable, Any
import math

class MetricRegistry:
    """
    Canonical source for financial formulas and metric definitions.
    Ensures consistent calculation logic across the entire platform.
    """
    
    FORMULAS: Dict[str, Callable[[Dict[str, Any]], float]] = {
        # Profitability
        "gross_margin": lambda d: (d["gross_profit"] / d["revenue"]) if d.get("revenue") else 0,
        "operating_margin": lambda d: (d["operating_income"] / d["revenue"]) if d.get("revenue") else 0,
        "net_margin": lambda d: (d["net_income"] / d["revenue"]) if d.get("revenue") else 0,
        "ebitda_margin": lambda d: (d["ebitda"] / d["revenue"]) if d.get("revenue") else 0,
        
        # Leverage & Liquidity
        "debt_to_equity": lambda d: (d["total_debt"] / d["total_equity"]) if d.get("total_equity") else 0,
        "current_ratio": lambda d: (d["current_assets"] / d["current_liabilities"]) if d.get("current_liabilities") else 0,
        "dscr": lambda d: (d["net_operating_income"] / d["total_debt_service"]) if d.get("total_debt_service") else 0,
        
        # Growth
        "yoy_growth": lambda curr, prev: ((curr - prev) / abs(prev)) if prev else 0,
        "cagr": lambda start, end, periods: (math.pow(end / start, 1 / periods) - 1) if start and start > 0 and periods > 0 else 0,
        
        # Sector Specific (Placeholders for now)
        "cost_per_mw": lambda d: (d["capex"] / d["capacity_mw"]) if d.get("capacity_mw") else 0,
        "ltv_cac": lambda d: (d["ltv"] / d["cac"]) if d.get("cac") else 0
    }

    @classmethod
    def calculate(cls, metric_name: str, **kwargs) -> float:
        """
        Executes a registered formula.
        """
        formula = cls.FORMULAS.get(metric_name)
        if not formula:
            raise ValueError(f"Metric '{metric_name}' not found in registry.")
        
        if metric_name in ["yoy_growth", "cagr"]:
             # These take direct values rather than a data dict
             return formula(*kwargs.values())
             
        return formula(kwargs)

metric_registry = MetricRegistry()
