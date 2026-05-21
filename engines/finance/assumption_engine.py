from typing import Dict, Any, Optional

class AssumptionEngine:
    """
    Tracks and stores all financial assumptions explicitly.
    Prevents 'hidden' AI assumptions by forcing external definition.
    """
    
    DEFAULT_ASSUMPTIONS = {
        "discount_rate": 0.10,
        "tax_rate": 0.21,
        "inflation_rate": 0.02,
        "terminal_growth": 0.02,
        "renewables": {
            "useful_life": 25,
            "itc_rate": 0.30,
            "degradation_rate": 0.005
        },
        "saas": {
            "churn_rate": 0.05,
            "expansion_rate": 0.10
        }
    }

    def __init__(self):
        self.active_assumptions = self.DEFAULT_ASSUMPTIONS.copy()

    def get_assumption(self, key: str, sector: Optional[str] = None) -> Any:
        if sector and sector in self.active_assumptions:
            return self.active_assumptions[sector].get(key)
        return self.active_assumptions.get(key)

    def set_assumption(self, key: str, value: Any, sector: Optional[str] = None):
        if sector:
            if sector not in self.active_assumptions:
                self.active_assumptions[sector] = {}
            self.active_assumptions[sector][key] = value
        else:
            self.active_assumptions[key] = value

    def get_all_active(self) -> Dict[str, Any]:
        return self.active_assumptions

assumption_engine = AssumptionEngine()
