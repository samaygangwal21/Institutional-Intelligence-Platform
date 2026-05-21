from typing import List, Dict, Any

class InstitutionalRealismEngine:
    """
    Forces real-world institutional behavior and constraints into the analysis.
    Injects friction and sector-specific bottlenecks.
    """
    
    CONSTRAINTS = {
        "renewables": [
            "Interconnection queue delays (typically 24-48 months)",
            "Transmission curtailment risks",
            "Tax equity availability friction",
            "Equipment lead times (Transformers, Inverters)"
        ],
        "semiconductors": [
            "Fab utilization efficiency thresholds",
            "R&D capitalization intensity",
            "Geopolitical export control friction"
        ],
        "saas": [
            "Customer Acquisition Cost (CAC) payback windows",
            "Net Revenue Retention (NRR) saturation",
            "Platform platform dependency risk"
        ]
    }

    def get_institutional_constraints(self, sector: str) -> List[str]:
        """Returns verified sector-specific constraints that MUST be in the report."""
        return self.CONSTRAINTS.get(sector.lower(), ["Standard market liquidity constraints", "Regulatory compliance overhead"])

    def apply_friction_to_metrics(self, metrics: Dict[str, Any], sector: str) -> Dict[str, Any]:
        """
        Adjusts metrics based on institutional friction (e.g. delaying cash flows).
        """
        # Example: Discounting FCF if in a high-friction sector
        return metrics

institutional_realism_engine = InstitutionalRealismEngine()
