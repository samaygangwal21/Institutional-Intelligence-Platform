from typing import List, Dict, Any

class FinancialValidationEngine:
    """
    Validates computed outputs against historical realism and accounting principles.
    Flags impossible or highly unrealistic financial metrics.
    """
    
    def validate_metrics(self, metrics: Dict[str, Any], sector: str = "generic") -> List[str]:
        warnings = []
        
        # 1. Margin Checks
        for m_name in ["gross_margin", "operating_margin", "net_margin"]:
            val = metrics.get(m_name)
            if val is not None:
                if val > 1.0:
                    warnings.append(f"CRITICAL: {m_name} > 100% ({val*100:.1f}%) is mathematically impossible.")
                if val < -5.0:
                    warnings.append(f"WARNING: Extreme {m_name} ({val*100:.1f}%) suggests data error or severe distress.")
        
        # 2. Relationship Checks
        gm = metrics.get("gross_margin")
        om = metrics.get("operating_margin")
        nm = metrics.get("net_margin")
        
        if gm is not None and om is not None and om > gm:
            warnings.append("WARNING: Operating Margin exceeds Gross Margin - invalid accounting logic.")
        if om is not None and nm is not None and nm > om:
             # This can happen with massive one-time tax credits/other income, but worth flagging
            warnings.append("INFO: Net Margin exceeds Operating Margin - check for non-operating income/tax credits.")

        # 3. Sector Specific Realism
        if sector == "renewables":
            irr = metrics.get("irr")
            if irr and irr > 0.30:
                warnings.append(f"WARNING: Project IRR of {irr*100:.1f}% is unusually high for infrastructure.")

        return warnings

financial_validation_engine = FinancialValidationEngine()
