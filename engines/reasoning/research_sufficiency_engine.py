"""
research_sufficiency_engine.py — Institutional Research Sufficiency Engine
========================================================================
Determines if sufficient validated evidence exists to support specific analytical domains.
"""

import logging
from typing import List, Dict, Any

log = logging.getLogger("ResearchSufficiencyEngine")

class ResearchSufficiencyEngine:
    def __init__(self):
        pass

    def evaluate_sufficiency(self, packets: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Evaluates the completeness, metrics, authority, and recency of available evidence.
        Returns a map indicating allowed reasoning domains and missing elements.
        """
        # Initialize flags
        financial_allowed = False
        risk_allowed = False
        ecosystem_allowed = False
        irr_allowed = False
        strategic_allowed = False
        
        missing_requirements = []
        
        # Track presence of different sources
        sec_count = 0
        market_count = 0
        news_count = 0
        ecosystem_count = 0
        vault_count = 0
        
        has_revenue = False
        has_cashflow = False
        has_supplier_details = False
        has_risk_factors = False
        
        for p in packets:
            source_type = p.get("source_type", "").lower()
            content = p.get("content", "").lower()
            
            # Count authoritative sources
            if source_type in ["sec", "live_sec"]:
                sec_count += 1
            elif source_type == "market":
                market_count += 1
            elif source_type == "news":
                news_count += 1
            elif source_type == "ecosystem":
                ecosystem_count += 1
            elif source_type == "vault":
                vault_count += 1
                
            # Perform content checks for specific metric keywords
            if any(k in content for k in ["revenue", "net income", "operating margin", "ebitda", "sales"]):
                has_revenue = True
            if any(k in content for k in ["cash flow", "cashflow", "capex", "capital expenditure"]):
                has_cashflow = True
            if any(k in content for k in ["supplier", "supply chain", "partner", "relationship", "customer"]):
                has_supplier_details = True
            if any(k in content for k in ["risk", "mitigation", "covenant", "liability", "uncertainty"]):
                has_risk_factors = True

        # Rule 1: Financial Performance Analysis Sufficiency
        # Requires SEC filings or deterministic market data and basic revenue metrics
        if (sec_count > 0 or market_count > 0 or vault_count > 0) and has_revenue:
            financial_allowed = True
        else:
            missing_requirements.append("Primary financial statement metrics (revenue/margins) from SEC filings or Vault data.")

        # Rule 2: IRR / Project Modeling Sufficiency
        # Requires explicit quantitative cashflow metrics or capex projections
        if financial_allowed and has_cashflow:
            irr_allowed = True
        else:
            missing_requirements.append("Quantitative cashflow statements or capex assumptions required for project IRR modeling.")

        # Rule 3: Ecosystem / Concentration Analysis Sufficiency
        # Requires corporate connection records or clear partnership entries
        if ecosystem_count > 0 or has_supplier_details:
            ecosystem_allowed = True
        else:
            missing_requirements.append("Supply chain dependencies, supplier concentration, or partner mappings.")

        # Rule 4: Risk Assessment Sufficiency
        # Requires risk factors or compliance deadline references
        if has_risk_factors and (sec_count > 0 or news_count > 0 or vault_count > 0):
            risk_allowed = True
        else:
            missing_requirements.append("Documented operational risk factors or compliance liability disclosures.")

        # Rule 5: Strategic Outlook Sufficiency
        # Requires news sentiment, SEC descriptions, or management discussion
        if news_count > 0 or sec_count > 0 or vault_count > 0:
            strategic_allowed = True
        else:
            missing_requirements.append("Management strategic outlook, market developments, or corporate communications.")

        # Calculate a robust base confidence score based on source variety & completeness
        score_components = [
            50 if financial_allowed else 0,
            15 if risk_allowed else 0,
            15 if ecosystem_allowed else 0,
            10 if strategic_allowed else 0,
            10 if irr_allowed else 0
        ]
        confidence_score = sum(score_components)
        
        # Penalize if authority is low (e.g. no SEC filings, only general news or web search)
        if sec_count == 0 and market_count == 0 and vault_count == 0:
            confidence_score = max(0, confidence_score - 30)
            
        return {
            "financial_analysis_allowed": financial_allowed,
            "risk_analysis_allowed": risk_allowed,
            "ecosystem_analysis_allowed": ecosystem_allowed,
            "irr_modeling_allowed": irr_allowed,
            "strategic_outlook_allowed": strategic_allowed,
            "confidence_score": confidence_score,
            "missing_requirements": missing_requirements
        }

sufficiency_engine = ResearchSufficiencyEngine()
