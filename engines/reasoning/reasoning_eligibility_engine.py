"""
reasoning_eligibility_engine.py — Institutional Reasoning Eligibility Engine
============================================================================
Controls analytical bounds and replaces unsupported sections with professional analyst reframing.
"""

import logging
from typing import Dict, Any, List

log = logging.getLogger("ReasoningEligibilityEngine")

class ReasoningEligibilityEngine:
    def __init__(self):
        # Map sections to sufficiency keys
        self.section_mapping = {
            "FINANCIAL_ANALYSIS": "financial_analysis_allowed",
            "COMPETITIVE_ECOSYSTEM": "ecosystem_analysis_allowed",
            "RISK_ASSESSMENT": "risk_analysis_allowed",
            "STRATEGIC_OUTLOOK": "strategic_outlook_allowed"
        }

    def check_eligibility(self, section: str, sufficiency: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validates if a given report section has enough evidence to be analyzed.
        Returns a dict containing eligibility status and appropriate guidance/fallback text.
        """
        # Executive Summary is always allowed, but must reflect the overall sufficiency profile
        if section == "EXECUTIVE_SUMMARY":
            return {
                "eligible": True,
                "reasoning_rules": "Summarize the research findings. If any domains (Financials, Ecosystem, Risks) have limited disclosure visibility or insufficient filing granularity, explicitly describe these constraints in a professional, senior-analyst tone and call out where further diligence is required."
            }

        suff_key = self.section_mapping.get(section)
        if not suff_key:
            # Helper/sub-sections are allowed
            return {"eligible": True, "reasoning_rules": ""}

        is_eligible = sufficiency.get(suff_key, False)
        
        if is_eligible:
            extra_instructions = ""
            if section == "FINANCIAL_ANALYSIS" and not sufficiency.get("irr_modeling_allowed", False):
                extra_instructions = "\nWARNING: A comprehensive project-level IRR sensitivity analysis cannot be performed due to restricted project cashflow granularity. Focus exclusively on actual historical figures and state that additional project-level operational disclosures would be required for a detailed IRR modeling."
                
            return {
                "eligible": True,
                "reasoning_rules": f"Ground every statement in the validated context. Avoid speculative filler or ungrounded optimistic projections.{extra_instructions}"
            }
        
        # Section is NOT eligible. Generate a highly polished, professional investment analyst fallback.
        fallback_md = self._get_professional_fallback(section, sufficiency)
        return {
            "eligible": False,
            "fallback_text": fallback_md
        }

    def _get_professional_fallback(self, section: str, sufficiency: Dict[str, Any]) -> str:
        """Generates a professional analyst-grade statement reframing the missing information."""
        gap_details = self._get_missing_requirements_for_section(section, sufficiency)
        bullets = "\n".join([f"- {item}" for item in gap_details])
        
        if section == "FINANCIAL_ANALYSIS":
            header = "### 1. FINANCIAL PERFORMANCE & DISCLOSURE VISIBILITY"
            desc = "A comprehensive financial analysis and quantitative modeling cannot be performed at this time due to limited disclosure visibility and insufficient filing granularity in the currently available corporate disclosures. Further diligence is required to verify these financial dimensions."
        elif section == "COMPETITIVE_ECOSYSTEM":
            header = "### 4. CORPORATE ECOSYSTEM & RELATIONSHIP TRANSPARENCY"
            desc = "An exhaustive corporate ecosystem and supply chain mapping is deferred due to restricted supplier-level transparency and limited customer segment visibility in the public reports. Further diligence is required to map these dependency networks."
        elif section == "RISK_ASSESSMENT":
            header = "### 5. RISK HORIZONS & COMPLIANCE GRANULARITY"
            desc = "A formalized compliance and operational risk audit is deferred due to restricted project-level transparency and lack of specific liability granularity in the current filing scope. Further diligence is required to outline definitive risk mitigation horizons."
        else: # STRATEGIC_OUTLOOK
            header = "### 7. STRATEGIC TRAJECTORY & DIRECTIONAL VISIBILITY"
            desc = "A forward-looking strategic execution roadmap is deferred due to limited management guidance visibility and lack of specific forward capex targets in the verified datasets. Further diligence is required to formulate execution milestones."

        fallback_md = f"""{header}

{desc}

**Key parameters requiring supplemental disclosure:**
{bullets}

*To finalize analysis for this domain, supplementary operational disclosures or additional filing granularity should be integrated into the primary research context.*
"""
        return fallback_md

    def _get_missing_requirements_for_section(self, section: str, sufficiency: Dict[str, Any]) -> List[str]:
        """Maps specific section failures to their corresponding missing details in a professional tone."""
        missing = []
        if section == "FINANCIAL_ANALYSIS":
            missing.append("Verified historical income statements and balance sheets (SEC 10-K/10-Q filing levels).")
            if not sufficiency.get("irr_modeling_allowed", False):
                missing.append("Project-level cashflow structures or forward capex schedules.")
        elif section == "COMPETITIVE_ECOSYSTEM":
            missing.append("Detailed supplier concentration metrics or customer connection records.")
        elif section == "RISK_ASSESSMENT":
            missing.append("Formalized operational risk logs or compliance trigger timelines.")
        elif section == "STRATEGIC_OUTLOOK":
            missing.append("Management strategic execution targets or forward quarterly KPI milestones.")
            
        return missing

eligibility_engine = ReasoningEligibilityEngine()
