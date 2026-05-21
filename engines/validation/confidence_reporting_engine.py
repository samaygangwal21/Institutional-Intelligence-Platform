"""
confidence_reporting_engine.py — Institutional Confidence Reporting Engine
========================================================================
Attaches confidence levels, evidence density, and retrieval quality audits to report sections.
"""

import logging
from typing import Dict, Any

log = logging.getLogger("ConfidenceReportingEngine")

class ConfidenceReportingEngine:
    def __init__(self):
        pass

    def format_section_metadata(self, section_name: str, density_report: Dict[str, Any], sufficiency_report: Dict[str, Any]) -> str:
        """
        Formats a clean, visual institutional metadata panel for a report section in debug mode only.
        """
        from pipelines.reasoning.observability_engine import DEBUG_MODE
        
        # Hide internal system metrics from customer-facing reports
        if not DEBUG_MODE:
            return ""
            
        score = density_report.get("density_score", 0)
        rating = density_report.get("reliability_rating", "SPECULATIVE")
        volume = density_report.get("volume", 0)
        
        # Color coding indicators
        if score >= 80:
            indicator = "🟢 DEFENSIBLE"
        elif score >= 60:
            indicator = "🟡 ADEQUATE"
        else:
            indicator = "🔴 SPECULATIVE / SPARSE"
            
        metadata_panel = f"""
> [!NOTE]
> **INSTITUTIONAL METADATA AUDIT — {section_name.replace('_', ' ').upper()}**
> - **Evidence Density:** `{score}%` | **Reliability Rating:** `{rating}`
> - **Retrieved Sources Count:** `{volume} packets` | **Validation Audit:** `{indicator}`
"""
        return metadata_panel.strip()

    def generate_overall_scorecard(self, sufficiency_report: Dict[str, Any]) -> str:
        """
        Generates the master reporting panel indicating approved reasoning domains in debug mode only.
        """
        from pipelines.reasoning.observability_engine import DEBUG_MODE
        
        # Hide overall sufficiency diagnostics from client-facing executive reports
        if not DEBUG_MODE:
            return ""
            
        score = sufficiency_report.get("confidence_score", 0)
        
        # Map domains to status bullet points
        financial = "🟢 Approved (SEC/Vault Grounded)" if sufficiency_report.get("financial_analysis_allowed") else "🔴 Suspended (Insufficient Filing Context)"
        irr = "🟢 Approved (Cashflow Context Complete)" if sufficiency_report.get("irr_modeling_allowed") else "🔴 Suspended (Missing Cashflow Assumptions)"
        ecosystem = "🟢 Approved (Corporate Mappings Present)" if sufficiency_report.get("ecosystem_analysis_allowed") else "🔴 Suspended (Ecosystem Data Missing)"
        risk = "🟢 Approved (Risk Disclosures Present)" if sufficiency_report.get("risk_analysis_allowed") else "🔴 Suspended (Missing Compliance context)"
        strategic = "🟢 Approved (Catalyst Feed Active)" if sufficiency_report.get("strategic_outlook_allowed") else "🔴 Suspended (No news or SEC summaries)"

        scorecard = f"""
## ⚖️ INSTITUTIONAL RESEARCH SUFFICIENCY PROFILE
**Overall Portfolio-Grade Confidence Score: `{score}/100`**

### Analytical Domain Eligibility Registry:
* **Quantitative Financial Performance Analysis:** {financial}
* **Project IRR & Allocation Modeling:** {irr}
* **Corporate Ecosystem & Dependency Mapping:** {ecosystem}
* **Geopolitical & Compliance Risk Auditing:** {risk}
* **Strategic Outlook & Executive Roadmap Formulation:** {strategic}

*All analysis contained in this report is strictly bound to the approved domains above. Unsupported sections are replaced by transparent Research Gap Analyses.*
---
"""
        return scorecard

confidence_reporting = ConfidenceReportingEngine()
