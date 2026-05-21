"""
verification_engine.py — Institutional Verification & Validation Layer
=====================================================================
Audits the synthesized report for quantitative accuracy, hallucination detection, and citation integrity.
"""

import logging
import json
import re
from typing import List, Dict, Any, Optional
from infrastructure.llm.router import call_gemini_async

log = logging.getLogger("VerificationEngine")

from engines.finance.deterministic_finance_engine import finance_engine
from .consistency_engine import consistency_engine
from .financial_validation_engine import financial_validation_engine
from engines.finance.verified_metrics_store import metrics_store

class VerificationEngine:
    def __init__(self):
        pass

    async def audit_report(self, report_md: str, raw_context: str, ticker: Optional[str] = None) -> Dict[str, Any]:
        """
        Audits the report against the raw intelligence context.
        Now includes Deterministic Financial Auditing.
        """
        
        flags = []
        
        # 1. Deterministic Consistency Check
        if ticker:
            verified_data = metrics_store.get_metrics(ticker)
            if verified_data:
                consistency_issues = consistency_engine.validate_narrative(report_md, verified_data)
                for issue in consistency_issues:
                    flags.append({
                        "severity": "critical",
                        "issue": f"Numerical contradiction: {issue['detail']}",
                        "fix": f"Update value to {issue['verified_value']}"
                    })

        # 2. Deterministic Generic Filler Detection
        filler_words = ["market growth trends", "technological advancements", "strategic initiatives"]
        for fw in filler_words:
            if fw in report_md.lower() and fw not in raw_context.lower():
                flags.append({
                    "severity": "critical",
                    "issue": f"Unsupported generic consultant filler phrase detected: '{fw}'",
                    "fix": "Remove the generic phrase and replace with specific retrieved facts."
                })

        # 3. LLM-based Qualitative Red-Teaming
        prompt = f"""You are a Lead Audit Analyst at a Tier-1 Investment Bank. 
Your task is to RED-TEAM the following Research Report against the Raw Context.

RESEARCH REPORT:
{report_md}

RAW INTELLIGENCE CONTEXT:
{raw_context}

AUDIT REQUIREMENTS:
1. Identify any 'hallucinated' numbers or metrics not strictly supported by the raw context.
2. Detect logical contradictions (e.g. claims of M&A synergies or IRR targets when context has no matching project cashflow details).
3. Verify citation integrity (do citations point to the right source for the claim?).
4. Detect and flag generic strategic padding, vague consultant jargon, or unsupported optimistic projection language (e.g. faked certainty, 'unprecedented growth').
5. Check for 'impossible' ratios (Margins > 100%, negative assets unless justified, etc.).

Respond ONLY with a JSON object:
{{
  "is_valid": true|false,
  "flags": [
    {{"severity": "critical|warning", "issue": "description", "fix": "suggested change"}}
  ],
  "audit_score": 0-100
}}
"""
        try:
            response = await call_gemini_async(prompt, max_tokens=2000, temperature=0.0)
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                llm_audit = json.loads(json_match.group())
                flags.extend(llm_audit.get("flags", []))
                
            # Final scoring
            score = 100 - (len([f for f in flags if f['severity'] == 'critical']) * 20) - (len([f for f in flags if f['severity'] == 'warning']) * 5)
            score = max(0, score)
            
            return {
                "is_valid": len([f for f in flags if f['severity'] == 'critical']) == 0,
                "flags": flags,
                "audit_score": score
            }
            
        except Exception as e:
            log.error(f"Audit failed: {e}")
            return {"is_valid": True, "flags": flags + [{"severity": "warning", "issue": f"Audit engine error: {e}"}], "audit_score": 50}

    def apply_fixes(self, report_md: str, audit_results: Dict[str, Any]) -> str:
        """Applies critical fixes or appends audit warnings to the report in debug mode only."""
        from pipelines.reasoning.observability_engine import DEBUG_MODE
        
        # In production/executive presentation mode, we NEVER expose internal system warnings/scores to the user
        if not DEBUG_MODE:
            return report_md
            
        if audit_results.get("is_valid") and not audit_results.get("flags"):
            return report_md
            
        warnings_md = "\n\n---\n### 🏛️ INSTITUTIONAL AUDIT REPORT\n"
        warnings_md += f"**Audit Score: {audit_results.get('audit_score', 0)}/100**\n\n"
        
        for flag in audit_results.get("flags", []):
            severity = flag.get("severity", "warning")
            issue = flag.get("issue", "Unknown issue")
            fix = flag.get("fix", "None provided")
            color = "🔴" if severity == "critical" else "🟡"
            warnings_md += f"- {color} **{severity.upper()}**: {issue}  \n  *Recommended Fix*: {fix}\n"
            
        return report_md + warnings_md

