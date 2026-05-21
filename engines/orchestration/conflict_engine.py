"""
conflict_resolution_engine.py — Analytical Conflict Detection & Resolution
========================================================================
Identifies contradictory narratives between agents and enforces an evidence-based hierarchy.
"""

import logging
import json
import re
from typing import List, Dict, Any, Optional
from infrastructure.llm.router import call_gemini_async

log = logging.getLogger("ConflictResolutionEngine")

class ConflictResolutionEngine:
    def __init__(self):
        pass

    async def audit_memory(self, memory_entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Audits shared memory for internal consistency and resolves conflicts."""
        if not memory_entries or len(memory_entries) < 2: return []
        
        try:
            # 1. Detect Conflicts via LLM reasoning over the claims
            # Using safe .get() to prevent KeyError if data shape varies
            memory_summary = "\n".join([
                f"AGENT {m.get('agent', m.get('agent_id', 'unknown'))}: {m.get('claim', 'No claim provided')}"
                for m in memory_entries
            ])
            
            prompt = f"""You are a Lead Conflict Resolution Analyst. Review the following institutional research claims and identify any logical contradictions or factual inconsistencies.

RESEARCH CLAIMS:
{memory_summary}

STRICT RESOLUTION RULES:
1. Prefer SEC filings over News.
2. Prefer Latest data over Historical data.
3. If two agents disagree on margins or growth, prioritize the Financial Analysis agent.
4. Flag any unresolved contradictions that require user attention.

Respond ONLY with a JSON list of resolved conflicts:
[
  {{
    "issue": "description",
    "resolution": "the final consensus narrative",
    "priority_source": "agent_id",
    "confidence": 0-1
  }}
]
"""
            response = await call_gemini_async(prompt, max_tokens=1000, temperature=0.0)
            json_match = re.search(r'\[.*\]', response, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
            return []

        except Exception as e:
            log.error(f"CRITICAL ERROR in audit_memory: {e}")
            if isinstance(memory_entries, list):
                for i, m in enumerate(memory_entries):
                    if isinstance(m, dict):
                        log.error(f"Entry {i} keys: {list(m.keys())}")
                    else:
                        log.error(f"Entry {i} is NOT a dict: {type(m)}")
            return []

    def reconcile_report(self, report_md: str, conflicts: List[Dict[str, Any]]) -> str:
        """Injects resolved consensus narratives into the final report or appends an audit note."""
        if not conflicts: return report_md
        
        audit_md = "\n\n----- \n### ⚖️ INSTITUTIONAL ANALYTICAL RECONCILIATION\n"
        for c in conflicts:
            conf_val = c.get('confidence', 0)
            res_val = c.get('resolution', 'Unresolved')
            issue_val = c.get('issue', 'Unknown conflict')
            audit_md += f"- **RESOLVED**: {issue_val} → *Consensus: {res_val}* (Confidence: {conf_val})\n"
            
        return report_md + audit_md
