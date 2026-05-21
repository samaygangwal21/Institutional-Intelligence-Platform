"""
observability_engine.py — Internal Governance & Observability Engine
===================================================================
Provides silent, internal auditing for debugging and logging analytical traces.
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, List

log = logging.getLogger("ObservabilityEngine")

# Configuration parameter for developer/admin observability
DEBUG_MODE = False

class ObservabilityEngine:
    def __init__(self):
        self.log_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            "diagnostics"
        )
        try:
            os.makedirs(self.log_dir, exist_ok=True)
        except Exception as e:
            log.warning(f"Could not create diagnostics directory: {e}")

    def record_run(self, session_id: str, query: str, sufficiency: Dict[str, Any], densities: Dict[str, Any], audit_results: Dict[str, Any]):
        """Silently logs validation trace to diagnostics for developers/admins."""
        log_file = os.path.join(self.log_dir, "observability_traces.jsonl")
        
        trace_data = {
            "timestamp": datetime.now().isoformat(),
            "session_id": session_id,
            "query": query,
            "sufficiency_profile": sufficiency,
            "evidence_densities": densities,
            "audit_results": audit_results
        }
        
        try:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(trace_data) + "\n")
            log.info(f"Observability trace recorded silently for session: {session_id}")
        except Exception as e:
            log.error(f"Failed to write observability trace: {e}")

observability_engine = ObservabilityEngine()
