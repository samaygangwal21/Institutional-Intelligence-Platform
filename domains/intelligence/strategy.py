import os
import json
import time
import re
from typing import Dict, Any, List, Optional
from loguru import logger
from infrastructure.llm.router import call_gemini_async
from domains.monitoring.governance import governance

class CausalityEngine:
    """Tier 4 Causality Engine. Maps standalone events to strategic outcomes."""
    def __init__(self, storage_path="causality_memory.json"):
        self.storage_path = storage_path
        self.chains: Dict[str, List[Dict[str, Any]]] = {}
        self._load_memory()

    def _load_memory(self):
        if os.path.exists(self.storage_path):
            try:
                with open(self.storage_path, "r") as f:
                    self.chains = json.load(f)
            except: pass

    def _save_memory(self):
        try:
            tmp_path = f"{self.storage_path}.tmp"
            with open(tmp_path, "w") as f:
                json.dump(self.chains, f, indent=2)
            os.replace(tmp_path, self.storage_path)
        except Exception as e:
            logger.error(f"Failed to atomically save memory: {e}")

    @governance.audit_wrapper
    async def map_event(self, ticker: str, new_event: str, prior_context: str) -> Dict[str, Any]:
        if ticker not in self.chains:
            self.chains[ticker] = []

        # 1. Analyze Causality
        prompt = f"Map the causal chain for {ticker}.\nContext: {prior_context}\nEvent: {new_event}\nRespond ONLY in JSON."
        raw = await call_gemini_async(prompt, temperature=0.2)
        try:
            clean_json = re.sub(r"```(?:json)?", "", raw).strip("` \n")
            analysis = json.loads(clean_json)
        except:
            analysis = {"event_summary": new_event, "decision_recommendation": "Monitor"}

        # 2. Verify (Internal Loop)
        verify_prompt = f"Verify this causal analysis for {ticker}: {json.dumps(analysis)}. Respond in JSON."
        raw_v = await call_gemini_async(verify_prompt, temperature=0.0)
        try:
            clean_v = re.sub(r"```(?:json)?", "", raw_v).strip("` \n")
            verified = json.loads(clean_v)
        except:
            verified = analysis

        verified["timestamp"] = time.time()
        self.chains[ticker].append(verified)
        self._save_memory()
        return verified

class ThesisEngine:
    """Tier 4 Investment Thesis Engine. Generates and evolves Bull/Bear theses."""
    def __init__(self, storage_path="thesis_memory.json"):
        self.storage_path = storage_path
        self.theses: Dict[str, Dict[str, Any]] = {}
        self._load_memory()

    def _load_memory(self):
        if os.path.exists(self.storage_path):
            try:
                with open(self.storage_path, "r") as f:
                    self.theses = json.load(f)
            except: pass

    def _save_memory(self):
        try:
            tmp_path = f"{self.storage_path}.tmp"
            with open(tmp_path, "w") as f:
                json.dump(self.theses, f, indent=2)
            os.replace(tmp_path, self.storage_path)
        except Exception as e:
            logger.error(f"Failed to atomically save memory: {e}")

    @governance.audit_wrapper
    async def process_event(self, ticker: str, new_event: str):
        if ticker not in self.theses:
            self.theses[ticker] = {"bull_thesis": "Fundamental Growth", "bear_thesis": "Macro Headwinds", "history": []}
            
        t = self.theses[ticker]
        prompt = f"Update investment thesis for {ticker}.\nBull: {t['bull_thesis']}\nBear: {t['bear_thesis']}\nEvent: {new_event}\nJSON ONLY."
        raw = await call_gemini_async(prompt, temperature=0.2)
        try:
            clean_json = re.sub(r"```(?:json)?", "", raw).strip("` \n")
            updated = json.loads(clean_json)
            self.theses[ticker].update(updated)
            self.theses[ticker]["history"].append({"timestamp": time.time(), "event": new_event})
            self._save_memory()
        except: pass

class StrategicStateEngine:
    """Tier 4 Strategic State Tracking."""
    def __init__(self, storage_path="strategic_state_memory.json"):
        self.storage_path = storage_path
        self.states: Dict[str, Dict[str, Any]] = {}
        self._load_memory()

    def _load_memory(self):
        if os.path.exists(self.storage_path):
            try:
                with open(self.storage_path, "r") as f:
                    self.states = json.load(f)
            except: pass

    def _save_memory(self):
        try:
            tmp_path = f"{self.storage_path}.tmp"
            with open(tmp_path, "w") as f:
                json.dump(self.states, f, indent=2)
            os.replace(tmp_path, self.storage_path)
        except Exception as e:
            logger.error(f"Failed to atomically save memory: {e}")

    async def update_state(self, ticker: str, new_event: str) -> bool:
        if ticker not in self.states:
            self.states[ticker] = {"current_state": "Stable Execution", "history": []}
            
        current = self.states[ticker]["current_state"]
        prompt = f"Shift state for {ticker}?\nCurrent: {current}\nEvent: {new_event}\nJSON: {{state_changed: bool, new_state: str, rationale: str}}"
        raw = await call_gemini_async(prompt, temperature=0.1)
        try:
            clean_json = re.sub(r"```(?:json)?", "", raw).strip("` \n")
            res = json.loads(clean_json)
            if res.get("state_changed"):
                self.states[ticker]["history"].append({"timestamp": time.time(), "from": current, "to": res["new_state"], "reason": res["rationale"]})
                self.states[ticker]["current_state"] = res["new_state"]
                self._save_memory()
                return True
        except: pass
        return False

class NarrativeEngine:
    """Tier 4 Narrative Engine. Tracks and evolves the core strategic narrative."""
    def __init__(self, storage_path="narrative_memory.json"):
        self.storage_path = storage_path
        self.narratives: Dict[str, Dict[str, Any]] = {}
        self._load_memory()

    def _load_memory(self):
        if os.path.exists(self.storage_path):
            try:
                with open(self.storage_path, "r") as f:
                    self.narratives = json.load(f)
            except: pass

    def _save_memory(self):
        try:
            tmp_path = f"{self.storage_path}.tmp"
            with open(tmp_path, "w") as f:
                json.dump(self.narratives, f, indent=2)
            os.replace(tmp_path, self.storage_path)
        except Exception as e:
            logger.error(f"Failed to atomically save memory: {e}")

    @governance.audit_wrapper
    async def process_event(self, ticker: str, new_event: str):
        if ticker not in self.narratives:
            self.narratives[ticker] = {"core_narrative": "Establishing baseline.", "history": []}
            
        current = self.narratives[ticker]["core_narrative"]
        prompt = f"Update the core strategic narrative for {ticker}.\nCurrent Narrative: {current}\nNew Event: {new_event}\nJSON ONLY: {{\"core_narrative\": \"updated string\"}}"
        raw = await call_gemini_async(prompt, temperature=0.2)
        try:
            clean_json = re.sub(r"```(?:json)?", "", raw).strip("` \n")
            updated = json.loads(clean_json)
            self.narratives[ticker]["core_narrative"] = updated.get("core_narrative", current)
            self.narratives[ticker]["history"].append({"timestamp": time.time(), "event": new_event})
            self._save_memory()
        except: pass

# --- CONSOLIDATED STRATEGY SERVICE ---
class StrategyService:
    def __init__(self):
        self.causality = CausalityEngine()
        self.thesis = ThesisEngine()
        self.state = StrategicStateEngine()
        self.narrative = NarrativeEngine()

    async def run_cycle(self, ticker: str, event: str, context: str):
        """Unified Tier 4 Strategy Cycle."""
        await self.thesis.process_event(ticker, event)
        state_changed = await self.state.update_state(ticker, event)
        causal_analysis = await self.causality.map_event(ticker, event, context)
        return {
            "state_changed": state_changed,
            "causal_analysis": causal_analysis,
            "current_state": self.state.states.get(ticker, {}).get("current_state", "Unknown")
        }

strategy_service = StrategyService()
causality_engine = strategy_service.causality
thesis_engine = strategy_service.thesis
strategic_state = strategy_service.state
narrative_engine = strategy_service.narrative
