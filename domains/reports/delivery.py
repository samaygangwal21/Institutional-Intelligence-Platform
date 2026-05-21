import os
import json
import time
import re
from typing import Dict, Any, List, Optional
from loguru import logger
from infrastructure.llm.router import call_gemini_async
from domains.monitoring.governance import governance

class IntelligencePrioritizer:
    """Tier 4 Intelligence Prioritizer. Ranks financial events for materiality."""
    @governance.audit_wrapper
    async def rank_event(self, ticker: str, event_source: str, event_content: str) -> Dict[str, Any]:
        prompt = f"""
        Analyze the following new intelligence event for {ticker} and rank its institutional materiality.
        Score from 1 (noise) to 10 (critical).
        Source: {event_source}
        Content: {event_content}
        
        Respond ONLY with a valid JSON object in this format: 
        {{"score": 8, "classification": "Strategic Pivot", "reasoning": "Company is moving into AI infrastructure..."}}
        """
        try:
            raw = await call_gemini_async(prompt, temperature=0.1)
            
            if not raw or "[Gemini Error" in raw:
                return {"score": 1, "classification": "API Error", "reasoning": f"Intelligence layer unavailable: {raw}"}
                
            # Improved JSON Extraction
            clean_json = raw.strip()
            # Find the first { and last }
            start = clean_json.find("{")
            end = clean_json.rfind("}")
            
            if start != -1 and end != -1:
                json_str = clean_json[start:end+1]
                # Remove common LLM markdown artifacts
                json_str = json_str.replace("```json", "").replace("```", "").strip()
                try:
                    return json.loads(json_str)
                except json.JSONDecodeError as e:
                    logger.error(f"JSON Parse Error for {ticker}: {e} | Raw: {raw}")
                    return {"score": 1, "classification": "Parse Error", "reasoning": f"Malformed intelligence output. Raw: {raw[:100]}..."}
            
            logger.warning(f"No JSON found in response for {ticker} | Raw: {raw}")
            return {"score": 1, "classification": "Format Error", "reasoning": "Intelligence output did not contain a valid JSON block."}
            
        except Exception as e:
            logger.error(f"Unexpected error in rank_event for {ticker}: {e}")
            return {"score": 1, "classification": "System Error", "reasoning": str(e)}

class AlertingEngine:
    """Tier 4 Institutional Alerting Engine."""
    def __init__(self, storage_path="alerts_memory.json"):
        self.storage_path = storage_path
        self.alerts: List[Dict[str, Any]] = []
        self._load_alerts()

    def _load_alerts(self):
        if os.path.exists(self.storage_path):
            try:
                with open(self.storage_path, "r") as f:
                    self.alerts = json.load(f)
            except: self.alerts = []

    def _save_alerts(self):
        try:
            tmp_path = f"{self.storage_path}.tmp"
            with open(tmp_path, "w") as f:
                json.dump(self.alerts, f, indent=2)
            os.replace(tmp_path, self.storage_path)
        except: pass

    # Error classifications that should never appear in the UI
    _ERROR_CLASSIFICATIONS = {"API Error", "Parse Error", "Format Error", "System Error"}

    def trigger_alert(self, ticker: str, priority_score: int, classification: str, reasoning: str, source_url: str = "") -> bool:
        # Silently discard low-quality error alerts — they are noise, not intelligence
        if classification in self._ERROR_CLASSIFICATIONS:
            logger.debug(f"[AlertingEngine] Discarding error-type alert for {ticker}: {classification}")
            return False
        if priority_score < 2:
            logger.debug(f"[AlertingEngine] Discarding low-priority alert for {ticker} (score={priority_score})")
            return False

        alert_data = {
            "timestamp": time.time(), "ticker": ticker, "score": priority_score,
            "classification": classification, "reasoning": reasoning,
            "source_url": source_url, "read": False
        }
        self.alerts.insert(0, alert_data)
        if len(self.alerts) > 100: self.alerts = self.alerts[:100]
        self._save_alerts()
        if priority_score >= 7:
            logger.warning(f"🚨 STRATEGIC ALERT [{ticker}]: {classification} - {reasoning}")
            return True
        return False

    def get_unread_alerts(self) -> List[Dict[str, Any]]:
        unread = [
            a for a in self.alerts
            if not a.get("read", False)
            and a.get("classification", "") not in self._ERROR_CLASSIFICATIONS
            and a.get("score", 1) >= 2
        ]
        return sorted(unread, key=lambda x: x["score"], reverse=True)

    def mark_all_read(self):
        for a in self.alerts: a["read"] = True
        self._save_alerts()

class DeliveryEngine:
    """Tier 4 Delivery & Distribution Engine."""
    def __init__(self, output_dir="autonomous_reports"):
        self.output_dir = output_dir
        if not os.path.exists(self.output_dir): os.makedirs(self.output_dir, exist_ok=True)

    def distribute_flash_briefing(self, ticker: str, report_md: str) -> str:
        filename = f"FLASH_BRIEFING_{ticker}_{int(time.time())}.md"
        filepath = os.path.join(self.output_dir, filename)
        try:
            with open(filepath, "w") as f: f.write(report_md)
            logger.info(f"💾 [Delivery Engine] Saved flash briefing to {filepath}")
        except Exception as e: logger.error(f"Failed to save briefing: {e}")
        logger.success(f"📧 [Delivery Engine] Dispatched {ticker} briefing to Institutional Trading Desk (MOCK).")
        return filepath

# --- CONSOLIDATED DELIVERY SERVICE ---
class IntelligenceDeliveryService:
    def __init__(self):
        self.prioritizer = IntelligencePrioritizer()
        self.alerting = AlertingEngine()
        self.delivery = DeliveryEngine()

    async def handle_event(self, ticker: str, source: str, content: str):
        """Unified handling of incoming intelligence events."""
        ranking = await self.prioritizer.rank_event(ticker, source, content)
        is_critical = self.alerting.trigger_alert(
            ticker=ticker, priority_score=ranking.get("score", 1),
            classification=ranking.get("classification", "Unknown"),
            reasoning=ranking.get("reasoning", "No reasoning provided.")
        )
        return is_critical, ranking

delivery_service = IntelligenceDeliveryService()
prioritizer = delivery_service.prioritizer
alerting_engine = delivery_service.alerting
delivery_engine = delivery_service.delivery
