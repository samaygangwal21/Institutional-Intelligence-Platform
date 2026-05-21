import os
import json
import time
from typing import Dict, Any, Optional, Tuple
from loguru import logger
from functools import wraps
import inspect

class GovernanceEngine:
    """
    Tier 4 AI Governance & Cost Control.
    Enforces token budgets, prevents hallucinations, and tracks institutional AI costs.
    """
    
    def __init__(self):
        self.daily_budget_usd = float(os.getenv("DAILY_API_BUDGET_USD", "10.0"))
        # Using Gemini 1.5 Flash approx pricing: $0.075 / 1M input, $0.30 / 1M output
        self.cost_per_1k_input = 0.000075
        self.cost_per_1k_output = 0.00030
        
        # Local state for rapid prototyping (In production, persist to Redis/Postgres)
        self.state = {
            "date": time.strftime("%Y-%m-%d"),
            "input_tokens_used": 0,
            "output_tokens_used": 0,
            "estimated_cost_usd": 0.0,
            "queries_blocked": 0
        }
        self._load_state()

    def _load_state(self):
        """Load state from local file for persistence across restarts."""
        try:
            if os.path.exists("governance_state.json"):
                with open("governance_state.json", "r") as f:
                    saved = json.load(f)
                    if saved.get("date") == time.strftime("%Y-%m-%d"):
                        self.state = saved
                    else:
                        self._save_state() # Reset for new day
        except Exception as e:
            logger.warning(f"Failed to load governance state: {e}")

    def _save_state(self):
        try:
            with open("governance_state.json", "w") as f:
                json.dump(self.state, f)
        except:
            pass

    def check_budget(self) -> Tuple[bool, str]:
        """Check if the system has exceeded its daily API budget."""
        if self.state["estimated_cost_usd"] >= self.daily_budget_usd:
            self.state["queries_blocked"] += 1
            self._save_state()
            logger.warning(f"GOVERNANCE ALERT: Daily API budget (${self.daily_budget_usd}) exceeded.")
            return False, "API budget exceeded. Throttling autonomous generation."
        return True, "OK"

    def track_usage(self, input_chars: int, output_chars: int):
        """
        Approximate token usage based on character count (approx 4 chars per token).
        """
        in_tokens = max(1, input_chars // 4)
        out_tokens = max(1, output_chars // 4)
        
        cost = (in_tokens / 1000.0) * self.cost_per_1k_input + (out_tokens / 1000.0) * self.cost_per_1k_output
        
        # Reset if new day
        if self.state["date"] != time.strftime("%Y-%m-%d"):
            self.state = {
                "date": time.strftime("%Y-%m-%d"),
                "input_tokens_used": 0,
                "output_tokens_used": 0,
                "estimated_cost_usd": 0.0,
                "queries_blocked": 0
            }
            
        self.state["input_tokens_used"] += in_tokens
        self.state["output_tokens_used"] += out_tokens
        self.state["estimated_cost_usd"] += cost
        self._save_state()

    def validate_output(self, prompt: str, output: str) -> Tuple[bool, str]:
        """
        Simple heuristic checks to prevent hallucination or unsafe outputs.
        """
        if not output or len(output.strip()) == 0:
            return False, "Empty output generated."
            
        if "[Gemini error:" in output:
            return False, "Underlying model error detected."
            
        # Example validation: if we asked for JSON, ensure it looks like JSON
        if "Respond ONLY with a JSON array" in prompt and not output.strip().startswith("["):
            return False, "Failed structural validation (Expected JSON array)."
            
        return True, "VALID"

    def audit_wrapper(self, func):
        """Decorator to wrap API calls with governance tracking."""
        if inspect.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                allowed, reason = self.check_budget()
                if not allowed:
                    return f"[GOVERNANCE BLOCKED: {reason}]"
                    
                prompt = str(args) + str(kwargs)
                
                # Execute original function
                output = await func(*args, **kwargs)
                
                # Validate
                if isinstance(output, str):
                    is_valid, val_reason = self.validate_output(prompt, output)
                    if not is_valid:
                        logger.error(f"Governance validation failed: {val_reason}")
                        return f"[GOVERNANCE FAILED: {val_reason}]"
                    
                # Track cost
                self.track_usage(len(prompt), len(str(output)))
                return output
            return async_wrapper
        else:
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                allowed, reason = self.check_budget()
                if not allowed:
                    return f"[GOVERNANCE BLOCKED: {reason}]"
                    
                prompt = str(args) + str(kwargs)
                
                # Execute original function
                output = func(*args, **kwargs)
                
                # Validate
                if isinstance(output, str):
                    is_valid, val_reason = self.validate_output(prompt, output)
                    if not is_valid:
                        logger.error(f"Governance validation failed: {val_reason}")
                        return f"[GOVERNANCE FAILED: {val_reason}]"
                    
                # Track cost
                self.track_usage(len(prompt), len(str(output)))
                return output
            return sync_wrapper

# Global Singleton
governance = GovernanceEngine()
