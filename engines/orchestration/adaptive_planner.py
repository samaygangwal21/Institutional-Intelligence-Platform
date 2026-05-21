import json
import re
from typing import Dict, Any, List
from loguru import logger
from infrastructure.llm.router import call_gemini_async
from domains.monitoring.governance import governance

class AdaptivePlanner:
    """
    Tier 4 Adaptive Research Planner.
    Analyzes intent and dynamically structures execution paths (simple vs complex task graphs).
    """
    

    async def analyze_intent(self, query: str) -> Dict[str, Any]:
        """
        Determines the complexity of the query.
        Returns a routing decision: 'direct' or 'multi_step'.
        """
        prompt = f"""
        Analyze the following user query to determine its research complexity.
        If it asks for a simple fact, metric, or summary, classify as "direct".
        If it asks for deep strategic analysis, risks, causality, or cross-company ecosystem mapping, classify as "multi_step".
        
        Query: "{query}"
        
        Respond ONLY with a JSON object:
        {{"classification": "direct" | "multi_step", "entities": ["extracted", "companies"]}}
        """
        
        raw = await call_gemini_async(prompt, temperature=0.1)
        raw_json = re.sub(r"```(?:json)?", "", raw).strip("` \n")
        
        try:
            decision = json.loads(raw_json)
            # Default fallback
            if "classification" not in decision:
                decision["classification"] = "direct"
            return decision
        except:
            return {"classification": "direct", "entities": []}

    async def generate_task_graph(self, query: str, entities: List[str]) -> List[Dict[str, str]]:
        """
        For 'multi_step' queries, generate a parallel task execution graph.
        """
        prompt = f"""
        Break down the following complex research objective into 3 to 5 distinct, parallel analytical tasks.
        
        Objective: "{query}"
        Entities: {entities}
        
        Respond ONLY with a JSON array of objects, where each object has:
        "task_name": short name (e.g. "Margin Analysis")
        "description": instruction for the sub-agent
        
        Example:
        [
          {{"task_name": "Margin Analysis", "description": "Extract gross and operating margins over 4 quarters."}},
          {{"task_name": "Supply Chain Risk", "description": "Identify critical supplier dependencies."}}
        ]
        """
        
        raw = await call_gemini_async(prompt, temperature=0.1)
        raw_json = re.sub(r"```(?:json)?", "", raw).strip("` \n")
        
        try:
            tasks = json.loads(raw_json)
            if isinstance(tasks, list) and len(tasks) > 0:
                return tasks
            return []
        except:
            return [{"task_name": "General Analysis", "description": "Analyze the request directly."}]

# Global Singleton
adaptive_planner = AdaptivePlanner()
