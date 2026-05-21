"""
coordination_engine.py — Agent Coordination & Intelligence Routing System
========================================================================
Manages analytical dependencies, routes cross-agent insights, and maintains narrative coherence.
"""

import logging
import asyncio
from typing import List, Dict, Any, Optional
from infrastructure.base_infra import memory_engine

log = logging.getLogger("CoordinationEngine")

class CoordinationEngine:
    """
    Orchestrates the sequencing and intelligence-sharing between specialized agents.
    """
    def __init__(self):
        self.memory = memory_engine
        # Define high-level dependencies
        self.dependencies = {
            "financial": [], # Foundation
            "market": ["financial"],
            "ecosystem": ["financial"],
            "competitive": ["financial", "market"],
            "risk": ["financial", "market", "ecosystem"],
            "strategic": ["financial", "market", "ecosystem", "risk"]
        }

    async def coordinate_reasoning(self, ticker: str, query: str, context: str, callback=None):
        """
        Runs agents in a dependency-aware sequence, sharing memory at each step.
        """
        from engines.reasoning.reasoning_engine import ReasoningEngine
        engine = ReasoningEngine()
        
        # 1. Determine Agent Sequence (simplified for this implementation)
        # Sequence: Financial -> (Market/Ecosystem) -> Competitive -> Risk -> Strategic
        layers = [
            ["financial"],
            ["market", "ecosystem"],
            ["competitive"],
            ["risk"],
            ["strategic"]
        ]
        
        for layer in layers:
            if callback: callback(f"Coordinating {', '.join(layer).upper()} intelligence layer...")
            
            # Prepare current memory context for the agents in this layer
            shared_ctx = self.memory.get_shared_context()
            combined_context = f"{context}\n{shared_ctx}"
            
            tasks = []
            for agent_id in layer:
                tasks.append(self._run_agent_and_memorize(engine, agent_id, ticker, query, combined_context, callback))
            
            try:
                # 600s timeout for the entire layer to accommodate free-tier pacing
                await asyncio.wait_for(asyncio.gather(*tasks), timeout=600.0)
            except asyncio.TimeoutError:
                log.error(f"Timeout reached for {layer} layer. Proceeding with partial intelligence.")
                if callback: callback(f"Layer {layer} partially timed out. Moving to next phase...")
            
        return self.memory.memory

    async def _run_agent_and_memorize(self, engine, agent_id, ticker, query, context, callback):
        """Runs an agent and captures its key findings into shared memory."""
        # 1. Execute Agent
        result = await engine.run_specialized_agent(agent_id, ticker, query, context)
        content = result["content"]
        
        # 2. Extract Memory Objects (Simplified LLM extraction step)
        # In a full implementation, we'd use a specific prompt to extract "claims" from the content
        # For now, we'll store the summary of the agent's work as a memory entry
        self.memory.store_claim(
            topic=f"{agent_id.upper()} Analysis of {ticker}",
            category=agent_id,
            claim=content[:500] + "...", # Store a summary/snippet
            agent_id=agent_id,
            confidence=0.9, # To be dynamically determined
            data={"full_content": content}
        )
        
        if callback: callback(f"Agent {agent_id.upper()} integrated into shared memory.")
        return result
