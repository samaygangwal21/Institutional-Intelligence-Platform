import logging
from typing import List, Dict, Any

log = logging.getLogger("ReportMemory")

class ReportMemoryEngine:
    """
    Maintains state and narrative consistency across section-wise report generation.
    """
    
    def __init__(self):
        self.section_summaries: Dict[str, str] = {}
        self.key_facts: List[str] = []
        self.numerical_evidence: Dict[str, Any] = {}

    def add_section(self, section_name: str, content: str):
        """Stores a generated section and extracts high-level summary."""
        self.section_summaries[section_name] = content[:1000] # store start for context

    def record_fact(self, fact: str):
        """Records a critical fact that must remain consistent."""
        self.key_facts.append(fact)

    def get_context_for_llm(self) -> str:
        """Returns a summary of previous sections to ensure continuity."""
        if not self.section_summaries:
            return "No previous sections generated."
            
        ctx = "### PREVIOUS SECTIONS SUMMARY (FOR CONSISTENCY):\n"
        for name, summary in self.section_summaries.items():
            ctx += f"- {name}: {summary[:300]}...\n"
        
        if self.key_facts:
            ctx += "\n### ESTABLISHED FACTS:\n"
            for fact in self.key_facts[-10:]: # last 10 facts
                ctx += f"- {fact}\n"
        
        return ctx

    def clear(self):
        self.section_summaries = {}
        self.key_facts = []
        self.numerical_evidence = {}

report_memory = ReportMemoryEngine()
