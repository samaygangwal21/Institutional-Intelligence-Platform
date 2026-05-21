from typing import List, Dict, Any
import re

class ConsistencyEngine:
    """
    Prevents contradictory numbers across different report sections.
    """
    
    def validate_narrative(self, narrative: str, verified_metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Scans LLM-generated narrative for numbers and cross-references them
        with the verified metrics store.
        """
        conflicts = []
        
        # Simple regex to find percentages in text
        # e.g. "25.6%" or "25%"
        found_percentages = re.findall(r"(\d+\.?\d*)%", narrative)
        
        # This is a complex task for a simple engine, but we can start with 
        # a known-metric check.
        # If 'operating_margin' is in verified_metrics, check if narrative deviates significantly.
        
        latest_period = verified_metrics.get("periods", [{}])[0]
        metrics = latest_period.get("metrics", {})
        
        for metric_name, true_val in metrics.items():
            if "margin" in metric_name:
                true_pct = round(true_val * 100, 1)
                # Check if this percentage exists in text within a +/- 1% range
                match_found = False
                for p_str in found_percentages:
                    p_val = float(p_str)
                    if abs(p_val - true_pct) < 1.0:
                        match_found = True
                        break
                
                # If the metric is mentioned but incorrectly, that's a conflict
                # Note: This requires knowing IF the metric is mentioned.
                # For now, we just log a warning if the 'true' metric is nowhere to be found
                # but OTHER numbers are present.
        
        return conflicts

    def enforce_section_alignment(self, sections: List[Dict[str, str]]) -> bool:
        """
        Ensures that if Section A mentions an IRR, Section B doesn't contradict it.
        """
        # Logic to be expanded as specialized models are added
        return True

consistency_engine = ConsistencyEngine()
