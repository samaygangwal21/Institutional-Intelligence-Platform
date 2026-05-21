
import logging
from typing import List, Dict, Any
from datetime import datetime

log = logging.getLogger("ContextBuilder")

class ContextBuilder:
    """
    Modular Retrieval & Context Intelligence Layer.
    Responsible for ranking, deduplicating, and compressing intelligence packets.
    """
    
    def __init__(self, query: str):
        self.query = query
        self.packets = []
        
    def add_packet(self, packet: Dict[str, Any]):
        """Adds a standardized intelligence packet."""
        required_keys = ["source_type", "title", "content", "importance_score", "timestamp"]
        if all(k in packet for k in required_keys):
            self.packets.append(packet)
        else:
            log.warning(f"Malformed packet rejected: {packet.get('title', 'Unknown')}")

    def _calculate_relevance(self, packet: Dict[str, Any]) -> float:
        """Simple relevance scoring based on query alignment."""
        content = packet.get("content", "").lower()
        query_terms = self.query.lower().split()
        match_count = sum(1 for term in query_terms if term in content)
        return match_count / len(query_terms) if query_terms else 0.0

    def rank_packets(self):
        """
        Ranks packets based on:
        1. Recency (Timestamp)
        2. Source Authority (SEC > News)
        3. Relevance to Query
        4. Base Importance Score from Agent
        """
        for p in self.packets:
            # Recency factor (simple decay)
            try:
                # Assuming ISO format or similar
                p_date = datetime.fromisoformat(p["timestamp"].split("T")[0])
                days_old = (datetime.now() - p_date).days
                recency_score = max(0, 1 - (days_old / 365)) # 1 year decay
            except:
                recency_score = 0.5
            
            # Source authority
            authority_map = {
                "sec": 1.0,
                "transcript": 0.9,
                "market": 0.8,
                "vault": 0.8,
                "ecosystem": 0.7,
                "news": 0.6,
                "web": 0.5
            }
            authority_score = authority_map.get(p["source_type"], 0.5)
            
            relevance_score = self._calculate_relevance(p)
            
            # Final Rank Score
            p["rank_score"] = (
                (recency_score * 0.3) + 
                (authority_score * 0.3) + 
                (relevance_score * 0.3) + 
                (p["importance_score"] * 0.1)
            )
            
        self.packets.sort(key=lambda x: x["rank_score"], reverse=True)

    def deduplicate(self):
        """Remove overlapping news headlines or redundant financial entries."""
        unique_content = set()
        deduped = []
        for p in self.packets:
            # Simple content hash/prefix check
            content_key = p["content"][:100].lower()
            if content_key not in unique_content:
                unique_content.add(content_key)
                deduped.append(p)
        self.packets = deduped

    def build_reasoning_packet(self, max_context_chars: int = 60000) -> str:
        """Constructs a numbered, citation-ready research packet for the LLM.
        Each source gets a sequential number [1], [2], etc. so the LLM can produce
        proper in-text citations when writing report sections.
        """
        self.deduplicate()
        self.rank_packets()
        
        output = "### RANKED RESEARCH PACKET\n"
        output += "Each source below is numbered. Use [n] in-text citations to reference them.\n\n"
        current_chars = 0
        
        for idx, p in enumerate(self.packets, start=1):
            # Store the citation number on the packet for later reference
            p["citation_number"] = idx
            
            source_type = p['source_type'].upper()
            title = p['title']
            date = p['timestamp']
            url = p.get('source_url', '')
            
            packet_str = f"[{idx}] SOURCE TYPE: {source_type} | {title}\n"
            packet_str += f"    DATE: {date}\n"
            if url:
                packet_str += f"    URL: {url}\n"
            packet_str += f"    CONTENT:\n{p['content']}\n"
            packet_str += f"{'─'*50}\n\n"
            
            if current_chars + len(packet_str) < max_context_chars:
                output += packet_str
                current_chars += len(packet_str)
            else:
                break
                
        return output

    def get_sources_manifest(self) -> List[Dict[str, str]]:
        """Returns a list of sources for UI traceability."""
        return [
            {"type": p["source_type"], "title": p["title"], "url": p.get("source_url", "")}
            for p in self.packets
        ]

    def build_evidence_packet(self, sufficiency: Dict[str, Any], densities: Dict[str, Any]) -> Dict[str, Any]:
        """
        Builds a structured EvidencePacket containing validated metrics, retrieved sources,
        approved and unsupported reasoning domains, and section confidence scores.
        """
        self.deduplicate()
        self.rank_packets()
        
        # Extract validated metrics if any
        validated_metrics = {}
        for p in self.packets:
            if p.get("source_type") == "market" and isinstance(p.get("content"), str):
                import json
                try:
                    # In market packets, the content is a JSON-encoded analytical dict
                    validated_metrics = json.loads(p["content"])
                except:
                    pass
                    
        approved = []
        unsupported = []
        
        domain_mapping = {
            "financial_analysis_allowed": "FINANCIAL_ANALYSIS",
            "irr_modeling_allowed": "IRR_MODELING",
            "ecosystem_analysis_allowed": "COMPETITIVE_ECOSYSTEM",
            "risk_analysis_allowed": "RISK_ASSESSMENT",
            "strategic_outlook_allowed": "STRATEGIC_OUTLOOK"
        }
        
        for k, domain_name in domain_mapping.items():
            if sufficiency.get(k, False):
                approved.append(domain_name)
            else:
                unsupported.append(domain_name)
                
        confidence_scores = {}
        for k, v in densities.items():
            confidence_scores[k] = v.get("density_score", 0)
            
        return {
            "validated_metrics": validated_metrics,
            "retrieved_sources": self.get_sources_manifest(),
            "approved_reasoning_domains": approved,
            "unsupported_domains": unsupported,
            "confidence_scores": confidence_scores
        }

