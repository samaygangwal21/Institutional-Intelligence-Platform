"""
evidence_density_engine.py — Evidence Density Scoring Engine
===========================================================
Measures evidence quantity, quality, diversity, and cross-source consistency.
"""

import logging
from typing import List, Dict, Any

log = logging.getLogger("EvidenceDensityEngine")

class EvidenceDensityEngine:
    def __init__(self):
        pass

    def calculate_density(self, packets: List[Dict[str, Any]], domain: str) -> Dict[str, Any]:
        """
        Calculates an evidence density score (0-100) for a given analytical domain.
        Takes into account the source authority mapping and volume.
        """
        # Filter packets relevant to the domain
        domain_keywords = {
            "financial": ["revenue", "margin", "income", "cash", "ebitda", "sec", "debt", "financials"],
            "ecosystem": ["supplier", "supply", "partner", "competitor", "connection", "relationship", "customer"],
            "risk": ["risk", "mitigation", "covenant", "liability", "compliance", "threat", "interconnection"],
            "strategic": ["guidance", "strategy", "outlook", "growth", "initiative", "milestone", "trend"]
        }
        
        keywords = domain_keywords.get(domain, [])
        relevant_packets = []
        
        for p in packets:
            content = p.get("content", "").lower()
            source_type = p.get("source_type", "").lower()
            
            # Match domain keywords or matching source types
            is_relevant = False
            if source_type in ["sec", "live_sec", "market"] and domain == "financial":
                is_relevant = True
            elif source_type == "ecosystem" and domain == "ecosystem":
                is_relevant = True
            elif any(kw in content for kw in keywords):
                is_relevant = True
                
            if is_relevant:
                relevant_packets.append(p)
                
        # If no relevant packets, return a zero density profile
        if not relevant_packets:
            return {
                "density_score": 0,
                "volume": 0,
                "diversity_index": 0.0,
                "reliability_rating": "CRITICAL RISK (NO EVIDENCE)",
                "sources_referenced": []
            }
            
        # Calculate authority score
        authority_weight = {
            "sec": 1.0,
            "live_sec": 1.0,
            "market": 0.9,
            "vault": 0.85,
            "ecosystem": 0.8,
            "news": 0.6,
            "live_news": 0.6,
            "web_search": 0.4
        }
        
        total_authority = sum(authority_weight.get(p.get("source_type", ""), 0.5) for p in relevant_packets)
        avg_authority = total_authority / len(relevant_packets)
        
        # Calculate source diversity
        unique_sources = set(p.get("source_type", "") for p in relevant_packets)
        diversity_index = len(unique_sources) / 4.0  # Normalized to a max of 4 types
        diversity_index = min(1.0, diversity_index)
        
        # Quantity score (diminishing returns after 6 packets)
        qty_score = min(100.0, (len(relevant_packets) / 6.0) * 100.0)
        
        # Overall Density Score formula:
        # 40% Authority + 30% Diversity + 30% Quantity
        density_score = (avg_authority * 40.0) + (diversity_index * 30.0) + (qty_score * 0.3)
        density_score = min(100.0, max(0.0, density_score))
        
        # Reliability Rating classification
        if density_score >= 80:
            rating = "HIGHLY DEFENSIBLE"
        elif density_score >= 60:
            rating = "MODERATELY SUPPORTED"
        elif density_score >= 35:
            rating = "SPARSE EVIDENCE (CAUTION REQUIRED)"
        else:
            rating = "SPECULATIVE / INSUFFICIENT"
            
        return {
            "density_score": round(density_score, 1),
            "volume": len(relevant_packets),
            "diversity_index": round(diversity_index, 2),
            "reliability_rating": rating,
            "sources_referenced": list(set(p.get("title", "Unknown Source") for p in relevant_packets[:5]))
        }

evidence_density_engine = EvidenceDensityEngine()
