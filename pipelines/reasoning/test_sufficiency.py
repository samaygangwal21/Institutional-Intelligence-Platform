"""
test_sufficiency.py — Automated Verification for Sufficiency & Evidence Density
=============================================================================
Asserts that missing evidence correctly blocks domains, generates Gap Analysis,
penalizes sparse contexts, and filters ungrounded filler.
"""

import sys
import os
import asyncio

# Setup pathing
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)

from engines.reasoning.research_sufficiency_engine import sufficiency_engine
from engines.reasoning.reasoning_eligibility_engine import eligibility_engine
from engines.validation.evidence_density_engine import evidence_density_engine
from engines.validation.verification_engine import VerificationEngine
from engines.validation.confidence_reporting_engine import confidence_reporting

async def run_tests():
    print("[TEST] Initiating Research Sufficiency & Evidence Grounding Verification Suite...")

    # ──── Test Case 1: Empty context (no data at all) ────
    print("\n--- 1. Evaluating Sufficiency on Empty Context ---")
    empty_packets = []
    suff_report = sufficiency_engine.evaluate_sufficiency(empty_packets)
    
    assert not suff_report["financial_analysis_allowed"], "FAIL: Financial analysis should be blocked!"
    assert not suff_report["ecosystem_analysis_allowed"], "FAIL: Ecosystem analysis should be blocked!"
    assert not suff_report["risk_analysis_allowed"], "FAIL: Risk analysis should be blocked!"
    assert suff_report["confidence_score"] == 0, "FAIL: Confidence score on empty context must be zero!"
    print("PASS: Empty context completely blocks all analytical domains.")

    # ──── Test Case 2: Eligibility & Research Gap Generation ────
    print("\n--- 2. Checking Eligibility & Gap Analysis fallbacks ---")
    financial_eligibility = eligibility_engine.check_eligibility("FINANCIAL_ANALYSIS", suff_report)
    
    assert not financial_eligibility["eligible"], "FAIL: Financial Analysis should be ineligible!"
    assert "DISCLOSURE VISIBILITY" in financial_eligibility["fallback_text"], "FAIL: fallback must contain professional disclosure visibility header!"
    assert "supplementary operational disclosures" in financial_eligibility["fallback_text"], "FAIL: fallback must detail missing items professionally!"
    print("PASS: Ineligible sections correctly return Research Gap Analysis fallback blocks.")

    # ──── Test Case 3: Sparse/Low-Density Context ────
    print("\n--- 3. Measuring Sparse vs. Grounded Evidence Density ---")
    sparse_packets = [
        {
            "source_type": "web_search",
            "title": "Generic Article on Tech",
            "content": "A company is doing some work on AI technology advancements and market growth trends.",
            "importance_score": 0.5,
            "timestamp": "2026-01-01T00:00:00"
        }
    ]
    
    density_report = evidence_density_engine.calculate_density(sparse_packets, "financial")
    print(f"Sparse Density Report: {density_report}")
    assert density_report["density_score"] < 40, "FAIL: Sparse context should result in very low density score!"
    assert any(term in density_report["reliability_rating"] for term in ["SPARSE", "SPECULATIVE", "CRITICAL RISK"]), f"FAIL: Sparse rating not flagged! Got: {density_report['reliability_rating']}"
    print("PASS: Sparse evidence correctly penalizes density and reliability ratings.")

    # ──── Test Case 4: Deterministic Filler Word Detection ────
    print("\n--- 4. Checking Qualitative Filler Red-Teaming ---")
    ve = VerificationEngine()
    
    bad_report = """
    # Deep Strategic Audit
    Nvidia is pursuing multiple strategic initiatives in market growth trends that leverage technological advancements.
    """
    
    raw_context = "Nvidia designs state-of-the-art GPU architectures for AI computing workloads."
    
    audit_res = await ve.audit_report(bad_report, raw_context)
    print(f"Audit Results: {audit_res}")
    
    critical_flags = [f for f in audit_res["flags"] if f["severity"] == "critical" and "filler" in f["issue"].lower()]
    assert len(critical_flags) > 0, "FAIL: Deterministic filter must catch ungrounded consulting phrases!"
    print("PASS: Generic filler words successfully triggered critical audit flags.")

    # ──── Test Case 5: DEBUG_MODE Presentation Separation ────
    print("\n--- 5. Verifying Separation of Internal Validation from User Presentation ---")
    
    # 5a. Check DEBUG_MODE = False (Default Executive Presentation Mode)
    import pipelines.reasoning.observability_engine as obs
    obs.DEBUG_MODE = False
    
    overall_scorecard = confidence_reporting.generate_overall_scorecard(suff_report)
    section_meta = confidence_reporting.format_section_metadata("FINANCIAL_ANALYSIS", density_report, suff_report)
    applied_fixes = ve.apply_fixes(bad_report, audit_res)
    
    assert overall_scorecard == "", "FAIL: Overall sufficiency profile scorecard must be silent when DEBUG_MODE is False!"
    assert section_meta == "", "FAIL: Section metadata audit must be silent when DEBUG_MODE is False!"
    assert "INSTITUTIONAL AUDIT REPORT" not in applied_fixes, "FAIL: Raw audit diagnostics/warnings must not be appended when DEBUG_MODE is False!"
    print("PASS: Silent/Clean output confirmed when DEBUG_MODE is False.")
    
    # 5b. Check DEBUG_MODE = True (Developer Observability Mode)
    obs.DEBUG_MODE = True
    overall_scorecard_dbg = confidence_reporting.generate_overall_scorecard(suff_report)
    section_meta_dbg = confidence_reporting.format_section_metadata("FINANCIAL_ANALYSIS", density_report, suff_report)
    applied_fixes_dbg = ve.apply_fixes(bad_report, audit_res)
    
    assert "INSTITUTIONAL RESEARCH SUFFICIENCY PROFILE" in overall_scorecard_dbg, "FAIL: Diagnostics must be visible when DEBUG_MODE is True!"
    assert "INSTITUTIONAL METADATA AUDIT" in section_meta_dbg, "FAIL: Diagnostics must be visible when DEBUG_MODE is True!"
    assert "INSTITUTIONAL AUDIT REPORT" in applied_fixes_dbg, "FAIL: Diagnostics must be visible when DEBUG_MODE is True!"
    print("PASS: Diagnostic traces correctly exposed when DEBUG_MODE is True.")

    print("\nALL SUFFICIENCY & GROUNDING TESTS COMPLETED SUCCESSFULLY!")

if __name__ == "__main__":
    asyncio.run(run_tests())
