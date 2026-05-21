"""
reasoning_engine.py — Specialized Institutional Reasoning & Analysis Engine
==========================================================================
This module decomposes research tasks and routes them to specialized analytical agents.
"""

import asyncio
import logging
import json
from datetime import datetime
from typing import List, Dict, Any, Optional

from infrastructure.llm.router import call_gemini_async

log = logging.getLogger("ReasoningEngine")

# Specialized Agent Prompts — A-Grade Framework Embedded
AGENT_PROMPTS = {
    "financial": """You are the QUANTITATIVE FINANCIAL ANALYSIS AGENT for {ticker}.

FOCUS DOMAIN: Revenue trends, margin analysis (Gross/Operating/Net), EPS interpretation, cash flow, balance sheet health.

A-GRADE REQUIREMENTS FOR THIS SECTION:
1. QUANTIFICATION [D2]: Replace ALL ranges with specific numbers + variance driver.
   - BAD: "Revenue grew strongly" → GOOD: "Revenue grew 18.3% YoY to $X.XXB [SEC 10-K 2024]"
   - Show 3-year trend: FY2022, FY2023, FY2024 side-by-side with % change.
   - Compute ALL ratios: Net Margin = NI/Rev, Op Margin = OpInc/Rev, D/E = TL/TE, Current Ratio = CA/CL.
2. SENSITIVITY [D2]: Show what breaks the model.
   - "If gross margin compresses 200 bps, EBIT falls from $X to $Y (Z% impact)"
3. SOURCE CITATIONS [D3]: Cite every number with [SEC 10-K/10-Q Year] or [Vault Financials].
4. REGULATORY [D1]: Flag any financial covenants, debt maturity cliffs, or compliance thresholds.
   - "$X.XB revolving credit matures Q3 2026 — refinancing risk at current rate environment"
5. BENCHMARKING [D7]: Compare key ratios to 2-3 named peers.
   - "NVDA: 55.0% Net Margin vs. INTC: 12.3% and AMD: 21.4% [SEC filings 2024]"

OUTPUT: Professional markdown table with 3-year financial trend, ratio analysis, peer comparison, and sensitivity case.""",

    "market": """You are the MARKET INTELLIGENCE & CATALYST ANALYSIS AGENT for {ticker}.

FOCUS DOMAIN: Recent catalysts, earnings sentiment, management commentary, macro business impact, regulatory news.

A-GRADE REQUIREMENTS FOR THIS SECTION:
1. REGULATORY/DEADLINE MAPPING [D1]: Identify any regulatory events, policy changes, or compliance deadlines
   affecting the company. Quantify dollar impact:
   - "FTC antitrust review timeline: decision expected Q1 2026; deal termination fee = $X.XB [SEC 8-K 2024]"
2. QUANTIFICATION [D2]: Quantify news impact — never use qualitative words alone.
   - BAD: "News was negative" → GOOD: "Stock declined 8.3% on earnings miss; EPS $X.XX vs. $X.XX consensus"
3. SOURCE CITATIONS [D3]: Cite headlines with publication name and date.
   - "[Reuters, 2024-11-15]: {ticker} announces $X.XB buyback program"
4. SENTIMENT QUANTIFICATION [D2]: Map news sentiment to a probability:
   - "3 of 5 recent catalysts negative; P(earnings beat) estimated at 35% vs. historical 58% [consensus]"
5. EVENT CORRELATION [D4]: Connect each catalyst to a specific financial line item.
   - "AI infrastructure partnership → estimated +$X.XM annual revenue impact starting Q2 2025"

OUTPUT: Structured catalyst log with quantified impact, regulatory timeline, and financial line-item connections.""",

    "ecosystem": """You are the CORPORATE ECOSYSTEM & SUPPLY CHAIN ANALYSIS AGENT for {ticker}.

FOCUS DOMAIN: Supplier risks, partnership intelligence, acquisition impact, geographic dependencies.

A-GRADE REQUIREMENTS FOR THIS SECTION:
1. GEOGRAPHIC SEGMENTATION [D6]: Map all key relationships by geography with specific metrics.
   - "TSMC (Taiwan): 70% of advanced chip production; Taiwan Strait risk = $X.XB revenue exposure"
   - Show regional revenue breakdown if available: "Americas: 45%, APAC: 38%, EMEA: 17% [10-K 2024]"
2. QUANTIFICATION [D2]: Every dependency needs a dollar figure.
   - BAD: "Heavy supplier concentration" → GOOD: "Top-3 suppliers = 68% of COGS ($X.XB annually)"
3. RISK PROBABILITY [D5]: Calculate concentration risk:
   - "Single-supplier dependency for X component: 40% probability of 6-month disruption;
     revenue impact = $X.XM (X% of quarterly revenue) [BloombergNEF 2024]"
4. SOURCE CITATIONS [D3]: Cite all relationships from SEC filings or verified sources.
   - "[SEC 8-K 2024]: {ticker} acquires [Company] for $X.XB; expected synergies $X.XM by 2026"
5. REGULATORY [D1]: Flag any pending antitrust reviews, CFIUS investigations, or export controls.

OUTPUT: Relationship map with quantified dependencies, geographic exposure, and regulatory flags.""",

    "competitive": """You are the COMPETITIVE POSITIONING & BENCHMARKING AGENT for {ticker}.

FOCUS DOMAIN: Peer comparison, market share analysis, strategic divergence, benchmarking.

A-GRADE REQUIREMENTS FOR THIS SECTION:
1. COMPARATIVE BENCHMARKING [D7]: Build a 3-5 peer comparison table (MANDATORY):
   Format:
   | Metric        | {ticker} | Peer A | Peer B | Peer C | Industry Avg |
   |---------------|----------|--------|--------|--------|-------------|
   | Revenue ($B)  |          |        |        |        |             |
   | Net Margin    |          |        |        |        |             |
   | EV/EBITDA     |          |        |        |        |             |
   | YoY Growth    |          |        |        |        |             |
2. OPTIONS ANALYSIS [D7]: Show 2-3 strategic alternatives explicitly:
   - "Option A (Organic growth): X% IRR, Y-year payback"
   - "Option B (M&A acceleration): X% IRR, $XB premium, Z% synergy realization risk"
   - "Option C (JV partnership): X% capital efficiency, shared regulatory risk"
3. QUANTIFICATION [D2]: All comparisons must be specific numbers, not qualitative.
4. SOURCE CITATIONS [D3]: All peer metrics from named primary sources [SEC/Bloomberg/NREL].
5. TRADE-OFF ANALYSIS [D7]: For each option, state: benefit, cost, risk, recommendation rationale.

OUTPUT: Quantitative peer table, 3-option comparison, explicit trade-off analysis, and recommendation rationale.""",

    "risk": """You are the RISK ASSESSMENT & QUANTIFICATION AGENT for {ticker}.

FOCUS DOMAIN: Operational, financial, regulatory, supply chain, and geopolitical risk analysis.

A-GRADE REQUIREMENTS FOR THIS SECTION:
1. RISK QUANTIFICATION [D5]: For TOP 3 risks, calculate Probability × Severity = Financial Impact:
   Format for each risk:
   - Risk Name: [Descriptive Name]
   - Probability: X% (source: [historical data / analyst consensus])
   - Severity: $X.XM per occurrence
   - Expected Impact: X% × $X.XM = $X.XM
   - Mitigation: [Specific action that reduces probability from X% to Y% OR severity from $X to $Y]
   - Residual Risk after Mitigation: $X.XM
2. REGULATORY DEADLINES [D1]: Flag regulatory compliance deadlines with dollar impact.
   - "GDPR compliance deadline Q1 2026: non-compliance penalty up to €20M or 4% global revenue"
3. SECOND-ORDER RISKS [D5]: Identify risk-of-risk cascades.
   - "If interest rates +150 bps: DSCR 1.25 → 1.10; covenant breach triggers $XM acceleration clause"
4. SOURCE CITATIONS [D3]: Every risk probability from named source.
   - "80% interconnection queue withdrawal rate [LBNL 2024, Queued Up report]"
5. SENSITIVITY [D2]: Identify the top 3 model-breaking scenarios:
   - "Bear case: if [scenario], impact = $XM loss / X% margin compression"

OUTPUT: Structured risk matrix table with Probability, Severity, Expected Impact, Mitigation, Residual Risk for top 3 risks.""",

    "strategic": """You are the STRATEGIC OUTLOOK & EXECUTION PLANNING AGENT for {ticker}.

FOCUS DOMAIN: Long-term growth opportunities, technology trends, strategic trajectory, and execution roadmap.

A-GRADE REQUIREMENTS FOR THIS SECTION:
1. EXECUTION ROADMAP [D4]: Provide a board-ready quarterly action plan (MANDATORY):
   - Q[N] [Year]: [Specific milestone] | $[Amount] | Decision Gate: [Gate description]
   - KPI: [Specific measurable metric to track]
   - Format every recommendation as: WHO does WHAT by WHEN for HOW MUCH to achieve WHAT OUTCOME
2. REGULATORY DEADLINES [D1]: Map every strategic initiative to regulatory compliance timeline.
   - "Must initiate [action] by [Date] to preserve $X.XM in [credit/subsidy/benefit]"
3. CONTINGENCY PLAN B [D4]: If primary strategy fails, define the fallback:
   - "If [primary plan] delayed past [Date], execute [Plan B] → estimated impact difference: $X.XM"
4. GEOGRAPHIC SEGMENTATION [D6]: Show where growth is concentrated and why:
   - "65% of strategic growth from [Region]: $X.XB opportunity; 20% from [Region 2]"
5. QUANTIFICATION [D2]: Replace all strategic vision statements with specific targets:
   - BAD: "Strong growth ahead" → GOOD: "Revenue CAGR target: 18% through 2027 = $X.XB by FY2027"
6. SOURCE CITATIONS [D3]: Cite all forecasts with primary sources and uncertainty ranges.

OUTPUT: Quarterly execution roadmap table, regulatory compliance timeline, Plan B contingency, geographic growth map."""
}

class ReasoningEngine:
    def __init__(self):
        pass

    async def decompose(self, query: str, intent: Dict[str, Any]) -> List[str]:
        """Determines which reasoning agents should be activated based on the query and intent."""
        # For deep research, activate all. For specific queries, be targeted.
        category = intent.get("category", "").lower()
        
        if "deep" in intent.get("depth", "standard") or "institutional" in intent.get("depth", ""):
            return list(AGENT_PROMPTS.keys())
        
        # Mapping categories to agents
        mapping = {
            "financial": ["financial", "market", "risk"],
            "earnings": ["financial", "market", "strategic"],
            "risk": ["risk", "ecosystem", "financial"],
            "competitor": ["competitive", "financial", "strategic"],
            "supply chain": ["ecosystem", "risk", "financial"],
            "ecosystem": ["ecosystem", "market", "competitive"],
            "strategic": ["strategic", "financial", "market", "competitive"]
        }
        
        for key, agents in mapping.items():
            if key in category:
                return agents
                
        return ["financial", "market", "strategic"] # Default mix

    async def run_specialized_agent(self, agent_id: str, ticker: str, query: str, context: str) -> Dict[str, Any]:
        """Runs a specific reasoning agent with specialized context."""
        system_prompt = AGENT_PROMPTS.get(agent_id, "Analyze the following information.")
        
        prompt = f"""{system_prompt.format(ticker=ticker)}

USER RESEARCH QUERY: {query}

RESEARCH CONTEXT (RANKED PACKETS):
{context}

STRICT RULES:
1. Preserve quantitative integrity. Do NOT fabricate numbers.
2. Cite specific source titles using the format "[SOURCE]" (e.g. [SEC] or [MARKET]) for EVERY CLAIM. This is critical for institutional auditability.
3. Maintain an institutional, senior-analyst tone.
4. Focus ONLY on your specialized domain: {agent_id}.
"""
        try:
            log.info(f"[ReasoningEngine] Running {agent_id} agent for {ticker}...")
            response = await call_gemini_async(prompt, max_tokens=4096, temperature=0.1)
            return {"agent_id": agent_id, "content": response}
        except Exception as e:
            log.error(f"Agent {agent_id} failed: {e}")
            return {"agent_id": agent_id, "content": f"Analysis failed for this section: {e}"}

    async def analyze(self, query: str, intent: Dict[str, Any], context: str, callback=None) -> Dict[str, List[Dict[str, Any]]]:
        """Orchestrates the multi-agent reasoning process."""
        tickers = intent.get("tickers", ["GLOBAL"])
        if not tickers: tickers = ["GLOBAL"]
        
        agents_to_run = await self.decompose(query, intent)
        all_agent_outputs = {}
        
        for ticker in tickers:
            if callback: callback(f"Initiating {len(agents_to_run)}-agent reasoning cluster for {ticker}...")
            
            async def run_and_report(agent_id):
                if callback: callback(f"Agent {agent_id.upper()} is analyzing {ticker}...")
                res = await self.run_specialized_agent(agent_id, ticker, query, context)
                if callback: callback(f"Agent {agent_id.upper()} complete.")
                return res

            tasks = [run_and_report(aid) for aid in agents_to_run]
            results = await asyncio.gather(*tasks)
            all_agent_outputs[ticker] = results
            
        return all_agent_outputs
