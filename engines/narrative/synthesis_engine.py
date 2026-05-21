"""
synthesis_engine.py — Structured Research Synthesis Engine
==========================================================
Merges specialized agent outputs into a coherent, institutional-grade research report.
"""

import logging
import asyncio
import re
from datetime import datetime
from typing import List, Dict, Any, Optional
from infrastructure.llm.router import call_gemini_async

log = logging.getLogger("SynthesisEngine")

SECTION_ORDER = [
    "executive_summary",
    "deadlines",           # D1: Regulatory/Deadline Mapping
    "financial",
    "market",
    "competitive",
    "ecosystem",
    "risk",
    "geographic",          # D6: Geographic Segmentation
    "strategic",
    "benchmarking",        # D7: Comparative Benchmarking
    "execution_roadmap",   # D4: Actionability & Execution Clarity
    "bibliography",        # D3: Source Transparency
    "monitoring"
]

SECTION_HEADERS = {
    "executive_summary":  "## 🏆 EXECUTIVE SUMMARY",
    "deadlines":          "## ⏰ CRITICAL DEADLINES & REGULATORY LANDSCAPE",
    "financial":          "## 1. FINANCIAL PERFORMANCE & QUANTITATIVE AUDIT",
    "market":             "## 2. MARKET INTELLIGENCE & CATALYST ANALYSIS",
    "competitive":        "## 3. COMPETITIVE POSITIONING & BENCHMARKING",
    "ecosystem":          "## 4. ECOSYSTEM & STRATEGIC DEPENDENCIES",
    "risk":               "## 5. RISK ASSESSMENT MATRIX",
    "geographic":         "## 6. GEOGRAPHIC & MARKET SEGMENTATION",
    "strategic":          "## 7. STRATEGIC OUTLOOK & TRAJECTORY",
    "benchmarking":       "## 📊 OPTIONS ANALYSIS & COMPARATIVE BENCHMARKING",
    "execution_roadmap":  "## 🗓️ EXECUTION ROADMAP",
    "bibliography":       "## 📚 BIBLIOGRAPHY & CITATIONS",
    "monitoring":         "## 🔍 KEY MONITORING AREAS"
}

class SynthesisEngine:
    def __init__(self):
        pass

    async def generate_dynamic_title(self, ticker: str, user_prompt: str) -> str:
        """Generates a compelling, topic-specific institutional headline."""
        prompt = f"""Generate a short, professional, board-ready institutional report title for {ticker} 
based on this research topic: '{user_prompt}'.
Avoid generic titles. Use strong financial phrasing. 
Example format: 'Geopolitical Risk & Supply Chain Resiliency: [Ticker] Strategic Audit'
Output ONLY the title string, no quotes."""
        title = await call_gemini_async(prompt, max_tokens=100, temperature=0.7)
        if "Error" in title or not title:
            return f"INSTITUTIONAL EQUITY RESEARCH: {ticker}"
        return title.strip().upper()


    async def generate_executive_summary(self, ticker: str, agent_outputs: List[Dict[str, Any]]) -> str:
        """Generates a high-level summary based on all specialized analyses."""
        full_context = "\n\n".join([f"SECTION {o['agent_id'].upper()}:\n{o['content']}" for o in agent_outputs])
        
        prompt = f"""You are a Senior Portfolio Manager conducting a final institutional review for {ticker}.

Review the following specialized research outputs and generate a powerful 3-paragraph Executive Summary
that achieves A-grade quality (90+/100) across all dimensions.

SPECIALIZED RESEARCH:
{full_context}

STRICT REQUIREMENTS:
1. REGULATORY PRIORITY [D1]: Open with the most critical regulatory deadline or compliance event.
   Format: "The most time-sensitive constraint is [deadline]: [dollar impact if missed]."
2. INSTITUTIONAL TAKE [D2]: State a specific, quantified recommendation with dollar allocation:
   "We recommend allocating $X.XM (X%) to [strategy] targeting [outcome] by [date]."
3. RISK SUMMARY [D5]: State the top risk with probability × impact calculation.
   "Primary risk: [risk name] at X% probability × $X.XM severity = $X.XM expected loss."
4. CITATIONS [D3]: Preserve all [SOURCE] citations from the specialized research.
5. GRADE TRANSPARENCY: End with one sentence stating the overall quality assessment.

Output: 3 focused paragraphs — Opening/Context, Core Recommendation, Risk/Outlook.
"""
        return await call_gemini_async(prompt, max_tokens=1200, temperature=0.1)

    async def generate_regulatory_deadlines_section(self, ticker: str, agent_outputs: List[Dict[str, Any]]) -> str:
        """Synthesizes all regulatory deadlines and compliance requirements across all agent analyses."""
        full_context = "\n\n".join([f"SECTION {o['agent_id'].upper()}:\n{o['content']}" for o in agent_outputs])
        
        prompt = f"""Extract and synthesize ALL regulatory deadlines, compliance requirements, and time-sensitive
financial triggers from the following research on {ticker}.

RESEARCH FINDINGS:
{full_context}

PRODUCE A STRUCTURED TABLE:
| Deadline Date | Regulatory Source | Financial Impact if Missed | Fallback Option | Q-Milestone to Meet |
|---------------|-------------------|---------------------------|-----------------|---------------------|

Then add:
- **Pending Regulatory Changes (Next 12-24 Months):** [list with expected dates]
- **Regulatory Risk Summary:** [1-2 sentences with total dollar exposure if all deadlines missed]

If NO hard deadlines exist, state: "No hard regulatory deadlines identified. Reviewed: [sources checked]."
Cite every deadline with its regulatory source and year.
"""
        return await call_gemini_async(prompt, max_tokens=1500, temperature=0.0)

    async def generate_execution_roadmap_section(self, ticker: str, agent_outputs: List[Dict[str, Any]]) -> str:
        """Generates a board-ready quarterly execution roadmap from all agent recommendations."""
        full_context = "\n\n".join([f"SECTION {o['agent_id'].upper()}:\n{o['content']}" for o in agent_outputs])
        
        prompt = f"""Synthesize all recommendations from the {ticker} research into a board-ready Execution Roadmap.

RESEARCH FINDINGS:
{full_context}

PRODUCE:
1. QUARTERLY MILESTONE TABLE:
| Quarter | Milestone | $ Allocation | Decision Gate | KPI Target | Owner |
|---------|-----------|--------------|---------------|------------|-------|

2. DECISION TRIGGERS (what activates Plan B):
- Trigger 1: If [condition], then [action]
- Trigger 2: If [condition], then [action]

3. CONTINGENCY PLAN B:
"If primary plan delayed past [date], execute: [specific alternative] → estimated impact difference: $X.XM"

All milestones must be specific: WHO, WHAT, WHEN, HOW MUCH, measured by WHAT KPI.
Cite regulatory deadlines that anchor each milestone.
"""
        return await call_gemini_async(prompt, max_tokens=1500, temperature=0.1)

    async def generate_geographic_section(self, ticker: str, agent_outputs: List[Dict[str, Any]]) -> str:
        """Generates a geographic and market segmentation analysis."""
        full_context = "\n\n".join([f"SECTION {o['agent_id'].upper()}:\n{o['content']}" for o in agent_outputs])
        
        prompt = f"""Synthesize the geographic and market segmentation insights for {ticker} from the research below.

RESEARCH FINDINGS:
{full_context}

PRODUCE A REGIONAL ANALYSIS TABLE:
| Region/Market | Revenue ($M/%total) | Growth Rate | Key Risk | Recommended Concentration |
|---------------|---------------------|-------------|----------|---------------------------|

Then:
- **Regional Cost Comparison:** [specific $/unit or $/MWh or $/kW by region]
- **Regional Risk Map:** [curtailment, queue, permitting issues by region with $ impact]
- **Cross-Border Opportunities:** [if applicable, with regulatory/tax differences]
- **Recommendation:** [which regions to overweight/underweight and why, with specific numbers]

If market is geographically uniform, state WHY with supporting data and source citation.
"""
        return await call_gemini_async(prompt, max_tokens=1200, temperature=0.1)

    async def generate_benchmarking_section(self, ticker: str, agent_outputs: List[Dict[str, Any]]) -> str:
        """Generates the comparative options analysis and benchmarking section."""
        full_context = "\n\n".join([f"SECTION {o['agent_id'].upper()}:\n{o['content']}" for o in agent_outputs])
        
        prompt = f"""From the {ticker} research, produce a rigorous Options Analysis and Comparative Benchmarking section.

RESEARCH FINDINGS:
{full_context}

PRODUCE:
1. OPTIONS COMPARISON TABLE (show 2-3 realistic alternatives):
| Metric          | Option A | Option B | Option C | Recommended |
|-----------------|----------|----------|----------|-------------|
| IRR / Return    |          |          |          |             |
| Cost ($M/unit)  |          |          |          |             |
| Payback Period  |          |          |          |             |
| Tax Credit Value|          |          |          |             |
| Key Risk        |          |          |          |             |

2. TRADE-OFF ANALYSIS:
- Option A: [benefit] BUT [cost/risk]
- Option B: [benefit] BUT [cost/risk]
- Option C: [benefit] BUT [cost/risk]

3. RECOMMENDATION RATIONALE:
"Recommend [Option X] because [specific reason tied to decision criteria with dollar impact].
 At marginal cost premium of $X.XM, NPV gain is $X.XM — [X]x return on premium."

4. INDUSTRY BENCHMARKS:
"Industry [metric] range: [X]%-[Y]%; recommendation targets [Z]% ([position in range])."
"""
        return await call_gemini_async(prompt, max_tokens=1200, temperature=0.1)

    async def generate_bibliography_section(self, ticker: str, agent_outputs: List[Dict[str, Any]]) -> str:
        """Extracts and formats all citations into a structured bibliography."""
        full_context = "\n\n".join([f"SECTION {o['agent_id'].upper()}:\n{o['content']}" for o in agent_outputs])
        
        prompt = f"""Extract ALL citations from the {ticker} research and format them as a structured bibliography.

RESEARCH CONTENT:
{full_context}

FORMAT each citation as:
**[SOURCE TYPE]** Organization/Author (Year): Document/Report Name. URL or Filing Reference if available.

Group by:
## Primary Sources (Government/Regulatory/SEC Filings)
## Secondary Sources (Industry Reports/News/Analysis)
## Vault Data (Internal IIP Database)

For each citation, note: Data Currency ("As of Q[N] [Year]")
Flag any citations that are vague (e.g., 'market data shows') and suggest the primary source to verify.
"""
        return await call_gemini_async(prompt, max_tokens=1000, temperature=0.0)

    async def generate_quality_scorecard(self, ticker: str, final_report: str) -> str:
        """Generates a self-assessment scorecard evaluating the report against all 7 A-grade dimensions."""
        prompt = f"""You are a Research Quality Auditor. Evaluate the following {ticker} report against the 7-dimension A-grade framework.

REPORT EXCERPT (first 6000 chars):
{final_report[:6000]}

SCORE each dimension 1-5 and calculate weighted total:
| Dimension                        | Score | Weight | Weighted Score |
|----------------------------------|-------|--------|----------------|
| 1. Regulatory/Deadline Mapping   | __/5  | 25%    | __             |
| 2. Quantification & Specificity  | __/5  | 20%    | __             |
| 3. Source Transparency           | __/5  | 15%    | __             |
| 4. Actionability & Clarity       | __/5  | 15%    | __             |
| 5. Risk Assessment Rigor         | __/5  | 10%    | __             |
| 6. Geographic Segmentation       | __/5  |  5%    | __             |
| 7. Comparative Benchmarking      | __/5  |  5%    | __             |
| **TOTAL WEIGHTED SCORE**         |       |        | **__/5.0**     |

Scoring guide:
- 5/5: Exemplary — fully satisfies all requirements for this dimension
- 4/5: Strong — mostly satisfies with minor gaps
- 3/5: Adequate — present but incomplete
- 2/5: Weak — partially addressed
- 1/5: Missing — dimension not addressed

After the table, add:
**Letter Grade:** [A/B+/B/B-/C] (4.5-5.0=A, 4.0-4.5=B+, 3.5-4.0=B, 3.0-3.5=B-, <3.0=C)
**Gaps to Address:** [List any dimension scoring <4/5 with specific fix needed]
**Strengths:** [2-3 strongest sections]
"""
        return await call_gemini_async(prompt, max_tokens=1000, temperature=0.0)

    async def generate_monitoring_areas(self, ticker: str, agent_outputs: List[Dict[str, Any]]) -> str:
        """Identifies key metrics/events to watch based on findings."""
        full_context = "\n\n".join([f"SECTION {o['agent_id'].upper()}:\n{o['content']}" for o in agent_outputs])
        
        prompt = f"""Based on the research for {ticker}, identify 3-5 'Key Monitoring Areas' (specific metrics or events) that institutional investors should watch over the next 12-24 months.

RESEARCH FINDINGS:
{full_context}
"""
        return await call_gemini_async(prompt, max_tokens=500, temperature=0.1)

    async def synthesize(self, ticker: str, agent_outputs: List[Dict[str, Any]], user_prompt: str = "", callback=None) -> str:
        """Assembles the final institutional report with a dynamic, topic-driven headline."""
        if callback: callback(f"Synthesizing final research report for {ticker}...")
        
        # 1. Map outputs to IDs for easy access
        output_map = {o["agent_id"]: o["content"] for o in agent_outputs}
        
        # 2. Generate Summary & Cross-Cutting Sections in Parallel
        if callback: callback("Generating cross-cutting institutional analysis in parallel...")
        
        tasks = {
            "dynamic_title": self.generate_dynamic_title(ticker, user_prompt),
            "executive_summary": self.generate_executive_summary(ticker, agent_outputs),
            "deadlines": self.generate_regulatory_deadlines_section(ticker, agent_outputs),
            "geographic": self.generate_geographic_section(ticker, agent_outputs),
            "benchmarking": self.generate_benchmarking_section(ticker, agent_outputs),
            "execution_roadmap": self.generate_execution_roadmap_section(ticker, agent_outputs),
            "bibliography": self.generate_bibliography_section(ticker, agent_outputs),
            "monitoring": self.generate_monitoring_areas(ticker, agent_outputs)
        }
        
        # Unpack keys and await tasks
        task_names = list(tasks.keys())
        task_coros = list(tasks.values())
        
        try:
            # 900s timeout for synthesis to be safe (free tier pacing)
            results = await asyncio.wait_for(asyncio.gather(*task_coros), timeout=900.0)
            
            # Map results back to output_map
            for name, result in zip(task_names, results):
                if name == "dynamic_title":
                    dynamic_title = result
                else:
                    output_map[name] = result
        except TimeoutError:
            log.error("Report synthesis timed out. Producing partial report.")
            if callback: callback("Synthesis timed out. Compiling partial results...")
            dynamic_title = f"INSTITUTIONAL EQUITY RESEARCH: {ticker}"
            # Fill missing with empty or error
            for name in task_names:
                if name not in output_map and name != "dynamic_title":
                    output_map[name] = "Section generation timed out."
        except Exception as e:
            log.error(f"Synthesis failed: {e}")
            dynamic_title = f"INSTITUTIONAL EQUITY RESEARCH: {ticker}"
        
        # 3. Assemble sections in order
        if callback: callback("Assembling institutional narrative...")
        report_parts = []
        report_parts.append(f"# {dynamic_title}")
        report_parts.append(f"*Ticker: {ticker} | Date: {datetime.now().strftime('%B %d, %Y')} | Level: Institutional Depth Analysis*")
        
        for section_id in SECTION_ORDER:
            content = output_map.get(section_id)
            if content:
                header = SECTION_HEADERS.get(section_id, f"## {section_id.upper()}")
                clean_content = re.sub(r'^#+ .*\n', '', content).strip()
                report_parts.append(f"\n{header}\n{clean_content}")
        
        assembled_report = "\n\n---\n".join(report_parts)
        
        return assembled_report
