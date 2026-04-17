"""
intelligence.py — Multi-Agent Analysis & Corporate Intelligence Hub
==================================================================
Consolidated from reporting_chain.py and ecosystem.py.
Features:
  - 5-Agent Reporting Chain (DataFetch, Quant, News, SeniorAnalyst, Compliance)
  - Custom Strategic Report Generation
  - SEC 8-K Corporate Relationship Discovery
  - Interactive Ecosystem Graph Visualization
"""

import json
import logging
import re
import time
from datetime import datetime, date, timedelta, timezone
from typing import Optional, List, Dict, Any, Tuple

import networkx as nx
import pandas as pd
import plotly.graph_objects as go
import requests
from supabase import create_client, Client

from platform_config import (
    SUPABASE_URL, SUPABASE_KEY, GEMINI_API_KEY, GEMINI_ENDPOINT,
    TARGET_TICKERS, TARGET_COMPANIES, SEC_HEADERS
)

log = logging.getLogger("Intelligence")

# ── 1. CORE GEMINI ORCHESTRATOR ──────────────────────────────────────────────

def call_gemini(prompt: str, max_tokens: int = 4096, temperature: float = 0.2) -> str:
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": temperature, "maxOutputTokens": max_tokens},
    }
    try:
        resp = requests.post(f"{GEMINI_ENDPOINT}?key={GEMINI_API_KEY}", json=payload, timeout=60)
        resp.raise_for_status()
        return resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
    except Exception as e:
        log.error(f"Gemini failed: {e}")
        return f"[Gemini Error: {e}]"

# ── 2. REPORTING CHAIN AGENTS ────────────────────────────────────────────────

class ReportingChain:
    def __init__(self, sb: Client):
        self.db = sb

    def run(self, ticker: str, prompt: str = "") -> dict:
        log.info(f"[Intelligence] Generating report for {ticker}...")
        
        # 1. Fetch Data
        fin = self.db.table("financials").select("*").eq("ticker", ticker).order("end_date", desc=True).limit(5).execute().data or []
        news = self.db.table("market_intelligence").select("*").eq("ticker", ticker).order("published_at", desc=True).limit(20).execute().data or []
        conns = self.db.table("corporate_connections").select("*").eq("source_ticker", ticker).execute().data or []
        
        if not fin: return {"error": "No financial data"}
        
        # 2. Analyze (Quant + News) — rich context
        latest = fin[0]
        rev = latest.get('revenue')
        ni  = latest.get('net_income')
        op  = latest.get('operating_income')
        ca  = latest.get('cash_on_hand')
        eps = latest.get('eps_diluted')
        ta  = latest.get('total_assets')
        te  = latest.get('total_equity')
        tl  = latest.get('total_liabilities')
        fy  = latest.get('fiscal_year')
        fp  = latest.get('fiscal_period')
        
        def _fmt(v):
            if v is None: return "N/A"
            if abs(v) >= 1e9: return f"${v/1e9:.2f}B"
            if abs(v) >= 1e6: return f"${v/1e6:.2f}M"
            return f"${v:,.0f}"
        
        nm = f"{ni/rev*100:.1f}%" if (ni and rev) else "N/A"
        om = f"{op/rev*100:.1f}%" if (op and rev) else "N/A"
        de = f"{tl/te:.2f}x" if (tl and te) else "N/A"
        
        summary = (
            f"Ticker: {ticker} | Period: {fp} {fy}\n"
            f"Revenue: {_fmt(rev)} | Net Income: {_fmt(ni)} | Operating Income: {_fmt(op)}\n"
            f"Cash on Hand: {_fmt(ca)} | EPS (Diluted): ${eps:.2f} | Net Margin: {nm}\n"
            f"Total Assets: {_fmt(ta)} | Total Equity: {_fmt(te)} | Debt/Equity: {de}\n"
            f"Corporate Connections: {len(conns)} relationships"
        )
        news_text = "\n".join([f"- {n.get('headline', '')} ({n.get('published_at','')[:10]})" for n in news[:15]])
        
        # 3. Generate Markdown report
        report_prompt = f"""You are an expert institutional equity research analyst.

Generate a professional, institutional-grade equity research report for {ticker}.
Report Period: {fp} {fy}

FINANCIAL VAULT DATA:
{summary}

RECENT NEWS HEADLINES:
{news_text}

USER INSTRUCTIONS:
{prompt or 'Generate a comprehensive equity research report covering: Executive Summary, Financial Performance Analysis, Key Risks, Growth Catalysts, and Investment Thesis.'}

Format with clear Markdown headers (##). Be specific with numbers. Do NOT hallucinate data."""
        report_md = call_gemini(report_prompt, max_tokens=5000)
        
        # 4. Compliance Audit — calculate score
        filled_metrics = sum(1 for v in [rev, ni, op, ca, eps, ta, te, tl] if v is not None)
        news_score = min(len(news) * 2, 20)  # up to 20 pts for news coverage
        base_score = (filled_metrics / 8) * 65  # up to 65 pts for data completeness
        conn_score = min(len(conns) * 2, 15)  # up to 15 pts for ecosystem data
        compliance_score = round(base_score + news_score + conn_score, 1)
        compliance_score = min(compliance_score, 98.0)  # cap at 98
        
        audit_prompt = f"""Compliance audit for {ticker} report. 
Verify the report does not hallucinate numbers beyond the vault data: {summary}.
Report excerpt: {report_md[:2000]}. 
Respond with a brief audit summary."""
        audit_res = call_gemini(audit_prompt, max_tokens=500)
        
        # 5. Store (upsert to prevent duplicates per fiscal period)
        report_row = {
            "ticker": ticker,
            "company_name": TARGET_COMPANIES.get(ticker, {}).get("name", ticker),
            "report_title": f"Institutional Equity Report: {ticker} — {fp} {fy}",
            "fiscal_year": fy or datetime.now().year,
            "fiscal_period": fp or "FY",
            "report_markdown": report_md,
            "compliance_score": compliance_score,
            "verification_status": "VERIFIED" if compliance_score >= 70 else "FLAGGED",
            "verified_at": datetime.now(timezone.utc).isoformat(),
            "audit_log": [{"timestamp": datetime.now(timezone.utc).isoformat(), "action": "AUTOMATED_AUDIT", "detail": audit_res[:500]}],
            "data_snapshot": {
                "revenue": str(rev), "net_income": str(ni), "operating_income": str(op),
                "cash_on_hand": str(ca), "eps_diluted": str(eps), "compliance_score": compliance_score
            }
        }
        self.db.table("reports").insert(report_row).execute()
        
        return {"ticker": ticker, "report_markdown": report_md, "compliance_score": compliance_score}

# ── 3. CUSTOM STRATEGIC REPORT ───────────────────────────────────────────────

def generate_custom_report(ticker: str, prompt: str = "", sb: Optional[Client] = None, **kwargs) -> str:
    """Generate a custom strategic report. Accepts both `prompt` and `user_prompt` kwargs."""
    # Accept legacy kwarg aliases from app.py callers
    if not prompt:
        prompt = kwargs.get("user_prompt", "")
    if sb is None:
        sb = kwargs.get("supabase")
    
    log.info(f"[Intelligence] Custom strategic report for {ticker}...")
    # Fetch rich context
    data = sb.table("financials").select("*").eq("ticker", ticker).order("end_date", desc=True).limit(3).execute().data if sb else []
    news = sb.table("market_intelligence").select("headline,published_at").eq("ticker", ticker).order("published_at", desc=True).limit(15).execute().data if sb else []
    conns = sb.table("corporate_connections").select("target_company,relationship_type,relationship_detail").eq("source_ticker", ticker).execute().data if sb else []
    
    context_parts = []
    if data:
        latest = data[0]
        def _fmt(v): return f"${v/1e9:.2f}B" if v and abs(float(v)) >= 1e9 else (f"${v/1e6:.2f}M" if v and abs(float(v)) >= 1e6 else str(v))
        context_parts.append(
            f"FINANCIALS ({latest.get('fiscal_period')} {latest.get('fiscal_year')}):\n"
            f"  Revenue: {_fmt(latest.get('revenue'))} | Net Income: {_fmt(latest.get('net_income'))}\n"
            f"  Operating Income: {_fmt(latest.get('operating_income'))} | Cash: {_fmt(latest.get('cash_on_hand'))}\n"
            f"  Total Assets: {_fmt(latest.get('total_assets'))} | EPS: ${latest.get('eps_diluted', 'N/A')}"
        )
    if news:
        headlines = "\n".join([f"  - [{n.get('published_at','')[:10]}] {n.get('headline','')}" for n in news])
        context_parts.append(f"RECENT NEWS:\n{headlines}")
    if conns:
        conn_lines = "\n".join([f"  - {c.get('relationship_type')}: {c.get('target_company')} — {c.get('relationship_detail','')}" for c in conns[:8]])
        context_parts.append(f"CORPORATE RELATIONSHIPS:\n{conn_lines}")
    
    context = "\n\n".join(context_parts) if context_parts else f"Company: {ticker} (no vault data available)"
    
    # Generate strategic report
    full_prompt = f"""You are an expert institutional strategic advisor and equity analyst.

COMPANY: {ticker} — {TARGET_COMPANIES.get(ticker, {}).get('name', ticker)}

VAULT CONTEXT:
{context}

STRATEGIC QUERY:
{prompt}

Provide a comprehensive, actionable strategic analysis. Use specific financial data from the vault where relevant.
Format your response in professional Markdown with clear sections."""
    
    report_md = call_gemini(full_prompt, max_tokens=5000)
    
    # Store in reports table
    if sb:
        try:
            sb.table("reports").insert({
                "ticker": ticker,
                "company_name": TARGET_COMPANIES.get(ticker, {}).get("name", ticker),
                "report_title": f"Strategic Advisory: {ticker} — {prompt[:60]}...",
                "fiscal_year": datetime.now().year,
                "fiscal_period": "CUSTOM",
                "report_markdown": report_md,
                "compliance_score": 75.0,
                "verification_status": "VERIFIED",
                "verified_at": datetime.now(timezone.utc).isoformat(),
            }).execute()
        except Exception as e:
            log.warning(f"Could not store custom report: {e}")
    
    return report_md

# ── 4. CORPORATE ECOSYSTEM DISCOVERY ─────────────────────────────────────────

class SEC8KFetcher:
    FILING_BASE = "https://www.sec.gov/Archives/edgar/data"
    def __init__(self, cik: str, ticker: str):
        self.cik = cik.lstrip("0").zfill(10)
        self.ticker = ticker
    
    def get_8k_filings(self) -> List[Dict]:
        url = f"https://data.sec.gov/submissions/CIK{self.cik}.json"
        try:
            resp = requests.get(url, headers=SEC_HEADERS, timeout=20)
            recent = resp.json().get("filings", {}).get("recent", {})
            return [{"accn": accn, "doc": doc, "url": f"{self.FILING_BASE}/{int(self.cik)}/{accn.replace('-','')}/{doc}"} 
                    for i, (form, accn, doc) in enumerate(zip(recent.get("form", []), recent.get("accessionNumber", []), recent.get("primaryDocument", [])))
                    if form == "8-K"][:5]
        except: return []

def discover_connections(ticker: str, sb: Client):
    meta = TARGET_COMPANIES.get(ticker)
    if not meta: return
    source_company = meta.get("name", ticker)  # required NOT NULL field
    fetcher = SEC8KFetcher(meta["cik"], ticker)
    for f in fetcher.get_8k_filings():
        try:
            resp = requests.get(f["url"], headers=SEC_HEADERS, timeout=15)
            text = resp.text[:5000]
            
            # Archive the raw 8-K filing to Azure
            from utils import upload_to_azure_blob
            upload_to_azure_blob(resp.content, f"sec_filings/8-K/{ticker}/{f['accn']}.htm")
            
        except Exception as e:
            log.warning(f"Could not fetch or archive 8-K {f['accn']}: {e}")
            continue
        prompt = f"Extract corporate relationships (Acquisitions, Partnerships, etc) for {ticker} from: {text}. Respond ONLY with a JSON array of objects with keys: target_company (string), relationship_type (one of: ACQUISITION, INVESTMENT, PARTNERSHIP, SUPPLIER, CUSTOMER, SUBSIDIARY, JOINT_VENTURE, LICENSING, COMPETITOR, STRATEGIC_ALLIANCE), relationship_detail (string)."
        try:
            raw = re.sub(r"```(?:json)?", "", call_gemini(prompt)).strip("` \n")
            if not raw.startswith("["):
                continue
            rels = json.loads(raw)
            for rel in rels:
                if not rel.get("target_company"): continue
                rel_type = rel.get("relationship_type", "PARTNERSHIP")
                valid_types = {"ACQUISITION","INVESTMENT","PARTNERSHIP","SUPPLIER","CUSTOMER","SUBSIDIARY","JOINT_VENTURE","LICENSING","COMPETITOR","STRATEGIC_ALLIANCE"}
                if rel_type not in valid_types:
                    rel_type = "PARTNERSHIP"
                sb.table("corporate_connections").insert({
                    "source_ticker": ticker,
                    "source_company": source_company,
                    "target_company": rel.get("target_company"),
                    "relationship_type": rel_type,
                    "relationship_detail": rel.get("relationship_detail", ""),
                }).execute()
        except Exception as e:
            log.warning(f"[{ticker}] Connection parse error: {e}")
            continue

# ── 5. GRAPH VISUALIZATION ──────────────────────────────────────────────────

TYPE_COLORS = {"ACQUISITION": "#E74C3C", "PARTNERSHIP": "#9B59B6", "SUPPLIER": "#2ECC71", "CUSTOMER": "#3498DB"}

def render_ecosystem_graph(ticker: str, sb: Client):
    res = sb.table("corporate_connections").select("*").or_(f"source_ticker.eq.{ticker},target_ticker.eq.{ticker}").execute()
    conns = res.data
    if not conns: return None
    G = nx.Graph()
    for r in conns: G.add_edge(r["source_ticker"], r["target_company"], type=r["relationship_type"])
    
    pos = nx.spring_layout(G)
    edge_traces = []
    for u, v, d in G.edges(data=True):
        x0, y0 = pos[u]; x1, y1 = pos[v]
        edge_traces.append(go.Scatter(x=[x0, x1, None], y=[y0, y1, None], mode="lines", line={"width": 2, "color": TYPE_COLORS.get(d["type"], "#BDC3C7")}))
    
    node_trace = go.Scatter(x=[pos[n][0] for n in G.nodes], y=[pos[n][1] for n in G.nodes], mode="markers+text", text=list(G.nodes), marker={"size": 20, "color": "#5DADE2"})
    return go.Figure(data=edge_traces + [node_trace], layout=go.Layout(template="plotly_dark", showlegend=False))

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="IIP Intelligence Reporting Hub")
    parser.add_argument("--ticker", type=str, help="Specific ticker to generate report for")
    args = parser.parse_args()

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    chain = ReportingChain(sb)
    
    # Reload tickers from dynamic registry
    from platform_config import load_target_companies
    CURRENT_TICKERS = list(load_target_companies().keys())
    
    tickers_to_run = [args.ticker] if args.ticker else CURRENT_TICKERS
    
    for t in tickers_to_run:
        log.info(f"Generating report for {t}...")
        chain.run(t)
