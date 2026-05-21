"""
============================================================
PART E: STREAMLIT DASHBOARD v2 — COMPANY INSIGHTS
Doc-extract-and-report | samaygangwal21@gmail.com

New in v2:
  • Sector-grouped sidebar navigation
  • 2-year Report Archive — browse all historical reports
  • Sector Heatmap — cross-company metric comparison
  • AI Research Chat — ask questions about any company
  • Smart Watchlist — flag companies for review
  • Quarterly Drill-Down — per-quarter revenue waterfall
  • Intelligence Library — simplified report archive
============================================================
"""

import os
import subprocess
import time
import sys
import asyncio
APP_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(APP_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# --- FORCE RELOAD 2026-05-15 13:59 ---
import streamlit as st # type: ignore
import pandas as pd # type: ignore
import plotly.graph_objects as go # type: ignore
import plotly.express as px # type: ignore
from supabase import create_client, Client # type: ignore
from datetime import datetime, date
import requests # type: ignore
import hashlib
import json
import httpx # type: ignore
import re
import logging as log
from typing import List, Dict, Optional, Any, cast

from infrastructure import config as platform_config
from infrastructure.config import ( # type: ignore
    SUPABASE_URL, SUPABASE_KEY, GEMINI_API_KEY, GEMINI_ENDPOINT,
    AZURE_STORAGE_CONNECTION_STRING, AZURE_STORAGE_CONTAINER_NAME,
    TARGET_COMPANIES, SECTOR_ICONS, load_target_companies, get_supabase, get_company_meta as _get_company_meta
)
from pipelines.reasoning.orchestrator import ResearchOrchestrator

# ── Infrastructure Layer ───────────────────────────────────────────────────
from infrastructure.base_infra import cache, resilience, obs, worker
from domains.financials.analytics import analytics_service
from domains.reports.vault import vault_service
from domains.intelligence.retrieval import retrieval_service
from infrastructure.workers.sync import sync_service
from domains.reports.delivery import alerting_engine

# Autonomous Monitoring Disabled


# Consolidated Modules
from pipelines.ingestion.collector import ExtractorEngine
from app.ui.visualization import render_ecosystem_graph
from infrastructure.utils import build_sec_ix_url, backfill_sec_urls
from pipelines.reasoning.orchestrator import ResearchOrchestrator
 
@st.cache_data(ttl=3600)
def get_company_meta(ticker: str):
    return _get_company_meta(ticker)

# Initial companies (limited for search context)
TARGET_COMPANIES = load_target_companies()

@st.cache_data(ttl=300)
def search_companies(query: str) -> List[Dict]:
    """Scalable company search — works for 5 or 8,000+ companies. Filter-first, lazy-loaded."""
    sb = get_supabase()
    try:
        if not query or len(query) < 1:
            # Return all current companies but capped at 50 (safe for any scale)
            res = sb.table("target_companies").select("ticker, company_name, sector").limit(50).execute()
        else:
            # ILIKE search — uses DB-level filtering (index-ready)
            res = sb.table("target_companies").select("ticker, company_name, sector").or_(
                f"ticker.ilike.%{query}%,company_name.ilike.%{query}%"
            ).limit(20).execute()
        return res.data or []
    except:
        # Fallback to local config if DB unreachable
        results = []
        for t, m in TARGET_COMPANIES.items():
            if query.lower() in t.lower() or query.lower() in m["name"].lower():
                results.append({"ticker": t, "company_name": m["name"], "sector": m.get("sector", "")})
        return results

@st.cache_data(ttl=300)
def load_uploaded_docs(ticker: str) -> List[Dict]:
    if not ticker:
        return []
    sb = get_supabase()
    try:
        res = sb.table("extracted_documents").select("*").eq("ticker", ticker).order("created_at", ascending=False).execute()
        return res.data or []
    except:
        return []

@st.cache_data(ttl=60)
def load_all_vault_docs() -> list:
    try:
        sb = get_supabase()
        res = sb.table("extracted_documents").select(
            "id, ticker, source_url, source_type, created_at, raw_text"
        ).order("created_at", desc=True).limit(100).execute()
        return res.data or []
    except:
        return []

# Page config now handled in app.py
pass

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=Outfit:wght@400;600;800&display=swap');

    [data-testid="stAppViewContainer"] {
        background-color: #0B0E14;
        font-family: 'Inter', sans-serif;
    }
    [data-testid="stHeader"] { background: rgba(0,0,0,0); }
    [data-testid="stSidebar"] {
        background-color: #0B0E14;
        border-right: 1px solid rgba(255,255,255,0.05);
    }
    [data-testid="stSidebar"] .stRadio > label { color: #58a6ff !important; font-weight: 700; font-size: 11px; letter-spacing: 0.1em; }

    /* Glassmorphism Cards */
    .metric-card {
        background: linear-gradient(135deg, rgba(30, 35, 45, 0.3) 0%, rgba(20, 25, 30, 0.5) 100%);
        backdrop-filter: blur(20px);
        border: 1px solid rgba(255, 255, 255, 0.05);
        border-radius: 20px;
        padding: 24px;
        margin: 10px 0;
        min-height: 120px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    }
    .metric-card:hover {
        transform: translateY(-4px);
        border-color: rgba(88, 166, 255, 0.3);
        background: linear-gradient(135deg, rgba(40, 45, 55, 0.4) 0%, rgba(30, 35, 40, 0.6) 100%);
    }

    .metric-label {
        color: #8b949e;
        font-size: 10px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.15em;
        font-family: 'Outfit', sans-serif;
    }
    .metric-value {
        color: #ffffff;
        font-size: 34px;
        font-weight: 800;
        margin-top: 4px;
        font-family: 'Outfit', sans-serif;
        letter-spacing: -0.02em;
    }
    .metric-sub { color: #8b949e; font-size: 12px; margin-top: 2px; }

    /* Custom Badges */
    .badge-verified {
        background: rgba(46, 160, 67, 0.15);
        color: #3fb950;
        border: 1px solid rgba(46, 160, 67, 0.3);
        padding: 4px 14px;
        border-radius: 100px;
        font-size: 10px;
        font-weight: 800;
        letter-spacing: 0.05em;
    }
    /* Section Headers */
    .section-header {
        font-size: 10px;
        font-weight: 800;
        color: #8b949e;
        text-transform: uppercase;
        letter-spacing: 0.2em;
        margin: 40px 0 16px 0;
        font-family: 'Outfit', sans-serif;
        display: flex;
        align-items: center;
        gap: 8px;
    }
    .section-header::after {
        content: '';
        flex: 1;
        height: 1px;
        background: rgba(255, 255, 255, 0.05);
    }

    .news-item {
        padding: 20px;
        margin: 16px 0;
        background: rgba(22, 27, 34, 0.3);
        border: 1px solid rgba(255, 255, 255, 0.03);
        border-radius: 16px;
        transition: all 0.2s;
    }
    .news-item:hover { 
        background: rgba(30, 35, 45, 0.5);
        border-color: rgba(255, 255, 255, 0.08);
    }
    
    /* Better Sidebar Branding */
    .sidebar-brand {
        padding: 24px 0;
        text-align: center;
    }
    .sidebar-logo {
        width: 48px;
        height: 48px;
        background: linear-gradient(135deg, #1f6feb 0%, #58a6ff 100%);
        border-radius: 12px;
        margin: 0 auto 16px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 24px;
        box-shadow: 0 4px 12px rgba(31, 111, 235, 0.3);
    }

    /* Chat bubbles */
    .chat-user {
        background: rgba(31, 111, 235, 0.15);
        border: 1px solid rgba(31, 111, 235, 0.3);
        border-radius: 16px 16px 4px 16px;
        padding: 12px 16px;
        margin: 8px 0;
        color: #f0f6fc;
        font-size: 14px;
        max-width: 85%;
        margin-left: auto;
    }
    .chat-ai {
        background: rgba(22, 27, 34, 0.6);
        border: 1px solid rgba(255,255,255,0.07);
        border-radius: 4px 16px 16px 16px;
        padding: 12px 16px;
        margin: 8px 0;
        color: #c9d1d9;
        font-size: 14px;
        max-width: 95%;
    }
    /* Report card */
    .report-card {
        background: rgba(22, 27, 34, 0.4);
        border: 1px solid rgba(255,255,255,0.05);
        border-radius: 12px;
        padding: 14px 16px;
        margin: 8px 0;
        transition: all 0.2s;
    }
    .report-card:hover {
        background: rgba(30, 35, 45, 0.6);
        border-color: rgba(255,255,255,0.1);
    }
    /* Sentiment news borders */
    .news-pos { border-left: 3px solid #3fb950 !important; }
    .news-neg { border-left: 3px solid #f85149 !important; }
    .news-neu { border-left: 3px solid rgba(255,255,255,0.1) !important; }
    /* Badge variants */
    .badge-flagged {
        background: rgba(227, 179, 65, 0.15); color: #e3b341;
        border: 1px solid rgba(227, 179, 65, 0.3);
        padding: 4px 14px; border-radius: 100px; font-size: 10px; font-weight: 800;
    }
    .badge-rejected {
        background: rgba(248, 81, 73, 0.15); color: #f85149;
        border: 1px solid rgba(248, 81, 73, 0.3);
        padding: 4px 14px; border-radius: 100px; font-size: 10px; font-weight: 800;
    }
    .badge-pending {
        background: rgba(139, 148, 158, 0.15); color: #8b949e;
        border: 1px solid rgba(139, 148, 158, 0.3);
        padding: 4px 14px; border-radius: 100px; font-size: 10px; font-weight: 800;
    }
    #MainMenu {visibility:hidden;} footer {visibility:hidden;}
    [data-testid="stToolbar"] {visibility:hidden;}
</style>
""", unsafe_allow_html=True)

def render_institutional_response(text: str):
    """Renders the institutional report with premium typography and section separation."""
    # Split text and chart data
    parts = re.split(r'```json_chart', text)
    main_text = parts[0]
    
    # Pre-process markdown for premium look
    # 1. Style citations [SEC], [MARKET], [Source: SEC], etc.
    main_text = re.sub(r'\[(?:Source:\s*)?(SEC|MARKET|NEWS|ECOSYSTEM|VAULT|TRANSCRIPT|WEB)(?:\s+[^\]]+)?\]', r'<span class="source-tag">\1</span>', main_text, flags=re.IGNORECASE)
    
    st.markdown(f"""
    <style>
        /* Target headers in the markdown area */
        .stMarkdown h1, .stMarkdown h2, .stMarkdown h3 {{
            color: #58a6ff !important;
            margin-top: 32px !important;
            margin-bottom: 16px !important;
            font-family: 'Outfit', sans-serif !important;
            border-bottom: 1px solid rgba(88, 166, 255, 0.1) !important;
            padding-bottom: 8px !important;
        }}
        .source-tag {{
            background: rgba(88, 166, 255, 0.15);
            color: #79c0ff;
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 10px;
            font-weight: 800;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin: 0 2px;
            vertical-align: middle;
            border: 1px solid rgba(88, 166, 255, 0.2);
        }}
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown(main_text, unsafe_allow_html=True)
    
    if len(parts) > 1:
        for p in parts[1:]:
            try:
                chart_json_str = p.split('```')[0].strip()
                c = json.loads(chart_json_str)
                df = pd.DataFrame(c["data"], columns=c["columns"])
                title = c.get("title", "Strategic Analysis Chart")
                
                if c.get("type") == "line":
                    fig = px.line(df, x=c["columns"][0], y=c["columns"][1], title=title, template="plotly_dark", markers=True)
                else:
                    fig = px.bar(df, x=c["columns"][0], y=c["columns"][1], title=title, template="plotly_dark")
                
                fig.update_layout(
                    font_family="Outfit",
                    title_font_size=18,
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                    margin=dict(l=0, r=0, t=40, b=0),
                    xaxis=dict(showgrid=False),
                    yaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.05)')
                )
                st.plotly_chart(fig, use_container_width=True)
            except Exception:
                pass

def render_workspace(workspace: Dict[str, Any]):
    """Renders the modular institutional intelligence workspace."""
    st.markdown(f"""
    <div style='background:rgba(88,166,255,0.05); border-left:4px solid #58a6ff; padding:16px; border-radius:4px; margin-bottom:24px;'>
        <div style='color:#8b949e; font-size:10px; font-weight:800; text-transform:uppercase; letter-spacing:0.1em;'>Intelligence Workspace Active</div>
        <div style='color:#f0f6fc; font-size:18px; font-weight:600;'>{workspace['query']}</div>
    </div>
    """, unsafe_allow_html=True)

    col_main, col_side = st.columns([3, 1])

    with col_main:
        for panel in workspace["panels"]:
            st.markdown(f"<div class='section-header'>{panel['title'].upper()}</div>", unsafe_allow_html=True)
            
            if panel["type"] == "analytics":
                # KPI Row
                kpi_cols = st.columns(len(panel["kpis"]))
                for i, kpi_html in enumerate(panel["kpis"]):
                    kpi_cols[i].markdown(kpi_html, unsafe_allow_html=True)
                
                # Chart Area
                if panel.get("chart"):
                    st.plotly_chart(panel["chart"], use_container_width=True)
            
            elif panel["type"] == "feed":
                ticker = panel.get("ticker")
                news = load_news(ticker) if ticker else []
                for n in news[:5]:
                    st.markdown(f"""
                    <div class='news-item'>
                        <div style='color:#58a6ff; font-size:11px; font-weight:700;'>{n.get('published_at','')[:10]}</div>
                        <div style='color:#f0f6fc; font-weight:600; margin:4px 0;'>{n.get('headline')}</div>
                        <div style='color:#8b949e; font-size:12px;'>Source: {n.get('source_url','Vault')}</div>
                    </div>
                    """, unsafe_allow_html=True)

    with col_side:
        st.markdown("<div class='section-header'>RESEARCH COPILOT</div>", unsafe_allow_html=True)
        st.info("I am monitoring this workspace. Ask me to drill down into any metric or relationship.")
        
        with st.expander("Workspace State", expanded=True):
            st.write(workspace.get("state", {}))
            if st.button("Snapshot Workspace", use_container_width=True):
                st.toast("Institutional Snapshot Saved.")

# ── Supabase ──────────────────────────────────────────────────────────────────
@st.cache_resource
def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

supabase = get_supabase()

# Global Orchestrator Instance
@st.cache_resource
def get_orchestrator():
    return ResearchOrchestrator(supabase)

orchestrator = get_orchestrator()

def execute_with_retry(query, retries=3, delay=1.0):
    for i in range(retries):
        try:
            return query.execute()
        except Exception as e:
            if i == retries - 1:
                st.error(f"Database connection error: {e}")
                # Return a mock object with empty data to prevent downstream crashes
                class MockResponse: data = []
                return MockResponse()
            time.sleep(delay)

# ── Data Loaders (Service-Backed) ───────────────────────────────────────────
def load_financials(ticker: str) -> list[dict]:
    return analytics_service.get_financials(ticker)

def load_all_financials() -> list[dict]:
    # In production this would be a specialized admin query
    return analytics_service.get_financials("ALL") 

def load_news(ticker: str, limit: int = 60) -> list[dict]:
    return retrieval_service.get_news(ticker, limit)

def load_connections(ticker: str) -> list[dict]:
    return retrieval_service.get_connections(ticker)

def load_all_reports(ticker: Optional[str] = None) -> list[dict]:
    return vault_service.get_all_reports(ticker)

def load_latest_report(ticker: str) -> dict | None:
    return vault_service.get_latest_report(ticker)

def load_sector_snapshot(sector: str) -> pd.DataFrame:
    return analytics_service.get_sector_snapshot(sector)

def get_all_sectors() -> List[str]:
    return analytics_service.get_all_sectors()

# ── Formatting ────────────────────────────────────────────────────────────────
def get_currency_symbol(currency):
    if not currency: return ""
    # Strip ratio units like USD/shares to just USD
    clean_curr = currency.split("/")[0] if "/" in currency else currency
    
    if clean_curr == "USD": return "$"
    if clean_curr == "INR": return "₹"
    if clean_curr == "EUR": return "€"
    if clean_curr == "GBP": return "£"
    return f"{clean_curr} " if clean_curr else ""

def fmt_b(v, currency="INR"):
    import math
    if v is None or (isinstance(v, float) and math.isnan(v)): return "–"
    symbol = get_currency_symbol(currency)
    
    # Absolute value for comparison
    av = abs(v)
    
    if av >= 1e12: return f"{symbol}{v/1e12:.2f}T"
    
    # Indian-style formatting for INR
    if currency == "INR":
        if av >= 1e7: return f"{symbol}{v/1e7:.2f}Cr"
        if av >= 1e5: return f"{symbol}{v/1e5:.2f}L"
    else:
        # Western-style formatting for USD/others
        if av >= 1e9: return f"{symbol}{v/1e9:.2f}B"
        if av >= 1e6: return f"{symbol}{v/1e6:.2f}M"
        
    return f"{symbol}{v:,.0f}"

def delta_pct(old, new):
    if not old or not new or old == 0: return None
    return ((new - old) / abs(old)) * 100

def badge(status):
    classes = {"VERIFIED": "badge-verified", "FLAGGED": "badge-flagged",
               "REJECTED": "badge-rejected", "PENDING": "badge-pending"}
    cls = classes.get(status, "badge-pending")
    return f"<span class='{cls}'>{status}</span>"

# ── Gemini Call ───────────────────────────────────────────────────────────────
def call_gemini(prompt: str, max_tokens: int = 2048, temperature: float = 0.3) -> str:
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": temperature, "maxOutputTokens": max_tokens},
        "safetySettings": [
            {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
        ],
    }
    try:
        resp = requests.post(f"{GEMINI_ENDPOINT}?key={GEMINI_API_KEY}", json=payload, timeout=45)
        resp.raise_for_status()
        candidates = resp.json().get("candidates", [])
        if candidates:
            return candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "")
    except Exception as e:
        return f"[Gemini error: {e}]"
    return "[No response]"

def get_embedding(text: str) -> list[float]:
    url = f"https://generativelanguage.googleapis.com/v1beta/models/embedding-001:embedContent?key={GEMINI_API_KEY}"
    try:
        r = requests.post(url, json={"model": "models/embedding-001", "content": {"parts": [{"text": text}]}}, timeout=10)
        if r.status_code == 200:
            return r.json().get("embedding", {}).get("values", [])
    except: pass
    from typing import cast, Any
    h: bytes = hashlib.sha256(text.encode()).digest()
    h_expanded: bytes = h * 24
    chunk: bytes = cast(Any, h_expanded)[:768]
    return [float(b - 128) / 128.0 for b in chunk]

# ── Watchlist (session state) ─────────────────────────────────────────────────
if "watchlist" not in st.session_state:
    st.session_state.watchlist = set()
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

# ── Checkpoint ──
# st.toast("Initializing UI Components...")
with st.sidebar:
    st.markdown("""
    <div class='sidebar-brand'>
        <div class='sidebar-logo'>🏛️</div>
        <div style='font-size:13px; font-weight:800; color:#f0f6fc; letter-spacing:0.05em;'>COMPANY INSIGHTS</div>
        <div style='font-size:9px; color:#8b949e; letter-spacing:0.3em; margin-top:2px;'>INSTITUTIONAL INTELLIGENCE</div>
    </div>
    """, unsafe_allow_html=True)

    # Hub Navigation
    st.markdown("""<div style='font-size:9px; font-weight:800; color:#484f58; text-transform:uppercase; 
    letter-spacing:0.2em; margin:8px 0 12px 0; padding-top:8px; 
    border-top:1px solid rgba(255,255,255,0.04);'>INTELLIGENCE HUBS</div>""", unsafe_allow_html=True)

    hub = st.radio("Hub", [
        "📊  Analysis Suite",
        "💬  Strategic Research Lab",
        "📚  Institutional Vault",
        "⚙️  Data Ingestion Suite",
    ], index=0, label_visibility="collapsed")

    st.markdown("<div style='margin-top:20px;'></div>", unsafe_allow_html=True)
    st.markdown("""<div style='font-size:9px; font-weight:800; color:#484f58; text-transform:uppercase; 
    letter-spacing:0.2em; margin-bottom:12px;'>RESEARCH SCOPE</div>""", unsafe_allow_html=True)

    # ── Company Search ──
    target_options = {f"{m['name']} ({t})": t for t, m in TARGET_COMPANIES.items()}
    sorted_labels = sorted(target_options.keys())

    ticker = st.session_state.get("selected_ticker")
    default_idx = None
    if ticker:
        current_label = next((l for l, t in target_options.items() if t == ticker), None)
        if current_label and current_label in sorted_labels:
            default_idx = sorted_labels.index(current_label)

    sel_label = st.selectbox(
        "🔍 Company", options=sorted_labels, index=default_idx,
        placeholder="Search company or ticker...",
        label_visibility="collapsed", key="search_autocomplete"
    )
    if sel_label:
        ticker = target_options[sel_label]
        st.session_state.selected_ticker = ticker
        
        # Auto-trigger Deep Network Discovery
        if "discovered_tickers" not in st.session_state:
            st.session_state.discovered_tickers = set()
            
        if ticker and ticker not in st.session_state.discovered_tickers:
            st.session_state.discovered_tickers.add(ticker)
            worker.submit(retrieval_service.discover_connections, ticker, job_name=f"Network Discovery: {ticker}")

    # ── Add New Ticker ──
    with st.expander("✨ Discover & Add Ticker"):
        new_ticker = st.text_input("Ticker Symbol", placeholder="e.g. NVDA", key="new_ticker_input").upper().strip()
        if new_ticker:
            if new_ticker in TARGET_COMPANIES:
                st.success(f"✓ {new_ticker} is already tracked.")
            else:
                if st.button(f"🔍 Discover {new_ticker}", key="discover_btn"):
                    with st.spinner("Searching SEC registry..."):
                        try:
                            from pipelines.ingestion.collector import ExtractorEngine
                            engine = ExtractorEngine()
                            result_disc = asyncio.run(engine.discover_ticker(new_ticker))
                            if result_disc:
                                st.success(f"Found: {result_disc.get('name', new_ticker)}")
                                TARGET_COMPANIES[new_ticker] = result_disc
                                st.rerun()
                            else:
                                st.error("Not found in SEC registry.")
                        except Exception as e:
                            st.error(f"Discovery error: {e}")



    # ── Watchlist ──
    if ticker:
        if ticker in st.session_state.watchlist:
            if st.button("★ Remove from Watchlist", use_container_width=True):
                st.session_state.watchlist.discard(ticker)
                st.rerun()
        else:
            if st.button("☆ Add to Watchlist", use_container_width=True):
                st.session_state.watchlist.add(ticker)
                st.toast(f"{ticker} added to watchlist.")

        if st.button("Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    # ── Status Bar ──
    st.markdown("""
    <div style='font-size:10px; color:#8b949e; text-align:center; margin-top:20px; 
    padding-top:12px; border-top:1px solid rgba(255,255,255,0.04);'>
        SEC EDGAR · Gemini 2.5 Flash · Supabase<br>
        <span style='color:#3fb950;'>●</span> Vault Connected
    </div>
    """, unsafe_allow_html=True)




# Save ticker to session state for persistence
st.session_state.last_ticker = ticker

# ── Load data for selected ticker (Conditional) ────────────────────────────────
company_names = {t: m["name"] for t, m in TARGET_COMPANIES.items()}
financials, connections, news, all_reports = [], [], [], []
latest_report = None
df_fy, df_q = pd.DataFrame(), pd.DataFrame()
latest_fin: Dict[str, Any] = {}
prior_fin: Dict[str, Any] = {}
curr = "USD"

is_analysis_hub  = hub == "📊  Analysis Suite"
is_research_hub  = hub == "💬  Strategic Research Lab"
is_vault_hub     = hub == "📚  Institutional Vault"
is_ingestion_hub = hub == "⚙️  Data Ingestion Suite"
is_workspace_hub = is_analysis_hub # Map legacy flag to new hub

# ── Sub-Navigation State Initialization ─────────────────────────────────────
# Pre-seed from session state so sub-mode is never "" during a hub-switch rerun
_WS_MODES = ["📊 Financial Pulse", "📅 Quarterly", "🌡 Sector Intelligence",
             "🕸 Ecosystem", "📰 Market Feed", "🧠 Autonomous Strategy"]
_LAB_MODES = ["💬 Research Chat", "📝 Report Builder"]

ws_mode  = st.session_state.get("ws_mode",  _WS_MODES[0])  if is_workspace_hub  else ""
lab_mode = st.session_state.get("lab_mode", _LAB_MODES[0]) if is_research_hub   else ""

# Validate — if stored value is stale/invalid, fall back to first option
if is_workspace_hub and ws_mode not in _WS_MODES:
    ws_mode = _WS_MODES[0]
if is_research_hub and lab_mode not in _LAB_MODES:
    lab_mode = _LAB_MODES[0]

if is_workspace_hub:
    st.markdown("""
    <style>
    div[data-testid='stRadio'] > label { display:none; }
    div[data-testid='stRadio'] > div { gap:4px; }
    div[data-testid='stRadio'] > div > label {
        background:rgba(22,27,34,0.6); border:1px solid rgba(255,255,255,0.06);
        border-radius:8px; padding:8px 16px; font-size:12px; font-weight:600;
        color:#8b949e; cursor:pointer; transition:all 0.2s;
    }
    div[data-testid='stRadio'] > div > label:has(input:checked) {
        background:rgba(31,111,235,0.2); border-color:#1f6feb; color:#58a6ff;
    }
    </style>
    """, unsafe_allow_html=True)
    ws_mode = st.radio("Workspace Mode", _WS_MODES,
        horizontal=True, label_visibility="collapsed", key="ws_mode")
    st.markdown("<hr style='border-color:rgba(255,255,255,0.04); margin:12px 0 20px;'>", unsafe_allow_html=True)

elif is_research_hub:
    lab_mode = st.radio("Research Mode", _LAB_MODES,
        horizontal=True, label_visibility="collapsed", key="lab_mode")
    st.markdown("<hr style='border-color:rgba(255,255,255,0.04); margin:12px 0 20px;'>", unsafe_allow_html=True)


try:
    if is_vault_hub:
        all_reports = load_all_reports(ticker if ticker else None)
    elif ticker and not is_research_hub and not is_ingestion_hub:
        financials   = load_financials(ticker)
        connections  = load_connections(ticker)
        news         = load_news(ticker)
        all_reports  = load_all_reports(ticker)
        latest_report = all_reports[0] if all_reports else None

        df_all = pd.DataFrame(financials) if financials else pd.DataFrame()
        df_fy  = df_all[df_all["fiscal_period"] == "FY"].sort_values("end_date") if not df_all.empty else pd.DataFrame()
        df_q   = df_all[df_all["fiscal_period"] != "FY"].sort_values("end_date") if not df_all.empty else pd.DataFrame()

        latest_fin = df_fy.iloc[-1].to_dict() if not df_fy.empty else {}
        prior_fin  = df_fy.iloc[-2].to_dict() if len(df_fy) > 1 else {}
        curr = latest_fin.get("currency", "USD")
except Exception as _data_err:
    st.warning(f"⚠️ Data loading error: {_data_err}")

# ── Global Page Header & Access Control ──────────────────────────────────────
# Only block pages that strictly require a company context
ticker_dependent_modes = ["📊 Financial Pulse", "📅 Quarterly", "🕸 Ecosystem", "📰 Market Feed", "🧠 Autonomous Strategy"]
hub_requires_ticker = is_workspace_hub and (ws_mode in ticker_dependent_modes)

# Guard: only stop if we are FULLY settled on this hub (sub-nav widget rendered)
if hub_requires_ticker and not ticker and st.session_state.get("ws_mode") in ticker_dependent_modes:
    st.markdown("""
    <div style='text-align:center; padding:100px 20px;'>
        <div style='font-size:64px; margin-bottom:20px;'>🏛️</div>
        <div style='font-size:24px; font-weight:800; color:#c9d1d9;'>Select a company to begin analysis</div>
        <div style='color:#8b949e; margin-top:8px;'>Use the sidebar to search 8,000+ companies in the SEC database,<br>or use the <b>Strategic Research Lab</b> for open-ended AI research.</div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

if ticker and is_workspace_hub:
    ticker_meta = get_company_meta(ticker)
    status_badge = badge(latest_report.get("verification_status", "PENDING")) if latest_report else badge("PENDING")
    st.markdown(f"""
    <div style='display:flex; align-items:flex-end; gap:12px; margin-bottom:4px;'>
        <div style='font-size:36px; font-weight:900; color:#f0f6fc; line-height:1;'>{ticker}</div>
        <div style='font-size:18px; font-weight:600; color:#c9d1d9; border-left:1px solid rgba(255,255,255,0.1); padding-left:12px; margin-bottom:2px;'>{ticker_meta.get('name','')}</div>
    </div>
    <div style='font-size:11px; color:#8b949e; letter-spacing:0.05em; display:flex; align-items:center; gap:12px;'>
        <span>Sector: <b style='color:#58a6ff;'>{ticker_meta.get('sector','N/A')}</b></span>
        <span>Period: <b style='color:#f0f6fc;'>FY{latest_fin.get('fiscal_year','–')}</b></span>
        <span>Audit: {status_badge}</span>
    </div>
    <hr style='border-color:rgba(255,255,255,0.05); margin:20px 0 24px;'>
    """, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
# HUB ROUTER — 4 Intelligence Hubs
# ══════════════════════════════════════════════════════════════



# ── Hub 1: Intelligence Workspace ────────────────────────────────
# ── Mode: Financial Pulse
if is_workspace_hub and ws_mode == "📊 Financial Pulse":
    # Task 2-A: UI-triggered pipeline button
    col_hdr, col_btn = st.columns([3, 1])
    with col_btn:
        if st.button("Refresh Latest Financials", width='stretch', key="fetch_financials_btn"):
            with st.spinner(f"Updating data for {ticker}..."):
                try:
                    # Updated to point to the consolidated ingest.py
                    res = subprocess.run(
                        [sys.executable, os.path.join(PROJECT_ROOT, "pipelines", "ingestion", "collector.py"), "--ticker", ticker, "--sec-only"],
                        capture_output=True, text=True, timeout=150,
                        cwd=PROJECT_ROOT
                    )
                    
                    if res.returncode != 0:
                        st.error(f"Ingestion Failed: {res.stderr[:300]}")
                    else:
                        st.toast("Financial metrics updated!")
                        st.success("Analysis engine synchronized with latest SEC filings.")
                        st.cache_data.clear()
                        time.sleep(1.5)
                        st.rerun()
                except Exception as e:
                    st.error(f"Execution error: {e}")

    if not latest_fin:
        st.warning(f"No financial data found for {ticker}. Use the 'Fetch Latest Financials' button above to initialize.")
        st.stop()

    rev = latest_fin.get("revenue")
    ni  = latest_fin.get("net_income")
    op  = latest_fin.get("operating_income")
    ca  = latest_fin.get("cash_on_hand")
    eps = latest_fin.get("eps_diluted")
    eq  = latest_fin.get("total_equity")
    ta  = latest_fin.get("total_assets")
    tl  = latest_fin.get("total_liabilities")

    prev_rev = prior_fin.get("revenue") if prior_fin else None
    prev_ni  = prior_fin.get("net_income") if prior_fin else None

    def kpi(col, label, value, curr="INR", suffix=""):
        col.markdown(f"""
        <div class='metric-card' style='min-height:90px;'>
            <div class='metric-label'>{label}</div>
            <div class='metric-value' style='font-size:26px;'>{fmt_b(value, curr)}{suffix}</div>
        </div>""", unsafe_allow_html=True)

    c1, c2, c3, c4 = st.columns(4)
    # Smart Fallback: If currency is missing in DB, check if it's an SEC company
    curr = latest_fin.get("currency")
    if not curr:
        ticker_meta = get_company_meta(ticker)
        curr = "USD" if ticker_meta.get("cik") else "INR"
    
    kpi(c1, "Total Revenue",      rev, curr)
    kpi(c2, "Net Income",         ni, curr)
    kpi(c3, "Operating Income",   op, curr)
    kpi(c4, "Cash & Equivalents", ca, curr)

    c5, c6, c7, c8 = st.columns(4)
    nm  = (ni / rev * 100) if (ni and rev) else None
    om  = (op / rev * 100) if (op and rev) else None
    roe = (ni / eq * 100)  if (ni and eq)  else None
    de  = (tl / eq) if (tl and eq) else None

    symbol = get_currency_symbol(curr)
    for col, label, val, fmt in [
        (c5, "EPS (Diluted)",       eps, lambda v: f"{symbol}{v:.2f}"),
        (c6, "Net Margin",          nm,  lambda v: f"{v:.1f}%"),
        (c7, "Operating Margin",    om,  lambda v: f"{v:.1f}%"),
        (c8, "Debt / Equity",       de,  lambda v: f"{v:.2f}x"),
    ]:
        display = fmt(val) if val is not None else "–"
        col.markdown(f"""
        <div class='metric-card' style='min-height:90px;'>
            <div class='metric-label'>{label}</div>
            <div class='metric-value' style='font-size:26px;'>{display}</div>
        </div>""", unsafe_allow_html=True)

    # Revenue + Net Income trend (Annual)
    st.markdown("<div class='section-header'>ANNUAL REVENUE & NET INCOME TREND</div>", unsafe_allow_html=True)
    if not df_fy.empty:
        df_fy["label"] = "FY" + df_fy["fiscal_year"].astype(str)
        fig = go.Figure()
        fig.add_trace(go.Bar(x=df_fy["label"], y=df_fy.get("revenue")/1e9, name="Revenue",
                             marker_color="#1f6feb", opacity=.85,
                             hovertemplate="<b>%{x}</b><br>Revenue: " + symbol + "%{y:.1f}B<extra></extra>"))
        fig.add_trace(go.Bar(x=df_fy["label"], y=df_fy["net_income"]/1e9, name="Net Income",
                             marker_color="#3fb950", opacity=.85,
                             hovertemplate="<b>%{x}</b><br>Net Income: " + symbol + "%{y:.1f}B<extra></extra>"))
        if df_fy["operating_income"].notna().any():
            fig.add_trace(go.Scatter(x=df_fy["label"], y=df_fy["operating_income"]/1e9,
                                     name="Operating Income", mode="lines+markers",
                                     line={"color": "#e3b341", "width": 2, "dash": "dot"}))
        fig.update_layout(plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                          font={"color": "#c9d1d9"}, barmode="group", height=360,
                          legend={"bgcolor": "#161b22", "bordercolor": "#30363d",
                                      "borderwidth": 1, "x": 0, "y": 1.1, "orientation": "h"},
                          xaxis={"gridcolor": "#21262d"},
                          yaxis={"gridcolor": "#21262d", "title": f"{curr} Billions"},
                          margin={"l": 0, "r": 0, "t": 10, "b": 0}, hovermode="x unified")
        st.plotly_chart(fig, width='stretch')

    # Balance sheet
    st.markdown("<div class='section-header'>BALANCE SHEET COMPOSITION</div>", unsafe_allow_html=True)
    bs_cols = st.columns(3)
    for col, label, val in [
        (bs_cols[0], "Total Assets",      ta),
        (bs_cols[1], "Total Equity",      eq),
        (bs_cols[2], "Total Liabilities", tl),
    ]:
        col.markdown(f"""
        <div class='metric-card'>
            <div class='metric-label'>{label}</div>
            <div class='metric-value' style='font-size:20px;'>{fmt_b(val, curr)}</div>
        </div>""", unsafe_allow_html=True)

    # Unified Document Access (Clubbed)
    filing_url = latest_fin.get("sec_filing_url")
    archived_url = latest_fin.get("archived_url")
    
    if archived_url or filing_url:
        primary_link = archived_url or filing_url
        label = "VIEW FULL DOCUMENT (VAULTED)" if archived_url else "VIEW ORIGINAL FILING (SEC)"
        bg_color = "#1a4731" if archived_url else "#1f3a5f"
        border_color = "#3fb950" if archived_url else "#1f6feb"
        text_color = "#aff5b4" if archived_url else "#79c0ff"

        st.markdown(f"""
        <div style='margin-top:16px; display:flex; align-items:center; gap:12px;'>
            <a href='{primary_link}' target='_blank' style='background:{bg_color}; border:1px solid {border_color};
               color:{text_color}; padding:10px 22px; border-radius:8px; text-decoration:none;
               font-size:14px; font-weight:700;'>
                {label} ↗
            </a>
            {f"<a href='{filing_url}' target='_blank' style='color:#8b949e; font-size:12px; text-decoration:none;'>Official Source ↗</a>" if archived_url and filing_url else ""}
        </div>""", unsafe_allow_html=True)



# ── Mode: Quarterly
elif is_workspace_hub and ws_mode == "📅 Quarterly":
    st.markdown(f"<div class='section-header'>QUARTERLY RESULTS — {ticker}</div>", unsafe_allow_html=True)

    if df_q.empty:
        st.warning("⚠️ No quarterly data found. Ensure ingestor ran with --cutoff-years 3.")
    else:
        df_q["label"] = df_q["fiscal_year"].astype(str) + " " + df_q["fiscal_period"]
        df_q = df_q.sort_values("end_date")

        # Revenue waterfall by quarter
        fig_q = go.Figure()
        colors = {"Q1": "#58a6ff", "Q2": "#3fb950", "Q3": "#e3b341"}
        for period in ["Q1", "Q2", "Q3"]:
            sub = df_q[df_q["fiscal_period"] == period]
            if not sub.empty:
                fig_q.add_trace(go.Bar(
                    x=sub["label"], y=sub.get("revenue")/1e9,
                    name=f"Revenue {period}", marker_color=colors.get(period, "#c9d1d9"),
                    hovertemplate="<b>%{x}</b><br>Revenue: " + get_currency_symbol(curr) + "%{y:.2f}B<extra></extra>",
                ))
        fig_q.update_layout(plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                            font={"color": "#c9d1d9"}, barmode="group", height=320,
                            legend={"bgcolor": "#161b22", "bordercolor": "#30363d",
                                        "borderwidth": 1, "x": 0, "y": 1.1, "orientation": "h"},
                            xaxis={"gridcolor": "#21262d"},
                            yaxis={"gridcolor": "#21262d", "title": f"Revenue ({curr} Billions)"},
                            margin={"l": 0, "r": 0, "t": 10, "b": 0})
        st.plotly_chart(fig_q, width='stretch')

        # Net Income by quarter
        st.markdown("<div class='section-header'>QUARTERLY NET INCOME</div>", unsafe_allow_html=True)
        fig_ni = go.Figure()
        fig_ni.add_trace(go.Scatter(x=df_q["label"], y=df_q["net_income"]/1e9,
                                    mode="lines+markers+text",
                                    line={"color": "#3fb950", "width": 2},
                                    text=[f"{get_currency_symbol(curr)}{v/1e9:.1f}B" if v else "" for v in df_q["net_income"]],
                                    textposition="top center",
                                    hovertemplate="<b>%{x}</b><br>Net Income: " + get_currency_symbol(curr) + "%{y:.2f}B<extra></extra>",
                                    fill="tozeroy", fillcolor="rgba(63,185,80,0.08)"))
        fig_ni.update_layout(plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                              font={"color": "#c9d1d9"}, height=280,
                              xaxis={"gridcolor": "#21262d"},
                              yaxis={"gridcolor": "#21262d", "title": f"Net Income ({curr} Billions)"},
                              margin={"l": 0, "r": 0, "t": 10, "b": 0})
        st.plotly_chart(fig_ni, width='stretch')

        # Quarterly data table
        st.markdown("<div class='section-header'>QUARTERLY RAW DATA</div>", unsafe_allow_html=True)
        display_cols = ["label", "revenue", "net_income", "operating_income", "eps_diluted", "end_date", "sec_filing_url"]
        df_show = df_q[[c for c in display_cols if c in df_q.columns]].copy()
        
        # Format metrics
        for col in ["revenue", "net_income", "operating_income"]:
            if col in df_show.columns:
                df_show[col] = df_show[col].apply(lambda v: fmt_b(v, curr) if v else "–")
        if "eps_diluted" in df_show.columns:
            df_show["eps_diluted"] = df_show["eps_diluted"].apply(lambda v: f"{get_currency_symbol(curr)}{v:.2f}" if v else "–")

        df_show.columns = [c.replace("_", " ").title() for c in df_show.columns]
        
        st.dataframe(
            df_show.set_index("Label"),
            width='stretch',
            column_config={
                "Sec Filing Url": st.column_config.LinkColumn(
                    "SEC Filing",
                    help="Official SEC EDGAR Filing",
                    validate="^https://",
                    display_text="View 10-Q ↗"
                )
            }
        )


# ── Mode: Sector Intelligence
elif is_workspace_hub and ws_mode == "🌡 Sector Intelligence":
    st.markdown("<div class='section-header'>CROSS-COMPANY SECTOR SNAPSHOT</div>", unsafe_allow_html=True)

    sec_col1, sec_col2 = st.columns(2)
    with sec_col1:
        metric_choice = st.selectbox("Metric to visualise",
            ["revenue", "net_income", "op_income", "cash", "equity", "assets"])
    with sec_col2:
        sectors = get_all_sectors()
        selected_sector = st.selectbox("Filter by sector", ["-- Select Sector --", "All"] + sectors)

    if selected_sector == "-- Select Sector --":
        st.info("Select a sector to view the latest financial snapshot across companies.")
        st.stop()

    with st.spinner(f"Loading {selected_sector} snapshot..."):
        df_sector = load_sector_snapshot(selected_sector)

    if df_sector.empty:
        st.warning(f"No financial data available for {selected_sector} yet.")
    else:
        label_map = {
            "revenue": "Annual Revenue",  "net_income": "Net Income",
            "op_income": "Operating Income", "cash": "Cash & Equivalents",
            "equity": "Total Equity",    "assets": "Total Assets",
        }

        df_heat = df_sector.copy().dropna(subset=[metric_choice, "sector"])

        if df_heat.empty:
            st.warning(f"No data points for {metric_choice} in {selected_sector}.")
        else:
            df_heat["value_fmt"] = df_heat[metric_choice].apply(fmt_b)
            df_heat["value_b"]   = df_heat[metric_choice] / 1e9

            fig_heat = px.treemap(
                df_heat, path=["sector", "ticker"], values="value_b",
                color="value_b", color_continuous_scale=[[0,"#1c2128"], [0.5,"#1f6feb"], [1,"#3fb950"]],
                custom_data=["company", "value_fmt"],
                title=f"Sector Treemap — {label_map[metric_choice]}",
            )
            fig_heat.update_traces(
                hovertemplate="<b>%{customdata[0]}</b><br>%{customdata[1]}<extra></extra>",
                texttemplate="<b>%{label}</b><br>%{customdata[1]}", textfont_size=13,
            )
            fig_heat.update_layout(
                paper_bgcolor="#0d1117", font={"color": "#c9d1d9"},
                margin={"l": 0, "r": 0, "t": 40, "b": 0}, height=420, coloraxis_showscale=False,
            )
            st.plotly_chart(fig_heat, width='stretch')

            st.markdown(f"<div class='section-header'>COMPARISON TABLE — {selected_sector}</div>", unsafe_allow_html=True)
            table_data = []
            for _, row in df_heat.iterrows():
                nm = (row["net_income"]/row["revenue"]*100) if (row["net_income"] and row.get("revenue")) else None
                roe = (row["net_income"]/row["equity"]*100) if (row["net_income"] and row["equity"]) else None
                table_data.append({
                    "Ticker":    row["ticker"],
                    "Company":   row["company"],
                    "Sector":    row["sector"],
                    "Revenue":   fmt_b(row.get("revenue")),
                    "Net Income": fmt_b(row["net_income"]),
                    "Net Margin": f"{nm:.1f}%" if nm else "–",
                    "ROE":        f"{roe:.1f}%" if roe else "–",
                    "Cash":       fmt_b(row.get("cash")),
                    "EPS":        f"${row['eps']:.2f}" if row.get("eps") else "–",
                })
            st.dataframe(pd.DataFrame(table_data).set_index("Ticker"), use_container_width=True)

        # Grouped bar — Revenue vs Net Income
        sector_label = selected_sector if selected_sector != "All" else "ALL SECTORS"
        st.markdown(f"<div class='section-header'>REVENUE vs NET INCOME ({sector_label.upper()})</div>", unsafe_allow_html=True)
        fig_bar = go.Figure()
        fig_bar.add_trace(go.Bar(x=df_heat["ticker"], y=df_heat.get("revenue")/1e9,
                                  name="Revenue", marker_color="#1f6feb"))
        fig_bar.add_trace(go.Bar(x=df_heat["ticker"], y=df_heat["net_income"]/1e9,
                                  name="Net Income", marker_color="#3fb950"))
        fig_bar.update_layout(plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                               font={"color": "#c9d1d9"}, barmode="group", height=320,
                               legend={"bgcolor": "#161b22", "bordercolor": "#30363d", "borderwidth": 1, "x": 0, "y": 1.1, "orientation": "h"},
                               xaxis={"gridcolor": "#21262d"},
                               yaxis={"gridcolor": "#21262d", "title": "USD Billions"},
                               margin={"l": 0, "r": 0, "t": 10, "b": 0})
        st.plotly_chart(fig_bar, width='stretch')


# ── Mode: Ecosystem
elif is_workspace_hub and ws_mode == "🕸 Ecosystem":
    st.markdown(f"<div class='section-header'>CORPORATE CONNECTIONS — {ticker}</div>", unsafe_allow_html=True)

    # Task 2-C: UI-triggered ecosystem pipeline
    eco_hdr, eco_btn_col = st.columns([3, 1])
    with eco_btn_col:
        if st.button("Deep Scan Network", width='stretch', key="update_ecosystem_btn"):
            worker.submit(retrieval_service.discover_connections, ticker, job_name=f"Manual Scan: {ticker}")
            st.toast("Deep Scan triggered in background.")
            st.rerun()

    # Load connections from service
    connections = load_connections(ticker)

    if not connections:
        st.info("No connections found in the knowledge graph vault.")
    else:
        TYPE_COLORS = {
            "ACQUISITION": "#6e40c9", "INVESTMENT": "#1a7f37",
            "PARTNERSHIP": "#9e6a03", "SUPPLIER": "#1f6feb",
            "CUSTOMER": "#0d7377",   "SUBSIDIARY": "#953800",
            "JOINT_VENTURE": "#8b0000", "LICENSING": "#1a4731",
            "COMPETITOR": "#6e1010",  "STRATEGIC_ALLIANCE": "#0c3b6e",
        }

        # Summary counts
        type_counts = {}
        for c in connections:
            t = c.get("relationship_type", "OTHER")
            type_counts[t] = type_counts.get(t, 0) + 1

        cols = st.columns(len(type_counts))
        for i, (rtype, count) in enumerate(sorted(type_counts.items())):
            color = TYPE_COLORS.get(rtype, "#30363d")
            cols[i].markdown(f"""
            <div class='metric-card' style='border-color:{color}40; text-align:center; padding:12px;'>
                <div class='metric-label'>{rtype}</div>
                <div class='metric-value' style='font-size:28px;'>{count}</div>
            </div>""", unsafe_allow_html=True)

        # Render dynamic graph
        st.markdown("<div style='margin-top:24px;'></div>", unsafe_allow_html=True)
        fig = render_ecosystem_graph(ticker, connections)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        # Network-style list grouped by type
        from itertools import groupby
        sorted_conns = sorted(connections, key=lambda x: x.get("relationship_type",""))
        for rtype, group in groupby(sorted_conns, key=lambda x: x.get("relationship_type","")):
            color = TYPE_COLORS.get(rtype, "#30363d")
            st.markdown(f"<div style='color:{color}; font-size:12px; font-weight:700; margin:12px 0 6px;'>▶ {rtype}</div>", unsafe_allow_html=True)
            for c in group:
                target = c.get("target_company", "Unnamed Entity")
                detail = c.get("relationship_detail", "")
                val    = c.get("deal_value_usd")
                status = c.get("status", "ACTIVE")
                sec_url = c.get("sec_filing_url", "")

                val_str = f" · Deal: {fmt_b(val)}" if val else ""
                link    = f' · <a href="{sec_url}" target="_blank" style="color:#58a6ff;">SEC ↗</a>' if sec_url else ""

                st.markdown(f"""
                <div class='report-card' style='border-left:3px solid {color};'>
                    <div style='color:#f0f6fc; font-weight:700;'>{target}</div>
                    <div style='color:#8b949e; font-size:13px; margin-top:4px;'>{detail}{val_str}</div>
                    <div style='font-size:11px; color:#58a6ff; margin-top:4px;'>Status: {status}{link}</div>
                </div>""", unsafe_allow_html=True)


# ── Mode: Market Feed
elif is_workspace_hub and ws_mode == "📰 Market Feed":
    st.markdown(f"<div class='section-header'>MARKET INTELLIGENCE — {ticker}</div>", unsafe_allow_html=True)

    # Task 2-B: Refresh News pipeline button
    feed_col, btn_col = st.columns([3, 1])
    with btn_col:
        if st.button("Refresh News", width='stretch', key="refresh_news_btn"):
            try:
                subprocess.run(
                    ["python", "ingest.py", "--ticker", ticker, "--news-only"],
                    check=False, timeout=120,
                )
            except Exception as e:
                st.error(f"News refresh error: {e}")
            st.cache_data.clear()
            st.rerun()

    # Search bar
    search_col, _ = st.columns([2, 1])
    with search_col:
        search_q = st.text_input("Search headlines...", placeholder="e.g. AI investment strategy, supply chain risk")

    if search_q:
        with st.spinner("Searching..."):
            emb = get_embedding(search_q)
            try:
                results = supabase.rpc("match_news", {
                    "query_embedding": emb, "match_threshold": 0.5,
                    "match_count": 10, "p_ticker": ticker
                }).execute().data or []
                news_display = results
                st.markdown(f"<div style='color:#8b949e; font-size:12px;'>Found {len(results)} semantic matches</div>", unsafe_allow_html=True)
            except:
                news_display = [n for n in news if search_q.lower() in (n.get("headline","") + n.get("summary","")).lower()]
                st.markdown(f"<div style='color:#e3b341; font-size:12px;'>⚠ Vector search unavailable — showing keyword matches ({len(news_display)})</div>", unsafe_allow_html=True)
    else:
        news_display = news

    if not news_display:
        st.info("No news found. Run `python ingest.py` to fetch articles.")
    else:
        # Sentiment filter
        sentiment_filter = st.radio("Filter by sentiment:", ["All", "Positive", "Negative", "Neutral"],
                                    horizontal=True)
        if sentiment_filter != "All":
            news_display = [n for n in news_display if
                            (n.get("sentiment") or "").lower() == sentiment_filter.lower()]

        # Explicitly cast to Any for linter silence
        from typing import cast, Any
        final_news: Any = list(news_display)
        for item in cast(Any, final_news)[:40]:  # type: ignore
            if not isinstance(item, dict):
                st.markdown(f"<div class='news-item news-neu'>{item}</div>", unsafe_allow_html=True)
                continue
                
            sent = (item.get("sentiment") or "neutral").lower() # type: ignore
            css_class = {"positive": "news-pos", "negative": "news-neg"}.get(sent, "news-neu")
            pub_dt = item.get("published_at", "")[:10]
            url = item.get("url", "")
            headline = item.get("headline", "")
            archived_url = item.get("archived_url", "")
            source = item.get("source", "")
            summary = item.get("summary", "")[:200]
            
            # Unified News Link (Clubbed)
            primary_news_url = archived_url or url
            
            st.markdown(f"""
            <div class='news-item {css_class}'>
                <div style='display:flex; justify-content:space-between; font-size:11px; margin-bottom:6px;'>
                    <span style='font-weight:800; color:#8b949e;'>{source.upper() if source else "MARKET"}</span>
                    <span>{pub_dt}</span>
                </div>
                <a href='{primary_news_url}' target='_blank' style='text-decoration:none;'>
                    <div style='color:#f0f6fc; font-weight:700; font-size:15px; margin-bottom:8px; line-height:1.4;'>{headline}</div>
                </a>
                <div style='color:#8b949e; font-size:13px; line-height:1.5; margin-bottom:12px;'>{summary}...</div>
                <div style='display:flex; gap:12px;'>
                    <a href='{primary_news_url}' target='_blank' style='color:#58a6ff; font-size:11px; font-weight:700; text-decoration:none;'>READ FULL ARTICLE ↗</a>
                    {f"<a href='{url}' target='_blank' style='color:#484f58; font-size:11px; text-decoration:none;'>Original Source</a>" if archived_url and url else ""}
                </div>
            </div>""", unsafe_allow_html=True)

# ── Mode: Autonomous Strategy (Tier 4 Core 3)
elif is_workspace_hub and ws_mode == "🧠 Autonomous Strategy":
    from domains.intelligence.strategy import strategic_state, narrative_engine, thesis_engine, causality_engine
    
    st.markdown("<div class='section-header'>AUTONOMOUS STRATEGIC REASONING</div>", unsafe_allow_html=True)
    
    col_hdr, col_btn = st.columns([3, 1])
    with col_hdr:
        st.markdown("<div style='color:#8b949e; font-size:13px; margin-bottom:16px;'>Continuously evolving strategic mapping generated automatically via background monitoring loops.</div>", unsafe_allow_html=True)
    with col_btn:
        if st.button("⚡ Force Strategic Re-Evaluation", use_container_width=True):
            with st.spinner(f"Running cognitive reasoning loop for {ticker}..."):
                import asyncio
                # Build context from latest real data
                ctx = f"Reviewing latest operational data for {ticker}. "
                if latest_fin:
                    ctx += f"Recent financials: Rev {latest_fin.get('revenue', 'N/A')}, Net Income {latest_fin.get('net_income', 'N/A')}. "
                if latest_report:
                    ctx += f"Latest SEC Insights: {str(latest_report.get('executive_summary', 'No summary'))[:400]}"
                
                async def _run_manual_cycle():
                    await narrative_engine.process_event(ticker, ctx)
                    await strategic_state.update_state(ticker, ctx)
                    await thesis_engine.process_event(ticker, ctx)
                    cur_narr = narrative_engine.narratives.get(ticker, {}).get("core_narrative", "")
                    await causality_engine.map_event(ticker, ctx, cur_narr)
                
                asyncio.run(_run_manual_cycle())
                st.toast("Strategic reasoning updated based on latest data.")
                time.sleep(1)
                st.rerun()

    # 1. Strategic State & Narrative
    st.markdown("### 1. Narrative & Strategic State")
    state_data = strategic_state.states.get(ticker, {"current_state": "Stable Execution"})
    narrative_data = narrative_engine.narratives.get(ticker, {"core_narrative": "No narrative tracked."})
    
    col1, col2 = st.columns([1, 2])
    with col1:
        st.markdown(f"""
        <div style='background:rgba(22,27,34,0.4); border:1px solid rgba(88,166,255,0.3); border-left:4px solid #58a6ff; border-radius:8px; padding:16px; height:100%;'>
            <div style='font-size:11px; color:#8b949e; text-transform:uppercase; font-weight:800; letter-spacing:0.1em; margin-bottom:8px;'>Current State</div>
            <div style='color:#f0f6fc; font-size:24px; font-weight:700;'>{state_data['current_state']}</div>
        </div>
        """, unsafe_allow_html=True)
    with col2:
        st.markdown(f"""
        <div style='background:rgba(22,27,34,0.4); border:1px solid rgba(255,255,255,0.05); border-radius:8px; padding:16px; height:100%;'>
            <div style='font-size:11px; color:#8b949e; text-transform:uppercase; font-weight:800; letter-spacing:0.1em; margin-bottom:8px;'>Core Strategic Narrative</div>
            <div style='color:#c9d1d9; font-size:14px; line-height:1.6;'>{narrative_data['core_narrative']}</div>
        </div>
        """, unsafe_allow_html=True)
        
    st.markdown("<hr style='border-color:rgba(255,255,255,0.05); margin:24px 0;'>", unsafe_allow_html=True)
    
    # 2. Bull vs Bear Thesis
    st.markdown("### 2. Autonomous Investment Thesis")
    t_data = thesis_engine.theses.get(ticker, {
        "bull_thesis": "No bull thesis tracked.", "bear_thesis": "No bear thesis tracked.",
        "bull_confidence": 50, "bear_confidence": 50
    })
    
    t_col1, t_col2 = st.columns(2)
    with t_col1:
        st.markdown(f"""
        <div style='background:rgba(63,185,80,0.05); border:1px solid rgba(63,185,80,0.2); border-top:3px solid #3fb950; border-radius:8px; padding:16px;'>
            <div style='display:flex; justify-content:space-between; align-items:center; margin-bottom:12px;'>
                <span style='font-size:14px; color:#3fb950; font-weight:800;'>BULL THESIS</span>
                <span style='font-size:12px; background:rgba(63,185,80,0.2); color:#3fb950; padding:2px 8px; border-radius:10px;'>{t_data['bull_confidence']}% Conf</span>
            </div>
            <div style='color:#c9d1d9; font-size:13px; line-height:1.5;'>{t_data['bull_thesis']}</div>
        </div>
        """, unsafe_allow_html=True)
    with t_col2:
        st.markdown(f"""
        <div style='background:rgba(248,81,73,0.05); border:1px solid rgba(248,81,73,0.2); border-top:3px solid #f85149; border-radius:8px; padding:16px;'>
            <div style='display:flex; justify-content:space-between; align-items:center; margin-bottom:12px;'>
                <span style='font-size:14px; color:#f85149; font-weight:800;'>BEAR THESIS</span>
                <span style='font-size:12px; background:rgba(248,81,73,0.2); color:#f85149; padding:2px 8px; border-radius:10px;'>{t_data['bear_confidence']}% Conf</span>
            </div>
            <div style='color:#c9d1d9; font-size:13px; line-height:1.5;'>{t_data['bear_thesis']}</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<hr style='border-color:rgba(255,255,255,0.05); margin:24px 0;'>", unsafe_allow_html=True)

    # 3. Causal Chains
    st.markdown("### 3. Causal Impact Analysis")
    chains = causality_engine.chains.get(ticker, [])
    if not chains:
        st.info("No causal chains tracked for this ticker yet. The background monitoring engine will generate them when critical events occur.")
    else:
        for c in reversed(chains[-5:]): # Show last 5
            st.markdown(f"""
            <div style='background:rgba(22,27,34,0.4); border:1px solid rgba(255,255,255,0.05); border-left:2px solid #8b949e; border-radius:8px; padding:16px; margin-bottom:12px;'>
                <div style='font-size:13px; font-weight:700; color:#f0f6fc; margin-bottom:8px;'>Event: {c.get('event_summary', 'Unknown Event')}</div>
                <div style='display:flex; gap:16px; font-size:12px;'>
                    <div style='flex:1;'>
                        <div style='color:#58a6ff; font-weight:600; margin-bottom:4px;'>1st Order Effect</div>
                        <div style='color:#c9d1d9;'>{c.get('first_order_effect', '-')}</div>
                    </div>
                    <div style='color:#484f58; padding-top:12px;'>→</div>
                    <div style='flex:1;'>
                        <div style='color:#e3b341; font-weight:600; margin-bottom:4px;'>2nd Order Effect</div>
                        <div style='color:#c9d1d9;'>{c.get('second_order_effect', '-')}</div>
                    </div>
                </div>
                <div style='margin-top:12px; padding-top:12px; border-top:1px dashed rgba(255,255,255,0.1);'>
                    <span style='color:#8b949e; font-size:11px; font-weight:700; text-transform:uppercase;'>System Recommendation: </span>
                    <span style='color:#f0f6fc; font-size:12px;'>{c.get('decision_recommendation', 'Monitor')}</span>
                </div>
            </div>
            """, unsafe_allow_html=True)

# ── Hub 3: Institutional Vault ──────────────────────────────────
elif is_vault_hub:
    st.markdown("<div class='section-header'>INSTITUTIONAL RESEARCH LIBRARY</div>", unsafe_allow_html=True)
    
    # ── Search & Filter ──
    col1, col2 = st.columns([3, 1])
    with col1:
        search_query = st.text_input("Search Intelligence Vault", placeholder="Search by ticker, keyword, or strategic theme...", label_visibility="collapsed")
    with col2:
        if st.button("🔄 Refresh Library", use_container_width=True):
            st.rerun()

    # ── Filter reports ──
    filtered_reports = [r for r in all_reports if r.get("report_markdown")]
    if search_query:
        # Tier 2: Semantic Search via VaultService
        with st.spinner("Searching Vault..."):
            filtered_reports = asyncio.run(vault_service.search_reports(search_query))

    if not filtered_reports:
        st.info("No research reports found matching your criteria.")
    else:
        # ── Library Grid ──
        if "viewing_report_id" not in st.session_state:
            st.markdown(f"<div style='color:#8b949e; font-size:12px; margin-bottom:16px; font-weight:600;'>{len(filtered_reports)} ARCHIVED REPORTS</div>", unsafe_allow_html=True)
            
            # Simple, clean list of report cards
            for r in filtered_reports:
                ticker_label = r.get("ticker", "GLOBAL")
                title = r.get("report_title", f"Institutional Analysis: {ticker_label}")
                date = r.get("created_at", "")[:10]
                
                # Two-column layout for Open vs Delete
                col_info, col_actions = st.columns([6, 1])
                
                with col_info:
                    if st.button(f"📄 {ticker_label} — {title} ({date})", key=f"open_{r['id']}", use_container_width=True):
                        st.session_state.viewing_report_id = r['id']
                        st.rerun()
                
                with col_actions:
                    if st.button("🗑", key=f"quick_del_{r['id']}", help="Quick Delete", use_container_width=True):
                        supabase.table("reports").delete().eq("id", r['id']).execute()
                        st.toast(f"Deleted: {ticker_label} Report")
                        st.rerun()
        else:
            # ── Detailed Reader View ──
            selected_report = next((r for r in all_reports if r['id'] == st.session_state.viewing_report_id), None)
            
            if not selected_report:
                del st.session_state.viewing_report_id
                st.rerun()
            
            if st.button("← Back to Library", key="back_to_lib"):
                del st.session_state.viewing_report_id
                st.rerun()
            
            st.markdown("<hr style='border-color:rgba(255,255,255,0.05);'>", unsafe_allow_html=True)
            
            # Action Bar
            col_d1, col_d2, col_d3 = st.columns([1, 1, 1])
            with col_d1:
                st.download_button("📥 Download (Markdown)", selected_report["report_markdown"], file_name=f"{selected_report.get('ticker','report')}_analysis.md", use_container_width=True)
            with col_d2:
                azure_url = selected_report.get("data_snapshot", {}).get("azure_url")
                if azure_url:
                    st.link_button("🌐 Azure Mirror Link", azure_url, use_container_width=True)
                else:
                    st.button("🌐 Azure Sync: Not Available", disabled=True, use_container_width=True)
            with col_d3:
                with st.popover("🗑 Remove Report", use_container_width=True):
                    st.warning("This will permanently delete this analysis from the Intelligence Vault.")
                    confirm = st.checkbox("Confirm Permanent Deletion")
                    if st.button("Confirm & Delete", type="primary", disabled=not confirm, use_container_width=True):
                        supabase.table("reports").delete().eq("id", selected_report["id"]).execute()
                        del st.session_state.viewing_report_id
                        st.toast("Report successfully removed from library.")
                        st.rerun()

            render_institutional_response(selected_report["report_markdown"])

            # ── Intelligence Resources traceability in Archive ──
            # Try to extract manifest from data_snapshot or similar if stored, 
            # but since we append to markdown, we rely on the markdown.
            # For a more dynamic experience, we'll try to find any saved manifest.
            manifest = selected_report.get("data_snapshot", {}).get("manifest", [])
            if manifest:
                st.markdown("<div class='section-header'>RANKED INTELLIGENCE RESOURCES</div>", unsafe_allow_html=True)
                for i, item in enumerate(manifest):
                    with st.expander(f"[{item['type'].upper()}] {item['title']}"):
                        st.markdown(f"**Source URL**: {item['url']}" if item.get('url') else "Internal Vault Source")
            
            st.markdown("<hr style='border-color:rgba(255,255,255,0.05);'>", unsafe_allow_html=True)


# ── Hub 4: Data Ingestion Suite ─────────────────────────────────
elif is_ingestion_hub:
    st.markdown("<div class='section-header'>DOCUMENT EXTRACTOR</div>", unsafe_allow_html=True)
    st.markdown("<div style='color:#8b949e; font-size:13px; margin-bottom:16px;'>Extract financial data from URLs, YouTube videos, or uploaded files (PDF, DOCX, TXT, MD). Powered by Gemini structured extraction.</div>", unsafe_allow_html=True)

    # Input section (Hidden if extraction successful for a 'clean' look)
    extract_url_btn = False
    extract_file_btn = False
    extract_yt_btn = False
    extract_url = None
    uploaded_file = None
    extract_yt = None
    
    # ── Tier 3 OLAP Sync Widget ──
    sync_hdr, sync_btn_col = st.columns([3, 1])
    with sync_btn_col:
        if st.button("Sync Data Warehouse", width='stretch', key="manual_sync_olap"):
            st.toast("OLAP Synchronization has been disabled per institutional requirements.")

    if "last_extraction" not in st.session_state:
        ext_col1, ext_col2 = st.columns(2)

        with ext_col1:
            st.markdown("<div style='color:#c9d1d9; font-size:13px; font-weight:600; margin-bottom:8px;'>Extract from URL</div>", unsafe_allow_html=True)
            extract_url = st.text_input("URL", placeholder="https://company.com/investor-relations", label_visibility="collapsed", key="extract_url")
            url_company = st.text_input("Company Name (Required)", placeholder="Enter Company Name", key="url_company")
            url_ticker = st.text_input("Ticker (Required)", placeholder="Enter Ticker", key="url_ticker")
            url_currency = st.selectbox("Currency (Override)", ["Auto-Detect", "INR", "USD", "EUR", "GBP", "ZAR"], index=0, key="url_curr")
            extract_url_btn = st.button("Search URL", width='stretch', key="extract_url_btn")

        with ext_col2:
            st.markdown("<div style='color:#c9d1d9; font-size:13px; font-weight:600; margin-bottom:8px;'>Upload File</div>", unsafe_allow_html=True)
            uploaded_file = st.file_uploader("Upload", type=["pdf", "docx", "txt", "md"], label_visibility="collapsed", key="file_upload")
            file_company = st.text_input("Company Name (Required)", placeholder="Enter Company Name", key="file_company")
            file_ticker = st.text_input("Ticker (Required)", placeholder="Enter Ticker", key="file_ticker")
            file_currency = st.selectbox("Currency (Override)", ["Auto-Detect", "INR", "USD", "EUR", "GBP", "ZAR"], index=0, key="file_curr")
            extract_file_btn = st.button("Extract File", width='stretch', key="extract_file_btn")
    else:
        st.markdown("""
        <div style='background:rgba(63,185,80,0.1); border:1px solid #3fb950; border-radius:8px; padding:16px; margin-bottom:24px; text-align:center;'>
            <div style='color:#3fb950; font-size:24px; margin-bottom:8px;'>Extraction Successful!</div>
            <div style='color:#8b949e; font-size:14px;'>The data has been extracted, normalized, and synchronized to the institutional vault.</div>
        </div>
        """, unsafe_allow_html=True)
        if st.button("New Extraction", width='stretch'):
            del st.session_state["last_extraction"]
            st.rerun()

    # Process URL extraction
    if extract_url_btn and extract_url:
        if not url_company or not url_ticker:
            st.error("⚠️ Company Name and Ticker are required for URL extraction.")
        else:
            with st.spinner("Extracting content and analyzing financials..."):
                try:
                    engine = ExtractorEngine()
                    result = engine.process(
                        extract_url,
                        ticker_override=url_ticker.strip().upper(),
                        company_override=url_company.strip().upper(),
                        currency_override=url_currency if url_currency != "Auto-Detect" else None
                    )
                    st.session_state["last_extraction"] = result
                    
                    st.success("✅ Data stored in Supabase")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    import traceback
                    with open('err.log', 'w') as errf: errf.write(traceback.format_exc())
                    st.error(f"Extraction failed: {e}")

    # Process file extraction
    if extract_file_btn and uploaded_file:
        if not file_company or not file_ticker:
            st.error("⚠️ Company Name and Ticker are required for File extraction.")
        else:
            with st.spinner("Parsing file and analyzing financials..."):
                try:
                    engine = ExtractorEngine()
                    file_bytes = uploaded_file.getvalue()
                    
                    # Phase 2: Archive Raw Document to Institutional Vault
                    azure_url = vault_service.upload_to_azure(
                        file_bytes, 
                        f"ingest/{file_ticker.strip().upper()}_{uploaded_file.name}"
                    )
                    if azure_url:
                        st.session_state["last_azure_upload"] = azure_url

                    result = engine.process(
                        file_bytes,
                        filename=uploaded_file.name,
                        ticker_override=file_ticker.strip().upper(),
                        company_override=file_company.strip().upper(),
                        currency_override=file_currency if file_currency != "Auto-Detect" else None
                    )
                    st.session_state["last_extraction"] = result
                    
                    st.success("✅ Data stored in Supabase")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    import traceback
                    with open('err.log', 'w') as errf: errf.write(traceback.format_exc())
                    st.error(f"Extraction failed: {e}")

    # Display extraction results
    if "last_extraction" in st.session_state:
        result = st.session_state["last_extraction"]

        if result.get("status") == "FAILED" or result.get("error"):
            st.warning(f"⚠️ Extraction Failed: {result.get('error')}")
            with st.expander("⚠️ Debug Info"):
                st.json(result)
        elif result.get("status") == "SUCCESS":
            st.markdown("<div class='section-header'>EXTRACTION STATUS</div>", unsafe_allow_html=True)
            
            with st.expander("🔍 Technical Audit (AI Insight)"):
                st.json(result)
            
            col1, col2, col3 = st.columns(3)
            
            col1.metric("Rows Extracted", "1")
            col2.metric("Rows Inserted", "1" if not result.get("rejected") else "0")
            # Calculate coverage from actual row data
            row_data = result.get("row") or {}
            _numeric_fields = ["revenue", "net_income", "operating_income", "total_assets",
                               "total_liabilities", "total_equity", "cash_on_hand", "eps_diluted"]
            _filled = sum(1 for f in _numeric_fields if row_data.get(f) is not None)
            cov = (_filled / len(_numeric_fields)) * 100 if len(_numeric_fields) > 0 else 0
            col3.metric("Coverage %", f"{cov:.0f}%")
            
            t1, t2, t3 = st.tabs(["Structured Financials", "Text Preview", "JSON Structure"])
            with t1:
                st.markdown("### Financial Highlights")
                import pandas as pd
                df = pd.DataFrame([result.get("row", {})])
                st.dataframe(df)
            with t2:
                st.markdown("### Raw Extraction Preview")
                st.text_area("Content", result.get("raw_text", "")[:10000], height=300)
            with t3:
                st.markdown("### JSON Metadata & Sections")
                st.json(result.get("analysis", {}))

    # NEW: VAULT BROWSER
    st.markdown("<div class='section-header'>📂 VAULTED INTELLIGENCE BROWSER</div>", unsafe_allow_html=True)
    uploaded_docs = load_uploaded_docs(ticker)
    
    if not uploaded_docs:
        st.info("No vaulted documents found for this ticker.")
    else:
        doc_df = pd.DataFrame(uploaded_docs)
        doc_df = doc_df[["source_url", "source_type", "created_at", "archived_url"]]
        doc_df.columns = ["Source", "Type", "Date", "Vault Link"]
        
        st.dataframe(
            doc_df,
            width='stretch',
            column_config={
                "Vault Link": st.column_config.LinkColumn(
                    "🏦 Vault Link",
                    help="Secure link to the archived document in Azure",
                    display_text="View in Vault ↗"
                ),
                "Source": st.column_config.TextColumn("Source", width="medium"),
            }
        )

# ── Hub 2: Strategic Research Lab ───────────────────────────────
# ── Mode: Report Builder
elif is_research_hub and lab_mode == "📝 Report Builder":
    try:
        st.markdown("<div class='section-header'>INSTITUTIONAL REPORT ORCHESTRATOR</div>", unsafe_allow_html=True)
        st.markdown(
            "<div style='color:#8b949e; font-size:13px; margin-bottom:16px;'>"
            "Generate deep, multi-source research reports powered by parallel intelligence agents "
            "detecting financials, news, ecosystem, and vault data."
            "</div>",
            unsafe_allow_html=True
        )


        # Input for independent report generation
        user_prompt = st.text_area(
            "What should this report cover?",
            placeholder="e.g. Generate a deep financial audit of Nvidia's Q3 performance vs its AI supply chain risk.",
            height=150,
            help="You can mention any company or strategic scenario. The orchestrator will detect tickers automatically."
        )

        col1, col2, col3 = st.columns([1, 1, 1])
        with col1:
            st.markdown("### ⚙️ Configuration")
            include_vault = st.toggle("Include Internal Vault Knowledge", value=True)
            depth = st.select_slider("Analysis Depth", options=["Standard", "Deep", "Institutional"], value="Institutional", help="Higher depth triggers more cross-referencing and validation agents.")
            custom_instructions = st.text_area(
                "🎯 Focus Areas / Custom Instructions (Optional)",
                placeholder="e.g. Focus on cash-flow quality, ADIA synergies, and compliance granularity. Keep citations highly rigorous.",
                height=100,
                help="Inject custom guidelines or emphasis directly into the generative intelligence prompts."
            )
            st.info("💡 **Dynamic Reporting Enabled**: The orchestrator determines the optimal structure based on prompt complexity.")

        with col2:
            st.markdown("### 📎 Upload New Files")
            uploaded_files = st.file_uploader("Upload Supplemental Documents (PDF/TXT)", type=["pdf", "txt"], accept_multiple_files=True)
            if uploaded_files:
                st.success(f"{len(uploaded_files)} files staged for analysis.")

            st.markdown("### 🏦 Database Context")
            include_tickers = st.multiselect(
                "Include Supabase Financials for:",
                options=list(TARGET_COMPANIES.keys()),
                help="Inject structured financial metrics from the IIP vault into the report."
            )

        with col3:
            st.markdown("### 📂 Vault Documents")
            st.caption("Select previously ingested files from the Data Ingestion Suite.")

            # Load all vaulted documents from extracted_documents table (function moved to top level)

            vault_docs = load_all_vault_docs()

            if not vault_docs:
                st.info("No vaulted documents found. Use the **Data Ingestion Suite** to extract and store files.")
                selected_vault_doc_ids = []
            else:
                # Build a label → id map safely handling None values
                doc_labels = {
                    f"[{d.get('ticker') or '?'}] {d.get('source_type') or 'DOC'} — {(d.get('source_url') or '')[:40]}... ({(d.get('created_at') or '')[:10]})": d['id']
                    for d in vault_docs
                }
                selected_labels = st.multiselect(
                    "Select Vault Docs",
                    options=list(doc_labels.keys()),
                    label_visibility="collapsed",
                    placeholder="Search vaulted documents..."
                )
                selected_vault_doc_ids = [doc_labels[l] for l in selected_labels]

                if selected_vault_doc_ids:
                    st.success(f"✅ {len(selected_vault_doc_ids)} vault document(s) selected.")
                    # Preview
                    with st.expander("Preview Selected"):
                        for doc in vault_docs:
                            if doc['id'] in selected_vault_doc_ids:
                                preview = (doc.get('raw_text') or '')[:500]
                                st.markdown(f"**{(doc.get('source_url') or '')[:60]}**")
                                st.text(preview + ("..." if len(doc.get('raw_text') or '') > 500 else ""))
                                st.divider()

        if st.button("Generate Institutional Report", width='stretch', type="primary"):
            if not user_prompt.strip():
                st.warning("Please enter a research query or prompt.")
            else:
                sb = get_supabase()
                orchestrator = ResearchOrchestrator(sb)
                status_box = st.empty()
                def update_status(text):
                    status_box.markdown(f"<div style='color:#58a6ff; font-size:12px; animation: pulse 2s infinite;'>Researching: {text}</div>", unsafe_allow_html=True)
            
                supp_context = ""
            
                # 1. Process Uploaded Files
                if uploaded_files:
                    import pymupdf4llm # type: ignore
                    import tempfile
                    import os

                    for uploaded_file in uploaded_files:
                        update_status(f"Extracting context from {uploaded_file.name}...")
                        try:
                            suffix = ".pdf" if uploaded_file.type == "application/pdf" else ".txt"
                            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                                tmp.write(uploaded_file.getvalue())
                                tmp_path = tmp.name

                            if uploaded_file.type == "application/pdf":
                                text = pymupdf4llm.to_markdown(tmp_path)
                            else:
                                text = uploaded_file.getvalue().decode("utf-8")

                            supp_context += f"\n--- Supplemental Document: {uploaded_file.name} ---\n{text}\n"
                            os.unlink(tmp_path)
                        except Exception as e:
                            st.error(f"Failed to extract text from {uploaded_file.name}: {e}")

                # 2. Process Vault Documents from Data Ingestion Suite
                if selected_vault_doc_ids:
                    update_status(f"Loading {len(selected_vault_doc_ids)} vault document(s)...")
                    for doc in vault_docs:
                        if doc['id'] in selected_vault_doc_ids:
                            raw = doc.get('raw_text') or ''
                            source = doc.get('source_url', 'Vault Document')
                            ticker_tag = doc.get('ticker', '')
                            if raw:
                                supp_context += f"\n--- Vaulted Document [{ticker_tag}]: {source} ---\n{raw[:8000]}\n"

                # 3. Process Supabase Financials
                if include_tickers:
                    update_status(f"Injecting vault financials for {', '.join(include_tickers)}...")
                    for t in include_tickers:
                        fin_data = load_financials(t)
                        if fin_data:
                            supp_context += f"\n--- Vault Financials for {t} ---\n{json.dumps(fin_data, indent=2)}\n"
                    
                        news_data = load_news(t)
                        if news_data:
                            supp_context += f"\n--- Vault Intelligence (News) for {t} ---\n{json.dumps(news_data[:10], indent=2)}\n"

                with st.spinner("Executing Research Orchestration..."):
                    # Run the orchestrator in REPORT mode
                    result = asyncio.run(orchestrator.run(
                        user_prompt, 
                        callback=update_status, 
                        mode="report",
                        supplemental_context=supp_context,
                        custom_instructions=custom_instructions
                    ))
                    status_box.empty()
                
                    # SAVE TO VAULT & AZURE
                    report_md = result.get("response", "")
                
                    # Append resources to the markdown itself for persistence and download
                    manifest = result.get("manifest", [])
                    if manifest:
                        report_md += "\n\n---\n## 📚 INSTITUTIONAL REFERENCES & CITATIONS\n"
                        report_md += "The following intelligence sources were retrieved, ranked, and analyzed to synthesize this report:\n\n"
                        for i, item in enumerate(manifest):
                            source_content = result.get("sources", [""] * len(manifest))[i] if i < len(result.get("sources", [])) else ""
                            report_md += f"### [{item['type'].upper()}] {item['title']}\n"
                            if item.get("url"):
                                report_md += f"**Source URL**: {item['url']}\n\n"
                            excerpt = source_content[:1000] + "..." if len(source_content) > 1000 else source_content
                            report_md += f"{excerpt}\n\n"
                
                    # Update result with enriched markdown
                    result["response"] = report_md
                
                    # Detect ticker from result or prompt (fallback to sidebar ticker then 'GLOBAL')
                    tickers_list = result.get("intent", {}).get("tickers", [])
                    detected_ticker = tickers_list[0] if tickers_list else None
                    if not detected_ticker:
                        detected_ticker = ticker if ticker else "GLOBAL"
                
                    saved_report = vault_service.save_report_to_vault(detected_ticker, report_md, user_prompt, manifest=result.get("manifest"))
                    if saved_report:
                        st.success(f"Report for {detected_ticker} archived in Institutional Vault & Azure Blob Storage.")

                    st.session_state["built_report"] = result
                    st.session_state["built_report_mode"] = "orchestrated"
                    st.rerun()

        # ── Display generated report ──────────────────────────────────────────────
        if "built_report" in st.session_state:
            result = st.session_state["built_report"]
            report_md = result.get("response", "")
        
            if report_md:
                st.markdown("<hr style='border-color:rgba(255,255,255,0.1);'>", unsafe_allow_html=True)
            
                # Extract dynamic title for the UI header
                ui_title = "GENERATED RESEARCH REPORT"
                if report_md.startswith('# '):
                    ui_title = report_md.split('\n')[0].replace('# ', '').strip().upper()
                
                st.markdown(f"<div class='section-header'>{ui_title}</div>", unsafe_allow_html=True)
            
                # Download button
                st.download_button(
                    "Download Report (Markdown)",
                    data=report_md,
                    file_name="research_report.md",
                    mime="text/markdown",
                )
            
                render_institutional_response(report_md)

            # Sources panel
            manifest = result.get("manifest", [])
            if manifest:
                st.markdown("<div class='section-header'>RANKED INTELLIGENCE RESOURCES</div>", unsafe_allow_html=True)
                for i, item in enumerate(manifest):
                    with st.expander(f"[{item['type'].upper()}] {item['title']}"):
                        sources = result.get("sources", [])
                        st.markdown(sources[i] if i < len(sources) else "*Source content not available*")
                        if item.get("url"):
                            st.link_button("View Original Source", item["url"])

            if st.button("Reset Report Builder", key="clear_report_btn"):
                del st.session_state["built_report"]
                if "built_report_mode" in st.session_state:
                    del st.session_state["built_report_mode"]
                st.rerun()
    except Exception as _rb_exc:
        import traceback as _tb
        st.error(f"**Report Builder Error:** {_rb_exc}")
        with st.expander("Full Traceback"):
            st.code(_tb.format_exc())

elif is_research_hub and lab_mode == "💬 Research Chat":
    st.markdown(f"<div class='section-header'>INSTITUTIONAL RESEARCH ORCHESTRATOR</div>", unsafe_allow_html=True)
    st.markdown("<div style='color:#8b949e; font-size:13px; margin-bottom:16px;'>Global Multi-Source Intelligence Engine. Powered by the IIP Orchestration Core.</div>", unsafe_allow_html=True)


    # Chat display using native Streamlit chat components
    for msg in st.session_state.chat_history:
        with st.chat_message(msg["role"]):
            if msg["role"] == "user":
                st.markdown(msg["content"])
            else:
                render_institutional_response(msg["content"])
                # Show sources if they exist in the message history
                manifest = msg.get("manifest", [])
                if manifest:
                    st.markdown("<div style='color:#8b949e; font-size:10px; font-weight:800; margin-top:8px; text-transform:uppercase; letter-spacing:0.1em;'>Intelligence Resources</div>", unsafe_allow_html=True)
                    for i, item in enumerate(manifest):
                        with st.expander(f"[{item['type'].upper()}] {item['title']}"):
                            st.markdown(msg["sources"][i])
                            if item.get("url"):
                                st.link_button("Source", item["url"])

    if "session_id" not in st.session_state:
        st.session_state.session_id = None

    # Suggestions (Only if no history)
    if not st.session_state.chat_history:
        st.markdown("<div style='color:#8b949e; font-size:12px; margin:8px 0;'>Strategic Research Starters:</div>", unsafe_allow_html=True)
        suggestions = [
            "Analyze Nvidia's AI supply chain and competitive risk.",
            "Compare Apple and Samsung profitability trends.",
            "What changed in Tesla's latest 10-Q filing?",
            "Analyze Western Alliance Bancorporation news sentiment."
        ]
        cols = st.columns(2)
        for i, q in enumerate(suggestions):
            if cols[i % 2].button(q, key=f"sugg_{i}", width='stretch'):
                st.session_state.chat_history.append({"role": "user", "content": q})
                
                status_box = st.empty()
                def update_status(text):
                    status_box.markdown(f"<div style='color:#58a6ff; font-size:12px; animation: pulse 2s infinite;'>Researching: {text}</div>", unsafe_allow_html=True)
                
                with st.spinner("Executing Research Plan..."):
                    result = asyncio.run(orchestrator.run(q, callback=update_status, mode="chat", session_id=st.session_state.session_id))
                    st.session_state.session_id = result.get("session_id")
                    status_box.empty()
                
                st.session_state.chat_history.append({
                    "role": "assistant", 
                    "content": result["response"], 
                    "sources": result.get("sources"), 
                    "context": result.get("context"),
                    "manifest": result.get("manifest")
                })
                st.rerun()

    # Chat Input
    if user_input := st.chat_input("Ask anything about any company or sector..."):
        st.session_state.chat_history.append({"role": "user", "content": user_input})
        
        # Display user message immediately
        with st.chat_message("user"):
            st.markdown(user_input)

        status_box = st.empty()
        def update_status(text):
            status_box.markdown(f"<div style='color:#58a6ff; font-size:12px;'>Researching: {text}</div>", unsafe_allow_html=True)
        
        with st.chat_message("assistant"):
            with st.spinner("Orchestrating intelligence agents..."):
                result = asyncio.run(orchestrator.run(user_input, callback=update_status, mode="chat", session_id=st.session_state.session_id))
                st.session_state.session_id = result.get("session_id")
                status_box.empty()
                render_institutional_response(result["response"])

        st.session_state.chat_history.append({
            "role": "assistant", 
            "content": result["response"], 
            "sources": result.get("sources"), 
            "context": result.get("context"),
            "manifest": result.get("manifest")
        })
        
        manifest = result.get("manifest", [])
        if manifest:
            st.markdown("<div style='color:#8b949e; font-size:10px; font-weight:800; margin-top:16px; text-transform:uppercase; letter-spacing:0.1em;'>Intelligence Resources</div>", unsafe_allow_html=True)
            for i, item in enumerate(manifest):
                with st.expander(f"[{item['type'].upper()}] {item['title']}"):
                    st.markdown(result["sources"][i])
                    if item.get("url"):
                        st.link_button("Source", item["url"])

        st.rerun()

    if st.session_state.chat_history:
        if st.button("Reset Workspace", width='content'):
            st.session_state.chat_history = []
            st.session_state.session_id = None
            st.rerun()

# ── Fallback: shown if no branch matched — ALWAYS renders something ─────────
else:
    # This fires when a hub IS active but its sub-mode didn’t match any branch,
    # OR when no hub is active. Either way, render a safe loading placeholder
    # so the main content area is never a black void.
    _active_hub = hub if isinstance(hub, str) else ""
    _debug_ws   = ws_mode  or st.session_state.get("ws_mode",  "")
    _debug_lab  = lab_mode or st.session_state.get("lab_mode", "")
    st.markdown(f"""
    <div style='text-align:center; padding:80px 20px;'>
        <div style='font-size:48px; margin-bottom:16px;'>🏛️</div>
        <div style='font-size:22px; font-weight:800; color:#c9d1d9; margin-bottom:8px;'>Loading…</div>
        <div style='color:#484f58; font-size:12px;'>Hub: {_active_hub} &nbsp;|&nbsp; Mode: {_debug_ws or _debug_lab}</div>
        <div style='color:#484f58; font-size:11px; margin-top:8px;'>If this persists, click any sidebar option to refresh.</div>
    </div>
    """, unsafe_allow_html=True)
