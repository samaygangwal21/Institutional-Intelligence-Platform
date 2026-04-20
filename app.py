"""
============================================================
PART E: STREAMLIT DASHBOARD v2 — INSTITUTIONAL INTELLIGENCE
Doc-extract-and-report | samaygangwal21@gmail.com

New in v2:
  • Sector-grouped sidebar navigation
  • 2-year Report Archive — browse all historical reports
  • Sector Heatmap — cross-company metric comparison
  • AI Research Chat — ask questions about any company
  • Smart Watchlist — flag companies for review
  • Quarterly Drill-Down — per-quarter revenue waterfall
  • Compliance Score Tracker — historical audit trail
============================================================
"""

import os
import subprocess
import streamlit as st # type: ignore
import pandas as pd # type: ignore
import plotly.graph_objects as go # type: ignore
import plotly.express as px # type: ignore
from supabase import create_client, Client # type: ignore
from datetime import datetime, date
import requests # type: ignore
import hashlib
import json
from typing import List, Dict, Optional, Any, cast

# Consolidated Modules
import platform_config
from platform_config import ( # type: ignore
    SUPABASE_URL, SUPABASE_KEY, GEMINI_API_KEY, GEMINI_ENDPOINT,
    TARGET_COMPANIES, COMPANY_ICONS, SECTOR_ICONS, load_target_companies,
    get_supabase
)
from ingest import ExtractorEngine
from intelligence import render_ecosystem_graph
from utils import build_sec_ix_url, backfill_sec_urls

# Refresh companies from registry
TARGET_COMPANIES = load_target_companies()
TARGET_TICKERS = list(TARGET_COMPANIES.keys())

def load_uploaded_docs(ticker: str) -> List[Dict]:
    sb = get_supabase()
    try:
        res = sb.table("extracted_documents").select("*").eq("ticker", ticker).order("created_at", ascending=False).execute()
        return res.data or []
    except:
        return []


# ── Page Config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Institutional Intelligence",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded",
)

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
    .auth-card {
        background: linear-gradient(135deg, rgba(30, 35, 45, 0.4) 0%, rgba(15, 20, 25, 0.7) 100%);
        backdrop-filter: blur(40px);
        border: 1px solid rgba(88, 166, 255, 0.2);
        padding: 50px;
        border-radius: 24px;
        max-width: 450px;
        margin: 100px auto;
        text-align: center;
        box-shadow: 0 20px 50px rgba(0,0,0,0.5);
    }
</style>
""", unsafe_allow_html=True)

# ── Auth Logic ──────────────────────────────────────────────────────────────
def render_auth():
    st.markdown("<div class='auth-card'>", unsafe_allow_html=True)
    st.title("🗄️ Institutional Intelligence")
    st.markdown("<p style='color:#8b949e; margin-bottom:30px;'>Secure Financial Research & Data Vault</p>", unsafe_allow_html=True)
    
    auth_tab = st.tabs(["Login", "Sign Up"])
    
    with auth_tab[0]:
        with st.form("login_form"):
            email = st.text_input("Corporate Email")
            password = st.text_input("Password", type="password")
            submitted = st.form_submit_button("Access Platform", use_container_width=True)
            if submitted:
                handle_login(email, password)
                
    with auth_tab[1]:
        with st.form("signup_form"):
            email = st.text_input("Corporate Email")
            pwd = st.text_input("Password", type="password")
            confirm = st.text_input("Confirm Password", type="password")
            submitted = st.form_submit_button("Create Account", use_container_width=True)
            if submitted:
                if pwd != confirm:
                    st.error("Passwords do not match.")
                else:
                    handle_signup(email, pwd)
    st.markdown("</div>", unsafe_allow_html=True)

def handle_login(email, password):
    sb = get_supabase()
    try:
        res = sb.auth.sign_in_with_password({"email": email, "password": password})
        if res.user:
            st.session_state.user = res.user
            st.success("Access Granted.")
            st.rerun()
    except Exception as e:
        st.error(f"Login Failed: {str(e)}")

def handle_signup(email, password):
    sb = get_supabase()
    try:
        res = sb.auth.sign_up({"email": email, "password": password})
        if res.user:
            st.info("Account created. Please check your email for verification before logging in.")
    except Exception as e:
        st.error(f"Signup Failed: {str(e)}")

def handle_logout():
    sb = get_supabase()
    sb.auth.sign_out()
    if "user" in st.session_state:
        del st.session_state.user
    st.rerun()

# ── Main Entry Point ───────────────────────────────────────────────────────
if "user" not in st.session_state:
    render_auth()
    st.stop()

# Logout button in sidebar
if st.sidebar.button("🚪 Logout", use_container_width=True):
    handle_logout()

# ── Supabase ──────────────────────────────────────────────────────────────────
# Using get_supabase() imported from platform_config

supabase = get_supabase()

# ── Data Loaders ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=30)   # Task 4: 30s TTL for real-time updates
def load_financials(ticker: str) -> list[dict]:
    # Force cache invalidation to load new NKE data
    return (supabase.table("financials").select("*")
            .eq("ticker", ticker).order("end_date").execute().data or [])

@st.cache_data(ttl=30)
def load_all_financials() -> list[dict]:
    return (supabase.table("financials").select("*")
            .order("end_date", desc=True).execute().data or [])

@st.cache_data(ttl=30)
def load_news(ticker: str, limit: int = 60) -> list[dict]:
    return (supabase.table("market_intelligence").select("*")
            .eq("ticker", ticker).order("published_at", desc=True)
            .limit(limit).execute().data or [])

@st.cache_data(ttl=30)
def load_connections(ticker: str) -> list[dict]:
    return (supabase.table("corporate_connections").select("*")
            .eq("source_ticker", ticker).execute().data or [])

@st.cache_data(ttl=30)
def load_all_reports(ticker: str) -> list[dict]:
    """Load ALL historical reports for a ticker (for 2-year archive)."""
    return (supabase.table("reports").select("*")
            .eq("ticker", ticker).order("created_at", desc=True)
            .execute().data or [])

@st.cache_data(ttl=30)
def load_latest_report(ticker: str) -> dict | None:
    data = load_all_reports(ticker)
    return data[0] if data else None

@st.cache_data(ttl=30)
def load_sector_snapshot() -> pd.DataFrame:
    """Loads latest FY record per company for sector heatmap."""
    rows = []
    for ticker, meta in TARGET_COMPANIES.items():
        # Try FY first
        fins = (supabase.table("financials").select("*")
                .eq("ticker", ticker).eq("fiscal_period", "FY")
                .order("end_date", desc=True).limit(1).execute().data or [])
        
        # Fallback to latest available (e.g. 10-Q) if no FY found
        if not fins:
            fins = (supabase.table("financials").select("*")
                    .eq("ticker", ticker)
                    .order("end_date", desc=True).limit(1).execute().data or [])
            
        if fins:
            r = fins[0]
            rows.append({
                "ticker":     ticker,
                "company":    meta["name"],
                "sector":     meta["sector"],
                "revenue":    r.get("revenue"),
                "net_income": r.get("net_income"),
                "op_income":  r.get("operating_income"),
                "cash":       r.get("cash_on_hand"),
                "equity":     r.get("total_equity"),
                "assets":     r.get("total_assets"),
                "eps":        r.get("eps_diluted"),
            })
    return pd.DataFrame(rows)

# ── Formatting ────────────────────────────────────────────────────────────────
def fmt_b(v):
    if v is None: return "–"
    if abs(v) >= 1e12: return f"${v/1e12:.2f}T"
    if abs(v) >= 1e9:  return f"${v/1e9:.2f}B"
    if abs(v) >= 1e6:  return f"${v/1e6:.2f}M"
    return f"${v:,.0f}"

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

# ── Sidebar Navigation ─────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div class='sidebar-brand'>
        <div class='sidebar-logo'>🏛️</div>
        <div style='font-size:15px; font-weight:800; color:#f0f6fc; letter-spacing:0.05em;'>INSTITUTIONAL</div>
        <div style='font-size:9px; color:#8b949e; letter-spacing:0.3em; margin-top:2px;'>INTELLIGENCE PLATFORM</div>
    </div>
    """, unsafe_allow_html=True)

    # Temporary variable to determine which view we're in WITHOUT creating the radio yet
    # We use a placeholder for now or just check a hidden state if needed, 
    # but the simplest way is to just put company selection above NO MATTER WHAT.
    
    st.markdown("<div class='section-header'>TARGET COMPANY</div>", unsafe_allow_html=True)
    
    # Pre-calculate company options WITHOUT EMOJIS
    company_options = {}
    for t in sorted(TARGET_COMPANIES.keys()):
        m = TARGET_COMPANIES[t]
        label = f"{m['name']} ({t}) — {m.get('sector', 'N/A')}"
        company_options[label] = t

    selected_label = st.selectbox("Company", list(company_options.keys()), label_visibility="collapsed", index=0)
    ticker = company_options[selected_label]

    # Watchlist toggle
    in_watchlist = ticker in st.session_state.watchlist
    wl_label = "⭐ Remove from Watchlist" if in_watchlist else "☆ Add to Watchlist"
    if st.button(wl_label, use_container_width=True):
        if in_watchlist: st.session_state.watchlist.discard(ticker)
        else: st.session_state.watchlist.add(ticker)
        st.rerun()

    st.markdown("<div class='section-header'>NAVIGATION</div>", unsafe_allow_html=True)
    view = st.radio("Navigation", [
        "📊 Financial Overview",
        "📈 Quarterly Drill-Down",
        "🌍 Sector Heatmap",
        "🌐 Corporate Ecosystem",
        "📰 Intelligence Feed",
        "📋 Report Archive",
        "📥 Document Extractor",
        "📝 Report Builder",
        "💬 AI Research Chat",
    ], label_visibility="collapsed")

    if view == "📥 Document Extractor":
        st.info("💡 Running in **Vault Mode**. Extracted data will be automatically cataloged.")

    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown("""
    <div style='font-size:10px; color:#8b949e; text-align:center; margin-top:16px;'>
        SEC EDGAR · Finnhub · Gemini 2.5 Flash<br>
        <span style='color:#3fb950;'>●</span> Vault Connected
    </div>
    """, unsafe_allow_html=True)

# Save ticker to session state for persistence
if view != "📥 Document Extractor":
    st.session_state.last_ticker = ticker

# ── Load data for selected ticker ─────────────────────────────────────────────
company_names = {t: m["name"] for t, m in TARGET_COMPANIES.items()}
financials   = load_financials(ticker)
connections  = load_connections(ticker)
news         = load_news(ticker)
all_reports  = load_all_reports(ticker)
latest_report = all_reports[0] if all_reports else None

# Split annual vs quarterly
df_all = pd.DataFrame(financials) if financials else pd.DataFrame()
df_fy  = df_all[df_all["fiscal_period"] == "FY"].sort_values("end_date") if not df_all.empty else pd.DataFrame()
df_q   = df_all[df_all["fiscal_period"] != "FY"].sort_values("end_date") if not df_all.empty else pd.DataFrame()

latest_fin = df_fy.iloc[-1].to_dict() if not df_fy.empty else {}
prior_fin  = df_fy.iloc[-2].to_dict() if len(df_fy) > 1 else {}

# ── Header ────────────────────────────────────────────────────────────────────
if view != "📥 Document Extractor":
    ticker_meta = TARGET_COMPANIES.get(ticker, {})
    status_badge = badge(latest_report.get("verification_status", "PENDING")) if latest_report else badge("PENDING")

    st.markdown(f"""
    <div style='display:flex; align-items:flex-end; gap:12px; margin-bottom:4px;'>
        <div style='font-size:36px; font-weight:900; color:#f0f6fc; line-height:1;'>{ticker}</div>
        <div style='font-size:18px; font-weight:600; color:#c9d1d9; border-left:1px solid rgba(255,255,255,0.1); padding-left:12px; margin-bottom:2px;'>{company_names.get(ticker,'')}</div>
    </div>
    <div style='font-size:11px; color:#8b949e; letter-spacing:0.05em; display:flex; align-items:center; gap:12px;'>
        <span>Sector: <b style='color:#58a6ff;'>{ticker_meta.get('sector','N/A')}</b></span>
        <span>Period: <b style='color:#f0f6fc;'>FY{latest_fin.get('fiscal_year','–')}</b></span>
        <span>Audit: {status_badge}</span>
    </div>
    <hr style='border-color:rgba(255,255,255,0.05); margin:20px 0 24px;'>
    """, unsafe_allow_html=True)
else:
    st.markdown(f"""
    <div style='margin-bottom:8px;'>
        <div style='font-size:32px; font-weight:900; color:#f0f6fc; line-height:1.1;'>Intelligence Vault</div>
        <div style='font-size:14px; font-weight:600; color:#8b949e; margin-top:4px;'>Advanced Entity Extraction & Financial Synthesis</div>
    </div>
    <hr style='border-color:rgba(255,255,255,0.05); margin:20px 0 24px;'>
    """, unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════
# VIEW 1: FINANCIAL OVERVIEW
# ════════════════════════════════════════════════════════════
if view == "📊 Financial Overview":
    # Task 2-A: UI-triggered pipeline button
    col_hdr, col_btn = st.columns([3, 1])
    with col_btn:
        if st.button("🔄 Fetch Latest Financials", use_container_width=True, key="fetch_financials_btn"):
            try:
                # Updated to point to the consolidated ingest.py
                subprocess.run(
                    ["python", "ingest.py", "--ticker", ticker, "--sec-only"],
                    check=False, timeout=120,
                )
            except Exception as e:
                st.error(f"Ingestor error: {e}")
            st.cache_data.clear()
            st.rerun()

    if not latest_fin:
        st.warning("⚠️ No financial data found. Run `python financial_ingestor.py` first.")
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

    def kpi(col, label, value, suffix=""):
        col.markdown(f"""
        <div class='metric-card' style='min-height:90px;'>
            <div class='metric-label'>{label}</div>
            <div class='metric-value' style='font-size:26px;'>{fmt_b(value)}{suffix}</div>
        </div>""", unsafe_allow_html=True)

    c1, c2, c3, c4 = st.columns(4)
    kpi(c1, "Total Revenue",      rev)
    kpi(c2, "Net Income",         ni)
    kpi(c3, "Operating Income",   op)
    kpi(c4, "Cash & Equivalents", ca)

    c5, c6, c7, c8 = st.columns(4)
    nm  = (ni / rev * 100) if (ni and rev) else None
    om  = (op / rev * 100) if (op and rev) else None
    roe = (ni / eq * 100)  if (ni and eq)  else None
    de  = (tl / eq) if (tl and eq) else None

    for col, label, val, fmt in [
        (c5, "EPS (Diluted)",       eps, lambda v: f"${v:.2f}"),
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
                             hovertemplate="<b>%{x}</b><br>Revenue: $%{y:.1f}B<extra></extra>"))
        fig.add_trace(go.Bar(x=df_fy["label"], y=df_fy["net_income"]/1e9, name="Net Income",
                             marker_color="#3fb950", opacity=.85,
                             hovertemplate="<b>%{x}</b><br>Net Income: $%{y:.1f}B<extra></extra>"))
        if df_fy["operating_income"].notna().any():
            fig.add_trace(go.Scatter(x=df_fy["label"], y=df_fy["operating_income"]/1e9,
                                     name="Operating Income", mode="lines+markers",
                                     line={"color": "#e3b341", "width": 2, "dash": "dot"}))
        fig.update_layout(plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                          font={"color": "#c9d1d9"}, barmode="group", height=360,
                          legend={"bgcolor": "#161b22", "bordercolor": "#30363d",
                                      "borderwidth": 1, "x": 0, "y": 1.1, "orientation": "h"},
                          xaxis={"gridcolor": "#21262d"},
                          yaxis={"gridcolor": "#21262d", "title": "USD Billions"},
                          margin={"l": 0, "r": 0, "t": 10, "b": 0}, hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)

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
            <div class='metric-value' style='font-size:20px;'>{fmt_b(val)}</div>
        </div>""", unsafe_allow_html=True)

    # Unified Document Access (Clubbed)
    filing_url = latest_fin.get("sec_filing_url")
    archived_url = latest_fin.get("archived_url")
    
    if archived_url or filing_url:
        primary_link = archived_url or filing_url
        label = "📂 VIEW FULL DOCUMENT (VAULTED)" if archived_url else "📄 VIEW ORIGINAL FILING (SEC)"
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



# ════════════════════════════════════════════════════════════
# VIEW 2: QUARTERLY DRILL-DOWN
# ════════════════════════════════════════════════════════════
elif view == "📈 Quarterly Drill-Down":
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
                    hovertemplate="<b>%{x}</b><br>Revenue: $%{y:.2f}B<extra></extra>",
                ))
        fig_q.update_layout(plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                            font={"color": "#c9d1d9"}, barmode="group", height=320,
                            legend={"bgcolor": "#161b22", "bordercolor": "#30363d",
                                        "borderwidth": 1, "x": 0, "y": 1.1, "orientation": "h"},
                            xaxis={"gridcolor": "#21262d"},
                            yaxis={"gridcolor": "#21262d", "title": "Revenue (USD Billions)"},
                            margin={"l": 0, "r": 0, "t": 10, "b": 0})
        st.plotly_chart(fig_q, use_container_width=True)

        # Net Income by quarter
        st.markdown("<div class='section-header'>QUARTERLY NET INCOME</div>", unsafe_allow_html=True)
        fig_ni = go.Figure()
        fig_ni.add_trace(go.Scatter(x=df_q["label"], y=df_q["net_income"]/1e9,
                                    mode="lines+markers+text",
                                    line={"color": "#3fb950", "width": 2},
                                    text=[f"${v/1e9:.1f}B" if v else "" for v in df_q["net_income"]],
                                    textposition="top center",
                                    hovertemplate="<b>%{x}</b><br>Net Income: $%{y:.2f}B<extra></extra>",
                                    fill="tozeroy", fillcolor="rgba(63,185,80,0.08)"))
        fig_ni.update_layout(plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                              font={"color": "#c9d1d9"}, height=280,
                              xaxis={"gridcolor": "#21262d"},
                              yaxis={"gridcolor": "#21262d", "title": "Net Income (Billions)"},
                              margin={"l": 0, "r": 0, "t": 10, "b": 0})
        st.plotly_chart(fig_ni, use_container_width=True)

        # Quarterly data table
        st.markdown("<div class='section-header'>QUARTERLY RAW DATA</div>", unsafe_allow_html=True)
        display_cols = ["label", "revenue", "net_income", "operating_income", "eps_diluted", "end_date", "sec_filing_url"]
        df_show = df_q[[c for c in display_cols if c in df_q.columns]].copy()
        
        # Format metrics
        for col in ["revenue", "net_income", "operating_income"]:
            if col in df_show.columns:
                df_show[col] = df_show[col].apply(lambda v: fmt_b(v) if v else "–")
        if "eps_diluted" in df_show.columns:
            df_show["eps_diluted"] = df_show["eps_diluted"].apply(lambda v: f"${v:.2f}" if v else "–")

        df_show.columns = [c.replace("_", " ").title() for c in df_show.columns]
        
        st.dataframe(
            df_show.set_index("Label"),
            use_container_width=True,
            column_config={
                "Sec Filing Url": st.column_config.LinkColumn(
                    "SEC Filing",
                    help="Official SEC EDGAR Filing",
                    validate="^https://",
                    display_text="View 10-Q ↗"
                )
            }
        )


# ════════════════════════════════════════════════════════════
# VIEW 3: SECTOR HEATMAP
# ════════════════════════════════════════════════════════════
elif view == "🌍 Sector Heatmap":
    st.markdown("<div class='section-header'>CROSS-COMPANY SECTOR SNAPSHOT</div>", unsafe_allow_html=True)

    with st.spinner("Loading sector data..."):
        df_sector = load_sector_snapshot()

    if df_sector.empty:
        st.warning("No data available. Run the ingestor first.")
    else:
        col_m, col_s = st.columns(2)
        with col_m:
            metric_choice = st.selectbox("Metric to visualise",
                ["revenue", "net_income", "op_income", "cash", "equity", "assets"])
        with col_s:
            all_sectors = sorted(df_sector["sector"].dropna().unique().tolist())
            selected_sector = st.selectbox("Filter by sector", ["All"] + all_sectors)

        # Metric label map
        label_map = {
            "revenue": "Annual Revenue",  "net_income": "Net Income",
            "op_income": "Operating Income", "cash": "Cash & Equivalents",
            "equity": "Total Equity",    "assets": "Total Assets",
        }

        df_heat = df_sector.copy().dropna(subset=[metric_choice])
        if selected_sector != "All":
            df_heat = df_heat[df_heat["sector"] == selected_sector]

        if df_heat.empty:
            st.warning(f"No data available for {selected_sector} in the heatmap.")
        else:
            df_heat["value_fmt"] = df_heat[metric_choice].apply(fmt_b)
            df_heat["value_b"]   = df_heat[metric_choice] / 1e9

        fig_heat = px.treemap(
            df_heat,
            path=["sector", "ticker"],
            values="value_b",
            color="value_b",
            color_continuous_scale=[[0,"#1c2128"], [0.5,"#1f6feb"], [1,"#3fb950"]],
            custom_data=["company", "value_fmt"],
            title=f"Sector Treemap — {label_map[metric_choice]}",
        )
        fig_heat.update_traces(
            hovertemplate="<b>%{customdata[0]}</b><br>%{customdata[1]}<extra></extra>",
            texttemplate="<b>%{label}</b><br>%{customdata[1]}",
            textfont_size=13,
        )
        fig_heat.update_layout(
            paper_bgcolor="#0d1117", font={"color": "#c9d1d9"},
            margin={"l": 0, "r": 0, "t": 40, "b": 0}, height=420,
            coloraxis_showscale=False,
        )
        st.plotly_chart(fig_heat, use_container_width=True)

        # Metrics comparison table
        st.markdown("<div class='section-header'>COMPARISON TABLE (LATEST FY)</div>", unsafe_allow_html=True)

        table_data = []
        for _, row in df_heat.iterrows():
            nm = (row["net_income"]/row.get("revenue")*100) if (row["net_income"] and row.get("revenue")) else None
            roe = (row["net_income"]/row["equity"]*100) if (row["net_income"] and row["equity"]) else None
            table_data.append({
                "Ticker":    row["ticker"],
                "Company":   row["company"],
                "Sector":    row["sector"],
                "Revenue":   fmt_b(row.get("revenue")),
                "Net Income": fmt_b(row["net_income"]),
                "Net Margin": f"{nm:.1f}%" if nm else "–",
                "ROE":        f"{roe:.1f}%" if roe else "–",
                "Cash":       fmt_b(row["cash"]),
                "EPS":        f"${row['eps']:.2f}" if row["eps"] else "–",
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
        st.plotly_chart(fig_bar, use_container_width=True)


# ════════════════════════════════════════════════════════════
# VIEW 4: CORPORATE ECOSYSTEM
# ════════════════════════════════════════════════════════════
elif view == "🌐 Corporate Ecosystem":
    st.markdown(f"<div class='section-header'>CORPORATE CONNECTIONS — {ticker}</div>", unsafe_allow_html=True)

    # Task 2-C: UI-triggered ecosystem pipeline
    eco_hdr, eco_btn_col = st.columns([3, 1])
    with eco_btn_col:
        if st.button("🌐 Update Ecosystem", use_container_width=True, key="update_ecosystem_btn"):
            with st.spinner("⏳ Scanning SEC 8-K filings for new relationships…"):
                try:
                    subprocess.run(
                        ["python", "ecosystem.py"],
                        check=False, timeout=180,
                    )
                    st.success("✅ Ecosystem knowledge graph updated!")
                except Exception as e:
                    st.error(f"Ecosystem update error: {e}")
            st.cache_data.clear()
            st.rerun()

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

        # Connection graph removed per user request

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


# ════════════════════════════════════════════════════════════
# VIEW 5: INTELLIGENCE FEED
# ════════════════════════════════════════════════════════════
elif view == "📰 Intelligence Feed":
    st.markdown(f"<div class='section-header'>MARKET INTELLIGENCE — {ticker}</div>", unsafe_allow_html=True)

    # Task 2-B: Refresh News pipeline button
    feed_col, btn_col = st.columns([3, 1])
    with btn_col:
        if st.button("📰 Refresh News", use_container_width=True, key="refresh_news_btn"):
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
        search_q = st.text_input("🔍 Semantic search headlines...", placeholder="e.g. AI investment strategy, supply chain risk")

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
            news_label = "📖 READ ARTICLE (VAULT)" if archived_url else "🔗 READ ARTICLE"
            news_color = "#3fb950" if archived_url else "#58a6ff"
            
            st.markdown(f"""
            <div class='news-item {css_class}'>
                <div style='display:flex; justify-content:space-between; align-items:flex-start;'>
                    <div style='font-weight:600; color:#f0f6fc; font-size:14px; flex:1;'>{headline}</div>
                    <a href='{primary_news_url}' target='_blank' style='color:{news_color}; font-size:12px; font-weight:700; text-decoration:none;'>
                        {news_label} ↗
                    </a>
                </div>
                <div style='color:#8b949e; font-size:11px; margin-top:4px;'>
                    {source} · {pub_dt} {f'· <a href="{url}" target="_blank" style="color:#8b949e;">Source ↗</a>' if archived_url and url else ""}
                </div>
                {f"<div style='color:#c9d1d9; font-size:12px; margin-top:6px;'>{summary}...</div>" if summary else ""}
            </div>""", unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════
# VIEW 6: REPORT ARCHIVE (2-Year History)
# ════════════════════════════════════════════════════════════
elif view == "📋 Report Archive":
    st.markdown(f"<div class='section-header'>REPORT ARCHIVE — {ticker} ({len(all_reports)} reports)</div>", unsafe_allow_html=True)

    # Task 2-D: UI-triggered report generation pipeline
    arc_hdr, arc_btn_col = st.columns([3, 1])
    with arc_btn_col:
        if st.button("📊 Generate Reports", use_container_width=True, key="gen_reports_btn"):
            with st.spinner("⏳ Running 5-agent reporting pipeline… (this may take ~2 minutes)"):
                try:
                    subprocess.run(
                        ["python", "intelligence.py", "--ticker", ticker],
                        check=False, timeout=300,
                    )
                    st.success("✅ Reports generated and stored!")
                except Exception as e:
                    st.error(f"Report generation error: {e}")
            st.cache_data.clear()
            st.rerun()

    if not all_reports:
        st.info(f"No reports yet. Run `python intelligence.py` to generate reports for {ticker}.")
    else:
        # Archive index
        col_idx, col_report = st.columns([1, 2])

        with col_idx:
            st.markdown("<div style='color:#8b949e; font-size:11px; font-weight:700; margin-bottom:8px;'>SELECT REPORT</div>", unsafe_allow_html=True)

            report_options = []
            for i, r in enumerate(all_reports):
                created = r.get("created_at", "")[:10]
                fy      = r.get("fiscal_year", "")
                period  = r.get("fiscal_period", "")
                status  = r.get("verification_status", "PENDING")
                score   = r.get("compliance_score") or 0  # guard against NULL from DB

                status_icon = {"VERIFIED": "✅", "FLAGGED": "⚠️",
                               "REJECTED": "❌", "PENDING": "⏳"}.get(status, "⏳")
                report_options.append(f"{status_icon} FY{fy} {period} · {created}")

            selected_idx = st.radio("", range(len(report_options)),
                                    format_func=lambda i: report_options[i],
                                    label_visibility="collapsed")

        selected_report = all_reports[selected_idx]

        with col_report:
            r = selected_report
            status = r.get("verification_status", "PENDING")
            st.markdown(f"""
            <div class='metric-card' style='margin-bottom:12px;'>
                <div style='display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;'>
                    <div>
                        <div class='metric-label'>VERIFICATION STATUS</div>
                        <div class='metric-value'></div>
                    </div>
                    <div>{badge(status)}</div>
                </div>
                <div style='color:#8b949e; font-size:11px; margin-top:6px;'>Generated: {created} · By ComplianceAgent_v1</div>
            </div>""", unsafe_allow_html=True)

            # Audit log
            audit_log = r.get("audit_log", [])
            if audit_log and isinstance(audit_log, list) and audit_log:
                with st.expander("🔍 View Audit Trail"):
                    for entry in audit_log:
                        ts  = entry.get("timestamp", "")[:19].replace("T"," ")
                        act = entry.get("action", "")
                        detail = entry.get("detail", "")
                        mismatches = entry.get("mismatches", [])
                        st.markdown(f"**{ts}** — {act}: {detail}")
                        if mismatches:
                            for mm in mismatches:
                                sev = mm.get("severity", "LOW")
                                sev_color = {"HIGH":"#f85149","MEDIUM":"#e3b341","LOW":"#8b949e"}.get(sev,"#8b949e")
                                st.markdown(
                                    f"<div style='background:#21262d; border-left:3px solid {sev_color}; "
                                    f"padding:6px 10px; border-radius:0 6px 6px 0; margin:4px 0; font-size:12px;'>"
                                    f"<b style='color:{sev_color};'>[{sev}]</b> {mm.get('claim_in_report','')}<br>"
                                    f"<span style='color:#8b949e;'>Vault: {mm.get('vault_value','')}</span></div>",
                                    unsafe_allow_html=True
                                )

            # Data snapshot
            snap = r.get("data_snapshot", {})
            if snap:
                with st.expander("📊 Data Snapshot used for verification"):
                    snap_df = pd.DataFrame([{"Metric": k, "Value": v} for k, v in snap.items()])
                    st.dataframe(snap_df.set_index("Metric"), use_container_width=True)

            # SEC link
            sec_url = r.get("sec_filing_url")
            if sec_url:
                st.markdown(f'<a href="{sec_url}" target="_blank" style="background:#1f3a5f; border:1px solid #1f6feb; color:#79c0ff; padding:8px 18px; border-radius:8px; text-decoration:none; font-size:13px; font-weight:600; display:inline-block; margin-bottom:12px;">📄 OPEN SEC FILING ↗</a>', unsafe_allow_html=True)

            # Full report markdown
            report_md = r.get("report_markdown", "")
            if report_md:
                with st.expander("📄 Full Research Report", expanded=(selected_idx == 0)):
                    st.markdown(report_md)
            else:
                st.info("No report content stored.")

        # Compliance score history chart (across all reports)
        if len(all_reports) > 1:
            st.markdown("<div class='section-header'>COMPLIANCE SCORE HISTORY</div>", unsafe_allow_html=True)
            hist_df = pd.DataFrame([{
                "date":  r.get("created_at","")[:10],
                "score": r.get("compliance_score", 0),
                "status": r.get("verification_status","PENDING"),
            } for r in reversed(all_reports)])
            hist_df = hist_df.sort_values("date")

            color_map = {"VERIFIED":"#3fb950","FLAGGED":"#e3b341","REJECTED":"#f85149","PENDING":"#8b949e"}
            fig_comp = go.Figure()
            fig_comp.add_trace(go.Scatter(
                x=hist_df["date"], y=hist_df["score"],
                mode="lines+markers",
                line={"color": "#58a6ff", "width": 2},
                marker={"color": [color_map.get(s, "#8b949e") for s in hist_df["status"]], "size": 10},
                hovertemplate="<b>%{x}</b><br>Score: %{y:.0f}/100<extra></extra>",
            ))
            fig_comp.add_hline(y=85, line_dash="dot", line_color="#3fb950",
                               annotation_text="VERIFIED threshold", annotation_position="right")
            fig_comp.update_layout(plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                                   font={"color": "#c9d1d9"}, height=250,
                                   xaxis={"gridcolor": "#21262d"},
                                   yaxis={"gridcolor": "#21262d", "title": "Compliance Score", "range": [0, 105]},
                                   margin={"l": 0, "r": 0, "t": 10, "b": 0})
            st.plotly_chart(fig_comp, use_container_width=True)


# ════════════════════════════════════════════════════════════
# VIEW 7: DOCUMENT EXTRACTOR
# ════════════════════════════════════════════════════════════
elif view == "📥 Document Extractor":
    st.markdown("<div class='section-header'>DOCUMENT EXTRACTOR</div>", unsafe_allow_html=True)
    st.markdown("<div style='color:#8b949e; font-size:13px; margin-bottom:16px;'>Extract financial data from URLs, YouTube videos, or uploaded files (PDF, DOCX, TXT, MD). Powered by Gemini structured extraction.</div>", unsafe_allow_html=True)

    # Input section (Hidden if extraction successful for a 'clean' look)
    extract_url_btn = False
    extract_file_btn = False
    extract_url = None
    uploaded_file = None
    
    if "last_extraction" not in st.session_state:
        ext_col1, ext_col2 = st.columns(2)

        with ext_col1:
            st.markdown("<div style='color:#c9d1d9; font-size:13px; font-weight:600; margin-bottom:8px;'>🌐 Extract from URL</div>", unsafe_allow_html=True)
            extract_url = st.text_input("URL", placeholder="https://company.com/investor-relations or YouTube URL", label_visibility="collapsed", key="extract_url")
            url_company = st.text_input("Company Name (optional)", placeholder="Auto-detect if blank", key="url_company")
            url_ticker = st.text_input("Ticker (optional)", placeholder="Auto-detect if blank", key="url_ticker")
            extract_url_btn = st.button("🔍 Extract from URL", use_container_width=True, key="extract_url_btn")

        with ext_col2:
            st.markdown("<div style='color:#c9d1d9; font-size:13px; font-weight:600; margin-bottom:8px;'>📎 Upload File</div>", unsafe_allow_html=True)
            uploaded_file = st.file_uploader("Upload", type=["pdf", "docx", "txt", "md"], label_visibility="collapsed", key="file_upload")
            file_company = st.text_input("Company Name (optional)", placeholder="Auto-detect if blank", key="file_company")
            file_ticker = st.text_input("Ticker (optional)", placeholder="Auto-detect if blank", key="file_ticker")
            extract_file_btn = st.button("📄 Extract from File", use_container_width=True, key="extract_file_btn")
    else:
        st.markdown("""
        <div style='background:rgba(63,185,80,0.1); border:1px solid #3fb950; border-radius:8px; padding:16px; margin-bottom:24px; text-align:center;'>
            <div style='color:#3fb950; font-size:24px; margin-bottom:8px;'>✨ Extraction Successful!</div>
            <div style='color:#8b949e; font-size:14px;'>The data has been extracted, normalized to USD, and synchronized to the institutional vault.</div>
        </div>
        """, unsafe_allow_html=True)
        if st.button("🔄 New Extraction", use_container_width=True):
            del st.session_state["last_extraction"]
            st.rerun()

    # Process URL extraction
    if extract_url_btn and extract_url:
        with st.spinner("Extracting content and analyzing financials..."):
            try:
                engine = ExtractorEngine()
                result = engine.process(
                    extract_url,
                    ticker_override=url_ticker,
                    company_override=url_company
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
        with st.spinner("Parsing file and analyzing financials..."):
            try:
                engine = ExtractorEngine()
                file_bytes = uploaded_file.getvalue()
                result = engine.process(
                    file_bytes,
                    filename=uploaded_file.name,
                    ticker_override=file_ticker,
                    company_override=file_company
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
            
            col1, col2, col3 = st.columns(3)
            
            col1.metric("Rows Extracted", "1")
            col2.metric("Rows Inserted", "1" if not result.get("rejected") else "0")
            # Calculate coverage from actual row data
            row_data = result.get("row", {})
            _numeric_fields = ["revenue", "net_income", "operating_income", "total_assets",
                               "total_liabilities", "total_equity", "cash_on_hand", "eps_diluted"]
            _filled = sum(1 for f in _numeric_fields if row_data.get(f) is not None)
            cov = (_filled / len(_numeric_fields)) * 100
            col3.metric("Coverage %", f"{cov:.0f}%")
            
            st.markdown("### Data Preview")
            if "row" in result:
                import pandas as pd
                df = pd.DataFrame([result["row"]])
                st.dataframe(df)
            
            with st.expander("⚠️ Debug Info"):
                st.json({
                    "mapped_fields": result.get("mapped_fields", {}),
                    "unmapped_fields": result.get("unmapped_fields", {}),
                    "rejected_rows": result.get("rejected", {})
                })

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
            use_container_width=True,
            column_config={
                "Vault Link": st.column_config.LinkColumn(
                    "🏦 Vault Link",
                    help="Secure link to the archived document in Azure",
                    display_text="View in Vault ↗"
                ),
                "Source": st.column_config.TextColumn("Source", width="medium"),
            }
        )

# ════════════════════════════════════════════════════════════
# VIEW 8: REPORT BUILDER (Task 3 — Prompt-Based Strategic Reports)
# ════════════════════════════════════════════════════════════
elif view == "📝 Report Builder":
    st.markdown("<div class='section-header'>REPORT BUILDER</div>", unsafe_allow_html=True)
    st.markdown(
        "<div style='color:#8b949e; font-size:13px; margin-bottom:16px;'>"
        "Generate institutional-grade equity research or custom strategic reports "
        "using all available data, powered by a 5-agent AI pipeline and custom prompts."
        "</div>",
        unsafe_allow_html=True
    )

    # Report type selector
    report_mode = st.radio(
        "Report Mode",
        ["📋 Standard Research Report", "🎯 Custom Strategic Report"],
        horizontal=True,
    )

    company_options_list = list(TARGET_COMPANIES.keys())
    default_index = company_options_list.index(ticker) if ticker in company_options_list else 0
    selected_company = st.selectbox("Select Primary Company:", company_options_list, index=default_index)

    if report_mode == "📋 Standard Research Report":
        default_prompt = (
            "Generate an institutional equity research report for this company.\n\n"
            "Focus on:\n* Financial performance\n* Key developments\n"
            "* Risks\n* Forward outlook\n\nUse only available data. Do not hallucinate."
        )
        user_prompt = st.text_area(
            "Prompt / Instructions:",
            value=default_prompt,
            height=140,
            help="Customize what the report should focus on.",
        )

        # NEW: CONTEXT SELECTOR
        st.markdown("<div style='color:#c9d1d9; font-size:13px; font-weight:600; margin-bottom:8px;'>🧠 INCLUDE VAULTED KNOWLEDGE</div>", unsafe_allow_html=True)
        vaulted_docs = load_uploaded_docs(ticker)
        selected_doc_ids = []
        if vaulted_docs:
            for doc in vaulted_docs:
                if st.checkbox(f"📄 {doc['source_url']} ({doc['created_at'][:10]})", key=f"ctx_{doc['id']}"):
                    selected_doc_ids.append(doc['id'])
        else:
            st.info("No vaulted documents available for context selection.")

        if st.button("🚀 Generate Standard Report", use_container_width=True, type="primary"):
            # Combine context from selected documents
            context_text = ""
            if selected_doc_ids:
                sel_docs = [d for d in vaulted_docs if d['id'] in selected_doc_ids]
                for d in sel_docs:
                    context_text += f"\n--- Supplemental Source: {d['source_url']} ---\n{d.get('raw_text','')}\n"

            with st.spinner("Compiling cross-platform data and generating report… (~60 seconds)"):
                try:
                    from intelligence import ReportingChain
                    from supabase import create_client as sc
                    sb = sc(SUPABASE_URL, SUPABASE_KEY)
                    chain = ReportingChain(sb)
                    # Note: In a real implementation, context_text would be passed to the chain
                    result = chain.run(selected_company, prompt=user_prompt + context_text)
                    result["prompt"] = user_prompt
                    st.session_state["built_report"] = result
                    st.session_state["built_report_mode"] = "standard"
                    st.cache_data.clear()
                    st.success("✅ Report generated and stored in Supabase.")
                except Exception as e:
                    st.error(f"Report generation failed: {e}")

    else:
        # Task 3: Custom Strategic Report Builder
        st.markdown(
            "<div style='background:rgba(31,111,235,0.08); border:1px solid rgba(31,111,235,0.3); "
            "border-radius:8px; padding:14px; margin:10px 0;'>"
            "<div style='color:#58a6ff; font-size:12px; font-weight:700; margin-bottom:6px;'>💡 STRATEGIC REPORT EXAMPLES</div>"
            "<div style='color:#8b949e; font-size:12px;'>"
            "• \"If my company wants to collaborate with Apple, how should we pitch and compete with existing suppliers?\"<br>"
            "• \"We want to acquire a Tesla competitor — what's the strategic fit and integration risk?\"<br>"
            "• \"How should Amazon's supply chain strategy inform our logistics partnership pitch?\"<br>"
            "• \"Compare Microsoft and Alphabet as potential cloud infrastructure partners for my fintech startup.\""
            "</div></div>",
            unsafe_allow_html=True,
        )

        user_prompt = st.text_area(
            "Enter Your Strategic Query:",
            placeholder=(
                "Example: If my company wants to collaborate with Apple, how should we "
                "pitch and compete with existing suppliers?"
            ),
            height=140,
            help="Mention specific companies, deals, or scenarios. Gemini will detect and analyze secondary companies automatically.",
        )

        if st.button("🎯 Generate Strategic Report", use_container_width=True, type="primary"):
            if not user_prompt.strip():
                st.warning("⚠️ Please enter a strategic query.")
            else:
                with st.spinner("🧠 Analyzing companies, detecting relationships, generating strategic intelligence…"):
                    try:
                        from intelligence import generate_custom_report
                        from supabase import create_client as sc
                        sb = sc(SUPABASE_URL, SUPABASE_KEY)
                        report_md = generate_custom_report(
                            ticker=selected_company,
                            user_prompt=user_prompt,
                            supabase=sb,
                        )
                        st.session_state["built_report"] = {
                            "report_markdown": report_md,
                            "quant": {},
                            "documents": [],
                            "prompt": user_prompt
                        }
                        st.session_state["built_report_mode"] = "custom"
                        st.cache_data.clear()
                        st.success("✅ Strategic report generated and stored in Supabase.")
                    except Exception as e:
                        st.error(f"Strategic report generation failed: {e}")

    # ── Display generated report ──────────────────────────────────────────────
    if "built_report" in st.session_state:
        result = st.session_state["built_report"]
        report_mode_tag = st.session_state.get("built_report_mode", "standard")

        quant = result.get("quant", {})
        docs  = result.get("documents", [])

        if not quant and not docs and report_mode_tag == "standard":
            st.warning("⚠️ Limited data available for this company. Consider running the financial ingestor first.")

        report_md = result.get("report_markdown", "")
        prompt_used = result.get("prompt", "")
        if report_md:
            mode_label = "CUSTOM STRATEGIC REPORT" if report_mode_tag == "custom" else "GENERATED REPORT"
            st.markdown(f"<div class='section-header'>{mode_label}</div>", unsafe_allow_html=True)
            
            if prompt_used:
                with st.expander("📋 View Generation Instructions"):
                    st.markdown(f"**Prompt/Instructions Used:**\n\n```text\n{prompt_used}\n```")
                    
            # Download button
            st.download_button(
                "⬇️ Download Report (Markdown)",
                data=report_md,
                file_name=f"{selected_company}_report.md",
                mime="text/markdown",
            )
            st.markdown(report_md)

        # Sources panel
        sec_url = quant.get("sec_filing_url") if quant else None
        if sec_url or docs:
            st.markdown("<div class='section-header' style='margin-top:24px;'>SOURCES</div>", unsafe_allow_html=True)
            source_cols = st.columns(2)
            with source_cols[0]:
                if sec_url:
                    st.markdown(f"""
                    <div class='report-card' style='border-left:3px solid #1f6feb;'>
                        <div style='color:#58a6ff; font-size:11px; font-weight:700;'>SEC FILING</div>
                        <a href='{sec_url}' target='_blank' style='color:#79c0ff; font-size:13px;'>Open Filing ↗</a>
                    </div>""", unsafe_allow_html=True)
            with source_cols[1]:
                for doc in docs[:5]:
                    src = doc.get("source_url") or doc.get("file_name") or "Document"
                    file_url = doc.get("file_url")
                    link = f"<a href='{file_url}' target='_blank' style='color:#79c0ff; font-size:12px;'>Download ↗</a>" if file_url else ""
                    st.markdown(f"""
                    <div class='report-card' style='border-left:3px solid #3fb950; margin-bottom:8px;'>
                        <div style='color:#3fb950; font-size:11px; font-weight:700;'>EXTRACTED DOCUMENT</div>
                        <div style='color:#c9d1d9; font-size:13px;'>{src[:60]}</div>
                        {link}
                    </div>""", unsafe_allow_html=True)

        if st.button("🗑️ Clear Report", key="clear_report_btn"):
            del st.session_state["built_report"]
            if "built_report_mode" in st.session_state:
                del st.session_state["built_report_mode"]
            st.rerun()


# ════════════════════════════════════════════════════════════
# VIEW 9: AI RESEARCH CHAT
# ════════════════════════════════════════════════════════════
elif view == "💬 AI Research Chat":
    st.markdown(f"<div class='section-header'>AI RESEARCH ASSISTANT — {ticker}</div>", unsafe_allow_html=True)
    st.markdown("<div style='color:#8b949e; font-size:13px; margin-bottom:16px;'>Ask anything about this company's financials, news, or competitive position. Powered by Gemini 2.5 Flash + live vault data.</div>", unsafe_allow_html=True)

    # Build context from vault
    fin_context = ""
    if not df_fy.empty:
        latest = df_fy.iloc[-1].to_dict()
        prior  = df_fy.iloc[-2].to_dict() if len(df_fy) > 1 else {}
        rev = latest.get("revenue")
        ni  = latest.get("net_income")
        op  = latest.get("operating_income")
        eps = latest.get("eps_diluted")
        fin_context = f"""
FINANCIAL DATA (from SEC EDGAR vault, FY{latest.get('fiscal_year')}):
- Revenue: {fmt_b(rev)}
- Net Income: {fmt_b(ni)}  
- Operating Income: {fmt_b(op)}
- EPS (Diluted): {f"${eps:.2f}" if eps else "N/A"}
- Net Margin: {f"{ni/rev*100:.1f}%" if (ni and rev) else "N/A"}
- Cash on Hand: {fmt_b(latest.get("cash_on_hand"))}
- Total Assets: {fmt_b(latest.get("total_assets"))}
- Prior Year Revenue: {fmt_b(prior.get("revenue") if prior else None)}
- SEC Filing URL: {latest.get("sec_filing_url", "N/A")}
"""

    news_headlines_list = []
    for n in news[:20]:
        if isinstance(n, dict):
            pub = n.get('published_at','')[:10]
            hld = n.get('headline','')
            news_headlines_list.append(f"- [{pub}] {hld}")
        else:
            news_headlines_list.append(f"- {n}")
    news_headlines = "\n".join(news_headlines_list)

    latest_report_ctx = ""
    if latest_report:
        latest_report_ctx = f"\nCOMPLIANCE STATUS: {latest_report.get('verification_status')}\n"

    system_context = f"""You are an expert institutional equity research analyst.
You have access to the following live vault data for {ticker} — {company_names.get(ticker,'')}:

{fin_context}

RECENT NEWS HEADLINES:
{news_headlines}

{latest_report_ctx}

Answer questions clearly and concisely with references to specific numbers from the vault where relevant.
Format your response in markdown. Do not speculate beyond the data provided.
"""

    # Chat display
    for msg in st.session_state.chat_history:
        if msg["role"] == "user":
            st.markdown(f"<div class='chat-user'>{msg['content']}</div>", unsafe_allow_html=True)
        else:
            st.markdown(f"<div class='chat-ai'>{msg['content']}</div>", unsafe_allow_html=True)

    # Suggested questions
    if not st.session_state.chat_history:
        st.markdown("<div style='color:#8b949e; font-size:12px; margin:8px 0;'>💡 Try asking:</div>", unsafe_allow_html=True)
        suggestions = [
            f"What is {ticker}'s revenue trend over the past 3 years?",
            f"How profitable is {ticker} compared to peers?",
            f"What are the main risks facing {ticker}?",
            f"What does the latest SEC filing say about {ticker}'s growth?",
        ]
        cols = st.columns(2)
        for i, q in enumerate(suggestions):
            if cols[i % 2].button(q, key=f"sugg_{i}", use_container_width=True):
                st.session_state.chat_history.append({"role": "user", "content": q})
                with st.spinner("Thinking..."):
                    messages = [{"role": "user", "content": system_context + "\n\nUser question: " + q}]
                    response = call_gemini(messages[0]["content"], max_tokens=1500, temperature=0.3)
                st.session_state.chat_history.append({"role": "assistant", "content": response})
                st.rerun()

    # Input
    with st.form("chat_form", clear_on_submit=True):
        col_inp, col_btn = st.columns([5, 1])
        with col_inp:
            user_input = st.text_input("", placeholder=f"Ask about {ticker}...", label_visibility="collapsed")
        with col_btn:
            submitted = st.form_submit_button("Send →", use_container_width=True)

    if submitted and user_input.strip():
        st.session_state.chat_history.append({"role": "user", "content": user_input})

        full_prompt = system_context + "\n\nUser question: " + user_input
        with st.spinner("Analyzing vault data..."):
            response = call_gemini(full_prompt, max_tokens=1500, temperature=0.3)

        st.session_state.chat_history.append({"role": "assistant", "content": response})
        st.rerun()

    if st.session_state.chat_history:
        if st.button("🗑️ Clear Chat", use_container_width=False):
            st.session_state.chat_history = []
            st.rerun()
