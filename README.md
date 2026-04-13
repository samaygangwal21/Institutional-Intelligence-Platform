# 🏛️ Institutional Intelligence Platform (Initial Version)

**AI-powered fund manager intelligence system for financial analysis, ecosystem mapping, and strategic research.**

Identity: Doc-extract-and-report | samaygangwal21@gmail.com

---

## 🚀 Core Capabilities

- **Financial Data Ingestion (SEC + Documents)** — Pulls 3-year GAAP financials directly from SEC EDGAR XBRL API, with supplemental extraction from PDFs, URLs, and YouTube transcripts
- **Corporate Ecosystem Mapping** — Monitors SEC 8-K filings for material agreements; extracts M&A, partnerships, supplier relationships, and investments using Gemini AI into a live knowledge graph
- **AI-Powered Report Generation** — 5-agent pipeline (DataFetch → Quant → News → SeniorAnalyst → Compliance) generates institutional-grade equity research reports with full audit trails
- **Real-time Dashboard Analytics** — Streamlit dashboard with 7 views: Financial Overview, Quarterly Drill-Down, Sector Heatmap, Corporate Ecosystem, Intelligence Feed, Report Archive, AI Research Chat
- **Custom Strategic Report Builder** — Prompt-based strategic analysis engine: enter any scenario (partnership pitches, competitive analysis, acquisition strategy) and receive a structured investment banking-grade report grounded in vault data

---

## Architecture

```
SEC EDGAR REST API ──────────────┐
Finnhub News API ────────────────┤──► financial_ingestor.py ──► Supabase Vault
                                 │
SEC 8-K Filings ─────────────────┤──► ecosystem.py ────────────► corporate_connections
+ Gemini AI Extraction           │
                                 │
Supabase Vault ──────────────────┤──► reporting_chain.py ───────► reports (full history)
+ Gemini 5-Agent Chain           │
                                 │
Streamlit Dashboard ─────────────┘──► app.py (9 views)
```

---

## Setup

### Step 1 — Database

1. Go to [Supabase SQL Editor](https://supabase.com/dashboard)
2. Run `schema.sql` (enables pgvector + report history)
3. Enable pgvector: `CREATE EXTENSION vector;`

### Step 2 — Environment

```bash
cp .env.example .env
# Fill in: SUPABASE_URL, SUPABASE_KEY, GEMINI_API_KEY, FINNHUB_KEY
pip install -r requirements.txt
```

### Step 3 — Ingest Financial Data

```bash
# All companies (3-year window)
python financial_ingestor.py --cutoff-years 3

# Specific tickers only
python financial_ingestor.py --ticker TSLA,AAPL

# Skip news (faster)
python financial_ingestor.py --skip-news
```

### Step 4 — Discover Ecosystem (optional)

```bash
python ecosystem.py
```

### Step 5 — Generate Reports

```bash
python reporting_chain.py
```

Runs the 5-agent pipeline for all target companies:

1. **DataFetchAgent** → Pulls vault data
2. **QuantAgent** → Computes ratios, YoY growth
3. **NewsAnalystAgent** → Gemini news synthesis
4. **SeniorAnalystAgent** → Full Markdown report
5. **ComplianceAgent** → Audits numbers vs vault, stores with verdict

### Step 6 — Launch Dashboard

```bash
streamlit run app.py
```

Opens at: http://localhost:8501

---

## Dashboard Views

| View | Description |
|------|-------------|
| 📊 Financial Overview | KPI cards, annual revenue/NI trend, balance sheet. **🔄 Fetch Latest Financials** button triggers live ingestor. |
| 📈 Quarterly Drill-Down | Per-quarter revenue waterfall, net income trend |
| 🌍 Sector Heatmap | Treemap + comparison table across all 5 companies |
| 🌐 Corporate Ecosystem | Knowledge graph of M&A, suppliers, investments. **🌐 Update Ecosystem** button triggers 8-K scan. |
| 📰 Intelligence Feed | Semantic + keyword news search, sentiment filter. **📰 Refresh News** button triggers Finnhub fetch. |
| 📋 Report Archive | All historical reports, audit trail, compliance score history. **📊 Generate Reports** triggers full pipeline. |
| 📥 Document Extractor | Upload PDFs/DOCX or paste URLs — extracts financials directly into vault |
| 📝 Report Builder | Standard or **Custom Strategic** reports — enter any scenario prompt for investment-banking-grade analysis |
| 💬 AI Research Chat | Gemini-powered Q&A grounded in vault data |

---

## Fiscal Year End Months

| Company | Ticker | FY End Month |
|---------|--------|--------------|
| Apple Inc. | AAPL | September (9) |
| Amazon.com | AMZN | December (12) |
| Microsoft | MSFT | June (6) |
| Alphabet | GOOGL | December (12) |
| Tesla | TSLA | December (12) |

---

## Custom Strategic Report Builder

The **📝 Report Builder → Custom Strategic Report** mode accepts free-text prompts and generates structured analysis covering:

- Strategic fit and synergies
- Competitive landscape
- Supply chain positioning
- Differentiation strategy
- Pitch framework

**Example prompts:**
- *"If my company wants to collaborate with Apple, how should we pitch and compete with existing suppliers?"*
- *"Analyze the strategic rationale for acquiring a Tesla competitor."*
- *"Compare Microsoft and Alphabet as cloud infrastructure partners."*

Reports are automatically stored in the `reports` table with `report_type = CUSTOM` for future reference.

---

## ⚠️ Version Note

This is the initial working version of the platform. Several enhancements are actively being improved, including expanded ticker coverage, deeper multi-company cross-analysis, and real-time streaming report generation.

---

*Institutional Intelligence Platform (Initial Version) | Prototype*
