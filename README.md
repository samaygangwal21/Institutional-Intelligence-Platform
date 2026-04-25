# 🏛️ Company Insights (Flairminds Platform)

**AI-powered fund manager intelligence system for financial analysis, ecosystem mapping, and strategic research.**

---

## 🚀 Core Capabilities

- **High-Fidelity Financial Ingestion (ingest.py)** — Consolidates SEC EDGAR XBRL API ingestion with a professional 7-step extraction protocol for PDFs and URLs. Handles spoken numerical formats and strict unit normalization.
- **Corporate Ecosystem Discovery** — Scans SEC 8-K filings for material agreements, M&A, and investments using Gemini AI to maintain a live connection graph.
- **Institutional Reporting Engine (intelligence.py)** — A multi-agent AI pipeline that generates validated equity research reports and custom strategic scenarios with a full compliance audit trail.
- **Smart Quota Resilience** — Built-in key rotation and adaptive cooldown logic to bypass Gemini API rate limits automatically.
- **Standardized Data Vault** — All extracted intelligence is archived in Azure Blob Storage and synchronized with a Supabase Vector vault.

---

## 🏗️ Technical Architecture

```
SOURCES                           PLATFORM CORE ENGINE               VAULT & UI
SEC EDGAR REST API ──────────┐                                  ┌────────────────┐
Company News (Finnhub) ──────┤──► ingest.py ────────────────────┤ Supabase Vault │
Investor PDFs / URLs ────────┘    (Data Inflow & Normalization) │ (Structured)   │
                                                                │                │
Supabase Vault ──────────────┐                                  │ Azure Blobs    │
+ Gemini 1.5 Pro Agents ─────┤──► intelligence.py ──────────────┤ (Unstructured) │
                             │    (Reporting & Ecosystem)       └───────┬────────┘
                             │                                          │
User Dashboard ──────────────┘──► app.py ───────────────────────────────┘
                                  (9 Institutional Views)
```

---

## 🛠️ Setup & Execution

### Step 1 — Configuration
1. **Database**: Initialize Supabase with `schema.sql` (enables pgvector and tables).
2. **Environment**: Copy `.env.example` to `.env` and provide your API keys.
3. **Dependencies**: `pip install -r requirements.txt`

### Step 2 — Data Ingestion (`ingest.py`)
Run the central ingestion hub to populate the vault:
```bash
# Ingest 3 years of data for specific tickers
python ingest.py --ticker TSLA,AAPL --years 3

# Full web-scale ingestion (requires crawl4ai)
python ingest.py --url "https://ir.tesla.com" --ticker TSLA --company TESLA
```

### Step 3 — AI Intelligence & Reporting (`intelligence.py`)
Trigger the reporting pipeline or ecosystem scan:
```bash
# Generate institutional reports for vaulted companies
python intelligence.py --mode report --tickers TSLA,AAPL

# Refresh corporate connections from 8-K filings
python intelligence.py --mode ecosystem
```

### Step 4 — Launch Dashboard (`app.py`)
```bash
streamlit run app.py
```
*Accessible at: http://localhost:8501*

---

## 📂 Project Structure

- `app.py`: Central Streamlit dashboard and user interface.
- `ingest.py`: The data inflow hub (SEC API + 7-step document parser).
- `intelligence.py`: The AI engine for reports and ecosystem mapping.
- `platform_config.py`: Global settings, API key rotation, and connection handles.
- `utils.py`: Shared utilities for Azure vaulting, fuzzy matching, and formatting.

---

*Company Insights | Global Intelligence Hub*
*Flairminds Platform — Stable Release 2.0*
