-- ============================================================
-- INSTITUTIONAL INTELLIGENCE PLATFORM — MASTER SCHEMA
-- Consolidated from all migrations
-- ============================================================

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "vector";   -- pgvector for semantic search
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================
-- TABLE 1: FINANCIALS
-- Stores hard financial metrics + greedy flex_metrics JSONB
-- ============================================================
CREATE TABLE IF NOT EXISTS financials (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ticker              TEXT NOT NULL,
    company_name        TEXT NOT NULL,
    fiscal_year         INTEGER NOT NULL,
    fiscal_period       TEXT NOT NULL,          -- Q1, Q2, Q3, FY
    end_date            DATE NOT NULL,
    filing_type         TEXT DEFAULT '10-K',    -- 10-K or 10-Q

    -- Hard Columns (GAAP Core)
    revenue             NUMERIC(20, 2),
    net_income          NUMERIC(20, 2),
    total_assets        NUMERIC(20, 2),
    total_liabilities   NUMERIC(20, 2),
    total_equity        NUMERIC(20, 2),
    eps_diluted         NUMERIC(10, 4),
    operating_income    NUMERIC(20, 2),
    cash_on_hand        NUMERIC(20, 2),
    operating_expense   NUMERIC(20, 2),
    gross_profit        NUMERIC(20, 2),
    ebitda              NUMERIC(20, 2),
    free_cash_flow      NUMERIC(20, 2),

    -- Greedy Sweep
    flex_metrics        JSONB DEFAULT '{}',

    -- Source traceability
    sec_cik             TEXT,
    sec_filing_url      TEXT,    -- Main URL (Interactive if possible)
    sec_ix_url          TEXT,    -- Explicitly the interactive XBRL link
    sec_raw_url         TEXT,    -- Raw SEC archive folder
    data_source         TEXT DEFAULT 'SEC_EDGAR',

    -- Audit
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT financials_unique UNIQUE (ticker, fiscal_year, fiscal_period)
);

CREATE INDEX IF NOT EXISTS idx_financials_ticker   ON financials(ticker);
CREATE INDEX IF NOT EXISTS idx_financials_end_date ON financials(end_date DESC);
CREATE INDEX IF NOT EXISTS idx_financials_flex     ON financials USING GIN (flex_metrics);

-- ============================================================
-- TABLE 2: MARKET_INTELLIGENCE
-- 250 news headlines per stock + optional pgvector embeddings
-- ============================================================
CREATE TABLE IF NOT EXISTS market_intelligence (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ticker              TEXT NOT NULL,
    company_name        TEXT,

    -- News
    headline            TEXT NOT NULL,
    summary             TEXT,
    source              TEXT,
    url                 TEXT,
    published_at        TIMESTAMPTZ,
    sentiment           TEXT CHECK (sentiment IN ('positive','negative','neutral','mixed')),
    sentiment_score     NUMERIC(4, 3),
    category            TEXT,

    -- Vector embedding (768-d from Gemini embedding-001)
    embedding           vector(768),

    -- Agent layer
    financial_analysis  TEXT,
    key_entities        JSONB DEFAULT '[]',
    risk_flags          JSONB DEFAULT '[]',
    opportunity_signals JSONB DEFAULT '[]',

    data_source         TEXT DEFAULT 'FINNHUB',
    created_at          TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT market_intel_unique UNIQUE (ticker, headline, published_at)
);

CREATE INDEX IF NOT EXISTS idx_mi_ticker    ON market_intelligence(ticker);
CREATE INDEX IF NOT EXISTS idx_mi_published ON market_intelligence(published_at DESC);

CREATE INDEX IF NOT EXISTS idx_mi_embedding
    ON market_intelligence USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- ============================================================
-- TABLE 3: CORPORATE_CONNECTIONS
-- Knowledge Graph: M&A, Investments, Partnerships, Supply Chain
-- ============================================================
CREATE TABLE IF NOT EXISTS corporate_connections (
    id                    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_ticker         TEXT NOT NULL,
    source_company        TEXT NOT NULL,
    target_company        TEXT NOT NULL,
    target_ticker         TEXT,

    relationship_type     TEXT NOT NULL CHECK (
        relationship_type IN (
            'ACQUISITION','INVESTMENT','PARTNERSHIP',
            'SUPPLIER','CUSTOMER','SUBSIDIARY',
            'JOINT_VENTURE','LICENSING','COMPETITOR','STRATEGIC_ALLIANCE'
        )
    ),
    relationship_detail   TEXT,
    deal_value_usd        NUMERIC(20, 2),
    announced_date        DATE,
    effective_date        DATE,
    status                TEXT DEFAULT 'ACTIVE' CHECK (
        status IN ('ANNOUNCED','PENDING','ACTIVE','TERMINATED','COMPLETED')
    ),

    sec_filing_url        TEXT,
    sec_form_type         TEXT,
    news_source_url       TEXT,
    data_source           TEXT DEFAULT 'SEC_8K',
    extraction_confidence NUMERIC(3,2),
    raw_excerpt           TEXT,

    created_at            TIMESTAMPTZ DEFAULT NOW(),
    updated_at            TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cc_source ON corporate_connections(source_ticker);
CREATE INDEX IF NOT EXISTS idx_cc_type   ON corporate_connections(relationship_type);

-- ============================================================
-- TABLE 4: REPORTS
-- Full audit trail — all historical reports stored
-- ============================================================
CREATE TABLE IF NOT EXISTS reports (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ticker              TEXT NOT NULL,
    company_name        TEXT NOT NULL,
    report_title        TEXT NOT NULL,
    fiscal_year         INTEGER,
    fiscal_period       TEXT,

    -- Content
    report_markdown     TEXT NOT NULL,
    executive_summary   TEXT,
    investment_thesis   TEXT,
    risk_factors        TEXT,

    -- Compliance
    verification_status TEXT DEFAULT 'PENDING' CHECK (
        verification_status IN ('PENDING','VERIFIED','REJECTED','FLAGGED')
    ),
    compliance_score    NUMERIC(5,2),
    verified_by_agent   TEXT DEFAULT 'ComplianceAgent_v1',
    verified_at         TIMESTAMPTZ,

    -- Audit log (append-only)
    audit_log           JSONB DEFAULT '[]',

    -- Data lineage
    source_financial_id UUID REFERENCES financials(id) ON DELETE SET NULL,
    sec_filing_url      TEXT,
    sec_ix_url          TEXT,
    sec_raw_url         TEXT,
    data_snapshot       JSONB DEFAULT '{}',
    report_documents    JSONB DEFAULT '[]',

    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_reports_ticker  ON reports(ticker);
CREATE INDEX IF NOT EXISTS idx_reports_status  ON reports(verification_status);
CREATE INDEX IF NOT EXISTS idx_reports_created ON reports(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_reports_fy      ON reports(ticker, fiscal_year);
CREATE INDEX IF NOT EXISTS idx_reports_audit   ON reports USING GIN (audit_log);

-- ============================================================
-- TABLE 5: EXTRACTED_DOCUMENTS (Structured Analysis)
-- ============================================================
CREATE TABLE IF NOT EXISTS extracted_documents (
    id                    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_name          TEXT,
    ticker                TEXT,
    source_type           TEXT NOT NULL CHECK (source_type IN ('url', 'file', 'youtube')),
    source_url            TEXT,
    file_url              TEXT,
    file_name             TEXT,
    raw_text              TEXT,
    extracted_financials  JSONB DEFAULT '{}',
    extracted_news        JSONB DEFAULT '[]',
    extracted_ecosystem   JSONB DEFAULT '[]',
    sector_info           TEXT,
    insights              TEXT,
    report_name           TEXT,
    extraction_status     TEXT DEFAULT 'PENDING' CHECK (
        extraction_status IN ('PENDING', 'SUCCESS', 'FAILED', 'PARTIAL')
    ),
    error_message         TEXT,
    data_source           TEXT DEFAULT 'DOCUMENT',
    created_at            TIMESTAMPTZ DEFAULT NOW(),
    updated_at            TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ed_ticker     ON extracted_documents(ticker);
CREATE INDEX IF NOT EXISTS idx_ed_status     ON extracted_documents(extraction_status);
CREATE INDEX IF NOT EXISTS idx_ed_created    ON extracted_documents(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ed_financials ON extracted_documents USING GIN (extracted_financials);
CREATE INDEX IF NOT EXISTS idx_ed_news       ON extracted_documents USING GIN (extracted_news);
CREATE INDEX IF NOT EXISTS idx_ed_ecosystem  ON extracted_documents USING GIN (extracted_ecosystem);

-- ============================================================
-- TABLE 6: EXTRACTED_DATA (Modular raw extraction)
-- ============================================================
CREATE TABLE IF NOT EXISTS extracted_data (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id    TEXT        NOT NULL,
    source     TEXT        NOT NULL,
    type       TEXT        NOT NULL CHECK (type IN ('youtube', 'web', 'file')),
    content    TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_extracted_data_user_id ON extracted_data (user_id);
CREATE INDEX IF NOT EXISTS idx_extracted_data_source  ON extracted_data (source);
CREATE INDEX IF NOT EXISTS idx_extracted_data_created_at ON extracted_data (created_at DESC);

-- ============================================================
-- TABLE 7: TARGET_COMPANIES (seed data)
-- ============================================================
CREATE TABLE IF NOT EXISTS target_companies (
    ticker       TEXT PRIMARY KEY,
    company_name TEXT NOT NULL,
    sec_cik      TEXT,
    sector       TEXT,
    added_at     TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO target_companies (ticker, company_name, sec_cik, sector) VALUES
    ('AAPL',  'Apple Inc.',            '0000320193', 'Technology'),
    ('AMZN',  'Amazon.com Inc.',       '0001018724', 'Consumer Discretionary'),
    ('MSFT',  'Microsoft Corporation', '0000789019', 'Technology'),
    ('GOOGL', 'Alphabet Inc.',         '0001652044', 'Communication Services'),
    ('TSLA',  'Tesla Inc.',            '0001318605', 'Consumer Discretionary')
ON CONFLICT (ticker) DO NOTHING;

-- ============================================================
-- RPC & HELPERS
-- ============================================================

CREATE OR REPLACE FUNCTION match_news(
    query_embedding vector(768),
    match_threshold FLOAT,
    match_count INT,
    p_ticker TEXT DEFAULT 'ALL'
)
RETURNS TABLE (
    id UUID, ticker TEXT, headline TEXT, summary TEXT,
    source TEXT, url TEXT, published_at TIMESTAMPTZ,
    sentiment TEXT, similarity FLOAT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        mi.id, mi.ticker, mi.headline, mi.summary,
        mi.source, mi.url, mi.published_at, mi.sentiment,
        1 - (mi.embedding <=> query_embedding) AS similarity
    FROM market_intelligence mi
    WHERE
        (p_ticker = 'ALL' OR mi.ticker = p_ticker)
        AND mi.embedding IS NOT NULL
        AND 1 - (mi.embedding <=> query_embedding) > match_threshold
    ORDER BY mi.embedding <=> query_embedding
    LIMIT match_count;
END;
$$;

CREATE OR REPLACE FUNCTION append_audit_log(p_report_id UUID, p_entry JSONB)
RETURNS VOID AS $$
BEGIN
    UPDATE reports
    SET audit_log = audit_log || jsonb_build_array(p_entry),
        updated_at = NOW()
    WHERE id = p_report_id;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- ============================================================
-- ROW LEVEL SECURITY
-- ============================================================
ALTER TABLE financials            ENABLE ROW LEVEL SECURITY;
ALTER TABLE market_intelligence   ENABLE ROW LEVEL SECURITY;
ALTER TABLE corporate_connections ENABLE ROW LEVEL SECURITY;
ALTER TABLE reports               ENABLE ROW LEVEL SECURITY;
ALTER TABLE extracted_documents   ENABLE ROW LEVEL SECURITY;
ALTER TABLE extracted_data        ENABLE ROW LEVEL SECURITY;

CREATE POLICY "service_role_all" ON financials            FOR ALL TO service_role USING (true);
CREATE POLICY "service_role_all" ON market_intelligence   FOR ALL TO service_role USING (true);
CREATE POLICY "service_role_all" ON corporate_connections FOR ALL TO service_role USING (true);
CREATE POLICY "service_role_all" ON reports               FOR ALL TO service_role USING (true);
CREATE POLICY "service_role_all" ON extracted_documents   FOR ALL TO service_role USING (true);
CREATE POLICY "service_role_all" ON extracted_data        FOR ALL TO service_role USING (true);

-- Anon / Public Policies (Adjust as needed)
CREATE POLICY "anon_read" ON financials            FOR SELECT TO anon USING (true);
CREATE POLICY "anon_read" ON market_intelligence   FOR SELECT TO anon USING (true);
CREATE POLICY "anon_read" ON corporate_connections FOR SELECT TO anon USING (true);
CREATE POLICY "anon_read" ON reports               FOR SELECT TO anon USING (true);
CREATE POLICY "anon_all"  ON extracted_documents   FOR ALL TO anon USING (true) WITH CHECK (true);
CREATE POLICY "anon_all"  ON extracted_data        FOR ALL TO anon USING (true) WITH CHECK (true);
