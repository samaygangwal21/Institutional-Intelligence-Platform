-- Institutional Intelligence Platform (IIP) - Supabase Schema
-- Run this in your Supabase SQL Editor to initialize the required tables.

-- 1. Target Companies
CREATE TABLE IF NOT EXISTS public.target_companies (
    ticker TEXT PRIMARY KEY,
    company_name TEXT NOT NULL,
    sec_cik TEXT,
    sector TEXT,
    fiscal_year_end_month INTEGER DEFAULT 12,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now())
);

-- 2. Financials
CREATE TABLE IF NOT EXISTS public.financials (
    id UUID DEFAULT extensions.uuid_generate_v4() PRIMARY KEY,
    ticker TEXT REFERENCES public.target_companies(ticker),
    fiscal_year INTEGER,
    fiscal_period TEXT,
    end_date DATE,
    revenues NUMERIC,
    net_income NUMERIC,
    eps NUMERIC,
    operating_income NUMERIC,
    free_cash_flow NUMERIC,
    total_assets NUMERIC,
    total_liabilities NUMERIC,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()),
    UNIQUE(ticker, fiscal_year, fiscal_period)
);

-- 3. Market Intelligence (News & Events)
CREATE TABLE IF NOT EXISTS public.market_intelligence (
    id UUID DEFAULT extensions.uuid_generate_v4() PRIMARY KEY,
    ticker TEXT REFERENCES public.target_companies(ticker),
    headline TEXT NOT NULL,
    published_at TIMESTAMP WITH TIME ZONE,
    url TEXT,
    archived_url TEXT,
    sentiment_score NUMERIC,
    summary TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()),
    UNIQUE(ticker, headline, published_at)
);

-- 4. Corporate Connections (Ecosystem)
CREATE TABLE IF NOT EXISTS public.corporate_connections (
    id UUID DEFAULT extensions.uuid_generate_v4() PRIMARY KEY,
    source_ticker TEXT REFERENCES public.target_companies(ticker),
    target_entity TEXT NOT NULL,
    relationship_type TEXT,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()),
    UNIQUE(source_ticker, target_entity, relationship_type)
);

-- 5. Extracted Documents (Vault)
CREATE TABLE IF NOT EXISTS public.extracted_documents (
    id UUID DEFAULT extensions.uuid_generate_v4() PRIMARY KEY,
    ticker TEXT REFERENCES public.target_companies(ticker),
    document_type TEXT,
    source_url TEXT,
    extracted_text TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now())
);

-- 6. Reports (Saved Syntheses)
CREATE TABLE IF NOT EXISTS public.reports (
    id UUID DEFAULT extensions.uuid_generate_v4() PRIMARY KEY,
    ticker TEXT REFERENCES public.target_companies(ticker),
    report_markdown TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now())
);

-- Note: To enable Vector search capabilities later, ensure pgvector is enabled:
-- CREATE EXTENSION IF NOT EXISTS vector;
