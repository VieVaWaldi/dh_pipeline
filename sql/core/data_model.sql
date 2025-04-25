-------------------------------------------------------------------
--- Core Data Model

-- Identifying sources: source ENUM

-- extra schemas for enrichment: OA, CR

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE core.researchoutput (
    id SERIAL PRIMARY KEY,
    source_system TEXT NOT NULL, -- 'cordis', 'arxiv', or 'coreac'
    source_id TEXT NOT NULL,     -- Original ID in source system

    doi TEXT,
    arxiv_id TEXT,

    id_original TEXT UNIQUE NOT NULL,
    type TEXT NOT NULL, -- eg publication, relatedResult for cordis ...


    title TEXT NOT NULL,
    abstract TEXT,
    full_text TEXT,

    publication_date DATE,

    journal TEXT,

    summary TEXT,

    comment TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
