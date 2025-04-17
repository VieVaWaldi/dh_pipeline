-----------------------------------------------
-- Coreac Data Model

CREATE SCHEMA IF NOT EXISTS coreac;

CREATE TABLE coreac.work (
    id SERIAL PRIMARY KEY,
    id_original TEXT UNIQUE NOT NULL,
    title TEXT UNIQUE NOT NULL,
    doi TEXT, -- UNIQUE
    arxiv_id TEXT, -- UNIQUE,
    mag_id TEXT, -- UNIQUE,
    pubmed_id TEXT, -- UNIQUE,
    oai_ids TEXT[],
    language_code TEXT,
    language_name TEXT,
    document_type TEXT,
    field_of_study TEXT,
    abstract TEXT,
    fulltext TEXT,
    publisher TEXT,
    authors TEXT[],
    contributors TEXT[],
    journals_title TEXT[],
    download_url TEXT,
    outputs TEXT[],
    source_fulltext_urls TEXT[],
    year_published TEXT,
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    published_date TIMESTAMP,
    deposited_date TIMESTAMP,
    accepted_date TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE coreac.link (
    id SERIAL PRIMARY KEY,
    url TEXT NOT NULL,
	type TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE coreac.j_work_link (
    work_id INTEGER REFERENCES coreac.work(id),
    link_id INTEGER REFERENCES coreac.link(id),
    PRIMARY KEY (work_id, link_id)
);

CREATE TABLE coreac.reference (
    id SERIAL PRIMARY KEY,
    id_original TEXT,
    title TEXT UNIQUE NOT NULL,
    authors TEXT[],
    date TIMESTAMP,
    doi TEXT,
    raw TEXT,
    cites TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE coreac.j_work_reference (
    work_id INTEGER REFERENCES coreac.work(id),
    reference_id INTEGER REFERENCES coreac.reference(id),
    PRIMARY KEY (work_id, reference_id)
);

CREATE TABLE coreac.data_provider (
    id SERIAL PRIMARY KEY,
    id_original TEXT,
    name TEXT NOT NULL,
    url TEXT,
    logo TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE coreac.j_work_data_provider (
    work_id INTEGER REFERENCES coreac.work(id),
    data_provider_id INTEGER REFERENCES coreac.data_provider(id),
    PRIMARY KEY (work_id, data_provider_id)
);

-----------------------------------------------
-- Indexes

CREATE INDEX idx_work_title ON coreac.work(title);
CREATE INDEX idx_work_year_published ON coreac.work(year_published);
CREATE INDEX idx_work_published_date ON coreac.work(published_date);
CREATE INDEX idx_reference_title ON coreac.reference(title);
CREATE INDEX idx_data_provider_name ON coreac.data_provider(name);

-- GIN indexes for text search
CREATE INDEX idx_work_abstract_gin ON coreac.work USING gin(to_tsvector('english', COALESCE(abstract, '')));
CREATE INDEX idx_work_title_gin ON coreac.work USING gin(to_tsvector('english', title));
-- CREATE INDEX idx_work_fulltext_gin ON coreac.work USING gin(to_tsvector('english', COALESCE(fulltext, '')));


-----------------------------------------------
-- Update Triggers

CREATE OR REPLACE FUNCTION coreac.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_work_updated_at BEFORE UPDATE
ON coreac.work FOR EACH ROW EXECUTE PROCEDURE
coreac.update_updated_at_column();

CREATE TRIGGER update_link_updated_at BEFORE UPDATE
ON coreac.link FOR EACH ROW EXECUTE PROCEDURE
coreac.update_updated_at_column();

CREATE TRIGGER update_reference_updated_at BEFORE UPDATE
ON coreac.reference FOR EACH ROW EXECUTE PROCEDURE
coreac.update_updated_at_column();

CREATE TRIGGER update_data_provider_updated_at BEFORE UPDATE
ON coreac.data_provider FOR EACH ROW EXECUTE PROCEDURE
coreac.update_updated_at_column();