-----------------------------------------------
-- Coreac Data Model

CREATE SCHEMA IF NOT EXISTS coreac;

CREATE TABLE coreac.work (
    id SERIAL PRIMARY KEY,
    id_original TEXT UNIQUE NOT NULL, -- source id
    title TEXT NOT NULL,
    doi TEXT,
    arxiv_id TEXT,
    mag_id TEXT, -- ignore
    pubmed_id TEXT, -- ignore
    oai_ids TEXT[], -- ignore
    language_code TEXT,
    language_name TEXT, -- ignore
    document_type TEXT, -- type
    field_of_study TEXT, -- ignore
    abstract TEXT,
    fulltext TEXT,
    publisher TEXT, -- ignore
    authors TEXT[], -- junction to core.authors where the junction has the type contributor
    contributors TEXT[], -- junction to core.authors where the junction has the type contributor
    journals_title TEXT[], -- junction to core.journal
    download_url TEXT, -- junction to core.link where type is download
    outputs TEXT[], -- ignore
    source_fulltext_urls TEXT[], -- ignore
    year_published TEXT, -- ignore
    created_date TIMESTAMP, -- ignore
    updated_date TIMESTAMP, -- updated_date
    published_date TIMESTAMP, -- publication_date
    deposited_date TIMESTAMP, -- ignore
    accepted_date TIMESTAMP, -- ignore
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE coreac.link ( -- junction to core.link where type is type
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