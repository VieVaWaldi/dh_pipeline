-------------------------------------------------------------------
--- Core Data Model

-------------------------------------------------------------------
--- Schema & Types

CREATE SCHEMA IF NOT EXISTS core;

CREATE TYPE core.source_type AS ENUM ('cordis', 'arxiv', 'coreac', 'openaire');

-------------------------------------------------------------------
--- Main Models

--- Project

--CREATE TABLE core.project (
--  WIP placeholder
--);

--- Researchoutput

CREATE TABLE core.researchoutput (
    id SERIAL PRIMARY KEY,
    source_id TEXT NOT NULL, -- Original ID in source system
    source_system core.source_type NOT NULL,

    doi TEXT,
    arxiv_id TEXT,

    publication_date DATE,
    updated_date DATE,
    language_code TEXT,

    type TEXT, -- eg publication, relatedResult for cordis ...
    title TEXT NOT NULL,
    abstract TEXT,
    full_text TEXT,
    comment TEXT,

    funding_number TEXT,

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT unique_source_researchoutput UNIQUE (source_system, source_id)
);

CREATE INDEX idx_ro_source ON core.researchoutput(source_system, source_id);
CREATE INDEX idx_ro_doi ON core.researchoutput(doi) WHERE doi IS NOT NULL;
CREATE INDEX idx_ro_publication_date ON core.researchoutput(publication_date);
CREATE INDEX idx_ro_title_search ON core.researchoutput USING gin(to_tsvector('english', title));
CREATE INDEX idx_ro_title_lower_trgm ON core.researchoutput USING gin (LOWER(title) gin_trgm_ops);
CREATE INDEX idx_ro_abstract_search ON core.researchoutput USING gin(to_tsvector('english', coalesce(abstract, '')));
-- FULL TEXT INDEX LATER

--CREATE TABLE core.reference (
--  WIP placeholder
--)

--CREATE TABLE core.j_researchoutput_reference (
--  WIP placeholder
--)

--- Institution

--CREATE TABLE core.institution (
--  WIP placeholder - people want department here
--);

-------------------------------------------------------------------
--- Other Models

--- Topic

CREATE TABLE core.topic (
    id SERIAL PRIMARY KEY,
    source_system core.source_type NOT NULL,

    name TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT unique_source_topic UNIQUE (source_system, name)
);

CREATE INDEX idx_topic_name ON core.topic(name);
CREATE INDEX idx_topic_source ON core.topic(source_system);

CREATE TABLE core.j_researchoutput_topic (
    researchoutput_id INTEGER REFERENCES core.researchoutput(id) ON DELETE CASCADE,
    topic_id INTEGER REFERENCES core.topic(id) ON DELETE CASCADE,
    PRIMARY KEY (researchoutput_id, topic_id)
);

--- Author

CREATE TABLE core.author (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_author_name ON core.author(name);
CREATE INDEX idx_author_name_search ON core.author USING gin(to_tsvector('english', name));


CREATE TABLE core.j_researchoutput_author (
    researchoutput_id INTEGER REFERENCES core.researchoutput(id) ON DELETE CASCADE,
    author_id INTEGER REFERENCES core.author(id) ON DELETE CASCADE,
    role TEXT NOT NULL,  -- 'author', 'contributor', etc.
    position INTEGER,
    PRIMARY KEY (researchoutput_id, author_id)
);

--- Publisher ---

CREATE TABLE core.publisher (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_publisher_name ON core.publisher(name);
CREATE INDEX idx_publisher_name_search ON core.publisher USING gin(to_tsvector('english', name));

--- Journal ---

CREATE TABLE core.journal (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    -- issn TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_journal_name ON core.journal(name);
CREATE INDEX idx_journal_name_search ON core.journal USING gin(to_tsvector('english', name));

CREATE TABLE core.j_journal_publisher (
    journal_id INTEGER REFERENCES core.journal(id) ON DELETE CASCADE,
    publisher_id INTEGER REFERENCES core.publisher(id) ON DELETE CASCADE,
    PRIMARY KEY (journal_id, publisher_id)
);

CREATE TABLE core.j_researchoutput_journal (
    researchoutput_id INTEGER REFERENCES core.researchoutput(id) ON DELETE CASCADE,
    journal_id INTEGER REFERENCES core.journal(id) ON DELETE CASCADE,
    PRIMARY KEY (researchoutput_id, journal_id)
);

--- Link ---

CREATE TABLE core.link (
    id SERIAL PRIMARY KEY,
    url TEXT UNIQUE NOT NULL,
    type TEXT,
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_link_url ON core.link(url);
CREATE INDEX idx_link_type ON core.link(type);

CREATE TABLE core.j_researchoutput_link (
    researchoutput_id INTEGER REFERENCES core.researchoutput(id) ON DELETE CASCADE,
    link_id INTEGER REFERENCES core.link(id) ON DELETE CASCADE,
    PRIMARY KEY (researchoutput_id, link_id)
);

-----------------------------------------------
-- Update Triggers

CREATE OR REPLACE FUNCTION core.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_core_researchoutput_updated_at BEFORE UPDATE
ON core.researchoutput FOR EACH ROW EXECUTE PROCEDURE
core.update_updated_at_column();

--- Same Create Trigger gor all updated_at's

CREATE TRIGGER update_topic_updated_at BEFORE UPDATE
ON core.topic FOR EACH ROW EXECUTE PROCEDURE
core.update_updated_at_column();

CREATE TRIGGER update_author_updated_at BEFORE UPDATE
ON core.author FOR EACH ROW EXECUTE PROCEDURE
core.update_updated_at_column();

CREATE TRIGGER update_publisher_updated_at BEFORE UPDATE
ON core.publisher FOR EACH ROW EXECUTE PROCEDURE
core.update_updated_at_column();

CREATE TRIGGER update_journal_updated_at BEFORE UPDATE
ON core.journal FOR EACH ROW EXECUTE PROCEDURE
core.update_updated_at_column();

CREATE TRIGGER update_link_updated_at BEFORE UPDATE
ON core.link FOR EACH ROW EXECUTE PROCEDURE
core.update_updated_at_column();

CREATE TRIGGER update_openalex_updated_at BEFORE UPDATE
ON core_enrichment.openalex FOR EACH ROW EXECUTE PROCEDURE
core.update_updated_at_column();