-----------------------------------------------
-- Arxiv Data Model

CREATE SCHEMA IF NOT EXISTS arxiv;

CREATE TABLE arxiv.entry (
    id SERIAL PRIMARY KEY,
    id_original TEXT UNIQUE NOT NULL,
    title TEXT UNIQUE NOT NULL,
    doi TEXT, -- UNIQUE
    summary TEXT,
	full_text TEXT,
    journal_ref TEXT,
    comment TEXT,
    primary_category TEXT,
    category_term TEXT,
    categories TEXT[],
    published_date TIMESTAMP,
    updated_date TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE arxiv.author (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    affiliation TEXT,
    affiliations TEXT[],
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE arxiv.j_entry_author (
    entry_id INTEGER REFERENCES arxiv.entry(id),
    author_id INTEGER REFERENCES arxiv.author(id),
    author_position INTEGER NOT NULL,
    PRIMARY KEY (entry_id, author_id)
);

CREATE TABLE arxiv.link (
    id SERIAL PRIMARY KEY,
    href TEXT NOT NULL,
    title TEXT,
    rel TEXT,
    type TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE arxiv.j_entry_link (
    entry_id INTEGER REFERENCES arxiv.entry(id),
    link_id INTEGER REFERENCES arxiv.link(id),
    PRIMARY KEY (entry_id, link_id)
);

-----------------------------------------------
-- Indexes

CREATE INDEX idx_entry_title ON arxiv.entry(title);
CREATE INDEX idx_entry_published_date ON arxiv.entry(published_date);
CREATE INDEX idx_entry_updated_date ON arxiv.entry(updated_date);
CREATE INDEX idx_entry_primary_category ON arxiv.entry(primary_category);
CREATE INDEX idx_author_name ON arxiv.author(name);

-----------------------------------------------
-- Update Triggers

CREATE OR REPLACE FUNCTION arxiv.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_entry_updated_at BEFORE UPDATE
ON arxiv.entry FOR EACH ROW EXECUTE PROCEDURE
arxiv.update_updated_at_column();

CREATE TRIGGER update_author_updated_at BEFORE UPDATE
ON arxiv.author FOR EACH ROW EXECUTE PROCEDURE
arxiv.update_updated_at_column();

CREATE TRIGGER update_link_updated_at BEFORE UPDATE
ON arxiv.link FOR EACH ROW EXECUTE PROCEDURE
arxiv.update_updated_at_column();