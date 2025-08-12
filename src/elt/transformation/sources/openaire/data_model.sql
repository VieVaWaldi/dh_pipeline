-----------------------------------------------
-- Openaire Data Model

CREATE SCHEMA IF NOT EXISTS openaire;

-----------------------------------------------
-- Core Entities

CREATE TABLE openaire.project (
    id SERIAL PRIMARY KEY,
    id_original TEXT UNIQUE NOT NULL,
    id_openaire TEXT UNIQUE NOT NULL,
    code TEXT NOT NULL,
    title TEXT NOT NULL,
    doi TEXT,
    acronym TEXT,
    start_date DATE,
    end_date DATE,
    duration TEXT,
    summary TEXT,
    keywords TEXT,

    -- Flags and mandates
    ec_article29_3 BOOLEAN,
    open_access_mandate_publications BOOLEAN,
    open_access_mandate_dataset BOOLEAN,
    ecsc39 BOOLEAN,

    -- Financial info
    total_cost FLOAT,
    funded_amount FLOAT,

    -- Web and call info
    website_url TEXT,
    call_identifier TEXT,

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE openaire.container (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    issn_printed TEXT,
    issn_online TEXT,
    issn_linking TEXT,

    -- Volume/issue/page info
    volume TEXT,
    issue TEXT,
    start_page TEXT,
    end_page TEXT,
    edition TEXT,

    -- Conference info
    conference_place TEXT,
    conference_date DATE,

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE openaire.researchoutput (
    id SERIAL PRIMARY KEY,
    id_original TEXT UNIQUE NOT NULL,
    main_title TEXT NOT NULL,
    sub_title TEXT,
    publication_date DATE,
    publisher TEXT,
    type TEXT,

    -- Language info
    language_code TEXT,
    language_label TEXT,

    -- Access and funding info
    open_access_color TEXT,
    publicly_funded BOOLEAN,
    is_green BOOLEAN,
    is_in_diamond_journal BOOLEAN,

    -- Description
    description TEXT,

    -- Citation metrics
    citation_count FLOAT,
    influence FLOAT,
    popularity FLOAT,
    impulse FLOAT,
    citation_class TEXT,
    influence_class TEXT,
    impulse_class TEXT,
    popularity_class TEXT,

    -- Container relationship
    container_id INTEGER REFERENCES openaire.container(id),

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE openaire.organization (
    id SERIAL PRIMARY KEY,
    original_id TEXT,
    legal_name TEXT UNIQUE NOT NULL,
    legal_short_name TEXT,
    is_first_listed BOOLEAN NOT NULL DEFAULT false,
    geolocation FLOAT[],
    alternative_names TEXT[],
    website_url TEXT,
    country_code TEXT,
    country_label TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-----------------------------------------------
-- Entities

CREATE TABLE openaire.subject (
    id SERIAL PRIMARY KEY,
	value TEXT UNIQUE NOT NULL,
	scheme TEXT,
    provenance_type TEXT,
    trust FLOAT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE openaire.measure (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    score TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-----------------------------------------------
-- Funding Entities

-- Funders (e.g., European Commission, Wellcome Trust)
CREATE TABLE openaire.funder (
    id SERIAL PRIMARY KEY,
    original_id TEXT UNIQUE NOT NULL,  -- Original ID from source
    name TEXT UNIQUE NOT NULL,         -- e.g., "European Commission"
    short_name TEXT NOT NULL,          -- e.g., "EC"
    jurisdiction TEXT NOT NULL,        -- e.g., "EU"
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Funding streams that can form a hierarchy
-- (e.g., H2020, Horizon Europe, or nested like EC::H2020::RIA)
CREATE TABLE openaire.funding_stream (
    id SERIAL PRIMARY KEY,
    original_id TEXT UNIQUE NOT NULL,   -- e.g., "EC::H2020::RIA"
    name TEXT NOT NULL,                 -- e.g., "Research and Innovation action"
    description TEXT,                   -- e.g., "Horizon 2020 Framework Programme - Research and Innovation action"
    parent_id INTEGER REFERENCES openaire.funding_stream(id),  -- FK For hierarchy
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- H2020 specific programmes
CREATE TABLE openaire.h2020_programme (
    id SERIAL PRIMARY KEY,
    code TEXT UNIQUE NOT NULL,          -- e.g., "H2020-EU.1.4.1.3."
    description TEXT,                   -- e.g., "Development, deployment and operation of ICT-based e-infrastructures"
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-----------------------------------------------
-- Research Output related entities

CREATE TABLE openaire.author (
    id SERIAL PRIMARY KEY,
    full_name TEXT UNIQUE NOT NULL,
    first_name TEXT,
    surname TEXT,
    pid TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-----------------------------------------------
-- Junction Tables

CREATE TABLE openaire.j_project_researchoutput (
    project_id INTEGER REFERENCES openaire.project(id) ON DELETE CASCADE,
    researchoutput_id INTEGER REFERENCES openaire.researchoutput(id) ON DELETE CASCADE,
    relation_type TEXT,
    PRIMARY KEY (project_id, researchoutput_id)
);

CREATE TABLE openaire.j_project_organization (
    project_id INTEGER REFERENCES openaire.project(id),
    organization_id INTEGER REFERENCES openaire.organization(id),
    relation_type TEXT,
    validation_date DATE,
    validated BOOLEAN,
    PRIMARY KEY (project_id, organization_id)
);

CREATE TABLE openaire.j_project_subject (
    project_id INTEGER REFERENCES openaire.project(id),
    subject_id INTEGER REFERENCES openaire.subject(id),
    PRIMARY KEY (project_id, subject_id)
);

CREATE TABLE openaire.j_project_measure (
    project_id INTEGER REFERENCES openaire.project(id),
    measure_id INTEGER REFERENCES openaire.measure(id),
    PRIMARY KEY (project_id, measure_id)
);

CREATE TABLE openaire.j_project_funder (
    project_id INTEGER REFERENCES openaire.project(id),
    funder_id INTEGER REFERENCES openaire.funder(id),
    -- Financial grant information
    currency TEXT,                      -- e.g., "EUR"
    funded_amount FLOAT,                -- e.g., 10000000.00
    total_cost FLOAT,                   -- e.g., 10000000.00
    PRIMARY KEY (project_id, funder_id)
);

CREATE TABLE openaire.j_project_funding_stream (
    project_id INTEGER REFERENCES openaire.project(id),
    funding_stream_id INTEGER REFERENCES openaire.funding_stream(id),
    PRIMARY KEY (project_id, funding_stream_id)
);

CREATE TABLE openaire.j_project_h2020_programme (
    project_id INTEGER REFERENCES openaire.project(id),
    h2020_programme_id INTEGER REFERENCES openaire.h2020_programme(id),
    PRIMARY KEY (project_id, h2020_programme_id)
);

CREATE TABLE openaire.j_researchoutput_author (
    research_output_id INTEGER REFERENCES openaire.researchoutput(id) ON DELETE CASCADE,
    author_id INTEGER REFERENCES openaire.author(id) ON DELETE CASCADE,
    rank FLOAT,
    PRIMARY KEY (research_output_id, author_id)
);

CREATE TABLE openaire.j_researchoutput_organization (
    research_output_id INTEGER REFERENCES openaire.researchoutput(id) ON DELETE CASCADE,
    organization_id INTEGER REFERENCES openaire.organization(id) ON DELETE CASCADE,
    relation_type TEXT,
    country_code TEXT,
    country_label TEXT,
    PRIMARY KEY (research_output_id, organization_id)
);

-----------------------------------------------
-- Indexes

CREATE INDEX idx_project_title ON openaire.project(title);
CREATE INDEX idx_project_code ON openaire.project(code);
CREATE INDEX idx_project_start_date ON openaire.project(start_date);
CREATE INDEX idx_project_end_date ON openaire.project(end_date);
CREATE INDEX idx_openaire_project_title_search ON openaire.project USING gin(to_tsvector('english', title));
CREATE INDEX idx_openaire_project_summary_search ON openaire.project USING gin(to_tsvector('english', coalesce(summary, '')));

CREATE INDEX idx_researchoutput_title ON openaire.researchoutput(main_title);
CREATE INDEX idx_researchoutput_publication_date ON openaire.researchoutput(publication_date);
CREATE INDEX idx_researchoutput_type ON openaire.researchoutput(type);
CREATE INDEX idx_researchoutput_citation_count ON openaire.researchoutput(citation_count);
CREATE INDEX idx_researchoutput_title_search ON openaire.researchoutput USING gin(to_tsvector('english', main_title));
CREATE INDEX idx_researchoutput_description_search ON openaire.researchoutput USING gin(to_tsvector('english', coalesce(description, '')));

CREATE INDEX idx_author_full_name ON openaire.author(full_name);
CREATE INDEX idx_author_surname ON openaire.author(surname);

CREATE INDEX idx_container_name ON openaire.container(name);
CREATE INDEX idx_container_issn_online ON openaire.container(issn_online);

CREATE INDEX idx_organization_legal_name ON openaire.organization(legal_name);

CREATE INDEX idx_subject_value ON openaire.subject(value);
CREATE INDEX idx_funder_name ON openaire.funder(name);
CREATE INDEX original_id ON openaire.funding_stream(original_id);

-----------------------------------------------
-- Update Triggers

CREATE OR REPLACE FUNCTION openaire.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_project_updated_at BEFORE UPDATE
ON openaire.project FOR EACH ROW EXECUTE PROCEDURE
openaire.update_updated_at_column();

CREATE TRIGGER update_researchoutput_updated_at BEFORE UPDATE
ON openaire.researchoutput FOR EACH ROW EXECUTE PROCEDURE
openaire.update_updated_at_column();

CREATE TRIGGER update_organization_updated_at BEFORE UPDATE
ON openaire.organization FOR EACH ROW EXECUTE PROCEDURE
openaire.update_updated_at_column();

CREATE TRIGGER update_subject_updated_at BEFORE UPDATE
ON openaire.subject FOR EACH ROW EXECUTE PROCEDURE
openaire.update_updated_at_column();

CREATE TRIGGER update_measure_updated_at BEFORE UPDATE
ON openaire.measure FOR EACH ROW EXECUTE PROCEDURE
openaire.update_updated_at_column();

CREATE TRIGGER update_funder_updated_at BEFORE UPDATE
ON openaire.funder FOR EACH ROW EXECUTE PROCEDURE
openaire.update_updated_at_column();

CREATE TRIGGER update_funding_stream_updated_at BEFORE UPDATE
ON openaire.funding_stream FOR EACH ROW EXECUTE PROCEDURE
openaire.update_updated_at_column();

CREATE TRIGGER update_h2020_programme_updated_at BEFORE UPDATE
ON openaire.h2020_programme FOR EACH ROW EXECUTE PROCEDURE
openaire.update_updated_at_column();

CREATE TRIGGER update_author_updated_at BEFORE UPDATE
ON openaire.author FOR EACH ROW EXECUTE PROCEDURE
openaire.update_updated_at_column();

CREATE TRIGGER update_container_updated_at BEFORE UPDATE
ON openaire.container FOR EACH ROW EXECUTE PROCEDURE
openaire.update_updated_at_column();