-----------------------------------------------
-- Cordis Data Model

CREATE SCHEMA IF NOT EXISTS cordis;

CREATE TABLE cordis.person (
    id SERIAL PRIMARY KEY,
    title TEXT,
    name TEXT,  -- If has only one name
    first_name TEXT,
    last_name TEXT,
    telephone_number TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT check_person_has_name CHECK (
        (name IS NOT NULL) OR (first_name IS NOT NULL AND last_name IS NOT NULL)
    )
);

CREATE TABLE cordis.topic (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    level INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE cordis.weblink (
    id SERIAL PRIMARY KEY,
    url TEXT UNIQUE NOT NULL,
    title TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE cordis.fundingprogramme (
    id SERIAL PRIMARY KEY,
    code TEXT UNIQUE NOT NULL,
    title TEXT,
    short_title TEXT,
    framework_programme TEXT,
    pga TEXT,
    rcn TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-----------------------------------------------
-- Cordis Main Tables

CREATE TABLE cordis.institution (
    id SERIAL PRIMARY KEY,
    legal_name TEXT UNIQUE NOT NULL,
    sme BOOLEAN,
    url TEXT,
    short_name TEXT,
    vat_number TEXT,
    street TEXT,
    postbox TEXT,
    postalcode TEXT,
    city TEXT,
    country TEXT,
    geolocation float[], -- POINT?
    type TEXT,
    type_title TEXT,
    nuts_level_0 TEXT,
    nuts_level_1 TEXT,
    nuts_level_2 TEXT,
    nuts_level_3 TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE cordis.researchoutput (
    id SERIAL PRIMARY KEY, -- ignore
    id_original TEXT UNIQUE NOT NULL, -- source_id
	from_pdf BOOLEAN, -- ignore
    type TEXT NOT NULL, -- type
    doi TEXT, -- doi
    title TEXT NOT NULL, -- title
    publication_date DATE, -- publication date
    journal TEXT, -- create junction to core journal
    summary TEXT, -- abstract
    comment TEXT, -- comment
    fulltext TEXT, -- fulltext
    funding_number TEXT, -- funding_number
    journal_number TEXT,
    journal_title TEXT,
    published_pages TEXT,
    published_year TEXT,
    publisher TEXT,
    issn TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE cordis.project (
    id SERIAL PRIMARY KEY,
    id_original TEXT UNIQUE NOT NULL,
    doi TEXT,
    title TEXT NOT NULL,
    acronym TEXT,
    status TEXT,
    start_date DATE,
    end_date DATE,
    ec_signature_date DATE,
    total_cost FLOAT,
    ec_max_contribution FLOAT,
    objective TEXT,
    call_identifier TEXT,
    call_title TEXT,
    call_rcn TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-----------------------------------------------
-- Junction Tables

-----------------------------------------------
-- Junction Institution

CREATE TABLE cordis.j_institution_person (
    institution_id INTEGER REFERENCES cordis.institution(id) ON DELETE CASCADE,
    person_id INTEGER REFERENCES cordis.person(id) ON DELETE CASCADE,
    PRIMARY KEY (institution_id, person_id)
);

-----------------------------------------------
-- Junction ResearchOutput

CREATE TABLE cordis.j_researchoutput_person (
    researchoutput_id INTEGER REFERENCES cordis.researchoutput(id) ON DELETE CASCADE,
    person_id INTEGER REFERENCES cordis.person(id) ON DELETE CASCADE,
    person_position INTEGER NOT NULL, -- Position of name in list
    PRIMARY KEY (researchoutput_id, person_id)
);

CREATE TABLE cordis.j_researchoutput_topic (
    researchoutput_id INTEGER REFERENCES cordis.researchoutput(id) ON DELETE CASCADE,
    topic_id INTEGER REFERENCES cordis.topic(id) ON DELETE CASCADE,
    PRIMARY KEY (researchoutput_id, topic_id)
);

CREATE TABLE cordis.j_researchoutput_weblink (
    researchoutput_id INTEGER REFERENCES cordis.researchoutput(id) ON DELETE CASCADE,
    weblink_id INTEGER REFERENCES cordis.weblink(id) ON DELETE CASCADE,
    PRIMARY KEY (researchoutput_id, weblink_id)
);

CREATE TABLE cordis.j_researchoutput_institution (
    researchoutput_id INTEGER REFERENCES cordis.researchoutput(id) ON DELETE CASCADE,
    institution_id INTEGER REFERENCES cordis.institution(id) ON DELETE CASCADE,
    PRIMARY KEY (researchoutput_id, institution_id)
);

-----------------------------------------------
-- Junction Project

CREATE TABLE cordis.j_project_topic (
    project_id INTEGER REFERENCES cordis.project(id) ON DELETE CASCADE,
    topic_id INTEGER REFERENCES cordis.topic(id) ON DELETE CASCADE,
    PRIMARY KEY (project_id, topic_id)
);

CREATE TABLE cordis.j_project_weblink (
    project_id INTEGER REFERENCES cordis.project(id) ON DELETE CASCADE,
    weblink_id INTEGER REFERENCES cordis.weblink(id) ON DELETE CASCADE,
    PRIMARY KEY (project_id, weblink_id)
);

CREATE TABLE cordis.j_project_fundingprogramme (
    project_id INTEGER REFERENCES cordis.project(id) ON DELETE CASCADE,
    fundingprogramme_id INTEGER REFERENCES cordis.fundingprogramme(id) ON DELETE CASCADE,
    PRIMARY KEY (project_id, fundingprogramme_id)
);

CREATE TABLE cordis.j_project_institution (
    project_id INTEGER REFERENCES cordis.project(id) ON DELETE CASCADE,
    institution_id INTEGER REFERENCES cordis.institution(id) ON DELETE CASCADE,

    institution_position INTEGER NOT NULL, -- Position of name in list
    ec_contribution FLOAT,
    net_ec_contribution FLOAT,
    total_cost FLOAT,
    type TEXT, -- coordinator, participant ...
    organization_id TEXT,
    rcn INTEGER,
    PRIMARY KEY (project_id, institution_id)
);

CREATE TABLE cordis.j_project_researchoutput (
    project_id INTEGER REFERENCES cordis.project(id) ON DELETE CASCADE,
    researchoutput_id INTEGER REFERENCES cordis.researchoutput(id) ON DELETE CASCADE,
    PRIMARY KEY (project_id, researchoutput_id)
);

-----------------------------------------------
-- Indexes

CREATE INDEX idx_project_title ON cordis.project(title);
CREATE INDEX idx_project_title_search ON cordis.project USING gin(to_tsvector('english', title));
CREATE INDEX idx_project_objective_search ON cordis.project USING gin(to_tsvector('english', coalesce(objective, '')));
CREATE INDEX idx_project_start_date ON cordis.project(start_date);
CREATE INDEX idx_project_end_date ON cordis.project(end_date);

CREATE INDEX idx_researchoutput_title ON cordis.researchoutput(title);
CREATE INDEX idx_researchoutput_publication_date ON cordis.researchoutput(publication_date);

CREATE INDEX idx_institution_name ON cordis.institution(legal_name);

CREATE INDEX idx_person_name ON cordis.person(name);
CREATE INDEX idx_person_fullname ON cordis.person(first_name, last_name);

CREATE INDEX idx_topic_name ON cordis.topic(name);

CREATE INDEX idx_fundingprogramme_code ON cordis.fundingprogramme(code);

-----------------------------------------------
-- Update Triggers

CREATE OR REPLACE FUNCTION cordis.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_person_updated_at BEFORE UPDATE
ON cordis.person FOR EACH ROW EXECUTE PROCEDURE
cordis.update_updated_at_column();

CREATE TRIGGER update_topic_updated_at BEFORE UPDATE
ON cordis.topic FOR EACH ROW EXECUTE PROCEDURE
cordis.update_updated_at_column();

CREATE TRIGGER update_weblink_updated_at BEFORE UPDATE
ON cordis.weblink FOR EACH ROW EXECUTE PROCEDURE
cordis.update_updated_at_column();

CREATE TRIGGER update_fundingprogramme_updated_at BEFORE UPDATE
ON cordis.fundingprogramme FOR EACH ROW EXECUTE PROCEDURE
cordis.update_updated_at_column();

CREATE TRIGGER update_researchoutput_updated_at BEFORE UPDATE
ON cordis.researchoutput FOR EACH ROW EXECUTE PROCEDURE
cordis.update_updated_at_column();

CREATE TRIGGER update_institution_updated_at BEFORE UPDATE
ON cordis.institution FOR EACH ROW EXECUTE PROCEDURE
cordis.update_updated_at_column();

CREATE TRIGGER update_project_updated_at BEFORE UPDATE
ON cordis.project FOR EACH ROW EXECUTE PROCEDURE
cordis.update_updated_at_column();