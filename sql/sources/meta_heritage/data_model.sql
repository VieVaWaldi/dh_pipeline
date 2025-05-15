-----------------------------------------------
-- Meta Heritage Data Model

-- ToDo:
-- CASCADE junction remove
-- Get filled rows from miro
-- is_other, can find better pattern?

CREATE TABLE stakeholder (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    abbreviation TEXT,
    webpage_url TEXT,
    social_media_url TEXT,
    legal_status TEXT,
    ownership TEXT CHECK (ownership IN ('public', 'private', 'mixed', NULL)),

    street_name TEXT,
    house_number TEXT,
    postal_code TEXT,
    city TEXT,
    country TEXT,

    nuts_code_id INTEGER REFERENCES nuts_code(id),
    nace_code_id INTEGER REFERENCES nace_code(id),

    -- @Ello only ever one?
    contact_firstname TEXT,
    contact_surname TEXT,
    contact_email TEXT,
    contact_phone TEXT,

    latitude NUMERIC,
    longitude NUMERIC,

    -- Provenance
    data_source_type TEXT, -- CHECK IN (...?)
    data_source_name TEXT, -- CHECK IN (...?)

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-----------------------------------------------
-- FK Relations - Codes

CREATE TABLE nuts_code (
    id SERIAL PRIMARY KEY,
    country_code TEXT NOT NULL,
    level_1 TEXT,
    level_2 TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(country_code, level_1, level_2)
);

CREATE TABLE nace_code (
    id SERIAL PRIMARY KEY,
    level_1 TEXT NOT NULL,
    level_2 TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(level_1_code, level_2_code)
);

-----------------------------------------------
-- EU Project

CREATE TABLE eu_project_participation (
    id SERIAL PRIMARY KEY,
    project_id TEXT,
    project_name TEXT,
    pic_code TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE j_stakeholder_eu_project_participation (
    stakeholder_id INTEGER NOT NULL REFERENCES stakeholder(id),
    eu_project_participation_id INTEGER NOT NULL REFERENCES eu_project_participation(id),
    -- ToDo: To stakeholder or remove
    -- participation_status TEXT NOT NULL CHECK (participation_status IN ('yes', 'interested', 'no')),
    PRIMARY KEY (stakeholder_id, eu_project_participation_id)
);

-----------------------------------------------
-- Organization Type

CREATE TABLE organization_type (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE j_stakeholder_organization_type (
    stakeholder_id INTEGER NOT NULL REFERENCES stakeholder(id),
    organization_type_id INTEGER NOT NULL REFERENCES organization_type(id),
    -- Difference between selectable networks from list and others in free text
    is_other BOOLEAN DEFAULT false,
    PRIMARY KEY (stakeholder_id, cultural_route_id)
);

-----------------------------------------------
-- Network

CREATE TABLE network (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE j_stakeholder_network_membership (
    stakeholder_id INTEGER NOT NULL REFERENCES stakeholder(id),
    network_id INTEGER NOT NULL REFERENCES network(id),
    -- Difference between selectable networks from list and others in free text
    is_other BOOLEAN DEFAULT false,
    -- ToDo: To stakeholder or remove
    -- membership_status TEXT CHECK (membership_status IN ('yes', 'interested', 'no')),
    PRIMARY KEY (stakeholder_id, network_id)
);

-----------------------------------------------
-- Cultural Route

-- @Ello was für keywords
CREATE TABLE cultural_route (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE j_stakeholder_cultural_route (
    stakeholder_id INTEGER NOT NULL REFERENCES stakeholder(id),
    cultural_route_id INTEGER NOT NULL REFERENCES cultural_route(id),
    -- Difference between selectable networks from list and others in free text
    is_other BOOLEAN DEFAULT false,
    PRIMARY KEY (stakeholder_id, cultural_route_id)
);

-----------------------------------------------
-- Cultural Heritage Topic

-- @Ello are topics set?
CREATE TABLE ch_topic (
    id SERIAL PRIMARY KEY,
    topic_number INTEGER NOT NULL UNIQUE,
    name TEXT NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE j_stakeholder_heritage_topic (
    stakeholder_id INTEGER NOT NULL REFERENCES stakeholder(id),
    heritage_topic_id INTEGER NOT NULL REFERENCES ch_topic(id),
    -- Difference between selectable networks from list and others in free text
    is_other BOOLEAN DEFAULT false, -- ToDo: Keep topic nr not null or not?
    PRIMARY KEY (stakeholder_id, heritage_topic_id)
);

INSERT INTO ch_topic (topic_number, name) VALUES
(1, 'Aristocratic heritage'),
(2, 'Natural heritage (incl. natural reserve, garden, park)'),
(3, 'Archeological heritage'),
(4, 'Architectural heritage'),
(5, 'Arts and crafts heritage (e.g. watch-maker)'),
(6, 'Corporate heritage'),
(7, 'Automotive heritage'),
(8, 'Industrial heritage'),
(9, 'Agricultural heritage'),
(10, 'Culinary heritage'),
(11, 'Beer brewery culture'),
(12, 'Tea & Coffee culture'),
(13, 'Viticulture'),
(14, 'Local history'),
(15, 'Immaterial heritage (e.g. oral history, folk traditions)'),
(16, 'Religious heritage'),
(17, 'Fashion heritage'),
(18, 'Other');

-----------------------------------------------
-- Indexes

-- Specific Columns
CREATE INDEX idx_stakeholder_name ON stakeholder(name);
CREATE INDEX idx_nuts_code_country_code ON nuts_code(country_code);
CREATE INDEX idx_nace_code_level_1_code ON nace_code(level_1_code);
CREATE INDEX idx_nace_code_level_1_description ON nace_code(level_1_description);
CREATE INDEX idx_network_name ON network(name);
CREATE INDEX idx_cultural_route_name ON cultural_route(name);
CREATE INDEX idx_ch_topic_topic_number ON ch_topic(topic_number);
CREATE INDEX idx_ch_topic_name ON ch_topic(name);

-- FK's
CREATE INDEX idx_stakeholder_nuts_code ON stakeholder(nuts_code_id);
CREATE INDEX idx_stakeholder_nace_code ON stakeholder(nace_code_id);

-- Junctions
CREATE INDEX idx_j_stakeholder_eu_project_participation_stakeholder ON j_stakeholder_eu_project_participation(stakeholder_id);
CREATE INDEX idx_j_stakeholder_eu_project_participation_project ON j_stakeholder_eu_project_participation(eu_project_participation_id);
CREATE INDEX idx_j_stakeholder_network_membership_stakeholder ON j_stakeholder_network_membership(stakeholder_id);
CREATE INDEX idx_j_stakeholder_network_membership_network ON j_stakeholder_network_membership(network_id);
CREATE INDEX idx_j_stakeholder_cultural_route_stakeholder ON j_stakeholder_cultural_route(stakeholder_id);
CREATE INDEX idx_j_stakeholder_cultural_route_route ON j_stakeholder_cultural_route(cultural_route_id);
CREATE INDEX idx_j_stakeholder_heritage_topic_stakeholder ON j_stakeholder_heritage_topic(stakeholder_id);
CREATE INDEX idx_j_stakeholder_heritage_topic_topic ON j_stakeholder_heritage_topic(heritage_topic_id);

-----------------------------------------------
-- Triggers for automatic timestamp updates

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_stakeholder_updated_at BEFORE UPDATE ON stakeholder
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_nuts_code_updated_at BEFORE UPDATE ON nuts_code
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_nace_code_updated_at BEFORE UPDATE ON nace_code
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_eu_project_participation_updated_at BEFORE UPDATE ON eu_project_participation
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_network_updated_at BEFORE UPDATE ON network
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_cultural_route_updated_at BEFORE UPDATE ON cultural_route
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER ch_topic_updated_at BEFORE UPDATE ON ch_topic
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();