-----------------------------------------------
-- Meta Heritage Data Model

-- CREATE SCHEMA IF NOT EXISTS meta_heritage;

-----------------------------------------------
-- FK Relations - Codes

CREATE TABLE meta_heritage.nuts_code (
    id SERIAL PRIMARY KEY,
    country_code TEXT NOT NULL,
    level_1 TEXT,
    level_2 TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(country_code, level_1, level_2)
);

CREATE TABLE meta_heritage.nace_code (
    id SERIAL PRIMARY KEY,
    level_1 TEXT NOT NULL,
    level_2 TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(level_1, level_2)
);

-----------------------------------------------
-- Main Entity

CREATE TABLE meta_heritage.stakeholder (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    abbreviation TEXT,
    webpage_url TEXT,
    social_media_url TEXT,
    legal_status TEXT,
	description TEXT,
    ownership TEXT CHECK (ownership IN ('public', 'private', 'mixed', NULL)),

    street_name TEXT,
    house_number TEXT,
    postal_code TEXT,
    city TEXT,
    country TEXT,

    nuts_code_id INTEGER REFERENCES meta_heritage.nuts_code(id),
    nace_code_id INTEGER REFERENCES meta_heritage.nace_code(id),

    contact_firstname TEXT,
    contact_surname TEXT,
    contact_email TEXT,
    contact_phone TEXT,

    latitude NUMERIC,
    longitude NUMERIC,

    data_source_type TEXT, -- CHECK IN (...?)
    data_source_name TEXT, -- CHECK IN (...?)

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-----------------------------------------------
-- EU Project

CREATE TABLE meta_heritage.eu_project_participation (
    id SERIAL PRIMARY KEY,
    project_id TEXT,
    project_name TEXT,
    pic_code TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE meta_heritage.j_stakeholder_eu_project_participation (
    stakeholder_id INTEGER REFERENCES meta_heritage.stakeholder(id)  ON DELETE CASCADE,
    eu_project_participation_id INTEGER REFERENCES meta_heritage.eu_project_participation(id)  ON DELETE CASCADE,
    PRIMARY KEY (stakeholder_id, eu_project_participation_id)
);

-----------------------------------------------
-- Organization Type

CREATE TABLE meta_heritage.organization_type (
    id SERIAL PRIMARY KEY,
    type_number INTEGER UNIQUE,
    name TEXT UNIQUE NOT NULL,
    -- true, if one of the 12 predefined organization types
    is_predefined BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE meta_heritage.j_stakeholder_organization_type (
    stakeholder_id INTEGER REFERENCES meta_heritage.stakeholder(id)  ON DELETE CASCADE,
    organization_type_id INTEGER REFERENCES meta_heritage.organization_type(id)  ON DELETE CASCADE,
    PRIMARY KEY (stakeholder_id, organization_type_id)
);

INSERT INTO meta_heritage.organization_type (type_number, name) VALUES
(1, 'Gallery, Library, Archive, Museum (GLAM)'),
(2, 'Research institution (e.g., university)'),
(3, 'Research and advocacy centre (e.g., ThinkTank)'),
(4, 'Company or Corporation'),
(5, 'Cooperative or other collaborative business form'),
(6, 'Business Cluster'),
(7, 'Business Network incl. Chamber of Commerce'),
(8, 'Association'),
(9, 'International Organisation'),
(10, 'Non-Profit Organisation (e.g., Charity, NGO, Foundation)'),
(11, 'Public Sector Organisation (e.g. Government agency, Municipal authority)'),
(12, 'Investor or funding body (VCs, Business Angels, grants)');

-----------------------------------------------
-- Network

CREATE TABLE meta_heritage.network (
    id SERIAL PRIMARY KEY,
    network_number INTEGER UNIQUE,
    name TEXT UNIQUE NOT NULL,
    -- true, if one of the 13 predefined networks
    is_predefined BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE meta_heritage.j_stakeholder_network_membership (
    stakeholder_id INTEGER REFERENCES meta_heritage.stakeholder(id)  ON DELETE CASCADE,
    network_id INTEGER REFERENCES meta_heritage.network(id)  ON DELETE CASCADE,
	membership_interest TEXT CHECK (membership_interest IN ('yes', 'interested', 'no')),
    PRIMARY KEY (stakeholder_id, network_id)
);

INSERT INTO meta_heritage.network (network_number, name) VALUES
(1, 'Grandi Giardini Italiani'),
(2, 'European Royal Residences'),
(3, 'European Historic Houses'),
(4, 'Clust-ER Turismo della Regione Emilia-Romagna'),
(5, 'Clust-ER Create della Regione Emilia-Romagna'),
(6, 'Clust-ER Innovate della Regione Emilia-Romagna'),
(7, 'Vogtlandpioniere'),
(8, 'Stiftung Familienunternehmen'),
(9, 'Mitteldeutsche Cluster Games und VR/AR/XR'),
(10, 'Tourismusnetzwerk Sachsen'),
(11, 'Kreatives Sachsen - Landesverband der Kultur- und Kreativwirtschaft Sachsen e.V.'),
(12, 'ITnet Thüringen'),
(13, 'Rede de Museus do Douro');

-----------------------------------------------
-- Cultural Route

CREATE TABLE meta_heritage.cultural_route (
    id SERIAL PRIMARY KEY,
    route_number INTEGER UNIQUE,
    name TEXT UNIQUE NOT NULL,
    -- true, if one of the 14 predefined routes
    is_predefined BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE meta_heritage.j_stakeholder_cultural_route (
    stakeholder_id INTEGER REFERENCES meta_heritage.stakeholder(id)  ON DELETE CASCADE,
    cultural_route_id INTEGER REFERENCES meta_heritage.cultural_route(id)  ON DELETE CASCADE,
    PRIMARY KEY (stakeholder_id, cultural_route_id)
);

INSERT INTO meta_heritage.cultural_route (route_number, name) VALUES
(1, 'European Route of Historic Thermal Towns'),
(2, 'Routes of the Olive Tree'),
(3, 'European Route of Ceramics'),
(4, 'European Route of Industrial Heritage'),
(5, 'European Route of Historic Cafés'),
(6, 'European Route of Historic Pharmacies'),
(7, 'European Route of Historic Gardens'),
(8, 'Santiago de Compostela Pilgrim Routes'),
(9, 'TRANSROMANICA – The Romanesque Routes of European Heritage'),
(10, 'Rota dos Vinhos do Douro e do Porto'),
(11, 'Ruta de la Camelia'),
(12, 'Galicia Sabe Amar'),
(13, 'Ruta de la Ribeira Sacra'),
(14, 'Thüringer Porzellanstraße'),
(15, 'Motor Valley');

-----------------------------------------------
-- Cultural Heritage Topic

CREATE TABLE meta_heritage.ch_topic (
    id SERIAL PRIMARY KEY,
    topic_number INTEGER UNIQUE,
    name TEXT NOT NULL UNIQUE,
    -- true, if one of the 17 predefined topics
    is_predefined BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE meta_heritage.j_stakeholder_heritage_topic (
    stakeholder_id INTEGER REFERENCES meta_heritage.stakeholder(id)  ON DELETE CASCADE,
    heritage_topic_id INTEGER REFERENCES meta_heritage.ch_topic(id)  ON DELETE CASCADE,
    PRIMARY KEY (stakeholder_id, heritage_topic_id)
);

INSERT INTO meta_heritage.ch_topic (topic_number, name) VALUES
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
(17, 'Fashion heritage');

-----------------------------------------------
-- Indexes

-- Specific Columns
CREATE INDEX idx_stakeholder_name ON meta_heritage.stakeholder(name);
CREATE INDEX idx_nuts_code_country_code ON meta_heritage.nuts_code(country_code);
CREATE INDEX idx_nace_code_level_1_code ON meta_heritage.nace_code(level_1);
CREATE INDEX idx_network_name ON meta_heritage.network(name);
CREATE INDEX idx_cultural_route_name ON meta_heritage.cultural_route(name);
CREATE INDEX idx_ch_topic_topic_number ON meta_heritage.ch_topic(topic_number);
CREATE INDEX idx_ch_topic_name ON meta_heritage.ch_topic(name);

-- FK's
CREATE INDEX idx_stakeholder_nuts_code ON meta_heritage.stakeholder(nuts_code_id);
CREATE INDEX idx_stakeholder_nace_code ON meta_heritage.stakeholder(nace_code_id);

-- Junctions
CREATE INDEX idx_j_stakeholder_eu_project_participation_stakeholder ON meta_heritage.j_stakeholder_eu_project_participation(stakeholder_id);
CREATE INDEX idx_j_stakeholder_eu_project_participation_project ON meta_heritage.j_stakeholder_eu_project_participation(eu_project_participation_id);
CREATE INDEX idx_j_stakeholder_network_membership_stakeholder ON meta_heritage.j_stakeholder_network_membership(stakeholder_id);
CREATE INDEX idx_j_stakeholder_network_membership_network ON meta_heritage.j_stakeholder_network_membership(network_id);
CREATE INDEX idx_j_stakeholder_cultural_route_stakeholder ON meta_heritage.j_stakeholder_cultural_route(stakeholder_id);
CREATE INDEX idx_j_stakeholder_cultural_route_route ON meta_heritage.j_stakeholder_cultural_route(cultural_route_id);
CREATE INDEX idx_j_stakeholder_heritage_topic_stakeholder ON meta_heritage.j_stakeholder_heritage_topic(stakeholder_id);
CREATE INDEX idx_j_stakeholder_heritage_topic_topic ON meta_heritage.j_stakeholder_heritage_topic(heritage_topic_id);

-----------------------------------------------
-- Triggers for automatic timestamp updates

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_stakeholder_updated_at BEFORE UPDATE ON meta_heritage.stakeholder
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_nuts_code_updated_at BEFORE UPDATE ON meta_heritage.nuts_code
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_nace_code_updated_at BEFORE UPDATE ON meta_heritage.nace_code
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_eu_project_participation_updated_at BEFORE UPDATE ON meta_heritage.eu_project_participation
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_organization_type_updated_at BEFORE UPDATE ON meta_heritage.organization_type
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_network_updated_at BEFORE UPDATE ON meta_heritage.network
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_cultural_route_updated_at BEFORE UPDATE ON meta_heritage.cultural_route
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_ch_topic_updated_at BEFORE UPDATE ON meta_heritage.ch_topic
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();