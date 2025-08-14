-------------------------------------------------------------------
--- Core Tables                                                 ---

CREATE TABLE Sources (
    id SERIAL PRIMARY KEY,
    source SOURCE_TYPE NOT NULL,
    entity_table TEXT NOT NULL,  -- Core Tables
    entity_id INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source, entity_table, entity_id)
);

CREATE TABLE People (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    title TEXT,
    telephone_number TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Topics (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    standardised_name TEXT,
	level INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Weblinks (
    id SERIAL PRIMARY KEY,
    link TEXT UNIQUE NOT NULL,
    name TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Dois (
    id SERIAL PRIMARY KEY,
    doi TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

SELECT count(*)
FROM ResearchOutputs;
WHERE created_at > '2024-11-01';

-- ResearchOutputs may refer as:
--    many to many People
--    many to many Topics
--    many to many Weblinks
--    one to one Doi
CREATE TABLE ResearchOutputs (
    id SERIAL PRIMARY KEY,
    id_original TEXT UNIQUE NOT NULL,
    type TEXT NOT NULL, -- eg publication, relatedResult for cordis ...

    doi_id INTEGER REFERENCES Dois(id), -- ToDo UNIQUE
    arxiv_id TEXT,

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

-- Institutions may refer as:
--      many to many People
--      many to many ResearchOutputs
CREATE TABLE Institutions (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    sme BOOLEAN,
    address_street TEXT,
    address_postbox TEXT,
    address_postalcode TEXT,
    address_city TEXT,
    address_country TEXT,
    address_geolocation float[], -- ADDED ARRAY
    url TEXT,
    short_name TEXT,
    vat_number TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE FundingProgrammes (
    id SERIAL PRIMARY KEY,
    code TEXT UNIQUE NOT NULL,
    title TEXT,
    short_title TEXT,
    framework_programme TEXT,
    pga TEXT,
    rcn INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Projects (
    id SERIAL PRIMARY KEY,
    id_original TEXT UNIQUE NOT NULL,
    doi_id INTEGER UNIQUE REFERENCES Dois(id), -- REF DOIS
    acronym TEXT,
    title TEXT NOT NULL,
    status TEXT,
    start_date DATE,
    end_date DATE,
    ec_signature_date DATE,
    total_cost DECIMAL(15,2),
    ec_max_contribution DECIMAL(15,2),
    objective TEXT,
    call_identifier TEXT,
    call_title TEXT,
    call_rcn TEXT,
    -- Note: There is also a list of calls ([_]) which contains master and sub calls that we're not using
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-------------------------------------------------------------------
--- Multi relationship tables                                   ---

--- ResearchOutputs to Others                                   ---

CREATE TABLE ResearchOutputs_People (
    publication_id INTEGER REFERENCES ResearchOutputs(id) ON DELETE CASCADE,
    person_id INTEGER REFERENCES People(id) ON DELETE CASCADE,
    -- Persons position in the naming list
    person_position INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (publication_id, person_id)
);

CREATE TABLE ResearchOutputs_Topics (
    publication_id INTEGER REFERENCES ResearchOutputs(id) ON DELETE CASCADE,
    topic_id INTEGER REFERENCES Topics(id) ON DELETE CASCADE,
    -- is_primary BOOLEAN DEFAULT true, -- REMOVED_2
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (publication_id, topic_id)
);

CREATE TABLE ResearchOutputs_Weblinks (
    publication_id INTEGER REFERENCES ResearchOutputs(id) ON DELETE CASCADE,
    weblink_id INTEGER REFERENCES Weblinks(id) ON DELETE CASCADE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (publication_id, weblink_id)
);

--- Institutions to Others                                      ---

CREATE TABLE Institutions_People (
    institution_id INTEGER REFERENCES Institutions(id) ON DELETE CASCADE,
    person_id INTEGER REFERENCES People(id) ON DELETE CASCADE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (institution_id, person_id)
);

CREATE TABLE Institutions_ResearchOutputs (
    institution_id INTEGER REFERENCES Institutions(id) ON DELETE CASCADE,
    publication_id INTEGER REFERENCES ResearchOutputs(id) ON DELETE CASCADE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (institution_id, publication_id)
);

--- Projects to Others                                          ---

-- ToDo: Understand what happens if i remove this project.
-- Will the topic also be removed or just the junction table?

CREATE TABLE Projects_Topics (
    project_id INTEGER REFERENCES Projects(id) ON DELETE CASCADE,
    topic_id INTEGER REFERENCES Topics(id) ON DELETE CASCADE,
    -- is_primary BOOLEAN DEFAULT true, -- REMOVED_2
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (project_id, topic_id)
);

CREATE TABLE Projects_Weblinks (
    project_id INTEGER REFERENCES Projects(id) ON DELETE CASCADE,
    weblink_id INTEGER REFERENCES Weblinks(id) ON DELETE CASCADE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (project_id, weblink_id)
);

CREATE TABLE Projects_ResearchOutputs (
    project_id INTEGER REFERENCES Projects(id) ON DELETE CASCADE,
    publication_id INTEGER REFERENCES ResearchOutputs(id) ON DELETE CASCADE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (project_id, publication_id)
);

CREATE TABLE Projects_Institutions (
    project_id INTEGER REFERENCES Projects(id) ON DELETE CASCADE,
    institution_id INTEGER REFERENCES Institutions(id) ON DELETE CASCADE,
    -- Institutions position in the naming list
    institution_position INTEGER NOT NULL, -- ADDED
    ec_contribution DECIMAL(15,2),
    net_ec_contribution DECIMAL(15,2),
    total_cost DECIMAL(15,2),
    type TEXT, -- coordinator, participant ... Can be used to get primary institution
    organization_id TEXT,
    rcn INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (project_id, institution_id)
);

CREATE TABLE Projects_FundingProgrammes (
    project_id INTEGER REFERENCES Projects(id) ON DELETE CASCADE,
    fundingprogramme_id INTEGER REFERENCES FundingProgrammes(id) ON DELETE CASCADE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (project_id, fundingprogramme_id)
);


-------------------------------------------------------------------
--- Indexes                                                     ---

-- WIP -- Must be reworked completely because we added without thinking about it too much --

-- Indexes for Institution and project search --

CREATE INDEX idx_institutions_name_lower ON institutions (lower(name));
CREATE INDEX idx_projects_title_lower ON projects (lower(title));
CREATE INDEX idx_projects_acronym_lower ON projects (lower(acronym));
CREATE INDEX idx_projects_objective_lower ON projects (lower(objective));

--- Basic Indexes for Core Tables                               ---

-- These support UNIQUE constraints and common lookups
CREATE INDEX idx_people_name ON People(name);
CREATE INDEX idx_topics_name ON Topics(name);
CREATE INDEX idx_weblinks_link ON Weblinks(link);
CREATE INDEX idx_dois_doi ON Dois(doi);
CREATE INDEX idx_publications_id_original ON ResearchOutputs(id_original);
CREATE INDEX idx_institutions_name ON Institutions(name);

-- ResearchOutputs specific indexes                                ---

-- For finding publications by title (potential partial matches)
CREATE INDEX idx_publications_title ON ResearchOutputs USING gin(to_tsvector('english', title));
-- For date-based queries
CREATE INDEX idx_publications_date ON ResearchOutputs(publication_date);
-- For DOI lookups
CREATE INDEX idx_publications_doi ON ResearchOutputs(doi_id);

--- Source lookups                                              ---

-- For finding all sources for an entity
CREATE INDEX idx_sources_entity ON Sources(entity_table, entity_id);

--- Junction table indexes                                      ---
-- Note: Primary key columns are automatically indexed

CREATE INDEX idx_pub_people_person ON ResearchOutputs_People(person_id);
-- Index for ordering people
CREATE INDEX idx_pub_people_position ON ResearchOutputs_People(publication_id, person_position);
CREATE INDEX idx_pub_topics_topic ON ResearchOutputs_Topics(topic_id);
-- For finding primary topics
CREATE INDEX idx_pub_weblinks_weblink ON ResearchOutputs_Weblinks(weblink_id);
CREATE INDEX idx_inst_people_person ON Institutions_People(person_id);
CREATE INDEX idx_inst_pub_pub ON Institutions_ResearchOutputs(publication_id);

--- Project specific indexes                                    ---

CREATE INDEX idx_projects_title ON Projects USING gin(to_tsvector('english', title));
CREATE INDEX idx_projects_dates ON Projects(start_date, end_date);
-- CREATE INDEX idx_projects_funding ON Projects(fundingprogramme_id); -- REMOVED
CREATE INDEX idx_projects_status ON Projects(status);
CREATE INDEX idx_projects_doi ON Projects(doi_id);
CREATE INDEX idx_projects_id_original ON Projects(id_original);

--- FundingProgrammes specific indexes                          ---

CREATE INDEX idx_fundingprogrammes_code ON FundingProgrammes(code);
CREATE INDEX idx_fundingprogrammes_title ON FundingProgrammes USING gin(to_tsvector('english', title));
CREATE INDEX idx_fundingprogrammes_framework ON FundingProgrammes(framework_programme);

--- Project junction table indexes                              ---

-- Projects_Topics
CREATE INDEX idx_proj_topics_topic ON Projects_Topics(topic_id);

-- Projects_Weblinks
CREATE INDEX idx_proj_weblinks_weblink ON Projects_Weblinks(weblink_id);

-- Projects_ResearchOutputs
CREATE INDEX idx_proj_outputs_output ON Projects_ResearchOutputs(publication_id);

-- Projects_Institutions (for the detailed junction table)
CREATE INDEX idx_proj_inst_institution ON Projects_Institutions(institution_id);
CREATE INDEX idx_proj_inst_type ON Projects_Institutions(type);
CREATE INDEX idx_proj_inst_orgid ON Projects_Institutions(organization_id);
CREATE INDEX idx_proj_inst_contribution ON Projects_Institutions(ec_contribution);