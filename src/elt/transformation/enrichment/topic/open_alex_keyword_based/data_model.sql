-------------------------------------------------------------------
--- OpenAlex Topic Data Model

-- See more here: https://docs.google.com/document/d/1bDopkhuGieQ4F8gGNj7sEc8WSE8mvLZS/edit?tab=t.0
-- And topic CSV we used for seeding here: https://docs.google.com/spreadsheets/d/1v-MAq64x4YjhO7RWcB-yrKV5D_2vOOsxl4u6GBKEXY8/edit?gid=983250122#gid=983250122

select topic_name, keywords from core.topic_openalex_keyword_density;
-- where lower(keywords) ILIKE '%cultural heritage%';

select count(*) from core.j_researchoutput_topic_openalex_keyword_density;

select r.title, r.publication_date, j_rt.score, t.topic_name, t.subfield_name, t.field_name, t.domain_name
from core.j_researchoutput_topic_openalex_keyword_density as j_rt
left join core.researchoutput r on r.id = j_rt.researchoutput_id
left join core.topic_openalex_keyword_density t on t.id = j_rt.topic_openalex_keyword_density_id
where j_rt.score < 0.3;
where lower(t.keywords) ILIKE '%cultural heritage%'
order by r.publication_date;

delete from core.j_researchoutput_topic_openalex_keyword_density;

-------------------------------------------------------------------
--- Main Models

CREATE TABLE core.topic_openalex_keyword_density (
    id INTEGER PRIMARY KEY, -- id is original topic_id, thats just way faster

    subfield_id INTEGER NOT NULL,
    field_id INTEGER NOT NULL,
    domain_id INTEGER NOT NULL,

    topic_name TEXT UNIQUE NOT NULL,
    subfield_name TEXT NOT NULL,
    field_name TEXT NOT NULL,
    domain_name TEXT NOT NULL,

    keywords TEXT,
    summary TEXT,
    wikipedia_url TEXT,

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_topic_openalex_domain ON core.topic_openalex_keyword_density(domain_name);
CREATE INDEX idx_topic_openalex_field ON core.topic_openalex_keyword_density(field_name);
CREATE INDEX idx_topic_openalex_subfield ON core.topic_openalex_keyword_density(subfield_name);
CREATE INDEX idx_topic_openalex_topic ON core.topic_openalex_keyword_density(topic_name);

-- Full-text search index for natural language keyword queries
CREATE INDEX idx_topic_openalex_keywords_fts 
ON core.topic_openalex_keyword_density 
USING gin(to_tsvector('english', keywords));

-- GIN index for pattern matching and LIKE operations
CREATE INDEX idx_topic_openalex_keywords_gin 
ON core.topic_openalex_keyword_density 
USING gin(keywords gin_trgm_ops);

CREATE TABLE core.j_researchoutput_topic_openalex_keyword_density (
    researchoutput_id INTEGER REFERENCES core.researchoutput(id) ON DELETE CASCADE,
    topic_openalex_keyword_density_id INTEGER REFERENCES core.topic_openalex_keyword_density(id) ON DELETE CASCADE,
    score NUMERIC,
    PRIMARY KEY (researchoutput_id, topic_openalex_keyword_density_id)
);

CREATE INDEX idx_j_ro_topic_openalex_ro ON core.j_researchoutput_topic_openalex_keyword_density(researchoutput_id);
CREATE INDEX idx_j_ro_topic_openalex_topic ON core.j_researchoutput_topic_openalex_keyword_density(topic_openalex_keyword_density_id);

-----------------------------------------------
-- Update Triggers

CREATE TRIGGER update_topic_openalex_keyword_density_updated_at BEFORE UPDATE
ON core.topic_openalex_keyword_density FOR EACH ROW EXECUTE PROCEDURE
core.update_updated_at_column();