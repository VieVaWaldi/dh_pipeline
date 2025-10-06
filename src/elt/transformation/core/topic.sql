select * from core.topicoa;

select * from core.project where source_system = 'openaire' and objective is not null;

select p.source_system, p.title, t.topic_name, jpt.score from core.project as p
inner join core.j_project_topicoa as jpt on jpt.project_id = p.id
inner join core.topicoa as t on jpt.topic_id = t.id
where p.source_system != 'cordis';

select count(*) from core.researchoutput where abstract is not null;

select count(*) from core.researchoutput as p
inner join core.j_researchoutput_topicoa as jpt on jpt.researchoutput_id = p.id
inner join core.topicoa as t on jpt.topic_id = t.id;

-- Gotta add this by hand

select distinct domain_name from core.topicoa;
-- 4516 rows with unique topic_name
-- 245 unique subfield_names
-- 26 unique field_names
-- 4 unique domain_names

-----------------------------------------------
-- Topic
-----------------------------------------------

-- Precreated OpenAlex Topics
CREATE TABLE core.topicoa (
    id SERIAL PRIMARY KEY,
    subfield_id TEXT NOT NULL,
    field_id TEXT NOT NULL,
    domain_id TEXT NOT NULL,
    topic_name TEXT NOT NULL,
    subfield_name TEXT NOT NULL,
    field_name TEXT NOT NULL,
    domain_name TEXT NOT NULL,
    keywords TEXT NOT NULL,
    summary TEXT NOT NULL,
    wikipedia_url TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Topicoa Junction to Project
CREATE TABLE core.j_project_topicoa (
    project_id TEXT,
    topic_id INTEGER,
	score FLOAT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (project_id, topic_id)
);

CREATE TABLE core.j_researchoutput_topicoa (
    researchoutput_id TEXT,
    topic_id INTEGER,
	score FLOAT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (researchoutput_id, topic_id)
);

drop table core.j_project_topicoa;