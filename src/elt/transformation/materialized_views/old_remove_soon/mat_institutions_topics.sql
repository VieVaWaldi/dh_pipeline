-----------------------------------------------
-- Institutions Topics                       --
-----------------------------------------------

-- Matches all Institutions to all Projects they worked on, and those projects to all Topics the Project belongs too.
-- Assigns these Topics deduplicated as a list to the institutions.
-- So this is basically a table of all the institutions each having a list of topics they worked on.

CREATE MATERIALIZED VIEW core_mats.mat_institutions_topics AS
SELECT 
    i.id as institution_id,
	array_agg(DISTINCT t.id) as topic_ids
	-- array_agg(DISTINCT ARRAY[t.level, t.id]) as topics
FROM core.institution as i
INNER JOIN core.j_project_institution as pi ON pi.institution_id = i.id
INNER JOIN core.project as p ON pi.project_id = p.id
INNER JOIN core.j_project_topic as pt ON pt.project_id = p.id
INNER JOIN core.topic as t ON pt.topic_id = t.id
GROUP BY i.id;

-----------------------------------------------
-- SELECTS                                   --
-----------------------------------------------

SELECT * FROM core_mats.mat_institutions_topics
LIMIT 100;

-- Topic Distribution --
SELECT 
    avg(array_length(topic_ids, 1)) as avg_topics_per_institution, -- 17 avg topics per institution
    max(array_length(topic_ids, 1)) as max_topics_per_institution, -- 556 max topics for one instiution
    min(array_length(topic_ids, 1)) as min_topics_per_institution, -- 1 min topic
    count(*) as total_institutions								    -- And 25k total institutions
FROM core_mats.mat_institutions_topics;

-----------------------------------------------
-- INDEXES                                   --
-----------------------------------------------

-- Primary index on institution id
CREATE UNIQUE INDEX idx_mat_institutions_topics_id ON mat_institutions_topics(id);

-- GIN index for searching within the topics array
-- CREATE INDEX idx_mat_institutions_topics_gin ON mat_institutions_topics USING gin(topics);

-- Optional: If you frequently search for specific topic names
CREATE INDEX idx_mat_institutions_topics_names ON mat_institutions_topics USING gin((topics[][1]));

-- Optional: If you frequently filter by level
CREATE INDEX idx_mat_institutions_topics_levels ON mat_institutions_topics USING gin((topics[][2]));

-----------------------------------------------
-- SIZE                                      --
-----------------------------------------------

-- Total table size including indexes
SELECT pg_size_pretty(pg_total_relation_size('core_mats.mat_institutions_topics'));

-- Data size without indexes
SELECT pg_size_pretty(pg_relation_size('core_mats.mat_institutions_topics'));

-- Size of topics array column
SELECT 
    pg_size_pretty(sum(pg_column_size(topic_ids))) as total_topics_size,
    pg_size_pretty(avg(pg_column_size(topic_ids))) as avg_topics_size
FROM core_mats.mat_institutions_topics;

-----------------------------------------------
-- DROP                                      --
-----------------------------------------------

DROP MATERIALIZED VIEW mat_institutions_topics;
DROP INDEX 
	idx_mat_institutions_topics_id, 
	idx_mat_institutions_topics_gin, 
	idx_mat_institutions_topics_names, 
	idx_mat_institutions_topics_levels;