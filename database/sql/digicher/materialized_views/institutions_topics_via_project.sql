-----------------------------------------------
-- Institution Topics                        --
-----------------------------------------------
-- Show all institutions and given the projects all topic codes this institution knows about

CREATE MATERIALIZED VIEW institution_topics AS
SELECT 
    i.id, 
    i.name, 
    i.address_geolocation,
    array_agg(DISTINCT t.code) as topic_codes
FROM projects AS p
INNER JOIN projects_institutions as pi ON pi.project_id = p.id
INNER JOIN institutions as i ON pi.institution_id = i.id
INNER JOIN projects_topics as pt ON pt.project_id = p.id
INNER JOIN topics as t ON pt.topic_id = t.id
GROUP BY i.id, i.name, i.address_geolocation;

CREATE INDEX idx_institution_topic_code
ON institution_topics(code);

SELECT * FROM institution_topics
limit 100;

-----------------------------------------------
-- DROP                                      --
-----------------------------------------------

DROP MATERIALIZED VIEW institution_topics;
DROP INDEX idx_institution_topic_code;

SELECT pg_size_pretty(pg_total_relation_size('institution_topics'));

