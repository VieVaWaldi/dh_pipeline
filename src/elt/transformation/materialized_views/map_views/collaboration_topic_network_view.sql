-----------------------------------------------
-- Collaboration Network for topics   	  	  --
-----------------------------------------------

CREATE MATERIALIZED VIEW core_mats.collaboration_by_topic AS                                                                                                                                      
SELECT DISTINCT                                                                                                                                                                                   
  pi1.institution_id AS a_institution_id,                                                                                                                                                       
  pi2.institution_id AS b_institution_id,                                                                                                                                                       
  i1.geolocation AS a_geolocation,
  i2.geolocation AS b_geolocation,
  i1.country AS a_country,
  i2.country AS b_country,
  pi1.project_id,
  p.start_date,
  p.end_date,
  jpt.topic_id,
  t.subfield_id,
  t.field_id,
  (SELECT array_agg(DISTINCT fp.framework_programme)
   FROM core.j_project_fundingprogramme jpf
   JOIN core.fundingprogramme fp ON fp.id = jpf.fundingprogramme_id
   WHERE jpf.project_id = pi1.project_id
  ) AS framework_programmes
FROM
  core.j_project_institution pi1
  JOIN core.j_project_institution pi2 ON pi1.project_id = pi2.project_id
  JOIN core.institution i1 ON i1.id = pi1.institution_id
  JOIN core.institution i2 ON i2.id = pi2.institution_id
  JOIN core.j_project_topicoa jpt ON jpt.project_id = pi1.project_id
  JOIN core.topicoa t ON t.id = jpt.topic_id
  JOIN core.project p ON p.id = pi1.project_id
WHERE
  pi1.institution_id < pi2.institution_id
  AND i1.geolocation IS NOT NULL
  AND i2.geolocation IS NOT NULL;

-----------------------------------------------
-- SELECTS                                   --
-----------------------------------------------

SELECT * FROM core_mats.collaboration_by_topic;

-----------------------------------------------
-- INDEXES                                   --
-----------------------------------------------

-- CREATE INDEX idx_institution_collab_source ON institution_collaborations(source_institution_id);
-- CREATE INDEX idx_institution_collab_target ON institution_collaborations(collaborator_id);

-----------------------------------------------
-- SIZE                                      --
-----------------------------------------------

SELECT pg_size_pretty( pg_total_relation_size('core_mats.collaboration_by_topic') );

-----------------------------------------------
-- DROP                                      --
-----------------------------------------------

-- old
-- DROP MATERIALIZED VIEW core_mats.mat_institution_collaborations;
-- DROP FUNCTION core_mats.get_institution_collaborators;
-- old new
-- DROP MATERIALIZED VIEW core_mats.collaborations;

-- new
DROP MATERIALIZED VIEW core_mats.collaboration_by_topic;