-----------------------------------------------
-- Institution View
-----------------------------------------------

-- ...
 
CREATE MATERIALIZED VIEW core_mats.institution_view AS
SELECT 
  i.id AS institution_id,
  p.id AS project_id,
  p.start_date,
  p.end_date,
  jpi.total_cost,
  i.geolocation,
  i.country AS country_code,
  i.type_title AS type,
  i.sme,
  i.nuts_level_0 AS nuts_0,
  i.nuts_level_1 AS nuts_1,
  i.nuts_level_2 AS nuts_2,
  i.nuts_level_3 AS nuts_3,
  ARRAY_AGG(DISTINCT fp.framework_programme) FILTER (WHERE fp.framework_programme IS NOT NULL) AS framework_programmes
FROM 
  core.institution i
  INNER JOIN core.j_project_institution jpi ON i.id = jpi.institution_id
  INNER JOIN core.project p ON jpi.project_id = p.id
  LEFT JOIN core_mats.fundingprogramme fp ON p.id = fp.project_id
WHERE i.geolocation IS NOT NULL
GROUP BY 
  i.id, p.id, p.start_date, p.end_date, jpi.total_cost, 
  i.geolocation, i.country, i.type_title, i.sme,
  i.nuts_level_0, i.nuts_level_1, i.nuts_level_2, i.nuts_level_3
ORDER BY i.id, p.start_date;

-----------------------------------------------
-- SELECTS                                   --
-----------------------------------------------

-- Takes 8 seconds
SELECT * FROM core_mats.institution_view;

-----------------------------------------------
-- INDEXES                                   --
-----------------------------------------------



-----------------------------------------------
-- SIZE                                      --
-----------------------------------------------

-- Total table size including indexes 99 MB
SELECT pg_size_pretty(pg_total_relation_size('core_mats.institution_view'));

-- Data size without indexes 61 MB
SELECT pg_size_pretty(pg_relation_size('core_mats.institution_view'));

-----------------------------------------------
-- DROP                                      --
-----------------------------------------------

DROP MATERIALIZED VIEW core_mats.institution_view;