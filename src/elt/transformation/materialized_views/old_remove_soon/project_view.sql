-----------------------------------------------
-- Project View
-----------------------------------------------

-- Projects have no geo location but an institution that is the primary coordinator has one.
-- We use this coordinators geolocation for the project. We added the year for filtering.
-- For Cordis there are xxx projects in total and xxx projects with a coordinator.

select * from core.project;

CREATE MATERIALIZED VIEW core_mats.project_view AS
SELECT 
  p.id AS project_id,
  i.id AS institution_id,
  p.start_date AS start_date,
  p.end_date AS end_date,
  p.total_cost AS total_cost,
  i.geolocation AS geolocation,
  i.country AS country_code,
  i.type_title AS type,
  i.sme AS sme,
  i.nuts_level_0 AS nuts_0,
  i.nuts_level_1 AS nuts_1,
  i.nuts_level_2 AS nuts_2,
  i.nuts_level_3 AS nuts_3,
  ARRAY_AGG(DISTINCT fp.framework_programme) FILTER (WHERE fp.framework_programme IS NOT NULL) AS framework_programmes
FROM 
  core.project p
  INNER JOIN core.j_project_institution jpi ON jpi.project_id = p.id
  INNER JOIN core.institution i ON jpi.institution_id = i.id
  LEFT JOIN core_mats.fundingprogramme fp ON p.id = fp.project_id
WHERE 
  (jpi.type = 'coordinator' or jpi.institution_position = 0) 
  AND i.geolocation is not null
GROUP BY 
  p.id, i.id, p.start_date, p.end_date, p.total_cost, 
  i.geolocation, i.country, i.type_title, i.sme,
  i.nuts_level_0, i.nuts_level_1, i.nuts_level_2, i.nuts_level_3
ORDER BY 
  p.start_date;

-----------------------------------------------
-- SELECTS                                   --
-----------------------------------------------

SELECT * FROM core_mats.project_view;
-- 51896 w/ where
-- 

SELECT * FROM core_mats.project_view
WHERE 
	2013 BETWEEN EXTRACT(YEAR FROM start_date) 
	AND EXTRACT(YEAR FROM end_date)
	AND geolocation IS NOT NULL;

-----------------------------------------------
-- INDEXES                                   --
-----------------------------------------------

-- ...

-----------------------------------------------
-- SIZE                                      --
-----------------------------------------------

-- Total table size including indexes
SELECT pg_size_pretty(pg_total_relation_size('core_mats.project_view'));

-- Data size without indexes
SELECT pg_size_pretty(pg_relation_size('core_mats.project_view'));

-----------------------------------------------
-- DROP                                      --
-----------------------------------------------

DROP MATERIALIZED VIEW core_mats.project_view;