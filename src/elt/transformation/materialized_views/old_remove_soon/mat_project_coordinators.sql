-----------------------------------------------
-- Project Coordinators				          --
-----------------------------------------------

-- Projects have no geo location but an institution that is the primary coordinator has one.
-- We use this coordinators geolocation for the project. We added the year for filtering.
-- For Cordis there are 12_329 projects in total and 12_313 projects with a coordinator.

CREATE MATERIALIZED VIEW core_mats.mat_projects_coordinator AS
SELECT 
  p.id AS project_id,
  p.start_date AS start_date,
  p.end_date AS end_date,
  p.total_cost AS total_cost,
  i.id AS coordinator_id,
  i.geolocation AS geolocation,
  i.country AS country
FROM 
  core.project p
  INNER JOIN core.j_project_institution pi ON p.id = pi.project_id
  INNER JOIN core.institution i ON pi.institution_id = i.id
WHERE 
  pi.type = 'coordinator';

-----------------------------------------------
-- SELECTS                                   --
-----------------------------------------------

SELECT * FROM core_mats.mat_projects_coordinator;

SELECT * FROM core_mats.mat_projects_coordinator
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
SELECT pg_size_pretty(pg_total_relation_size('core_mats.mat_projects_coordinator'));

-- Data size without indexes
SELECT pg_size_pretty(pg_relation_size('core_mats.mat_projects_coordinator'));

-----------------------------------------------
-- DROP                                      --
-----------------------------------------------

DROP MATERIALIZED VIEW core_mats.mat_projects_coordinator;