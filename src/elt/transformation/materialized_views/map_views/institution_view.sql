-----------------------------------------------
-- Institution View
-----------------------------------------------

-- One row per institution with aggregated project participations.
-- Only includes institutions that have at least one project.
-- Geolocation filtering (for map display) should be done downstream.

CREATE MATERIALIZED VIEW core_mats.institution_view AS
WITH institution_projects AS (
  -- Get all (institution, project) pairs with project details
  SELECT
    i.id AS institution_id,
    i.geolocation,
    i.country AS country_code,
    i.type_title AS type,
    i.sme,
    p.id AS project_id,
    p.start_date,
    p.end_date,
    p.total_cost AS project_total_cost,
    jpi.total_cost AS participation_cost
  FROM core.institution i
  INNER JOIN core.j_project_institution jpi ON i.id = jpi.institution_id
  INNER JOIN core.project p ON jpi.project_id = p.id
),
projects_with_fp AS (
  -- Add framework programmes to each project
  SELECT
    ip.*,
    COALESCE(
      ARRAY_AGG(DISTINCT fp.framework_programme) FILTER (WHERE fp.framework_programme IS NOT NULL),
      ARRAY[]::text[]
    ) AS framework_programmes
  FROM institution_projects ip
  LEFT JOIN core_mats.fundingprogramme fp ON fp.project_id = ip.project_id
  GROUP BY
    ip.institution_id, ip.geolocation, ip.country_code, ip.type, ip.sme,
    ip.project_id, ip.start_date, ip.end_date, ip.project_total_cost, ip.participation_cost
)
SELECT
  institution_id,
  geolocation,
  country_code,
  type,
  sme,
  -- SUM(participation_cost) AS total_cost,
  jsonb_agg(
    jsonb_build_object(
      'id', project_id,
      'start', start_date,
      'end', end_date,
      'total_cost', project_total_cost,
	  'participation_cost', participation_cost,
      'framework_programmes', framework_programmes
    )
  ) AS projects
FROM projects_with_fp
GROUP BY institution_id, geolocation, country_code, type, sme
ORDER BY institution_id;

-----------------------------------------------
-- SELECTS                                   --
-----------------------------------------------

-- Takes 0.4 seconds
SELECT * FROM core_mats.institution_view
WHERE institution_id = '88024035d5b20baf23b1da560e415d76';

-- WHERE geolocation is not null;

SELECT * FROM core.institution 
WHERE legal_name ilike '%Friedrich SChiller%';

-- "88024035d5b20baf23b1da560e415d76"
-- "bc9368fff7ab1d5fe0a877c8053299df"
-- "7452f0b2918485dfa43903736c580e3d"
-- "91021e0830c969cc851400f8b2ab5fa1"
-- "ae368ce635da2123212ce8147b3e78c3"
-- "3ff52c3a48186f1ac41f7d74cfc5a968"
-- "007fb0405669d075e5a7d4badab5cf56"

-----------------------------------------------
-- INDEXES                                   --
-----------------------------------------------



-----------------------------------------------
-- SIZE                                      --
-----------------------------------------------

-- Total table size including indexes 60 MB
SELECT pg_size_pretty(pg_total_relation_size('core_mats.institution_view'));

-- Data size without indexes 49 MB
SELECT pg_size_pretty(pg_relation_size('core_mats.institution_view'));

-----------------------------------------------
-- DROP                                      --
-----------------------------------------------

DROP MATERIALIZED VIEW core_mats.institution_view;
