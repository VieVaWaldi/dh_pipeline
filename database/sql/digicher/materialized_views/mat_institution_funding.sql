-----------------------------------------------
-- Institutions Funding                      --
-----------------------------------------------

-- Matches all Institutions to all Projects_Institutions where the funding information is.
-- Aggregates all projects and their costs into a array for further filtering.

CREATE TYPE project_funding_type AS (
    project_id integer,
    ec_contribution numeric,
    net_ec_contribution numeric,
    total_cost numeric
);

-- Then use this type in the materialized view
CREATE MATERIALIZED VIEW mat_institution_funding AS
SELECT 
    i.id as id,
    i.address_geolocation as address_geolocation,
    i.address_country as address_country,
    array_agg( -- This saves 30% size but needs a custom string parser
        (pi.project_id, pi.ec_contribution, pi.net_ec_contribution, pi.total_cost)::project_funding_type
    ) as projects_funding
	-- json_agg( -- No string parsing needed
	-- 	json_build_object(
	-- 		'project_id', pi.project_id,
	-- 		'ec_contribution', pi.ec_contribution,
	-- 		'net_ec_contribution', pi.net_ec_contribution,
	-- 		'total_cost', pi.total_cost
	-- 	)
	-- ) as projects_funding
FROM 
    Institutions i
    INNER JOIN Projects_Institutions pi ON i.id = pi.institution_id
WHERE 
    pi.total_cost IS NOT NULL
    AND i.address_geolocation IS NOT NULL
GROUP BY 
    i.id
ORDER BY i.id;

-----------------------------------------------
-- SELECTS                                   --
-----------------------------------------------

SELECT * FROM mat_institution_funding
LIMIT 100;

SELECT 
    'JSON Type' as type,
    pg_size_pretty(pg_total_relation_size('mat_institution_funding')) as total_size,
    pg_size_pretty(sum(pg_column_size(projects_funding))) as projects_funding_size
FROM mat_institution_funding;

-----------------------------------------------
-- DROP                                      --
-----------------------------------------------

REFRESH MATERIALIZED VIEW mat_institution_funding;
DROP MATERIALIZED VIEW IF EXISTS mat_institution_funding;

-----------------------------------------------
-- OLD VERSION                               --
-----------------------------------------------

-- CREATE MATERIALIZED VIEW mat_institution_funding AS
-- SELECT
--     i.id as id,
--     i.address_geolocation as address_geolocation,
--     i.address_country as address_country,
--     COALESCE(SUM(pi.net_ec_contribution), 0) as total_eu_funding,  -- Handle NULLs
--     COUNT(DISTINCT pi.project_id)::text as number_of_projects,
--     CASE 
--         WHEN COUNT(DISTINCT pi.project_id) > 0 
--         THEN ROUND(SUM(COALESCE(pi.net_ec_contribution, 0)) / COUNT(DISTINCT pi.project_id), 2)::text
--         ELSE '0'
--     END as avg_project_funding
-- FROM 
--     Institutions i
--     LEFT JOIN Projects_Institutions pi ON i.id = pi.institution_id
-- WHERE 
--     i.address_geolocation IS NOT NULL
--     AND array_length(i.address_geolocation, 1) = 2
-- GROUP BY 
--     i.id, i.name, i.address_geolocation
-- ORDER BY 
--     total_eu_funding DESC NULLS LAST;