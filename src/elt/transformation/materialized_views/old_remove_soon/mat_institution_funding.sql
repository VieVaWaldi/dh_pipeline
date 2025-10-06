-----------------------------------------------
-- Institutions Funding                      --
-----------------------------------------------

-- Matches all Institutions to all Projects_Institutions where the funding information is.
-- Aggregates all projects and their costs into a array for further filtering.

--ALTER TYPE project_funding_type ADD ATTRIBUTE start_date date;

CREATE TYPE project_funding_type AS (
    project_id text,
    ec_contribution double precision,
    net_ec_contribution double precision,
    total_cost double precision,
    start_date date
);

-- Then use this type in the materialized view
CREATE MATERIALIZED VIEW core_mats.mat_institution_funding AS
SELECT 
    i.id as id,
    i.geolocation as geolocation,
    i.country as country,
    array_agg( -- This saves 30% size but needs a custom string parser
        (pi.project_id::TEXT, pi.ec_contribution, pi.net_ec_contribution, pi.total_cost, p.start_date)::project_funding_type
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
    core.institution i
    INNER JOIN core.j_project_institution pi ON i.id = pi.institution_id
    INNER JOIN core.project p ON p.id = pi.project_id
WHERE 
    pi.total_cost IS NOT NULL
    AND i.geolocation IS NOT NULL
GROUP BY 
    i.id, i.geolocation, i.country
ORDER BY i.id;

-----------------------------------------------
-- SELECTS                                   --
-----------------------------------------------

SELECT * FROM core_mats.mat_institution_funding
LIMIT 100;

SELECT 
    'JSON Type' as type,
    pg_size_pretty(pg_total_relation_size('core_mats.mat_institution_funding')) as total_size,
    pg_size_pretty(sum(pg_column_size(projects_funding))) as projects_funding_size
FROM core_mats.mat_institution_funding;

-----------------------------------------------
-- DROP                                      --
-----------------------------------------------

REFRESH MATERIALIZED VIEW core_mats.mat_institution_funding;
DROP MATERIALIZED VIEW IF EXISTS core_mats.mat_institution_funding;