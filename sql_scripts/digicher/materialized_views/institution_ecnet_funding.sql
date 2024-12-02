CREATE MATERIALIZED VIEW institution_ecnet_funding AS
SELECT 
    i.id as institution_id,
    i.name as institution_name,
    i.address_geolocation as address_geolocation,
    COALESCE(SUM(pi.net_ec_contribution), 0) as total_eu_funding,  -- Handle NULLs
    COUNT(DISTINCT pi.project_id)::text as number_of_projects,
    CASE 
        WHEN COUNT(DISTINCT pi.project_id) > 0 
        THEN ROUND(SUM(COALESCE(pi.net_ec_contribution, 0)) / COUNT(DISTINCT pi.project_id), 2)::text
        ELSE '0'
    END as avg_project_funding
FROM 
    Institutions i
    LEFT JOIN Projects_Institutions pi ON i.id = pi.institution_id
WHERE 
    i.address_geolocation IS NOT NULL
    AND array_length(i.address_geolocation, 1) = 2
GROUP BY 
    i.id, i.name, i.address_geolocation
ORDER BY 
    total_eu_funding DESC NULLS LAST;

-- DROP MATERIALIZED VIEW IF EXISTS institution_ecnet_funding;
-- REFRESH MATERIALIZED VIEW institution_ecnet_funding;

SELECT * FROM institution_ecnet_funding
WHERE total_eu_funding != 0
ORDER BY total_eu_funding DESC; -- 13k