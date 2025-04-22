--------------------------------------------------------------------------------------------
-- Get all projects with their main institution
-- Assuming the main institution is the one with highest contribution
--------------------------------------------------------------------------------------------

-- Get all project information with main institution name and geolocation
SELECT
    p.*,
    i.name AS institution_name,
    i.address_geolocation AS institution_geolocation,
    pi.ec_contribution AS institution_contribution,
    pi.total_cost AS institution_total_cost,
    pi.type AS institution_type
FROM
    Projects p
LEFT JOIN LATERAL (
    SELECT
        project_id,
        institution_id,
        ec_contribution,
        total_cost,
        type
    FROM
        Projects_Institutions
    WHERE
        project_id = p.id
    ORDER BY
        ec_contribution DESC NULLS LAST,
        total_cost DESC NULLS LAST
    LIMIT 1
) pi ON true
LEFT JOIN
    Institutions i ON i.id = pi.institution_id
ORDER BY
    p.start_date DESC NULLS LAST;