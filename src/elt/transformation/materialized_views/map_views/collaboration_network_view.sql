-----------------------------------------------
-- Collaboration Network View       		 --
-----------------------------------------------

-- Note: Same project can appear multiple times per collaborator pair when an institution
-- has multiple organizational entities (different organization_id) in the same project
-- (e.g., as both 'participant' and 'thirdParty'). This is correct data, not a bug.
-- React keys must account for this by using array index or combined_institution_cost.

-- Query this to get all collaborators with the respective projects from one institution
CREATE MATERIALIZED VIEW core_mats.collaboration_network_view AS
SELECT
    pi1.institution_id AS institution_id,
    pi2.institution_id AS collaborator_id,
    i2.geolocation AS collaborator_geolocation,
    i2.country_code AS collaborator_country_code,
    i2.type_title AS collaborator_type,
    i2.sme AS collaborator_sme,
    JSONB_AGG(
        JSONB_BUILD_OBJECT(
            'project_id', pi1.project_id,
            'total_cost', p.total_cost,
            'combined_institution_cost', (pi1.total_cost + pi2.total_cost),
            'start_date', p.start_date,
            'end_date', p.end_date,
            'framework_programmes', fprog.framework_programmes
        ) ORDER BY pi1.project_id
    ) AS projects
FROM
    core.j_project_institution pi1
    JOIN core.j_project_institution pi2 ON pi1.project_id = pi2.project_id
    JOIN core.institution i1 ON i1.id = pi1.institution_id
    JOIN core.institution i2 ON i2.id = pi2.institution_id
    JOIN core.project p ON p.id = pi1.project_id
    LEFT JOIN (
        SELECT
            jpf.project_id,
            ARRAY_AGG(DISTINCT f.framework_programme) FILTER (WHERE f.framework_programme IS NOT NULL) AS framework_programmes
        FROM core.j_project_fundingprogramme jpf
        JOIN core.fundingprogramme f ON f.id = jpf.fundingprogramme_id
        GROUP BY jpf.project_id
    ) fprog ON fprog.project_id = p.id
WHERE
    pi1.institution_id != pi2.institution_id
    AND i1.geolocation IS NOT NULL
    AND i2.geolocation IS NOT NULL
GROUP BY
    pi1.institution_id, pi2.institution_id, i2.geolocation, i2.country_code, i2.type, i2.sme;

-----------------------------------------------
-- SELECTS                                   --
-----------------------------------------------

select * from core.institution
where legal_name like '%AIRBUS DEUTSCHLAND GMBH%'; -- SAGEM DEFENSE SECURITE

SELECT * FROM core_mats.collaboration_network_view;
SELECT * FROM core_mats.collaboration_network_view
WHERE institution_id = '0bf931af9ac6a26f4fec9d6e62bbfbcb'
AND collaborator_id = '9b39410702e82f3ebcf1e0754123e9ff'; -- 0bf931af9ac6a26f4fec9d6e62bbfbcb

-----------------------------------------------
-- INDEXES                                   --
-----------------------------------------------

CREATE INDEX idx_institution_network_source ON core_mats.collaboration_network_view(institution_id);

-----------------------------------------------
-- DROP                                      --
-----------------------------------------------

DROP MATERIALIZED VIEW core_mats.collaboration_network_view;