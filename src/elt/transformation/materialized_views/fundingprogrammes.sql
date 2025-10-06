select distinct framework_programme from core.fundingprogramme;

-----------------------------------------------
-- Fundingprogramme
-----------------------------------------------

CREATE MATERIALIZED VIEW core_mats.fundingprogramme AS
SELECT ranked.project_id, ranked.funding_id, ranked.framework_programme
FROM (
    SELECT p.id as project_id, f.id as funding_id, f.framework_programme,
        ROW_NUMBER() OVER (PARTITION BY p.id, f.framework_programme ORDER BY f.id) as rank
    FROM core.project as p
    LEFT JOIN core.j_project_fundingprogramme as jpf ON jpf.project_id = p.id
    LEFT JOIN core.fundingprogramme as f ON jpf.fundingprogramme_id = f.id
    WHERE f.framework_programme IS NOT NULL
) ranked
WHERE rank = 1;

-----------------------------------------------
-- SELECTS                                   --
-----------------------------------------------

SELECT * FROM core_mats.fundingprogramme;

-----------------------------------------------
-- INDEXES                                   --
-----------------------------------------------

-- ...

-----------------------------------------------
-- SIZE                                      --
-----------------------------------------------

-- Total table size including indexes
SELECT pg_size_pretty(pg_total_relation_size('core_mats.fundingprogramme'));

-- Data size without indexes
SELECT pg_size_pretty(pg_relation_size('core_mats.fundingprogramme'));

-----------------------------------------------
-- DROP                                      --
-----------------------------------------------

DROP MATERIALIZED VIEW core_mats.fundingprogramme;