-----------------------------------------------
-- Institution Get Collaborators    		 --
-----------------------------------------------

CREATE MATERIALIZED VIEW core_mats.collaborations AS
SELECT DISTINCT
    pi1.institution_id AS a_institution_id,
    pi2.institution_id AS b_institution_id,
    i1.geolocation AS a_geolocation,
    i2.geolocation AS b_geolocation,
    pi1.project_id AS project_id
FROM
    core.j_project_institution pi1
    JOIN core.j_project_institution pi2 ON pi1.project_id = pi2.project_id
    JOIN core.institution i1 ON i1.id = pi1.institution_id
    JOIN core.institution i2 ON i2.id = pi2.institution_id
WHERE
    pi1.institution_id < pi2.institution_id
    AND i1.geolocation IS NOT NULL 
    AND i2.geolocation IS NOT NULL;

SELECT * FROM core_mats.collaborations;

-- Institution Get Collaborators --

CREATE OR REPLACE FUNCTION core_mats.get_institution_collaborators(input_institution_id TEXT)
	RETURNS TABLE (
	    institution_id TEXT,
		geolocation float[],
		country TEXT
	)
	LANGUAGE plpgsql
	AS
'
BEGIN
    RETURN QUERY
    SELECT
        mic.institution_id,
        mic.geolocation,
		mic.country
    FROM
        core_mats.mat_institution_collaborations mic
    WHERE
        mic.source_institution_id = input_institution_id;
END;
';

-----------------------------------------------
-- SELECTS                                   --
-----------------------------------------------

SELECT * FROM core_mats.mat_institution_collaborations;
SELECT * FROM core_mats.get_institution_collaborators('4527e4b8b62d7b338eee4fd970202a65');

-----------------------------------------------
-- INDEXES                                   --
-----------------------------------------------

-- CREATE INDEX idx_institution_collab_source ON institution_collaborations(source_institution_id);
-- CREATE INDEX idx_institution_collab_target ON institution_collaborations(collaborator_id);

-----------------------------------------------
-- SIZE                                      --
-----------------------------------------------

SELECT pg_size_pretty( pg_total_relation_size('core_mats.mat_institution_collaborations') );

-----------------------------------------------
-- DROP                                      --
-----------------------------------------------

DROP MATERIALIZED VIEW core_mats.mat_institution_collaborations;
DROP FUNCTION core_mats.get_institution_collaborators;

DROP MATERIALIZED VIEW core_mats.collaborations;