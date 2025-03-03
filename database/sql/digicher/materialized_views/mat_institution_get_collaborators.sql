-----------------------------------------------
-- Institution Get Collaborators    		 --
-----------------------------------------------

--

CREATE MATERIALIZED VIEW mat_institution_collaborations AS
SELECT DISTINCT
    pi1.institution_id AS source_institution_id,
    i2.id AS institution_id,
	i2.address_geolocation AS address_geolocation,
	i2.address_country AS address_country
FROM
    Projects_Institutions pi1
    JOIN Projects_Institutions pi2 ON pi1.project_id = pi2.project_id
    JOIN Institutions i2 ON i2.id = pi2.institution_id
WHERE
    pi1.institution_id != pi2.institution_id
    AND i2.address_geolocation IS NOT NULL;

-- Institution Get Collaborators --

CREATE OR REPLACE FUNCTION get_institution_collaborators(input_institution_id INTEGER)
	RETURNS TABLE (
	    institution_id INTEGER,
		address_geolocation float[],
		address_country TEXT
	)  
	LANGUAGE plpgsql
	AS 
'
BEGIN
    RETURN QUERY
    SELECT 
        mic.institution_id,
        mic.address_geolocation,
		mic.address_country
    FROM 
        mat_institution_collaborations mic
    WHERE 
        mic.source_institution_id = input_institution_id;
END;
';

-----------------------------------------------
-- SELECTS                                   --
-----------------------------------------------

SELECT * FROM mat_institution_collaborations;
SELECT * FROM get_institution_collaborators(1);

-----------------------------------------------
-- INDEXES                                   --
-----------------------------------------------

-- CREATE INDEX idx_institution_collab_source ON institution_collaborations(source_institution_id);
-- CREATE INDEX idx_institution_collab_target ON institution_collaborations(collaborator_id);

-----------------------------------------------
-- SIZE                                      --
-----------------------------------------------

SELECT pg_size_pretty( pg_total_relation_size('mat_institution_collaborations') );

-----------------------------------------------
-- DROP                                      --
-----------------------------------------------

DROP MATERIALIZED VIEW mat_institution_collaborations;
DROP FUNCTION get_institution_collaborators;