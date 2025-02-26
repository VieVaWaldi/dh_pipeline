SELECT 
	i1.id AS institution_id,
	COUNT(DISTINCT i2.id) AS collaboration_weight
FROM 
	Institutions i1
	JOIN Projects_Institutions pi1 ON i1.id = pi1.institution_id
	JOIN Projects_Institutions pi2 ON pi1.project_id = pi2.project_id
	JOIN Institutions i2 ON i2.id = pi2.institution_id
WHERE 
	i1.id != i2.id  -- Don't count self-collaborations
	AND i1.address_geolocation IS NOT NULL
GROUP BY 
	i1.id;




-- 237 IS NOT 299 for id=2

	
-----------------------------------------------
-- Institution Collaboration Weights		  --
-----------------------------------------------

-- For each institution, theSubQuery selects all its projects, and given these projects
-- selects all the institutions who worked on all these projects. 
-- Then cleans it for duplicates and self count.


CREATE MATERIALIZED VIEW mat_institutions_collaboration_weights AS
WITH CollaborationCounts AS (
    SELECT 
        i1.id AS institution_id,
        COUNT(DISTINCT i2.id) AS collaboration_weight
    FROM 
        Institutions i1
        JOIN Projects_Institutions pi1 ON i1.id = pi1.institution_id
        JOIN Projects_Institutions pi2 ON pi1.project_id = pi2.project_id
        JOIN Institutions i2 ON i2.id = pi2.institution_id
    WHERE 
        i1.id != i2.id  -- Don't count self-collaborations
        AND i1.address_geolocation IS NOT NULL
    GROUP BY 
        i1.id
)
SELECT 
    i.id AS institution_id,
    i.name AS institution_name,
    i.address_geolocation,
	i.address_country AS address_country,
    COALESCE(cc.collaboration_weight, 0) AS collaboration_weight
FROM 
    Institutions i
    LEFT JOIN CollaborationCounts cc ON i.id = cc.institution_id
WHERE 
    i.address_geolocation IS NOT NULL;

-----------------------------------------------
-- SELECTS                                   --
-----------------------------------------------

SELECT * FROM mat_institutions_collaboration_weights
ORDER BY collaboration_weight DESC;

-----------------------------------------------
-- INDEXES                                   --
-----------------------------------------------

-- CREATE INDEX idx_institution_weights_id ON institution_collaboration_weights(institution_id);

-----------------------------------------------
-- DROP                                      --
-----------------------------------------------

DROP MATERIALIZED VIEW mat_institutions_collaboration_weights;
