select count(*) from core.researchoutput where fulltext is null;

-----------------------------------------------
-- Institution Collaboration Weights		  --
-----------------------------------------------

-- For each institution, theSubQuery selects all its projects, and given these projects
-- selects all the institutions who worked on all these projects. 
-- Then cleans it for duplicates and self count.

CREATE MATERIALIZED VIEW core_mats.mat_institutions_collaboration_weights AS
WITH CollaborationCounts AS (
    SELECT 
        i1.id AS institution_id,
        COUNT(DISTINCT i2.id) AS collaboration_weight
    FROM 
        core.institution i1
        JOIN core.j_project_institution pi1 ON i1.id = pi1.institution_id
        JOIN core.j_project_institution pi2 ON pi1.project_id = pi2.project_id
        JOIN core.institution i2 ON i2.id = pi2.institution_id
    WHERE 
        i1.id != i2.id  -- Don't count self-collaborations
        AND i1.geolocation IS NOT NULL
		AND i2.geolocation IS NOT NULL -- Also exclude collaborators without location
    GROUP BY 
        i1.id
)
SELECT 
    i.id AS institution_id,
    i.geolocation AS geolocation,
	i.country AS country,
    COALESCE(cc.collaboration_weight, 0) AS collaboration_weight
FROM 
    core.institution i
    LEFT JOIN CollaborationCounts cc ON i.id = cc.institution_id
WHERE 
    i.geolocation IS NOT NULL;

-----------------------------------------------
-- SELECTS                                   --
-----------------------------------------------

SELECT * FROM core_mats.mat_institutions_collaboration_weights
ORDER BY collaboration_weight DESC;

-----------------------------------------------
-- INDEXES                                   --
-----------------------------------------------

-- CREATE INDEX idx_institution_weights_id ON institution_collaboration_weights(institution_id);

-----------------------------------------------
-- DROP                                      --
-----------------------------------------------

DROP MATERIALIZED VIEW core_mats.mat_institutions_collaboration_weights;
