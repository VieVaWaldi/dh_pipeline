CREATE MATERIALIZED VIEW institution_collaboration_weights AS
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
    COALESCE(cc.collaboration_weight, 0) AS collaboration_weight
FROM 
    Institutions i
    LEFT JOIN CollaborationCounts cc ON i.id = cc.institution_id
WHERE 
    i.address_geolocation IS NOT NULL;

CREATE INDEX idx_institution_weights_id ON institution_collaboration_weights(institution_id);

CREATE MATERIALIZED VIEW institution_collaborations AS
SELECT DISTINCT
    pi1.institution_id AS source_institution_id,
    i2.id AS collaborator_id,
    i2.name AS collaborator_name,
    i2.address_geolocation AS collaborator_location
FROM 
    Projects_Institutions pi1
    JOIN Projects_Institutions pi2 ON pi1.project_id = pi2.project_id
    JOIN Institutions i2 ON i2.id = pi2.institution_id
WHERE 
    pi1.institution_id != pi2.institution_id
    AND i2.address_geolocation IS NOT NULL;

-- Create indexes for faster lookups
CREATE INDEX idx_institution_collab_source ON institution_collaborations(source_institution_id);
CREATE INDEX idx_institution_collab_target ON institution_collaborations(collaborator_id);

-- Create function to get collaborators for a specific institution
CREATE OR REPLACE FUNCTION get_institution_collaborators(input_institution_id INTEGER)
RETURNS TABLE (
    collaborator_id INTEGER,
    collaborator_name TEXT,
    collaborator_location float[]
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        ic.collaborator_id,
        ic.collaborator_name,
        ic.collaborator_location
    FROM 
        institution_collaborations ic
    WHERE 
        ic.source_institution_id = input_institution_id;
END;
$$ LANGUAGE plpgsql;

-- Create function to refresh both materialized views
-- CREATE OR REPLACE FUNCTION refresh_institution_collaboration_views()
-- RETURNS void AS $$
-- BEGIN
--     REFRESH MATERIALIZED VIEW CONCURRENTLY institution_collaboration_weights;
--     REFRESH MATERIALIZED VIEW CONCURRENTLY institution_collaborations;
-- END;
-- $$ LANGUAGE plpgsql;

SELECT refresh_institution_collaboration_views();

--------

SELECT * FROM institution_collaboration_weights
ORDER BY collaboration_weight DESC;

SELECT * FROM get_institution_collaborators(3);