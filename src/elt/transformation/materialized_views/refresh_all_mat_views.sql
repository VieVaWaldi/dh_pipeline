-- select * from topics; 
-- select * from projects_institutions; 
-- select * from fundingprogrammes limit 20;
-- where lower(framework_programme) like lower('%INFRAIA%')
-- or lower(pga) like lower('%INFRAIA%')
-- or lower(code) like lower('%INFRAIA%');

-- select * from institutions where address_geolocation is null;

-- select * from institutions
-- where updated_at > '2025-03-07'::date;
-- and address_geolocation is null;


-- First Cordis Batch where updated_at > '2025-03-03'::date;
-- Second Cordis Batch where updated_at > '2025-03-07'::date;
-- Open Aire Batch where updated_at > '2025-03-07'::date;

-----------------------------------------------
-- Refresh all Mat Views                     --
-----------------------------------------------

CREATE SCHEMA IF NOT EXISTS core_mats;

REFRESH MATERIALIZED VIEW mat_institution_funding;
REFRESH MATERIALIZED VIEW mat_institution_collaborations;
REFRESH MATERIALIZED VIEW mat_institutions_collaboration_weights;
REFRESH MATERIALIZED VIEW mat_institutions_fundingprogrammes;
REFRESH MATERIALIZED VIEW mat_institutions_topics;
REFRESH MATERIALIZED VIEW mat_projects_coordinator;

-- Function no need for refresh right?
-- FUNCTION get_institution_collaborators