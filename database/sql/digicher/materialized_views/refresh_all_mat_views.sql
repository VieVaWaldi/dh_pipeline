SELECT title, similarity(LOWER(title), LOWER('Digitisation of cultural heritage of minority communities for equity and renewed engagement')) AS sim
FROM projects
WHERE LOWER(title) % LOWER('Sami cosmologies')
ORDER BY sim DESC;

select * from projects 
where lower(title) like lower('Digitisation of cultural heritage of minority communities for equity and renewed engagement%');

select * from researchoutputs order by id desc limit 100;

select id, name, address_geolocation from institutions
where lower(name) like '%dresden%';

select * from institutions where address_geolocation is null;

select * from institutions
where updated_at > '2025-03-03'::date;
-- and address_geolocation is null;

-----------------------------------------------
-- Refresh all Mat Views                     --
-----------------------------------------------

REFRESH MATERIALIZED VIEW mat_institution_funding;
REFRESH MATERIALIZED VIEW mat_institution_collaborations;
REFRESH MATERIALIZED VIEW mat_institutions_collaboration_weights;
REFRESH MATERIALIZED VIEW mat_institutions_fundingprogrammes;
REFRESH MATERIALIZED VIEW mat_institutions_topics;
REFRESH MATERIALIZED VIEW mat_projects_coordinator;

-- Function no need for refresh right?
-- FUNCTION get_institution_collaborators