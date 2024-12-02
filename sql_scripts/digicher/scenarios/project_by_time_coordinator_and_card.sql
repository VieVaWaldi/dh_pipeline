-- First Query: Get project IDs and coordinator geolocations for a given year
SELECT 
    p.id AS project_id,
	i.id AS coordinator_id,
    i.address_geolocation AS coordinator_location
FROM 
    Projects p
    INNER JOIN Projects_Institutions pi ON p.id = pi.project_id
    INNER JOIN Institutions i ON pi.institution_id = i.id
WHERE 
    pi.type = 'coordinator'
    AND 2010-01-01 BETWEEN EXTRACT(YEAR FROM p.start_date) 
                 AND EXTRACT(YEAR FROM p.end_date)
 	AND i.address_geolocation IS NOT NULL;

-- Second Query: Get all project information for a specific ID
SELECT *
FROM Projects
WHERE id = 2550;

SELECT *
FROM Institutions
WHERE id = 8699;