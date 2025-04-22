select * from mat_institutions_topics;
-----------------------------------------------
-- Project Funding programmes                --
-----------------------------------------------

-- Simple query to get a list of all funding programmes belonging to a project

SELECT 
	p.id as project_id,
	array_agg(DISTINCT f.id) as funding_ids
FROM projects AS p
LEFT JOIN projects_fundingprogrammes AS pf ON pf.project_id = p.id
LEFT JOIN fundingprogrammes AS f ON pf.fundingprogramme_id = f.id
GROUP BY p.id;