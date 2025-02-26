-----------------------------------------------
-- Project Topics                            --
-----------------------------------------------

-- Simple query to get a list of all topics belonging to a project

SELECT 
	p.id as project_id,
	array_agg(DISTINCT t.id) as topic_ids
FROM projects AS p
LEFT JOIN projects_topics AS pt ON pt.project_id = p.id
LEFT JOIN topics AS t ON pt.topic_id = t.id
GROUP BY p.id;