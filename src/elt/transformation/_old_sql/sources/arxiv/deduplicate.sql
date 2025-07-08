-----------------------------------------------
-- Decuplication

-----------------------------------------------
-- Entry

select * from arxiv.entry;

-----------------------------------------------
-- Author

select * from arxiv.author;

-----------------------------------------------
-- Link

-- Identify Duplicates
select href, count(*) as oc
from arxiv.link
group by href
HAVING COUNT(*) > 1;

-- Check Duplicates
SELECT a.*
FROM arxiv.link a
INNER JOIN (
    SELECT href
    FROM arxiv.link
    GROUP BY href
    HAVING COUNT(*) > 1
) b ON a.href = b.href
ORDER BY a.href, a.id;

-- 1. Temp table for IDS
CREATE TEMPORARY TABLE duplicate_map
SELECT 
	MIN(id) AS keep_id,
	array_agg(id) AS all_ids,
	href
FROM arxiv.link
GROUP BY href
HAVING COUNT(*) > 1;

select * from duplicate_map;

-- 2. Update Junction table
UPDATE arxiv.j_entry_link AS j
SET link_id = dm.keep_id
FROM duplicate_map AS dm
WHERE j.link_id = ANY(m.all_ids) AND j.link_id != m.keep_id;
