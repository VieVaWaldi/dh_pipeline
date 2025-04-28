WITH title_blocks AS (
    SELECT id, title, substring(LOWER(title) from 1 for 5) AS block
    FROM core.researchoutput
	WHERE LOWER(title) NOT LIKE '%attachment%'
)
SELECT t1.id as id1, t2.id as id2,
       t1.title as title1, t2.title as title2,
       similarity(LOWER(t1.title), LOWER(t2.title)) as similarity_score
FROM title_blocks t1
JOIN title_blocks t2 ON t1.id < t2.id AND t1.block = t2.block
WHERE similarity(LOWER(t1.title), LOWER(t2.title)) > 0.9
ORDER BY similarity_score DESC;