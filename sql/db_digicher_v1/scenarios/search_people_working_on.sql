-----------------------------------------------
-- Search for specific people that published --
-----------------------------------------------

SELECT DISTINCT
    r.id,
    r.title,
    r.publication_date,
    r.journal,
    p.name as author_name,
    rp.person_position as author_position,
    d.doi
FROM
    ResearchOutputs r
    JOIN ResearchOutputs_People rp ON r.id = rp.publication_id
    JOIN People p ON p.id = rp.person_id
    LEFT JOIN Dois d ON r.doi_id = d.id
WHERE
    LOWER(p.name) LIKE LOWER('%sander%') -- sander kristina.kovaite
    AND LOWER(p.name) LIKE LOWER('%münster%') -- münster
ORDER BY
    r.publication_date DESC,
    r.title;

----------------------------------------------
-- Search for all people working on a paper --
----------------------------------------------

WITH target_publication AS (
    SELECT id
    FROM ResearchOutputs
    WHERE LOWER(title) = LOWER('How to involve inhabitants in urban design planning by using digital tools? An overview on a state of the art, key challenges and promising approaches')
)
SELECT
    p.name,
    rp.person_position as author_position
FROM
    target_publication tp
    JOIN ResearchOutputs_People rp ON tp.id = rp.publication_id
    JOIN People p ON p.id = rp.person_id
ORDER BY
    rp.person_position;