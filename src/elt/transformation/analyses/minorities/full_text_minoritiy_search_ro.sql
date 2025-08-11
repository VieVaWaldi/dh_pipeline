select count(*) from core.researchoutput
where title is not null
and abstract is not null;

-- -> Searching on 268497 RO's

SELECT *
FROM core.researchoutput
WHERE title IS NOT NULL 
  AND abstract IS NOT NULL
  AND (
    to_tsvector('english', title) @@ 'ladin'::tsquery
    OR 
    to_tsvector('english', coalesce(abstract, '')) @@ 'ladin'::tsquery
  );

-- 1. Simple one word Keyword search

-- ladin: 1 result
-- sami: 30 results
-- jewish: 106 results

-- 2. Multi Keyword search

-- ladin | ladino | gardenese | badiese | fascian | marebbano | ampezzan
-- -> 3 results
-- sami | saami | sapmi | same | joik | yoik
-- -> 58 results
-- jewish | jew | jews | judaism | hebrew | yiddish
-- -> 199 results