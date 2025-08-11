select count(*) from cordis.project
where title is not null
and objective is not null;
-- 12637 projects

select count(*) from openaire.project 
where title is not null 
and summary is not null;
-- 5436 projects

-- Searching on 18073 projects

SELECT 'cordis' as source, id, acronym, title, objective as description
FROM cordis.project
WHERE title IS NOT NULL 
  AND objective IS NOT NULL
  AND (
    to_tsvector('english', title) @@ 'jewish | jew | jews | judaism | hebrew | yiddish'::tsquery
    OR 
    to_tsvector('english', coalesce(objective, '')) @@ 'jewish | jew | jews | judaism | hebrew | yiddish'::tsquery
  )
UNION ALL
SELECT 'openaire' as source, id, acronym, title, summary as description
FROM openaire.project
WHERE title IS NOT NULL 
  AND summary IS NOT NULL
  AND (
    to_tsvector('english', title) @@ 'jewish | jew | jews | judaism | hebrew | yiddish'::tsquery
    OR 
    to_tsvector('english', coalesce(summary, '')) @@ 'jewish | jew | jews | judaism | hebrew | yiddish'::tsquery
  );

-- 1. Simple one word Keyword search

-- ladin: 1 result -> Only DIGICHer
-- sami: 9 results
-- jewish: 112 results

-- 2. Multi Keyword search

-- ladin | ladino | gardenese | badiese | fascian | marebbano | ampezzan
-- -> 8 results
-- sami | saami | sapmi | same | joik | yoik
-- -> 16 results
-- jewish | jew | jews | judaism | hebrew | yiddish
-- -> 157 results