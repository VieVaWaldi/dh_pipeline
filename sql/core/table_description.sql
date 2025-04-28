-----------------------------------------------
-- ENTITIES COUNT

select count(*) from core.researchoutput;
--
select count(*) from core.person;
--
select count(*) from core.link;
--
select count(*) from core.journal;
--

-----------------------------------------------
-- JUNCTIONS COUNT

select count(*) from core.j_researchoutput_person;
-- 
select count(*) from core.j_researchoutput_link;
-- 

-----------------------------------------------
-- RMME TESTs

select count(*) from core.researchoutput;
where full_text is not null;

select count(*) from core.researchoutput
where source_system = 'cordis';
--
select count(*) from core.researchoutput
where source_system = 'arxiv';
--
select count(*) from core.researchoutput
where source_system = 'coreac';

-- DEDUP for source

SELECT source_id, source_system, COUNT(*) 
FROM core.researchoutput 
WHERE source_system = 'coreac'
GROUP BY source_id, source_system
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;

-- DEDUP for all

-- First find potential blocks of similar titles
-- dont search within same source
WITH title_blocks AS (
    SELECT id, title, substring(LOWER(title) from 1 for 5) AS block
    FROM core.researchoutput
)
SELECT t1.id as id1, t2.id as id2, 
       t1.title as title1, t2.title as title2,
       similarity(LOWER(t1.title), LOWER(t2.title)) as similarity_score
FROM title_blocks t1
JOIN title_blocks t2 ON t1.id < t2.id AND t1.block = t2.block
WHERE similarity(LOWER(t1.title), LOWER(t2.title)) > 0.9
-- AND title not like '%attachment%'
ORDER BY similarity_score DESC;