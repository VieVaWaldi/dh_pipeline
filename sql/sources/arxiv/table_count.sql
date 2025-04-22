-----------------------------------------------
-- CORE ENTITIES

select * from arxiv.entry limit 100;
-- 20_598
select count(*) from arxiv.author;
-- 58_285
select count(*) from arxiv.link;
-- 45_0342

-----------------------------------------------
-- JUNCTIONS

select count(*) from arxiv.j_entry_author;
-- 90_292
select count(*) from arxiv.j_entry_link;
-- 45_033
