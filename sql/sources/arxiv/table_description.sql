-----------------------------------------------
-- ENTITIES COUNT

select count(*) from arxiv.entry;
where full_text is not null;
-- 46_312
-- 7_844 with doi
-- 44_385 with fulltext
select count(*) from arxiv.author;
-- 58_285
select count(*) from arxiv.link;
-- 45_0342

-----------------------------------------------
-- JUNCTIONS COUNT

select count(*) from arxiv.j_entry_author;
-- 90_292
select count(*) from arxiv.j_entry_link;
-- 45_033

-----------------------------------------------
-- Entry

select * from coreac.work limit 100;