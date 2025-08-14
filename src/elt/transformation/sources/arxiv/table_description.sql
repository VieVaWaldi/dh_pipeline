-----------------------------------------------
-- ENTITIES COUNT

select count(*) from arxiv.entry;
where doi is not null;
-- 46_312
-- 7_844 with doi
-- 44_385 with fulltext
select count(*) from arxiv.author;
-- 112_484
select count(*) from arxiv.link;
-- 100_481

-----------------------------------------------
-- JUNCTIONS COUNT

select count(*) from arxiv.j_entry_author;
-- 219_058
select count(*) from arxiv.j_entry_link;
-- 100_489 - 12