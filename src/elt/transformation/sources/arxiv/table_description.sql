-----------------------------------------------
-- ENTITIES COUNT

select count(*) from arxiv.entry;
-- 99_200
-- 24_095 with doi
-- 44_385 with fulltext
select count(*) from arxiv.author;
-- 244_329
select count(*) from arxiv.link;
-- 222_513

-----------------------------------------------
-- JUNCTIONS COUNT

select count(*) from arxiv.j_entry_author;
-- 527_495
select count(*) from arxiv.j_entry_link;
-- 222_535