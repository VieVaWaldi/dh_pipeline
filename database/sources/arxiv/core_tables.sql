-----------------------------------------------
-- CORE ENTITIES

select * from arxiv.entry;
--
select count(*) from arxiv.author;
--
select * from arxiv.link;
--

-----------------------------------------------
-- JUNCTIONS

select count(*) from arxiv.j_entry_author;
--
select count(*) from arxiv.j_entry_link;
--
