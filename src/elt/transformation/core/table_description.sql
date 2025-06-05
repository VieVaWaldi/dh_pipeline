-----------------------------------------------
-- ENTITIES COUNT

select count(*) from core.researchoutput;
where full_text is null
limit 1000;
-- 451_800
-- 148_545 doi
-- 294_346 fulltext
select count(*) from core.topic;
-- 1_980
select count(*) from core.person;
-- 697_904
select count(*) from core.publisher;
-- 0
select distinct name from core.journal
order by name;
-- 44_259
select count(*) from core.link;
-- 891_010

-----------------------------------------------
-- JUNCTIONS COUNT

select count(*) from core.j_researchoutput_topic;
-- 94_448
select count(*) from core.j_researchoutput_person;
-- 1_019_255
select count(*) from core.j_journal_publisher;
-- 0
select count(*) from core.j_researchoutput_journal;
-- 112_451
select count(*) from core.j_researchoutput_link;
-- 895_712