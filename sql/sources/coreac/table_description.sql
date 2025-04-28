-----------------------------------------------
-- CORE ENTITIES

-- MIA
-- * Looks like i am parsing author names wrong, i am seperating where no separation is needed

select count(*) from coreac.work;
where fulltext is not null;
-- 201_116
-- 65_221 with doi
-- 199_935
select count(*) from coreac.link;
-- 800_610
select count(*) from coreac.reference;
-- 368_309
select count(*) from coreac.data_provider;
-- 0

-----------------------------------------------
-- JUNCTIONS

select count(*) from coreac.j_work_link;
-- 800_738
select count(*) from coreac.j_work_reference;
-- 382_183
select count(*) from coreac.j_work_data_provider;
-- 0

