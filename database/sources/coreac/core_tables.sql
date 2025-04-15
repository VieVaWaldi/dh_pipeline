-----------------------------------------------
-- CORE ENTITIES

-- MIA
-- * Looks like i am parsing author names wrong, i am seperating where no separation is needed

select count(*) from coreac.work;
-- 
select count(*) from coreac.link;
--
select count(*) from coreac.reference;
--
select count(*) from coreac.data_provider;
--

-----------------------------------------------
-- JUNCTIONS

select count(*) from coreac.j_work_link;
--
select count(*) from coreac.j_work_reference;
--
select count(*) from coreac.j_work_data_provider;
--
