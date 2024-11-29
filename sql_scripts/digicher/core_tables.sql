-----------------------------------------------
-- CORE ENTITIES							  --
----------------------------------------------- 

-- MIA sources

select * from people; -- 270k
-- MIA title, phone 
select * from topics; -- 929 for euroSciVoc
-- FIX, only euroSciVoc
select * from weblinks; -- 45k
select * from dois;
-- MIA dois
select distinct(type) from researchoutputs; -- 140k
-- MIA publication date, doi
-- FIX type publication for arxiv, core
select * from institutions; -- 31k
-- FIX sme: false
select * from fundingprogrammes; -- 3k
select * from projects; -- 12k
-- MIA doi, funding


-----------------------------------------------
-- JUNCTIONS							      --
----------------------------------------------- 


--- ResearchOutputs to Others ---

select * from researchoutputs_people; -- 480k
select * from researchoutputs_topics; -- 414k
select * from researchoutputs_weblinks; -- 43k

--- Institutions to Others ---

select count(*) from institutions_people;
select count(*) from institutions_researchOutputs;

--- Projects to Others ---

select count(*) from projects_topics;
select count(*) from projects_weblinks;
select count(*) from projects_researchOutputs;
select count(*) from projects_institutions;