-----------------------------------------------
-- CORE ENTITIES							  --
----------------------------------------------- 

-- MIA sources

select * from people; -- 270k
-- MIA title, phone 
select * from topics; -- 929 for euroSciVoc +170ish extra
    -- FIX, only euroSciVoc
select * from weblinks; -- 45k
select * from dois;
	-- MIA dois
select * from researchoutputs; -- 140k
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

select * from institutions_people; -- 0
-- MIA
select * from institutions_researchOutputs; -- 5k

--- Projects to Others ---

select * from projects_topics; -- 70k
select * from projects_weblinks; -- 1.7k
select * from projects_researchOutputs; -- 140k
select * from projects_institutions; -- 79k
select count(*) from projects_fundingprogrammes; -- 79k