-----------------------------------------------
-- CORE ENTITIES							  --
----------------------------------------------- 

-- Size --
SELECT pg_size_pretty(pg_database_size('db_digicher'));

-- MIA sources
select count(*) from people; 				-- 270k
-- MIA title, phone 
select * from topics; 						-- 929
select count(*) from weblinks; 			-- 45k
select count(*)from dois; 					-- 73k
select count(*) from researchoutputs; 		-- 140k
-- ADD type publication for arxiv, core
select * from institutions; 				-- 31k
-- FIX geoLocation is swapped
select count(*) from fundingprogrammes; 	-- 3k
select * from projects; 					-- 12k / 6k with doi

-----------------------------------------------
-- JUNCTIONS							      --
----------------------------------------------- 


--- ResearchOutputs to Others ---

select * from researchoutputs_people; 		-- 480k
select * from researchoutputs_topics; 		-- 414k
select * from researchoutputs_weblinks; 	-- 43k

--- Institutions to Others ---

select * from institutions_people; 		-- 0
-- MIA
select count(*) from institutions_researchOutputs; -- 5k

--- Projects to Others ---

select * from projects_topics; -- 70k
select * from projects_weblinks; -- 1.7k
select * from projects_researchOutputs; -- 140k
select * from projects_institutions; -- 79k
select count(*) from projects_fundingprogrammes; -- 29k

-----------------------------------------------
-- MATERIALIZED 							  --
----------------------------------------------- 

select p.id_original, p.title, r.title 
from projects as p
LEFT JOIN projects_researchoutputs as pr ON p.id = pr.project_id
LEFT JOIN researchoutputs as r ON r.id = pr.publication_id
where p.id_original = '658760';