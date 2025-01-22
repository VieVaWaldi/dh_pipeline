DROP TABLE topics CASCADE;
DROP TABLE researchoutputs_topics;
DROP TABLE projects_topics;

-----------------------------------------------
-- CORE ENTITIES							  --
----------------------------------------------- 

SELECT version();
SELECT pg_size_pretty(pg_database_size('db_digicher'));

select count(*) from people; 				-- 276k
select count(*) from topics; 			    -- 929 -- 944
select count(*) from weblinks; 			-- 45k
select count(*) from dois; 					-- 73k
select count(*) from researchoutputs; 		-- 140k
select count(*) from institutions; 		-- 31k
select count(*) from fundingprogrammes; 	-- 3k
select count(*) from projects; 			-- 12k / 6k with doi

-- MIA sources, people: title phone
-- ADD type publication for arxiv, core
-- FIX geoLocation is swapped

-----------------------------------------------
-- JUNCTIONS							      --
----------------------------------------------- 

--- ResearchOutputs to Others ---

select count(*) from researchoutputs_people; 		-- 480k
select count(*) from researchoutputs_topics; 		-- 414k -- 0
select count(*) from researchoutputs_weblinks; 	-- 43k

--- Institutions to Others ---

select count(*) from institutions_people; 		    -- 0
select count(*) from institutions_researchOutputs; -- 5k

--- Projects to Others ---

select count(*) from projects_topics;              -- 70k // 0k // 104k
select count(*) from projects_weblinks;            -- 1.7k
select count(*) from projects_researchOutputs;     -- 140k
select count(*) from projects_institutions;        -- 79k
select count(*) from projects_fundingprogrammes;   -- 29k