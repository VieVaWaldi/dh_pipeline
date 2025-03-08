select * from topics
where created_at >= '2024-03-07'; 			    -- 195
select * from weblinks
where created_at >= '2024-03-07'; 			-- 1293
select * from dois
where created_at >= '2024-03-07'; 					-- 982
select * from institutions
where created_at >= '2024-03-07'; 		-- 553
select * from fundingprogrammes
where created_at >= '2024-03-07'; 	-- 62
select * from projects
where created_at >= '2024-03-07'; 			-- 68

-----------------------------------------------
-- CORE ENTITIES							  --
----------------------------------------------- 

SELECT version();
SELECT pg_size_pretty(pg_database_size('db_digicher'));

select count(*) from people; 				-- 276k
select count(*) from topics; 			    -- 944
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

{53.33306,-6.24889}

--- ResearchOutputs to Others ---

select count(*) from researchoutputs_people; 		-- 480k
select count(*) from researchoutputs_topics; 		-- 0
select count(*) from researchoutputs_weblinks; 	-- 43k

--- Institutions to Others ---

select count(*) from institutions_people; 		    -- 0 (about 5 k missing)
select count(*) from institutions_researchOutputs; -- 5k

--- Projects to Others ---

select count(*) from projects_topics;              -- 104k
select count(*) from projects_weblinks;            -- 1.7k
select count(*) from projects_researchOutputs;     -- 140k
select count(*) from projects_institutions;        -- 79k
select count(*) from projects_fundingprogrammes;   -- 29k