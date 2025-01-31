SELECT distinct address_country from institutions;
WHERE lower(address_city) like lower('%jena%');


select * from topics ORDER BY level;
select * from weblinks limit 100;
select * from institutions limit 100;
select * from fundingprogrammes limit 100;
select distinct code from fundingprogrammes;
select * from projects limit 1000;

SELECT 
    pg_size_pretty(pg_total_relation_size('topics')) as topics_size,
    pg_size_pretty(pg_total_relation_size('fundingprogrammes')) as fundingprogrammes_size;

--- Projects to Others ---

select * from projects_topics;
select * from projects_weblinks;
select * from projects_institutions;
select * from projects_fundingprogrammes;


-- Dont need
-- select * from people;
-- select * from dois;
-- select * from researchoutputs;

-- select * from researchoutputs_people;
-- select * from researchoutputs_topics;
-- select * from researchoutputs_weblinks;

-- select * from institutions_people;
-- select * from institutions_researchOutputs;
-- select * from projects_researchOutputs;

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