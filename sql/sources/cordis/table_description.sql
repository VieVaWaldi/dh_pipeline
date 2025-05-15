-----------------------------------------------
-- CORDIS ENTITIES

select * from cordis.project;
-- 12_792
select count(*) from cordis.researchoutput;
-- 204_372
-- 75_480 with doi
-- 50_026 with fulltext
select count(*) from cordis.institution;
-- 32_518
select count(*) from cordis.person;
-- 452_216
select count(*) from cordis.topic;
-- 950
select count(*) from cordis.weblink;
-- 49_944
select count(*) from cordis.fundingprogramme;
-- 3_214


-----------------------------------------------
-- JUNCTIONS

-----------------------------------------------
-- JUNCTIONS Institution

select count(*) from cordis.j_institution_person;
-- 1_381

-----------------------------------------------
-- JUNCTIONS Research Output

select count(*) from cordis.j_researchoutput_person;
-- 0
select count(*) from cordis.j_researchoutput_topic;
-- 0
select count(*) from cordis.j_researchoutput_weblink;
-- 0
select count(*) from cordis.j_researchoutput_institution;
-- 0

-----------------------------------------------
-- JUNCTIONS Project

select count(*) from cordis.j_project_topic;
-- 101_311
select count(*) from cordis.j_project_weblink;
-- 1_800
select count(*) from cordis.j_project_fundingprogramme;
-- 30_316
select count(*) from cordis.j_project_institution;
-- 82_868
select count(*) from cordis.j_project_researchoutput;
-- 204_372

-----------------------------------------------
-- Analysis

SELECT extract(year from publication_date) as year, COUNT(*) 
FROM cordis.researchoutput 
GROUP BY year
order by year;