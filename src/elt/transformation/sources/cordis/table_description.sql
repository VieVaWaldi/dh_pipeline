-----------------------------------------------
-- CORDIS ENTITIES

SELECT COUNT(*) FROM cordis.project;
-- 63_992
SELECT COUNT(*) FROM cordis.researchoutput;
-- 633_393
-- 279_010 with doi
-- 548_356 not from pdf
-- 5817 with fulltext -- OOM error
SELECT COUNT(*) FROM cordis.institution;
-- 92_587
SELECT COUNT(*) FROM cordis.person;
-- 972_968
SELECT COUNT(*) FROM cordis.topic;
-- 1_023
SELECT COUNT(*) FROM cordis.weblink;
-- 183_976
SELECT COUNT(*) FROM cordis.fundingprogramme;
-- 8_569


-----------------------------------------------
-- JUNCTIONS

-----------------------------------------------
-- JUNCTIONS Institution

SELECT COUNT(*) FROM cordis.j_institution_person;
-- 0

-----------------------------------------------
-- JUNCTIONS Research Output

SELECT COUNT(*) FROM cordis.j_researchoutput_person;
-- 1_060_625
SELECT COUNT(*) FROM cordis.j_researchoutput_topic;
-- 0
SELECT COUNT(*) FROM cordis.j_researchoutput_weblink;
-- 246
SELECT COUNT(*) FROM cordis.j_researchoutput_institution;
-- 3_885

-----------------------------------------------
-- JUNCTIONS Project

SELECT COUNT(*) FROM cordis.j_project_topic;
-- 498_778
SELECT COUNT(*) FROM cordis.j_project_weblink;
-- 9_089
SELECT COUNT(*) FROM cordis.j_project_fundingprogramme;
-- 141_363
SELECT COUNT(*) FROM cordis.j_project_institution;
-- 369_998
SELECT COUNT(*) FROM cordis.j_project_researchoutput;
-- 633_393

-----------------------------------------------
-- Analysis

select * from cordis.project
order by start_date DESC
limit 100;
-- latest 2027-07-01

select distinct framework_programme from cordis.fundingprogramme;
-- 21 FPs

SELECT extract(year from publication_date) as year, COUNT(*) 
FROM cordis.researchoutput 
GROUP BY year
order by year;