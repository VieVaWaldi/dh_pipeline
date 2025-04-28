-----------------------------------------------
-- CORDIS ENTITIES

select * from openaire.organization;
-- 17_470

select * from openaire.project;
-- 10_263

-------

select * from cordis.project;
-- 12_792
select count(*) from cordis.researchoutput;
-- 204_372
-- 75_480 with doi
-- 50_026 with fulltext
select count(*) from cordis.institution;
--
select count(*) from cordis.person;
--
select count(*) from cordis.topic;
--
select count(*) from cordis.weblink;
--
select count(*) from cordis.fundingprogramme;
--


-----------------------------------------------
-- JUNCTIONS

-----------------------------------------------
-- JUNCTIONS Institution

select count(*) from cordis.j_institution_person;
--

-----------------------------------------------
-- JUNCTIONS Research Output

select count(*) from cordis.j_researchoutput_person;
--
select count(*) from cordis.j_researchoutput_topic;
--
select count(*) from cordis.j_researchoutput_weblink;
--
select count(*) from cordis.j_researchoutput_institution;
--

-----------------------------------------------
-- JUNCTIONS Project

select count(*) from cordis.j_project_topic;
--
select count(*) from cordis.j_project_weblink;
--
select count(*) from cordis.j_project_fundingprogramme;
--
select count(*) from cordis.j_project_institution;
--
select count(*) from cordis.j_project_researchoutput;
--

-----------------------------------------------
-- Analysis

SELECT extract(year from publication_date) as year, COUNT(*) 
FROM cordis.researchoutput 
GROUP BY year
order by year;