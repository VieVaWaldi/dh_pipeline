-----------------------------------------------
-- CORE ENTITIES

select * from cordis.person;
--
select * from cordis.topic;
--
select count(*) from cordis.weblink;
--
select count(*) from cordis.fundingprogramme;
--

-----------------------------------------------
-- MAIN TABLES

select * from cordis.institution;
--
select count(*) from cordis.researchoutput;
--
select * from cordis.project;
-- 23

-----------------------------------------------
-- JUNCTIONS

-----------------------------------------------
-- Institution

select count(*) from cordis.j_institution_person;
--

-----------------------------------------------
-- Research Output

select count(*) from cordis.j_researchoutput_person;
--
select count(*) from cordis.j_researchoutput_topic;
--
select count(*) from cordis.j_researchoutput_weblink;
--
select count(*) from cordis.j_researchoutput_institution;
--

-----------------------------------------------
-- Project

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