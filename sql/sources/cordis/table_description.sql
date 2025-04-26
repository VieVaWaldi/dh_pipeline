-----------------------------------------------
-- CORE ENTITIES

-- NOT DONE do not COUNT

select * from cordis.project;
-- 7k
select count(*) from cordis.researchoutput;
-- 90_911
-- 34_302 with doi
-- 18_019 with fulltext
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