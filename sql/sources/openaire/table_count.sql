-----------------------------------------------
-- CORE ENTITIES

select * from openaire.project;
-- 
select * from openaire.researchoutput;
--
select * from openaire.organization;
-- the is_first_listed is dumb obviously, put it in the junction
-- make country_code and label None, not unknown

-----------------------------------------------
-- ENTITIES

select * from openaire.subject;
--
select * from openaire.measure;
--

-----------------------------------------------
-- FUNDING ENTITIES

select * from openaire.funder;
--
select * from openaire.funding_stream;
--
select * from openaire.h2020_programme;
--

-----------------------------------------------
-- JUNCTION TABLES

-----------------------------------------------
-- Project

select count(*) from openaire.j_project_researchoutput;
--
select count(*) from openaire.j_project_organization;
--
select count(*) from openaire.j_project_subject;
--
select count(*) from openaire.j_project_measure;
--
select count(*) from openaire.j_project_funder;
--
select count(*) from openaire.j_project_funding_stream;
--
select count(*) from openaire.j_project_h2020_programme;
--