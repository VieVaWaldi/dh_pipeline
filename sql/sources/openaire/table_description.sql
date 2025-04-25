-----------------------------------------------
-- Openaire Table Description

-----------------------------------------------
-- Entities Count

select count(*) from openaire.project;
-- 10263
select count(*) from openaire.researchoutput;
-- 0
select count(*) from openaire.organization;
-- 17470
-- the is_first_listed is dumb obviously, put it in the junction
-- make country_code and label None, not unknown

select count(*) from openaire.subject;
-- 264
select count(*) from openaire.measure;
-- 1904

select count(*) from openaire.funder;
-- 21
select count(*) from openaire.funding_stream;
-- 238
select count(*) from openaire.h2020_programme;
-- 0

-----------------------------------------------
-- JUNCTION Count

select count(*) from openaire.j_project_researchoutput;
-- 0
select count(*) from openaire.j_project_organization;
-- 33505
select count(*) from openaire.j_project_subject;
-- 1264
select count(*) from openaire.j_project_measure;
-- 9959
select count(*) from openaire.j_project_funder;
-- 8728
select count(*) from openaire.j_project_funding_stream;
-- 4552
select count(*) from openaire.j_project_h2020_programme;
-- 0

-----------------------------------------------
-- Project

SELECT
  'doi' AS column_name,
  COUNT(*) - COUNT(doi) AS null_count,
  ROUND(COUNT(doi) * 100.0 / COUNT(*), 2) AS not_null_percentage
FROM openaire.project
UNION ALL
SELECT 'acronym', COUNT(*) - COUNT(acronym), ROUND(COUNT(acronym) * 100.0 / COUNT(*), 2) FROM openaire.project
UNION ALL
SELECT 'start_date', COUNT(*) - COUNT(start_date), ROUND(COUNT(start_date) * 100.0 / COUNT(*), 2) FROM openaire.project
UNION ALL
SELECT 'end_date', COUNT(*) - COUNT(end_date), ROUND(COUNT(end_date) * 100.0 / COUNT(*), 2) FROM openaire.project
UNION ALL
SELECT 'duration', COUNT(*) - COUNT(duration), ROUND(COUNT(duration) * 100.0 / COUNT(*), 2) FROM openaire.project
UNION ALL
SELECT 'summary', COUNT(*) - COUNT(summary), ROUND(COUNT(summary) * 100.0 / COUNT(*), 2) FROM openaire.project
UNION ALL
SELECT 'total_cost', COUNT(*) - COUNT(total_cost), ROUND(COUNT(total_cost) * 100.0 / COUNT(*), 2) FROM openaire.project
UNION ALL
SELECT 'funded_amount', COUNT(*) - COUNT(funded_amount), ROUND(COUNT(funded_amount) * 100.0 / COUNT(*), 2) FROM openaire.project
UNION ALL
SELECT 'website_url', COUNT(*) - COUNT(website_url), ROUND(COUNT(website_url) * 100.0 / COUNT(*), 2) FROM openaire.project
UNION ALL
SELECT 'call_identifier', COUNT(*) - COUNT(call_identifier), ROUND(COUNT(call_identifier) * 100.0 / COUNT(*), 2) FROM openaire.project;

-- "doi"	9067	11.65
-- "call_identifier"	9142	10.92
-- "website_url"	10263	0.00
-- "funded_amount"	0	100.00
-- "total_cost"	0	100.00
-- "summary"	4827	52.97
-- "acronym"	8661	15.61
-- "end_date"	3219	68.63
-- "start_date"	3137	69.43
-- "duration"	0	100.00

-----------------------------------------------
-- Organizations

select * from openaire.organization limit 100;