-----------------------------------------------
-- OPEN AIRE ENTITIES

-----------------------------------------------
-- Entities Count

SELECT COUNT(*) FROM openaire.project;
-- 29_486
-- 4_887 with doi
SELECT COUNT(*) FROM openaire.researchoutput; -- forgot pid: doi
-- 140_352
SELECT COUNT(*) FROM openaire.organization;
-- 90_483

SELECT COUNT(*) FROM openaire.container;
-- Journal and publisher information for RO
-- 17_622
SELECT COUNT(*) FROM openaire.author;
-- 413_596

SELECT COUNT(*) FROM openaire.subject;
-- 1_074
SELECT COUNT(*) FROM openaire.measure;
-- 2_170

SELECT COUNT(*) FROM openaire.funder;
-- eg european commision or national ones
-- 33
SELECT COUNT(*) FROM openaire.funding_stream;
-- 552
SELECT COUNT(*) FROM openaire.h2020_programme;
-- 0

-----------------------------------------------
-- JUNCTION Count

SELECT COUNT(*) FROM openaire.j_project_researchoutput;
-- 140_331
SELECT COUNT(*) FROM openaire.j_project_organization;
-- 80_167
SELECT COUNT(*) FROM openaire.j_project_subject;
-- 4_404
SELECT COUNT(*) FROM openaire.j_project_measure;
-- 30_776
SELECT COUNT(*) FROM openaire.j_project_funder;
-- 18_291
SELECT COUNT(*) FROM openaire.j_project_funding_stream;
-- 15_585
SELECT COUNT(*) FROM openaire.j_project_h2020_programme;
-- 0

SELECT COUNT(*) FROM openaire.j_researchoutput_author;
-- 738_177
SELECT COUNT(*) FROM openaire.j_researchoutput_organization;
-- 215_537

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

select * FROM openaire.organization limit 100;