select * from core.researchoutput r
join core.j_researchoutput_link jrl on jrl.researchoutput_id = r.id
join core.link l on jrl.link_id = l.id
limit 100;

-----------------------------------------------
-- ResearchOutput Data View
-----------------------------------------------

-----------------------------------------------
-- Filters

-- startYear, endYear
-- ft search
-- topic				-> j_topic
-- FP					-> j_FP

-----------------------------------------------
-- View
-----------------------------------------------

CREATE VIEW core.table_view_project AS
SELECT
	p.id,
	p.title,
	p.start_date, 
	p.end_date,
	p.acronym,
	p.objective,
	t.id as topic_id,
	t.subfield_id,
	t.field_id,
	t.domain_id,
	ARRAY_AGG(DISTINCT f.framework_programme) FILTER (WHERE f.framework_programme IS NOT NULL) AS framework_programmes
FROM core.project p
LEFT JOIN core.j_project_topicoa jpt on jpt.project_id = p.id
LEFT JOIN core.topicoa t on jpt.topic_id = t.id
LEFT JOIN core.j_project_fundingprogramme jpf on jpf.project_id = p.id
LEFT JOIN core.fundingprogramme f on jpf.fundingprogramme_id = f.id
GROUP BY p.id, p.title, p.start_date, p.end_date, p.acronym, p.objective, t.id, t.topic_name, t.subfield_name, t.field_name, t.domain_name;

-- 91956 prj
-- 91956 j_prj_top
-- 91956 f
-- 91665 j_prj_f -> group by dups i suppose

-----------------------------------------------
-- Select
-----------------------------------------------
	
select count(*) 
from core.table_view_project 
-- where lower(acronym) ='digicher'
limit 100;

select topic_name, id, subfield_id from core.topicoa
where id = '14346';

-----------------------------------------------
-- DROP                                      --
-----------------------------------------------

DROP VIEW core.table_view_project;