select count(*) from people; -- 225
delete from people;
select count(*) from dois; -- 3
delete from dois;
select count(*) from topics; -- 19
select count(*) from weblinks; -- 87
select count(*) from researchoutputs; -- 42
select count(*) from institutions; -- 42
select count(*) from fundingprogrammes;

select count(*) from researchoutputs_people; -- 225
select count(*) from researchoutputs_topics; -- 77
select count(*) from researchoutputs_weblinks; --  87
select count(*) from institutions_people; -- 0


SELECT DISTINCT r.*
FROM ResearchOutputs r
JOIN ResearchOutputs_Topics rt ON r.id = rt.publication_id
JOIN Topics t ON rt.topic_id = t.id
WHERE t.name = 'cs.AI';

SELECT 
    t.name as topic_name,
    t.standardised_name,
    COUNT(rt.publication_id) as research_output_count
FROM Topics t
LEFT JOIN ResearchOutputs_Topics rt ON t.id = rt.topic_id
GROUP BY t.id, t.name, t.standardised_name
ORDER BY COUNT(rt.publication_id) DESC;

select s.source, Count(*) -- pub.title, s.source 
from publications as pub
join sources as s on s.entity_id = pub.id
group by s.source;
