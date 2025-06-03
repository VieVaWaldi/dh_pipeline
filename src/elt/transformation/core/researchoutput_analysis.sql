-----------------------------------------------
-- Core Researchoutput Analysis

-----------------------------------------------
-- Table Description

select count(*) from core.researchoutput;
-- 451_800 papers in total

-- Describe all ro's
SELECT 
    source_system AS "Source System",
    COUNT(*) AS "Total Papers",
    SUM(CASE WHEN doi IS NOT NULL THEN 1 ELSE 0 END) AS "DOIs Present",
    (SUM(CASE WHEN doi IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric(10,2) AS "DOI Coverage %",
    SUM(CASE WHEN language_code IS NOT NULL THEN 1 ELSE 0 END) AS "Language Specified",
    (SUM(CASE WHEN language_code IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric(10,2) AS "Language Coverage %",
    SUM(CASE WHEN type IS NOT NULL THEN 1 ELSE 0 END) AS "Type Specified",
    (SUM(CASE WHEN type IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric(10,2) AS "Type Coverage %",
    SUM(CASE WHEN abstract IS NOT NULL THEN 1 ELSE 0 END) AS "Abstracts Present",
    (SUM(CASE WHEN abstract IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric(10,2) AS "Abstract Coverage %",
    SUM(CASE WHEN full_text IS NOT NULL THEN 1 ELSE 0 END) AS "Full Texts Present",
    (SUM(CASE WHEN full_text IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric(10,2) AS "Full Text Coverage %",
    SUM(CASE WHEN comment IS NOT NULL THEN 1 ELSE 0 END) AS "Comments Present",
    (SUM(CASE WHEN comment IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*))::numeric(10,2) AS "Comment Coverage %"
FROM 
    core.researchoutput
GROUP BY 
    source_system
ORDER BY 
    "Total Papers" DESC;
FROM 
    core.researchoutput
GROUP BY 
    source_system
ORDER BY 
    total_count DESC;

-- Mistakes: 
-- - Arxiv actually has no language_code, so only half of coreac data has one
-- - Arxiv and Cordis have no type, Coreac gets type publication when it has no type

-----------------------------------------------
-- Researchoutput per year per source

SELECT EXTRACT(YEAR FROM publication_date) AS year, 
		source_system,
       COUNT(*) AS paper_count
FROM core.researchoutput
WHERE publication_date IS NOT NULL
GROUP BY year, source_system
ORDER BY source_system, year DESC;

-- Big jump cordis in 2020, also with duplicates?
-- Coreac way less papers 2024, probably because of extractor mistakes

-----------------------------------------------
-- Most prominent Arxiv topics
-- - Cant do good topic analysis before we didnt assign our own topics
-- - Only Arxiv has topics, Cordis only has it for projects, Coreac has none

SELECT t.name, t.source_system, COUNT(*) AS paper_count
FROM core.topic t
JOIN core.j_researchoutput_topic jrt ON t.id = jrt.topic_id
GROUP BY t.name, t.source_system
ORDER BY t.source_system, paper_count DESC
LIMIT 20;

-- For Arxiv only:
-- 1. CV Computer Vision
-- 2. CL Computation and Language
-- 3. AI Aritifical Intelligence
-- 4. LG Machine Learning
-- 5. HC Human Computer Interaction
-- 6. CY Computer and Society
-- 7. RO Robotics
-- 8. eess.IV Image and Video Processing
-- 9. stat.ML Machine Learning
-- 10.NE Neural and Evolutionary Computing

-- 8 from top 10 are cs, but arxiv is more technical in nature

-----------------------------------------------
-- Top Authors

SELECT p.name, COUNT(*) AS paper_count
FROM core.person p
JOIN core.j_researchoutput_person jrp ON p.id = jrp.person_id
WHERE jrp.role = 'author'
GROUP BY p.name
ORDER BY paper_count DESC
LIMIT 100;

-- Lots of institutional names in here, its a list of the most common names after that
-- a couple of actual people though!

-- Top collaborators
SELECT p1.name AS author1, p2.name AS author2, COUNT(*) AS collaboration_count
FROM core.j_researchoutput_person jrp1
JOIN core.j_researchoutput_person jrp2 ON jrp1.researchoutput_id = jrp2.researchoutput_id
JOIN core.person p1 ON jrp1.person_id = p1.id
JOIN core.person p2 ON jrp2.person_id = p2.id
WHERE jrp1.role = 'author' AND jrp2.role = 'author'
AND jrp1.person_id < jrp2.person_id  -- Avoid duplicate pairs
GROUP BY p1.name, p2.name
ORDER BY collaboration_count DESC
LIMIT 30;

-- havent run this yet

-----------------------------------------------
-- Journals
-- - There is no information about publishers


-- Top 20 Journals
SELECT j.name, COUNT(*) AS paper_count
FROM core.journal j
JOIN core.j_researchoutput_journal jrj ON j.id = jrj.journal_id
GROUP BY j.name
ORDER BY paper_count DESC
LIMIT 20;

-- This is actually useful data, top is something cultural for children
-- then 2 times nature, sustainability which is another big journal

-- Average papers per journal
SELECT AVG(paper_count) AS avg_papers_per_journal
FROM (
    SELECT j.id, COUNT(*) AS paper_count
    FROM core.journal j
    JOIN core.j_researchoutput_journal jrj ON j.id = jrj.journal_id
    GROUP BY j.id
) AS journal_counts;

--  2.5, alright, cool

-----------------------------------------------
-- Temporal Analysis

SELECT source_system, 
	   EXTRACT(YEAR FROM publication_date) AS year,
       EXTRACT(MONTH FROM publication_date) AS month,
       COUNT(*) AS paper_count
FROM core.researchoutput
WHERE publication_date IS NOT NULL
	AND EXTRACT(YEAR FROM publication_date) != 2020
	AND EXTRACT(YEAR FROM publication_date) != 2024
GROUP BY source_system, year, month
ORDER BY source_system, year DESC, month DESC;

-- > Made plot in python, more papers every year
-- Arxiv no trend
-- Coreac consistently most papers in january
-- Cordis spike in may and oct/dez

-----------------------------------------------
-- Word Analysis

-- For titles 
SELECT word, COUNT(*) AS word_count
FROM (
    SELECT regexp_split_to_table(LOWER(title), '\s+') AS word
    FROM core.researchoutput
) AS words
WHERE length(word) > 3  -- Exclude short words
AND word !~ '[^a-z]'    -- Only include alphabetic words
GROUP BY word
ORDER BY word_count DESC
LIMIT 1000;

-- > Made a word cloud out of it in python

-- For abstracts
SELECT word, COUNT(*) AS word_count
FROM (
    SELECT regexp_split_to_table(LOWER(abstract), '\s+') AS word
    FROM core.researchoutput
) AS words
WHERE length(word) > 3  -- Exclude short words
AND word !~ '[^a-z]'    -- Only include alphabetic words
GROUP BY word
ORDER BY word_count DESC
LIMIT 1000;