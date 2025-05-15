-----------------------------------------------
-- Deduplication Understanding

-- Source_id from original source is unique, so duplicates should be low within source
-- Cant analyse cordis full_text papers because they have no title
-- Also lots of admin docs in cordis which have to be excluded

-----------------------------------------------
-- Deduplication Projects

-----------------------------------------------
-- DOI

-- Dois with duplicates
SELECT doi, COUNT(*) AS count
FROM core.researchoutput
WHERE doi IS NOT NULL
GROUP BY doi
HAVING COUNT(*) > 1
ORDER BY count DESC;

-- Sum of Dois with duplicates
SELECT SUM(count - 1) AS total_doi_duplicates
FROM (
    SELECT doi, COUNT(*) AS count
    FROM core.researchoutput
    WHERE doi IS NOT NULL
    GROUP BY doi
    HAVING COUNT(*) > 1
) AS dupes;

-- There are 3_112 papers reusing a doi

-----------------------------------------------
-- Matching Titles

-- Clean Cordis for administrative papers 
-- and full text papers which dont have a title yet!
SELECT id, title
FROM core.researchoutput
WHERE (
    (source_system = 'cordis' AND full_text IS NULL AND (
        title not ILIKE '%data management%' AND
        title not ILIKE '%report%' AND
        title not ILIKE '%congress%' AND 
        title not ILIKE '%communication plan%' AND
        title not ILIKE '%dissemination dtrategy%' AND 
        title not ILIKE '%call for the%'
    ))
    OR
    (source_system != 'cordis')
);
-- This reduces the duplicate search set to 370k papers 
-- from the original 450k papers

select count(*) from cordis.researchoutput where fulltext is not null;

-- Find Duplicates with exact title matches
WITH filtered_papers AS (
    SELECT id, source_id, source_system, title
    FROM core.researchoutput
    WHERE (
        (source_system = 'cordis' AND full_text IS NULL AND (
            title NOT ILIKE '%data management%' AND
            title NOT ILIKE '%report%' AND
            title NOT ILIKE '%congress%' AND 
            title NOT ILIKE '%communication plan%' AND
            title NOT ILIKE '%dissemination strategy%' AND 
            title NOT ILIKE '%call for the%'
        ))
        OR
        (source_system != 'cordis')
    )
)
SELECT DISTINCT LEAST(a.id, b.id) AS id1, GREATEST(a.id, b.id) AS id2, 
       a.title, a.source_system AS source1, b.source_system AS source2, a.source_id, b.source_id
FROM filtered_papers a
JOIN filtered_papers b ON 
    LOWER(a.title) = LOWER(b.title) AND 
    a.id < b.id
    -- Do not compare within source
    -- AND a.source_system != b.source_system
;
-- 2k duplicates, only comparing different sources
-- 73k duplicates, comparing everything

-----------------------------------------------
-- Matching Fuzzy Titles

DROP MATERIALIZED VIEW IF EXISTS dedupe_candidates;

-- Materialized view with normalized titles and title length for efficiency
CREATE MATERIALIZED VIEW dedupe_candidates AS
SELECT 
    id, 
    source_system, 
    title, 
    LOWER(regexp_replace(title, '[[:punct:][:space:]]+', '', 'g')) AS normalized_title,
    LENGTH(LOWER(regexp_replace(title, '[[:punct:][:space:]]+', '', 'g'))) AS title_length
FROM core.researchoutput
WHERE (
    (source_system = 'cordis' AND full_text IS NULL AND (
        title NOT ILIKE '%data management%' AND
        title NOT ILIKE '%report%' AND
        title NOT ILIKE '%congress%' AND 
        title NOT ILIKE '%communication plan%' AND
        title NOT ILIKE '%dissemination strategy%' AND 
        title NOT ILIKE '%call for the%'
    ))
    OR
    (source_system != 'cordis')
);

-- Indexes!
CREATE INDEX idx_dedupe_norm_title ON dedupe_candidates(normalized_title);
CREATE INDEX idx_dedupe_title_length ON dedupe_candidates(title_length);

-- Query with length filter
SELECT DISTINCT LEAST(a.id, b.id) AS id1, GREATEST(a.id, b.id) AS id2, 
       a.title AS title1, b.title AS title2,
       a.source_system AS source1, b.source_system AS source2,
       similarity(a.normalized_title, b.normalized_title) AS sim_score
FROM dedupe_candidates a
JOIN dedupe_candidates b ON 
    a.id < b.id AND
    -- Length constraint: only compare titles with similar lengths
    ABS(a.title_length - b.title_length) <= 5 AND
    -- Similarity threshold
    similarity(a.normalized_title, b.normalized_title) > 0.9 AND
    -- Different sources
    a.source_system != b.source_system
ORDER BY sim_score DESC;
