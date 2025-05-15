-----------------------------------------------
-- Deduplication Projects

CREATE EXTENSION IF NOT EXISTS pg_trgm;

select count (*) from cordis.project
where doi is not null;
-- 12792, 11367 with acronym, 7053 with doi

select count (*) from openaire.project
where doi is not null;
-- 10263, 1602 with acronym, 1196 with doi

-----------------------------------------------
-- Cordis Projects Deduplication

-----------------------------------------------
-- Project Deduplication

-----------------------------------------------
-- Project Deduplication Between Cordis and OpenAIRE

-----------------------------------------------
-- Exact Title Matching

-- Find duplicates with exact title matches
WITH unified_projects AS (
    SELECT id, title, 'cordis' AS source_system, id_original AS source_id
    FROM cordis.project
    UNION ALL
    SELECT id, title, 'openaire' AS source_system, id_original AS source_id  
    FROM openaire.project
)
SELECT DISTINCT LEAST(a.id, b.id) AS id1, GREATEST(a.id, b.id) AS id2, 
       a.title, a.source_system AS source1, b.source_system AS source2,
       a.source_id AS source_id1, b.source_id AS source_id2
FROM unified_projects a
JOIN unified_projects b ON 
    LOWER(a.title) = LOWER(b.title) AND 
    a.id < b.id 
;
-- 12792 +10263 projects = 23055
-- 3497 duplicate projects found = 19558

-----------------------------------------------
-- Fuzzy Title Matching

DROP MATERIALIZED VIEW IF EXISTS project_dedupe_candidates;

-- Materialized view with normalized titles and title length for efficiency  
CREATE MATERIALIZED VIEW project_dedupe_candidates AS
SELECT 
    id, 
    source_system,
    title,
    LOWER(regexp_replace(title, '[[:punct:][:space:]]+', '', 'g')) AS normalized_title,
    LENGTH(LOWER(regexp_replace(title, '[[:punct:][:space:]]+', '', 'g'))) AS title_length,
    id_original AS source_id,
    acronym,
    start_date,
    end_date
FROM (
    SELECT id, title, 'cordis' AS source_system, id_original, acronym, start_date, end_date
    FROM cordis.project
    UNION ALL  
    SELECT id, title, 'openaire' AS source_system, id_original, acronym, start_date, end_date
    FROM openaire.project
) combined_projects;

-- Indexes for performance
CREATE INDEX idx_project_dedupe_norm_title ON project_dedupe_candidates(normalized_title);
CREATE INDEX idx_project_dedupe_title_length ON project_dedupe_candidates(title_length);
CREATE INDEX idx_project_dedupe_source ON project_dedupe_candidates(source_system);
CREATE INDEX idx_project_dedupe_acronym ON project_dedupe_candidates(acronym) WHERE acronym IS NOT NULL;

-- Query with fuzzy matching and additional constraints
SELECT DISTINCT LEAST(a.id, b.id) AS id1, GREATEST(a.id, b.id) AS id2, 
       a.title AS title1, b.title AS title2,
       a.source_system AS source1, b.source_system AS source2,
       a.source_id AS source_id1, b.source_id AS source_id2,
       a.acronym AS acronym1, b.acronym AS acronym2,
       a.start_date AS start_date1, b.start_date AS start_date2,
       a.end_date AS end_date1, b.end_date AS end_date2,
       similarity(a.normalized_title, b.normalized_title) AS sim_score
FROM project_dedupe_candidates a
JOIN project_dedupe_candidates b ON 
    a.id < b.id AND
    -- Different sources only  
    a.source_system != b.source_system AND
    -- Length constraint: only compare titles with similar lengths
    ABS(a.title_length - b.title_length) <= 5 AND
    -- Similarity threshold for titles
    similarity(a.normalized_title, b.normalized_title) > 0.9
WHERE
    -- Optional: Add acronym matching when available
    (a.acronym IS NULL OR b.acronym IS NULL OR LOWER(a.acronym) = LOWER(b.acronym))
    AND
    -- Optional: Date range overlap check (if dates are available)
    (a.start_date IS NULL OR b.start_date IS NULL OR b.end_date IS NULL OR a.start_date IS NULL OR
     a.start_date <= b.end_date AND a.end_date >= b.start_date)
ORDER BY sim_score DESC;

-----------------------------------------------
-- Analysis Query to Check Deduplication Results

-- Count exact title matches
WITH exact_matches AS (
    SELECT COUNT(*) AS exact_count
    FROM (
        SELECT DISTINCT LEAST(a.id, b.id) AS id1, GREATEST(a.id, b.id) AS id2
        FROM project_dedupe_candidates a
        JOIN project_dedupe_candidates b ON 
            LOWER(a.title) = LOWER(b.title) AND 
            a.id < b.id AND
            a.source_system != b.source_system
    ) exact
)
-- Count fuzzy matches
, fuzzy_matches AS (
    SELECT COUNT(*) AS fuzzy_count  
    FROM (
        SELECT DISTINCT LEAST(a.id, b.id) AS id1, GREATEST(a.id, b.id) AS id2
        FROM project_dedupe_candidates a
        JOIN project_dedupe_candidates b ON 
            a.id < b.id AND
            a.source_system != b.source_system AND
            ABS(a.title_length - b.title_length) <= 5 AND
            similarity(a.normalized_title, b.normalized_title) > 0.9
    ) fuzzy
)
SELECT 
    (SELECT exact_count FROM exact_matches) AS exact_title_matches,
    (SELECT fuzzy_count FROM fuzzy_matches) AS fuzzy_title_matches,
    (SELECT COUNT(*) FROM cordis.project) AS cordis_project_count,
    (SELECT COUNT(*) FROM openaire.project) AS openaire_project_count;

-----------------------------------------------
-- Optional: Query to find potential mismatches with same acronym but different titles
SELECT 
    a.id AS cordis_id,
    b.id AS openaire_id,
    a.title AS cordis_title,
    b.title AS openaire_title,
    a.acronym AS acronym,
    similarity(a.normalized_title, b.normalized_title) AS sim_score,
    a.start_date AS cordis_start,
    b.start_date AS openaire_start,
    a.end_date AS cordis_end,
    b.end_date AS openaire_end
FROM project_dedupe_candidates a
JOIN project_dedupe_candidates b ON
    a.source_system = 'cordis' AND
    b.source_system = 'openaire' AND
    LOWER(a.acronym) = LOWER(b.acronym) AND
    a.acronym IS NOT NULL AND
    -- Different titles
    LOWER(a.title) != LOWER(b.title)
ORDER BY sim_score DESC;