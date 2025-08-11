------------------------------
----  Data Exploration Arxiv

------------------------------
----  Entries

------------------------------
-- Doi Coverage
SELECT
    COUNT(*) as total_entries,
    COUNT(doi) as entries_with_doi,
    ROUND(COUNT(doi) * 100.0 / COUNT(*), 2) as doi_coverage_pct
FROM arxiv.entry;

-- total:99200, with doi: 24095, doi pctg: 24.29

------------------------------
-- Doi Duplicates
SELECT doi, COUNT(*) as duplicate_count
FROM arxiv.entry
WHERE doi IS NOT NULL
GROUP BY doi
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;
-- List of 17 items. One row with count 3; 16 with count 2.

------------------------------
-- Exact title matches 
-- (case-insensitive)
SELECT LOWER(TRIM(title)) as clean_title, COUNT(*) as duplicate_count
FROM arxiv.entry 
GROUP BY LOWER(TRIM(title))
HAVING COUNT(*) > 1 
ORDER BY duplicate_count DESC;
-- 8 rows with duplicate count 2 each

------------------------------
-- ArXiv ID patterns 
-- (these should be unique)
SELECT id_original, COUNT(*) as duplicate_count
FROM arxiv.entry 
GROUP BY id_original 
HAVING COUNT(*) > 1 
ORDER BY duplicate_count DESC;
-- no results

------------------------------
-- Potential Version duplicates 
-- (ArXiv papers often have v1, v2, etc.)
SELECT 
    REGEXP_REPLACE(id_original, 'v\d+$', '') as base_arxiv_id,
    COUNT(*) as version_count,
    array_agg(id_original ORDER BY id_original) as versions
FROM arxiv.entry 
GROUP BY REGEXP_REPLACE(id_original, 'v\d+$', '')
HAVING COUNT(*) > 1
ORDER BY version_count DESC;
-- 178 rows, 1 row with 3 versions, rest with 2

------------------------------
----  Links

-- --> No link deduplication, its few dups anyway

SELECT 
    href,
    COUNT(*) as duplicate_count,
    COUNT(DISTINCT type) as different_types,
    COUNT(DISTINCT title) as different_titles
FROM arxiv.link 
GROUP BY href 
HAVING COUNT(*) > 1 
ORDER BY duplicate_count DESC;
-- duplicate count 4 times 3 dups, 4 times 2 dups